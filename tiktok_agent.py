import os
import re
import time
import json
import html
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

import requests
import websocket


def env_int(name, default):
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        return int(default)
    return int(value)


BOT = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT = str(os.getenv("TELEGRAM_CHAT_ID") or "")
HEARTBEAT_MINUTES = env_int("HEARTBEAT_MINUTES", 60)

STATE_FILE = "/data/tiktok_agent_state.json"
OFFSET_FILE = "/data/telegram_offset.json"

PAIR_COOLDOWN_SECONDS = 48 * 60 * 60
TICKER_COOLDOWN_SECONDS = 24 * 60 * 60
TICKER_LIMIT = 4

start_time = time.time()
ws_connected = False
tokens_seen = 0
alerts_sent = 0
last_coin = "None yet"
last_alert = "None yet"

seen_mints = set()

state = {
    "pair_alerts": {},
    "ticker_alerts": {},
    "ticker_cooldowns": {},
    "bypasses": {},
    "watchlist": [],
    "stats": {
        "tiktok_found": 0,
        "watch_hits": 0,
        "pair_suppressed": 0,
        "ticker_suppressed": 0,
        "bypassed": 0,
        "fetch_errors": 0,
        "fetch_429s": 0,
    }
}

rate_limited_until = 0


def n():
    return "\n"


def esc(x):
    return html.escape(str(x or ""))


def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def uptime():
    s = int(time.time() - start_time)
    return f"{s // 3600} hr {(s % 3600) // 60} min"


def money(x):
    try:
        return "$" + format(float(x), ",.0f")
    except Exception:
        return "Unknown"


def norm(x):
    return str(x or "").strip().lower()


def pair_key(name, symbol):
    return norm(name) + "|" + norm(symbol)


def ticker_key(symbol):
    return norm(symbol)


def match_text(name, symbol):
    return (str(name or "") + " " + str(symbol or "")).lower()


def phrase_matches(phrase, text):
    phrase = norm(phrase)
    text = norm(text)

    if not phrase or not text:
        return False

    # Whole-word-ish matching:
    # /watch bro matches "bro" or "bro coin"
    # but does NOT match "lebron" or "brother".
    pattern = r"(?<![a-zA-Z0-9])" + re.escape(phrase) + r"(?![a-zA-Z0-9])"
    return re.search(pattern, text) is not None


def matched_watch_phrase(name, symbol):
    text = match_text(name, symbol)

    for phrase in state.get("watchlist", []):
        if phrase_matches(phrase, text):
            return phrase

    return None


def stat_inc(key, amount=1):
    state.setdefault("stats", {})
    state["stats"][key] = int(state["stats"].get(key, 0)) + amount


def load_state():
    global state
    try:
        with open(STATE_FILE, "r") as f:
            loaded = json.load(f)
            state.update(loaded)
    except Exception:
        pass

    state.setdefault("pair_alerts", {})
    state.setdefault("ticker_alerts", {})
    state.setdefault("ticker_cooldowns", {})
    state.setdefault("bypasses", {})
    state.setdefault("watchlist", [])
    state.setdefault("stats", {})

    for key in [
        "tiktok_found",
        "watch_hits",
        "pair_suppressed",
        "ticker_suppressed",
        "bypassed",
        "fetch_errors",
        "fetch_429s",
    ]:
        state["stats"].setdefault(key, 0)


def save_state():
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception as e:
        print("save state error", e, flush=True)


def load_offset():
    try:
        with open(OFFSET_FILE, "r") as f:
            return int(json.load(f).get("offset", 0))
    except Exception:
        return 0


def save_offset(offset):
    try:
        with open(OFFSET_FILE, "w") as f:
            json.dump({"offset": offset}, f)
    except Exception:
        pass


def drain_updates():
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{BOT}/getUpdates",
            params={"timeout": 0},
            timeout=10,
        )
        updates = r.json().get("result", [])
        if updates:
            newest = updates[-1]["update_id"] + 1
            save_offset(newest)
            return newest
    except Exception:
        pass

    return load_offset()


load_state()


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"running")

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()


threading.Thread(
    target=lambda: HTTPServer(("0.0.0.0", 3003), HealthHandler).serve_forever(),
    daemon=True,
).start()


def tg(text):
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT}/sendMessage",
            json={
                "chat_id": CHAT,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": False,
            },
            timeout=10,
        )
    except Exception as e:
        print("telegram msg error", e, flush=True)


def tg_photo(img, caption):
    global alerts_sent

    try:
        if img:
            requests.post(
                f"https://api.telegram.org/bot{BOT}/sendPhoto",
                json={
                    "chat_id": CHAT,
                    "photo": img,
                    "caption": caption,
                    "parse_mode": "HTML",
                },
                timeout=10,
            )
        else:
            tg(caption)

        alerts_sent += 1
    except Exception as e:
        print("telegram photo error", e, flush=True)


def fetch_coin(mint):
    global rate_limited_until

    if time.time() < rate_limited_until:
        return None

    try:
        url = f"https://frontend-api-v3.pump.fun/coins/{mint}?sync=true"
        r = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10,
        )

        if r.status_code == 429:
            stat_inc("fetch_429s")
            rate_limited_until = time.time() + 60
            print("429 from pump.fun, backing off 60s", flush=True)
            save_state()
            return None

        if r.status_code != 200:
            stat_inc("fetch_errors")
            print("fetch status", r.status_code, mint, flush=True)
            save_state()
            return None

        text = r.text.strip()
        if not text:
            stat_inc("fetch_errors")
            save_state()
            return None

        d = r.json()
        return d.get("data", d)

    except Exception as e:
        stat_inc("fetch_errors")
        print("fetch error", e, flush=True)
        save_state()
        return None


def find_tiktok(text):
    text = str(text or "")
    text = text.replace("\\", " ").replace('"', " ").replace("'", " ")
    text = text.replace("<", " ").replace(">", " ")

    for part in text.split():
        lower = part.lower()
        if "tiktok.com" in lower or "vm.tiktok.com" in lower or "vt.tiktok.com" in lower:
            if part.startswith("http"):
                return part.strip(",)]}")

    return None


def clean_old_state():
    now = time.time()

    state["pair_alerts"] = {
        k: v for k, v in state.get("pair_alerts", {}).items()
        if now - float(v) < PAIR_COOLDOWN_SECONDS
    }

    state["ticker_cooldowns"] = {
        k: v for k, v in state.get("ticker_cooldowns", {}).items()
        if now < float(v)
    }

    state["ticker_alerts"] = {
        k: v for k, v in state.get("ticker_alerts", {}).items()
        if now - float(v.get("first_seen", now)) < TICKER_COOLDOWN_SECONDS
    }

    save_state()


def is_bypassed(name, symbol):
    pkey = pair_key(name, symbol)
    skey = ticker_key(symbol)

    bypasses = state.get("bypasses", {})

    if pkey in bypasses:
        return True, "name+ticker bypass"

    if skey in bypasses:
        return True, "ticker bypass"

    return False, ""


def should_alert(name, symbol):
    clean_old_state()

    pkey = pair_key(name, symbol)
    skey = ticker_key(symbol)
    now = time.time()

    watch_phrase = matched_watch_phrase(name, symbol)
    if watch_phrase:
        stat_inc("watch_hits")
        return True, "watchlist: " + watch_phrase

    bypassed, reason = is_bypassed(name, symbol)
    if bypassed:
        stat_inc("bypassed")
        return True, reason

    # Exact same name+ticker: max once per 48 hours.
    if pkey in state["pair_alerts"]:
        if now - float(state["pair_alerts"][pkey]) < PAIR_COOLDOWN_SECONDS:
            stat_inc("pair_suppressed")
            save_state()
            return False, "same name+ticker suppressed"

    # Same ticker: max 4 alerts, then ticker cooldown 24 hours.
    if skey in state["ticker_cooldowns"] and now < float(state["ticker_cooldowns"][skey]):
        stat_inc("ticker_suppressed")
        save_state()
        return False, "ticker cooldown active"

    ticker_data = state["ticker_alerts"].get(skey, {
        "count": 0,
        "first_seen": now,
    })

    if int(ticker_data.get("count", 0)) >= TICKER_LIMIT:
        state["ticker_cooldowns"][skey] = now + TICKER_COOLDOWN_SECONDS
        state["ticker_alerts"].pop(skey, None)
        stat_inc("ticker_suppressed")
        save_state()
        return False, "ticker limit hit, cooldown started"

    return True, "allowed"


def record_alert(name, symbol):
    pkey = pair_key(name, symbol)
    skey = ticker_key(symbol)
    now = time.time()

    state["pair_alerts"][pkey] = now

    ticker_data = state["ticker_alerts"].get(skey, {
        "count": 0,
        "first_seen": now,
    })

    ticker_data["count"] = int(ticker_data.get("count", 0)) + 1
    ticker_data["last_seen"] = now
    state["ticker_alerts"][skey] = ticker_data

    if ticker_data["count"] >= TICKER_LIMIT:
        state["ticker_cooldowns"][skey] = now + TICKER_COOLDOWN_SECONDS
        state["ticker_alerts"].pop(skey, None)

    save_state()


def send_tiktok_alert(coin, mint, tiktok, allow_reason):
    global last_alert

    name = coin.get("name", "Unknown")
    symbol = coin.get("symbol", "Unknown")
    image = coin.get("image_uri") or coin.get("image") or ""
    pump = f"https://pump.fun/coin/{mint}"

    caption = (
        "🚨 <b>New Pump.fun TikTok Coin</b>" + n() + n()
        + "🪙 <b>Name:</b> " + esc(name) + n()
        + "🏷 <b>Ticker:</b> " + esc(symbol) + n()
        + "💰 <b>Market Cap:</b> " + money(coin.get("usd_market_cap")) + n() + n()
        + "🎵 <b>TikTok:</b>" + n() + esc(tiktok) + n() + n()
        + "🚀 <b>Pump.fun:</b>" + n() + pump + n() + n()
        + "🧬 <b>CA:</b>" + n() + "<code>" + esc(mint) + "</code>"
    )

    if allow_reason.startswith("watchlist:"):
        caption += n() + n() + "👁 <b>Watchlist:</b> " + esc(allow_reason.replace("watchlist:", "").strip())

    if "bypass" in allow_reason:
        caption += n() + n() + "🟢 <b>Bypass:</b> " + esc(allow_reason)

    tg_photo(image, caption)
    last_alert = f"{name} / {symbol}"


def check_coin(mint, delay):
    time.sleep(delay)

    coin = fetch_coin(mint)
    if not coin:
        return

    text = json.dumps(coin)
    tiktok = find_tiktok(text)

    if not tiktok:
        return

    stat_inc("tiktok_found")

    name = coin.get("name", "Unknown")
    symbol = coin.get("symbol", "Unknown")

    allowed, reason = should_alert(name, symbol)

    if not allowed:
        print("suppressed", reason, name, symbol, flush=True)
        return

    record_alert(name, symbol)
    send_tiktok_alert(coin, mint, tiktok, reason)


def schedule_checks(mint):
    for delay in [10, 45, 180]:
        threading.Thread(target=check_coin, args=(mint, delay), daemon=True).start()


def on_open(ws):
    global ws_connected

    ws_connected = True
    print("websocket connected", flush=True)
    ws.send(json.dumps({"method": "subscribeNewToken"}))


def on_close(ws, *args):
    global ws_connected

    ws_connected = False
    print("websocket closed", flush=True)


def on_message(ws, message):
    global tokens_seen, last_coin

    try:
        event = json.loads(message)
        mint = event.get("mint") or event.get("mintAddress") or event.get("ca")

        if not mint or mint in seen_mints:
            return

        seen_mints.add(mint)
        tokens_seen += 1
        last_coin = mint

        schedule_checks(mint)

    except Exception as e:
        print("message error", e, flush=True)


def status_text():
    stats = state.get("stats", {})
    return (
        "✅ <b>TikTok Agent Status</b>" + n() + n()
        + "🔌 <b>Websocket:</b> " + ("connected" if ws_connected else "disconnected") + n()
        + "⏱ <b>Uptime:</b> " + uptime() + n()
        + "👀 <b>Tokens seen:</b> " + str(tokens_seen) + n()
        + "🎵 <b>TikTok found:</b> " + str(stats.get("tiktok_found", 0)) + n()
        + "🚨 <b>Alerts sent:</b> " + str(alerts_sent) + n()
        + "👁 <b>Watchlist hits:</b> " + str(stats.get("watch_hits", 0)) + n()
        + "🔁 <b>Pair suppressed:</b> " + str(stats.get("pair_suppressed", 0)) + n()
        + "🏷 <b>Ticker suppressed:</b> " + str(stats.get("ticker_suppressed", 0)) + n()
        + "🟢 <b>Bypassed:</b> " + str(stats.get("bypassed", 0)) + n()
        + "🚧 <b>429s:</b> " + str(stats.get("fetch_429s", 0)) + n()
        + "⚠️ <b>Fetch errors:</b> " + str(stats.get("fetch_errors", 0)) + n() + n()
        + "🪙 <b>Last coin:</b>" + n() + "<code>" + esc(last_coin) + "</code>" + n()
        + "📣 <b>Last alert:</b> " + esc(last_alert) + n()
        + "🕒 <b>Checked:</b> " + now_utc()
    )


def watchlist_text():
    watchlist = state.get("watchlist", [])

    if not watchlist:
        return "👁 Watchlist is empty."

    lines = ["👁 <b>Watchlist</b>", ""]

    for phrase in sorted(watchlist):
        lines.append("• <code>" + esc(phrase) + "</code>")

    return n().join(lines)


def bypasses_text():
    bypasses = state.get("bypasses", {})

    if not bypasses:
        return "🟢 No active bypasses."

    lines = ["🟢 <b>Active Bypasses</b>", ""]

    for key, value in sorted(bypasses.items()):
        lines.append("• <code>" + esc(key) + "</code> — " + esc(value))

    return n().join(lines)


def cooldowns_text():
    clean_old_state()
    now = time.time()

    lines = ["🧊 <b>Cooldowns</b>", ""]

    if state.get("ticker_cooldowns"):
        lines.append("<b>Ticker cooldowns:</b>")
        for ticker, until in sorted(state["ticker_cooldowns"].items()):
            mins = max(0, int((float(until) - now) // 60))
            lines.append("• <code>" + esc(ticker) + "</code> — " + str(mins) + " min left")
    else:
        lines.append("No active ticker cooldowns.")

    return n().join(lines)


def command_loop():
    offset = drain_updates()

    while True:
        try:
            r = requests.get(
                f"https://api.telegram.org/bot{BOT}/getUpdates",
                params={"timeout": 25, "offset": offset},
                timeout=30,
            )

            for update in r.json().get("result", []):
                offset = update["update_id"] + 1
                save_offset(offset)

                msg = update.get("message", {})
                chat = str(msg.get("chat", {}).get("id", ""))
                text = (msg.get("text") or "").strip()

                if chat != CHAT:
                    continue

                lower = text.lower()

                if lower in ["/status", "status", "/ping", "ping"]:
                    tg(status_text())

                elif lower in ["/cooldowns", "cooldowns"]:
                    tg(cooldowns_text())

                elif lower in ["/bypasses", "bypasses"]:
                    tg(bypasses_text())

                elif lower in ["/watchlist", "watchlist"]:
                    tg(watchlist_text())

                elif lower.startswith("/watch "):
                    phrase = text.split(" ", 1)[1].strip().lower()

                    if not phrase:
                        tg("Use it like:" + n() + "<code>/watch bro</code>")
                        continue

                    if phrase not in state["watchlist"]:
                        state["watchlist"].append(phrase)
                        state["watchlist"] = sorted(list(set(state["watchlist"])))
                        save_state()

                    tg(
                        "👁 Watching name/ticker phrase:" + n()
                        + "<code>" + esc(phrase) + "</code>" + n() + n()
                        + "Whole-word matching is ON."
                    )

                elif lower.startswith("/unwatch "):
                    phrase = text.split(" ", 1)[1].strip().lower()

                    if phrase in state["watchlist"]:
                        state["watchlist"].remove(phrase)
                        save_state()
                        tg("🗑 Removed watch phrase:" + n() + "<code>" + esc(phrase) + "</code>")
                    else:
                        tg("That phrase is not in the watchlist.")

                elif lower.startswith("/bypass "):
                    raw = text.split(" ", 1)[1].strip()
                    parts = raw.split("|")

                    if len(parts) == 2:
                        name = parts[0].strip()
                        symbol = parts[1].strip()
                        key = pair_key(name, symbol)
                        state["bypasses"][key] = "name+ticker"
                        save_state()
                        tg("🟢 Bypassing exact name+ticker:" + n() + "<code>" + esc(key) + "</code>")
                    else:
                        symbol = raw.strip()
                        key = ticker_key(symbol)
                        state["bypasses"][key] = "ticker"
                        save_state()
                        tg("🟢 Bypassing ticker:" + n() + "<code>" + esc(key) + "</code>")

                elif lower.startswith("/unbypass "):
                    raw = text.split(" ", 1)[1].strip()
                    parts = raw.split("|")

                    if len(parts) == 2:
                        key = pair_key(parts[0].strip(), parts[1].strip())
                    else:
                        key = ticker_key(raw)

                    if key in state["bypasses"]:
                        del state["bypasses"][key]
                        save_state()
                        tg("🗑 Removed bypass:" + n() + "<code>" + esc(key) + "</code>")
                    else:
                        tg("Could not find that bypass.")

                elif lower in ["/restart", "restart"]:
                    tg("♻️ Restarting TikTok agent...")
                    time.sleep(1)
                    os._exit(0)

                elif lower in ["/help", "help"]:
                    tg(
                        "🤖 <b>Commands</b>" + n() + n()
                        + "/status - health check" + n()
                        + "/watch phrase - alert matching TikTok launches" + n()
                        + "/unwatch phrase - remove watch phrase" + n()
                        + "/watchlist - show watched phrases" + n()
                        + "/cooldowns - show active ticker cooldowns" + n()
                        + "/bypasses - show active bypasses" + n()
                        + "/bypass TICKER - bypass ticker cooldowns" + n()
                        + "/bypass NAME | TICKER - bypass exact name+ticker cooldowns" + n()
                        + "/unbypass TICKER - remove ticker bypass" + n()
                        + "/unbypass NAME | TICKER - remove exact bypass" + n()
                        + "/restart - restart bot"
                    )

        except Exception as e:
            print("command error", e, flush=True)
            time.sleep(5)


def heartbeat_loop():
    while True:
        time.sleep(HEARTBEAT_MINUTES * 60)
        tg("🟢 <b>Scheduled TikTok Agent Checkup</b>" + n() + n() + status_text())


threading.Thread(target=command_loop, daemon=True).start()
threading.Thread(target=heartbeat_loop, daemon=True).start()

while True:
    try:
        websocket.WebSocketApp(
            "wss://pumpportal.fun/api/data",
            on_open=on_open,
            on_message=on_message,
            on_close=on_close,
        ).run_forever()

    except Exception as e:
        print("websocket crash", e, flush=True)

    print("reconnecting websocket in 5s", flush=True)
    time.sleep(5)

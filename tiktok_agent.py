import os, re, json, time, html, heapq, random, threading, logging, ipaddress, socket
from collections import deque
from datetime import datetime, timezone
from urllib.parse import urlparse, urlunparse, quote
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import requests
import websocket

BOT = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
CHAT = os.getenv('TELEGRAM_CHAT_ID', '').strip()
PORT = int(os.getenv('PORT', '3003'))
HEARTBEAT_MINUTES = max(5, int(os.getenv('HEARTBEAT_MINUTES', '60')))
WORKERS = max(1, min(8, int(os.getenv('METADATA_WORKERS', '3'))))
HOST_INTERVAL = max(0.2, float(os.getenv('HOST_REQUEST_INTERVAL', '2.0')))
GLOBAL_INTERVAL = max(0.1, float(os.getenv('GLOBAL_REQUEST_INTERVAL', '0.6')))
MAX_ATTEMPTS = max(3, int(os.getenv('MAX_METADATA_ATTEMPTS', '8')))
MAX_PENDING = max(1000, int(os.getenv('MAX_PENDING', '12000')))
PUMPPORTAL_API_KEY = os.getenv('PUMPPORTAL_API_KEY', '').strip()
DATA = os.getenv('DATA_DIR', '/data')
os.makedirs(DATA, exist_ok=True)
STATE_PATH = os.path.join(DATA, 'tiktok_agent_state.json')
OFFSET_PATH = os.path.join(DATA, 'telegram_offset.json')
LOG = logging.getLogger('tiktok-agent')
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
if not BOT or not CHAT:
    raise SystemExit('Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID')

lock = threading.RLock()
start = time.time()
connected = False
last_event = 0.0
last_success = 0.0
last_alert = ''
last_coin = ''
last_problem_notice = 0.0
pending = []
pending_ids = set()
inflight = set()
sequence = 0
state = {
    'watchlist': [], 'bypasses': {}, 'pair_alerts': {}, 'ticker_alerts': {},
    'ticker_cooldowns': {}, 'alerted_mints': {}, 'mints': {}, 'stats': {}
}
DEFAULT_STATS = ('tokens_seen','metadata_ok','metadata_fail','metadata_404','metadata_429','metadata_403',
                 'metadata_other_http','metadata_missing_uri','tiktok_found','alerts_sent','alert_errors',
                 'pair_suppressed','ticker_suppressed','watch_hits','retries','queue_dropped','non_tiktok',
                 'websocket_reconnects','checks_completed')
recent = deque(maxlen=10000)
# These limits are intentionally shared across workers, including metadata retries.
host_next = {}
host_strikes = {}
global_next = 0.0
metadata_cache = {}  # URI -> (expiry, metadata); cache only successful JSON
host_throttled = 0
cache_hits = 0


def utc(ts=None):
    return datetime.fromtimestamp(ts or time.time(), timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
def norm(s):
    return ' '.join(str(s or '').casefold().split())
def pair(name, ticker):
    return norm(name)+'|'+norm(ticker)
def esc(s):
    return html.escape(str(s or ''))
def number(x):
    try: return f'${float(x):,.0f}'
    except (TypeError,ValueError): return 'Unknown'
def inc(key, n=1):
    with lock: state['stats'][key] = state['stats'].get(key,0)+n
def save():
    with lock:
        tmp = STATE_PATH+'.tmp'
        with open(tmp,'w') as f: json.dump(state,f,separators=(',',':'))
        os.replace(tmp,STATE_PATH)
def load():
    try:
        with open(STATE_PATH) as f: old=json.load(f)
        if isinstance(old,dict):
            for k in state:
                if k in old: state[k]=old[k]
    except (OSError,ValueError) as e: LOG.info('New state or state unreadable: %s',e)
    # Compatibility with old agent, whose pair_alerts values were timestamps.
    if isinstance(state['watchlist'],dict): state['watchlist']=list(state['watchlist'].keys())
    if not isinstance(state['watchlist'],list): state['watchlist']=[]
    for k in ('bypasses','pair_alerts','ticker_alerts','ticker_cooldowns','alerted_mints','mints','stats'):
        if not isinstance(state[k],dict): state[k]={}
    for k in DEFAULT_STATS: state['stats'].setdefault(k,0)
    state['watchlist'] = list(dict.fromkeys(norm(x) for x in state['watchlist'] if norm(x)))
    LOG.info('Loaded %d watches, %d mint histories',len(state['watchlist']),len(state['mints']))
load()

def prune():
    now=time.time()
    with lock:
        state['pair_alerts']={k:[float(x) for x in (v if isinstance(v,list) else [v]) if now-float(x)<172800]
                              for k,v in state['pair_alerts'].items()}
        state['pair_alerts']={k:v for k,v in state['pair_alerts'].items() if v}
        state['ticker_alerts']={k:[float(x) for x in (v if isinstance(v,list) else []) if now-float(x)<86400]
                                for k,v in state['ticker_alerts'].items()}
        state['ticker_alerts']={k:v for k,v in state['ticker_alerts'].items() if v}
        state['ticker_cooldowns']={k:v for k,v in state['ticker_cooldowns'].items() if float(v)>now}
        state['alerted_mints']={k:v for k,v in state['alerted_mints'].items() if now-float(v)<604800}
        state['mints']={k:v for k,v in state['mints'].items() if now-float(v.get('seen_at',now))<259200}

def telegram(text, image=None):
    base=f'https://api.telegram.org/bot{BOT}/'
    if image and len(text)<=1024:
        try:
            r=requests.post(base+'sendPhoto',json={'chat_id':CHAT,'photo':image,'caption':text,'parse_mode':'HTML'},timeout=12)
            if r.ok and r.json().get('ok'): return True
            LOG.warning('sendPhoto failed %s %s',r.status_code,r.text[:150])
        except Exception as e: LOG.warning('sendPhoto exception %s',e)
    try:
        r=requests.post(base+'sendMessage',json={'chat_id':CHAT,'text':text[:4000],'parse_mode':'HTML',
                                                  'disable_web_page_preview':True},timeout=12)
        if r.ok and r.json().get('ok'): return True
        LOG.warning('sendMessage failed %s %s',r.status_code,r.text[:150])
    except Exception as e: LOG.warning('sendMessage exception %s',e)
    return False

def phrase_match(phrase, text):
    return bool(re.search(r'(?<![\w])'+re.escape(norm(phrase))+r'(?![\w])',norm(text),re.UNICODE))
def watch_match(name,ticker):
    return next((w for w in state['watchlist'] if phrase_match(w,name) or phrase_match(w,ticker)),None)

def extract_tiktok(meta):
    # Explicitly look at social/description fields; don't count metadata image URL or arbitrary JSON keys.
    fields=('website','twitter','telegram','description','tiktok','socials','external_url')
    values=[]
    def gather(v):
        if isinstance(v,str): values.append(v)
        elif isinstance(v,list):
            for x in v: gather(x)
        elif isinstance(v,dict):
            for x in v.values(): gather(x)
    for field in fields: gather(meta.get(field))
    for value in values:
        for url in re.findall(r'https?://[^\s<>"\']+',value,re.I):
            url=url.rstrip('.,;!)]}')
            try: host=(urlparse(url).hostname or '').lower()
            except ValueError: continue
            if host=='tiktok.com' or host.endswith('.tiktok.com'):
                return url
    return None

def normalize_uri(raw):
    if not isinstance(raw,str): return None
    raw=raw.strip()
    if raw.startswith('ipfs://'): raw='https://ipfs.io/ipfs/'+raw[7:].lstrip('/')
    parsed=urlparse(raw)
    if parsed.scheme!='https' or not parsed.hostname or parsed.username or parsed.password: return None
    # Avoid SSRF: metadata URLs must be public hosts, not internal or localhost.
    host=parsed.hostname.lower()
    if host=='localhost' or host.endswith(('.local','.internal')): return None
    try:
        if not ipaddress.ip_address(host).is_global: return None
    except ValueError: pass
    if parsed.port not in (None,443): return None
    return raw

def metadata_host(uri):
    normalized = normalize_uri(uri)
    return (urlparse(normalized).hostname or '').lower() if normalized else ''

def reserve_slot(host):
    """Nonblocking reservation: returns seconds until eligible, or zero if reserved."""
    global global_next
    with lock:
        now = time.time()
        ready = max(host_next.get(host, 0), global_next)
        if ready > now:
            return ready - now
        host_next[host] = now + HOST_INTERVAL
        global_next = now + GLOBAL_INTERVAL
        return 0.0

def throttle_host(host, retry_after=None):
    """Honor Retry-After, or apply capped exponential backoff with jitter."""
    global host_throttled
    with lock:
        strikes = min(8, host_strikes.get(host, 0) + 1)
        host_strikes[host] = strikes
        backoff = min(900, 60 * 2 ** (strikes - 1)) + random.uniform(0, 5)
        if retry_after is not None:
            try:
                backoff = max(backoff, min(900, float(retry_after)))
            except (ValueError, TypeError):
                pass
        host_next[host] = max(host_next.get(host, 0), time.time() + backoff)
        host_throttled += 1
    LOG.warning('Metadata host %s rate-limited; waiting about %.0fs', host, backoff)

def fetch_metadata(uri):
    uri = normalize_uri(uri)
    if not uri:
        return None, 'invalid_uri'
    with lock:
        cached = metadata_cache.get(uri)
        if cached and cached[0] > time.time():
            global cache_hits
            cache_hits += 1
            return cached[1], 'cached'
    target = uri
    for _ in range(2):
        host = metadata_host(target)
        if not host:
            return None, 'invalid_uri'
        delay = reserve_slot(host)
        if delay > 0:
            return None, 'deferred:' + str(round(delay + 0.05, 2))
        try:
            r = requests.get(target, headers={'Accept':'application/json',
                             'User-Agent':'TikTokLaunchRadar/3.0'},
                             timeout=(4, 9), allow_redirects=False, stream=True)
            try:
                if r.status_code in (301,302,303,307,308):
                    redirected = normalize_uri(requests.compat.urljoin(target, r.headers.get('Location','')))
                    if not redirected:
                        return None, 'invalid_redirect'
                    target = redirected
                    continue
                if r.status_code == 429:
                    throttle_host(host, r.headers.get('Retry-After'))
                    return None, '429'
                if r.status_code != 200:
                    return None, str(r.status_code)
                if int(r.headers.get('Content-Length', 0) or 0) > 500000:
                    return None, 'oversized'
                content = bytearray()
                for chunk in r.iter_content(chunk_size=32768):
                    content.extend(chunk)
                    if len(content) > 500000:
                        return None, 'oversized'
                obj = json.loads(content)
                if not isinstance(obj, dict):
                    return None, 'invalid_json'
                with lock:
                    host_strikes[host] = 0
                    if len(metadata_cache) >= 4000:
                        # Drop expired entries first, then the oldest insertion.
                        for k, v in list(metadata_cache.items()):
                            if v[0] < time.time():
                                metadata_cache.pop(k, None)
                        if len(metadata_cache) >= 4000:
                            metadata_cache.pop(next(iter(metadata_cache)))
                    metadata_cache[uri] = (time.time() + 1800, obj)
                return obj, 'ok'
            finally:
                r.close()
        except (requests.RequestException, ValueError, json.JSONDecodeError) as e:
            return None, type(e).__name__
    return None, 'redirect_limit'

def record_check(mint, outcome, attempt, detail=''):
    with lock:
        m=state['mints'].get(mint)
        if m is not None:
            m['last_result']=outcome
            m['last_checked']=time.time()
            m['attempts']=attempt
            m['detail']=str(detail)[:160]
        recent.append((time.time(),outcome))
        inc('checks_completed')

def enqueue(mint, uri, event, attempt=1, delay=0):
    global sequence
    with lock:
        if mint in state['alerted_mints'] or mint in pending_ids or mint in inflight: return False
        if len(pending)>=MAX_PENDING:
            inc('queue_dropped')
            return False
        sequence+=1
        heapq.heappush(pending,(time.time()+delay,sequence,mint,uri,event,attempt))
        pending_ids.add(mint)
        return True

def evaluate(name,ticker,mint):
    now=time.time()
    pk=pair(name,ticker); tk=norm(ticker)
    bypass=(tk in state['bypasses'] or pk in state['bypasses'])
    watched=watch_match(name,ticker)
    if watched: inc('watch_hits')
    if bypass: return True,'bypass',watched
    pair_times=state['pair_alerts'].get(pk,[])
    if not isinstance(pair_times,list): pair_times=[pair_times]
    if sum(now-float(t)<172800 for t in pair_times)>=2:
        inc('pair_suppressed'); return False,'pair cooldown (2 in 48h)',watched
    if now<float(state['ticker_cooldowns'].get(tk,0)):
        inc('ticker_suppressed'); return False,'ticker cooldown',watched
    ticker_times=state['ticker_alerts'].get(tk,[])
    if isinstance(ticker_times,list) and sum(now-float(t)<86400 for t in ticker_times)>=4:
        inc('ticker_suppressed'); return False,'ticker limit (4 in 24h)',watched
    return True,'allowed',watched

def alert(mint, event, meta, tiktok):
    global last_alert
    name=meta.get('name') or event.get('name') or 'Unknown'
    ticker=meta.get('symbol') or event.get('symbol') or 'Unknown'
    image=meta.get('image') or meta.get('image_uri')
    with lock:
        if mint in state['alerted_mints']: return
        allowed,reason,watched=evaluate(name,ticker,mint)
        if not allowed:
            state['mints'][mint]['last_result']='suppressed: '+reason
            return
        # Prevent concurrent sends of same mint.
        state['alerted_mints'][mint]=time.time()
    cap=(f'🎵 <b>TikTok-linked launch</b>\n\n<b>Name:</b> {esc(name)}\n'
         f'<b>Ticker:</b> {esc(ticker)}\n<b>Market cap:</b> '
         f'{number(event.get("usd_market_cap")) if event.get("usd_market_cap") else "Not available at launch"}\n'
         f'<b>TikTok:</b> {esc(tiktok)}\n'
         f'<b>Pump.fun:</b> https://pump.fun/coin/{esc(mint)}\n'
         f'<b>CA:</b> <code>{esc(mint)}</code>')
    if watched: cap+=f'\n<b>Watch match:</b> {esc(watched)}'
    if reason=='bypass': cap+='\n<b>Bypass:</b> enabled'
    sent=telegram(cap,image=image if isinstance(image,str) and image.startswith('https://') else None)
    with lock:
        if not sent:
            state['alerted_mints'].pop(mint,None)
            inc('alert_errors')
            state['mints'][mint]['last_result']='telegram_error'
            return
        now=time.time(); pk=pair(name,ticker); tk=norm(ticker)
        state['pair_alerts'].setdefault(pk,[])
        if not isinstance(state['pair_alerts'][pk],list): state['pair_alerts'][pk]=[state['pair_alerts'][pk]]
        state['pair_alerts'][pk].append(now)
        state['ticker_alerts'].setdefault(tk,[])
        state['ticker_alerts'][tk].append(now)
        if len([t for t in state['ticker_alerts'][tk] if now-float(t)<86400])>=4:
            state['ticker_cooldowns'][tk]=now+86400
        state['mints'][mint]['alerted_at']=now
        state['mints'][mint]['last_result']='alerted'
        last_alert=f'{name} / {ticker}'
        inc('alerts_sent')
    save()

def check(mint,uri,event,attempt):
    global last_success
    meta,result=fetch_metadata(uri)
    if meta is None:
        if result.startswith('deferred:'):
            with lock:
                inflight.discard(mint)
            try:
                delay = max(0.2, float(result.split(':', 1)[1]))
            except ValueError:
                delay = 2.0
            enqueue(mint, uri, event, attempt, delay)
            return
        inc('metadata_fail')
        if result=='404': inc('metadata_404')
        elif result=='429': inc('metadata_429')
        elif result in ('401','403'): inc('metadata_403')
        elif result.isdigit(): inc('metadata_other_http')
        record_check(mint,'fetch_error',attempt,result)
        # Retry a bounded number of times, including 404 (often indexing delay).
        if attempt < MAX_ATTEMPTS:
            inc('retries')
            with lock: inflight.discard(mint)
            # 429s are already handled by the host limiter. These delays also
            # spread other transient failures without blocking fresh launches.
            delay = min(900, 12 * (2 ** (attempt - 1))) + random.uniform(0, 6)
            enqueue(mint, uri, event, attempt + 1, delay)
        return
    last_success=time.time()
    inc('metadata_ok')
    tiktok=extract_tiktok(meta)
    if tiktok:
        inc('tiktok_found')
        record_check(mint,'tiktok_found',attempt,tiktok)
        alert(mint,event,meta,tiktok)
    else:
        inc('non_tiktok')
        record_check(mint,'no_tiktok',attempt)

def scheduler():
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        while True:
            item = None
            with lock:
                now = time.time()
                # Reposition blocked work without consuming a worker.
                # Check at most 100 due items each pass, allowing other hosts through.
                inspected = 0
                while (pending and pending[0][0] <= now
                       and len(inflight) < WORKERS and inspected < 100):
                    candidate = heapq.heappop(pending)
                    due, seq, mint, uri, event, attempt = candidate
                    host = metadata_host(uri)
                    if host:
                        next_ready = max(host_next.get(host, 0), global_next)
                    else:
                        next_ready = now
                    if next_ready > now:
                        heapq.heappush(pending, (next_ready + 0.05, seq, mint, uri, event, attempt))
                        inspected += 1
                        continue
                    pending_ids.discard(mint)
                    inflight.add(mint)
                    item = candidate
                    break
            if not item:
                time.sleep(0.15)
                continue
            _, _, mint, uri, event, attempt = item
            def task(m=mint, u=uri, e=event, a=attempt):
                try:
                    check(m, u, e, a)
                except Exception:
                    LOG.exception('check failed for %s', m)
                finally:
                    with lock:
                        inflight.discard(m)
            pool.submit(task)

def on_open(ws):
    global connected
    connected=True
    ws.send(json.dumps({'method':'subscribeNewToken'}))
    LOG.info('WebSocket connected and subscribed')
def on_close(ws,*args):
    global connected
    connected=False
    LOG.warning('WebSocket closed %s',args)
def on_error(ws,error): LOG.warning('WebSocket error %s',error)
def on_message(ws,message):
    global last_event,last_coin
    try: ev=json.loads(message)
    except ValueError: return
    if not isinstance(ev,dict): return
    mint=ev.get('mint') or ev.get('mintAddress') or ev.get('ca')
    if not mint: return
    uri=ev.get('uri') or ev.get('metadataUri') or ev.get('metadata_uri')
    with lock:
        if mint in state['mints']: return
        last_event=time.time(); last_coin=mint
        state['mints'][mint]={'seen_at':last_event,'name':ev.get('name',''),
                              'symbol':ev.get('symbol',''),'uri':str(uri or '')[:500],
                              'attempts':0,'last_result':'queued' if uri else 'missing_uri'}
        inc('tokens_seen')
    if not uri:
        inc('metadata_missing_uri')
        return
    enqueue(mint,uri,{'name':ev.get('name',''),'symbol':ev.get('symbol',''),
                      'marketCapSol':ev.get('marketCapSol')})

def load_offset():
    try:
        with open(OFFSET_PATH) as f: return int(json.load(f).get('offset',0))
    except (OSError,ValueError): return 0
def save_offset(n):
    tmp=OFFSET_PATH+'.tmp'
    with open(tmp,'w') as f: json.dump({'offset':n},f)
    os.replace(tmp,OFFSET_PATH)

def diagnostics():
    with lock:
        now=time.time(); samples=[result for ts,result in recent if now-ts<3600]
        counts={k:samples.count(k) for k in set(samples)}
        s=state['stats']; q=len(pending); f=len(inflight)
        return (f'🔬 <b>Detection diagnostics</b>\n'
                f'WebSocket: {"connected" if connected else "disconnected"}\n'
                f'Last launch: {int(now-last_event)}s ago\n' if last_event else '🔬 <b>Detection diagnostics</b>\nNo launches received this uptime\n') + (
                f'Last metadata success: {int(now-last_success)}s ago\n' if last_success else 'No successful metadata fetch this uptime\n') + (
                f'Pending: {q} | Active: {f}\n'
                f'Rate limiter: {len(host_next)} hosts; {host_throttled} throttles; {cache_hits} cache hits\n'
                f'Longest host wait: {max(0, int(max(host_next.values(), default=now)-now))}s\n'
                f'Last hour: {len(samples)} checks; {counts.get("tiktok_found",0)} TikTok links; '
                f'{counts.get("no_tiktok",0)} no TikTok; {counts.get("fetch_error",0)} fetch failures\n'
                f'Cumulative (includes previous deployments): {s["tokens_seen"]} launches; {s["metadata_ok"]} metadata successes; '
                f'{s["metadata_404"]} 404s; {s["metadata_429"]} 429s; '
                f'{s["metadata_403"]} 401/403s; {s["metadata_missing_uri"]} missing URIs; '
                f'{s["alerts_sent"]} alerts; {s["queue_dropped"]} queue drops\n'
                f'Checked: {utc()}')

def status():
    with lock:
        s=state['stats']
        return (f'✅ <b>TikTok Agent Status</b>\n\n'
                f'🔌 WebSocket: {"connected" if connected else "disconnected"}\n'
                f'⏱ Uptime: {int((time.time()-start)//3600)}h {int((time.time()-start)%3600//60)}m\n'
                f'👀 Tokens seen: {s["tokens_seen"]}\n'
                f'🎵 TikTok found: {s["tiktok_found"]}\n'
                f'🚨 Alerts sent: {s["alerts_sent"]}\n'
                f'🔁 Pair suppressed: {s["pair_suppressed"]}\n'
                f'🏷 Ticker suppressed: {s["ticker_suppressed"]}\n'
                f'🚧 Metadata 404s: {s["metadata_404"]}\n'
                f'⚠️ Metadata errors: {s["metadata_fail"]}\n'
                f'🪙 Last coin: <code>{esc(last_coin)}</code>\n'
                f'📣 Last alert: {esc(last_alert or "None this uptime")}\n'
                f'🕒 Checked: {utc()}')

def debug(mint):
    with lock: record=state['mints'].get(mint)
    if not record: return ('🧪 No recorded history for this mint. It may predate the update, '
                           'have been missed by the WebSocket, or be outside the 72h retention period.\n'
                           f'<code>{esc(mint)}</code>')
    return (f'🧪 <b>Mint history</b>\nName: {esc(record.get("name"))}\n'
            f'Ticker: {esc(record.get("symbol"))}\n'
            f'Seen: {utc(record.get("seen_at"))}\n'
            f'Metadata URI: {"present" if record.get("uri") else "missing"}\n'
            f'Attempts: {record.get("attempts",0)}\n'
            f'Last result: {esc(record.get("last_result"))}\n'
            f'Detail: {esc(record.get("detail"))}\n'
            f'Alerted: {utc(record["alerted_at"]) if record.get("alerted_at") else "No"}\n'
            f'CA: <code>{esc(mint)}</code>')

def command_loop():
    offset=load_offset()
    while True:
        try:
            r=requests.get(f'https://api.telegram.org/bot{BOT}/getUpdates',
                           params={'offset':offset,'timeout':25},timeout=32)
            if not r.ok:
                LOG.warning('getUpdates HTTP %s %s',r.status_code,r.text[:150]); time.sleep(5); continue
            for update in r.json().get('result',[]):
                offset=update['update_id']+1; save_offset(offset)
                msg=update.get('message') or {}
                if str((msg.get('chat') or {}).get('id',''))!=CHAT: continue
                command=(msg.get('text') or '').strip()
                if not command: continue
                first,_,arg=command.partition(' ')
                cmd=first.split('@')[0].lower().lstrip('/')
                arg=arg.strip()
                if cmd in ('status','ping'): telegram(status())
                elif cmd=='diagnostics': telegram(diagnostics())
                elif cmd=='watchlist': telegram('👀 Watches:\n'+('\n'.join('• '+esc(w) for w in state['watchlist']) or 'None'))
                elif cmd=='watch' and arg:
                    with lock:
                        if norm(arg) not in state['watchlist']: state['watchlist'].append(norm(arg))
                    save(); telegram('👀 Watching <code>'+esc(norm(arg))+'</code>')
                elif cmd=='unwatch' and arg:
                    with lock: state['watchlist']=[x for x in state['watchlist'] if x!=norm(arg)]
                    save(); telegram('Removed <code>'+esc(norm(arg))+'</code>')
                elif cmd=='bypass' and arg:
                    # Accept ticker or "name | ticker".
                    key=pair(*[x.strip() for x in arg.split('|',1)]) if '|' in arg else norm(arg)
                    with lock: state['bypasses'][key]=time.time()
                    save(); telegram('🟢 Bypass added: <code>'+esc(arg)+'</code>')
                elif cmd=='unbypass' and arg:
                    key=pair(*[x.strip() for x in arg.split('|',1)]) if '|' in arg else norm(arg)
                    with lock: state['bypasses'].pop(key,None)
                    save(); telegram('Bypass removed: <code>'+esc(arg)+'</code>')
                elif cmd=='bypasses': telegram('🟢 Bypasses:\n'+('\n'.join(esc(k) for k in state['bypasses']) or 'None'))
                elif cmd=='cooldowns':
                    with lock:
                        lines=[f'{esc(k)} — {int((float(v)-time.time())/60)}m' for k,v in state['ticker_cooldowns'].items() if float(v)>time.time()]
                    telegram('🔇 Ticker cooldowns:\n'+('\n'.join(lines[:50]) or 'None'))
                elif cmd=='debug' and arg: telegram(debug(arg))
                elif cmd in ('recheck','forcecheck') and arg:
                    with lock: record=state['mints'].get(arg)
                    if not record or not record.get('uri'): telegram('No stored metadata URI for this mint.'); continue
                    with lock: state['alerted_mints'].pop(arg,None)
                    if enqueue(arg,record['uri'],{'name':record.get('name'),'symbol':record.get('symbol')},attempt=1):
                        telegram('Queued metadata recheck. Normal cooldown rules still apply.')
                    else: telegram('Mint already queued or queue full.')
                elif cmd=='restart':
                    telegram('♻️ Restarting agent'); save(); os._exit(1)
                elif cmd=='help':
                    telegram('Commands: /status /diagnostics /watch phrase /unwatch phrase /watchlist '
                             '/bypass TICKER /bypass NAME | TICKER /unbypass TICKER '
                             '/bypasses /cooldowns /debug CA /recheck CA /restart')
        except Exception: LOG.exception('Telegram command loop error'); time.sleep(5)

def monitor():
    global last_problem_notice
    last_heartbeat=0
    while True:
        time.sleep(60)
        now=time.time()
        with lock:
            last=last_event; good=last_success; q=len(pending); s=state['stats'].copy()
        reasons=[]
        if now-start>600 and (not last or now-last>600): reasons.append('No launch events for 10 minutes')
        if now-start>600 and last and now-last<300 and (not good or now-good>600):
            reasons.append('Launches arriving but no metadata fetch has succeeded for 10 minutes')
        if q>MAX_PENDING*.8: reasons.append('Metadata queue over 80% capacity')
        if reasons and now-last_problem_notice>3600:
            if telegram('🚨 <b>TikTok detection warning</b>\n'+'\n'.join(esc(x) for x in reasons)+'\n\n/diagnostics'):
                last_problem_notice=now
        if now-last_heartbeat>=HEARTBEAT_MINUTES*60:
            telegram('🟢 <b>Scheduled TikTok Agent Checkup</b>\n\n'+status())
            last_heartbeat=now
        with lock:
            for host, deadline in list(host_next.items()):
                if deadline < now - 3600:
                    host_next.pop(host, None)
                    host_strikes.pop(host, None)
            for uri, (expiry, _) in list(metadata_cache.items()):
                if expiry < now:
                    metadata_cache.pop(uri, None)
        prune(); save()

class Health(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.end_headers(); self.wfile.write(b'running')
    def do_HEAD(self): self.send_response(200); self.end_headers()
    def log_message(self,*args): pass

def main():
    threading.Thread(target=lambda:ThreadingHTTPServer(('0.0.0.0',PORT),Health).serve_forever(),daemon=True).start()
    threading.Thread(target=scheduler,daemon=True).start()
    threading.Thread(target=command_loop,daemon=True).start()
    threading.Thread(target=monitor,daemon=True).start()
    url='wss://pumpportal.fun/api/data'
    if PUMPPORTAL_API_KEY: url+='?api-key='+quote(PUMPPORTAL_API_KEY,safe='')
    global connected
    while True:
        try:
            websocket.WebSocketApp(url,on_open=on_open,on_close=on_close,
                                   on_error=on_error,on_message=on_message).run_forever(
                                       ping_interval=30,ping_timeout=10)
        except Exception: LOG.exception('WebSocket crash')
        connected=False; inc('websocket_reconnects'); time.sleep(5)
if __name__=='__main__': main()

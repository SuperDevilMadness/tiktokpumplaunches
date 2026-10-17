    meta,result=fetch_metadata(uri)
    if meta is None:
        inc('metadata_fail')
        if result=='404': inc('metadata_404')
        elif result=='429': inc('metadata_429')
        elif result in ('401','403'): inc('metadata_403')
        elif result.isdigit(): inc('metadata_other_http')
        record_check(mint,'fetch_error',attempt,result)
        # Retry a bounded number of times, including 404 (often indexing delay).
        if attempt<5:
            inc('retries')
            with lock: inflight.discard(mint)
            enqueue(mint,uri,event,attempt+1,[0,15,60,180,600][attempt]+random.random()*3)
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
            item=None
            with lock:
                if pending and pending[0][0]<=time.time() and len(inflight)<WORKERS:
                    item=heapq.heappop(pending)
                    mint=item[2]
                    pending_ids.discard(mint)
                    inflight.add(mint)
            if not item:
                time.sleep(.15); continue
            _,_,mint,uri,event,attempt=item
            def task(m=mint,u=uri,e=event,a=attempt):
                try: check(m,u,e,a)
                except Exception: LOG.exception('check failed for %s',m)
                finally:
                    with lock: inflight.discard(m)
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
                f'Last hour: {len(samples)} checks; {counts.get("tiktok_found",0)} TikTok links; '
                f'{counts.get("no_tiktok",0)} no TikTok; {counts.get("fetch_error",0)} fetch failures\n'
                f'Cumulative: {s["tokens_seen"]} launches; {s["metadata_ok"]} metadata successes; '
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

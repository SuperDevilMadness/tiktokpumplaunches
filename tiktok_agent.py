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
WORKERS = max(1, min(12, int(os.getenv('METADATA_WORKERS', '5'))))
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

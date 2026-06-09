"""
Insider Signal Terminal v4 — Speed + Bug Fixes
vs v3:
  - TTL_PRICE 1s → 5s  (elimina re-fetches inúteis)
  - sec_form4: NÃO faz requests XML individuais por default (era o maior source de delay)
    → sec_form4_deep() faz parse XML lazy só quando abre o tab Insider (max 5 requests paralelos)
  - fetch_prices_batch: yf.download() batch real (1 request para N tickers)
  - broadcast_loop: batch de 40 tickers + cache agressivo
  - history(): threads=True
  - statements(): paraleliza income/balance/cashflow
  - Congress trades: carrega em background no arranque
  - Fair Value: DCF, Graham, Lynch, EV/EBITDA, Analyst Consensus
  - Gráfico: Y-axis autorange correto; candle mode bloqueia overlays incompatíveis
  - SEC links: URLs corretos (Archives/edgar/data/{cik}/{acc}/{doc})
"""

from flask import Flask, jsonify as _flask_jsonify, request, Response, redirect as _flask_redirect
import json as _json

def _fmp_single_quote(ticker):
    """FMP single ticker quote."""
    if not FMP_API_KEY: return None
    try:
        r = requests.get(f"https://financialmodelingprep.com/api/v3/quote/{ticker}?apikey={FMP_API_KEY}", timeout=3)
        if not r.ok: return None
        data = r.json()
        if not data or not isinstance(data, list): return None
        q = data[0]
        price = sf(q.get("price"), 2)
        prev  = sf(q.get("previousClose"), 2)
        chg   = sf(q.get("change"), 2)
        chgp  = sf(q.get("changesPercentage"), 2)
        vol   = si(q.get("volume"))
        if not price: return None
        return {"ticker":ticker,"label":TICKER_DISPLAY.get(ticker,MARKET_TAPE.get(ticker,ticker)),
                "price":price,"prev_close":prev,"change":chg,"change_pct":chgp,"volume":vol,
                "provider":"fmp","ts":datetime.now().strftime("%H:%M:%S"),"error":None}
    except: return None


def _clean(obj):
    """Strip surrogate chars recursively from any serialisable object."""
    if isinstance(obj, str):
        # encode to bytes replacing surrogates, then decode back
        return obj.encode('utf-8', 'replace').decode('utf-8', 'replace')
    if isinstance(obj, dict):
        return {_clean(k): _clean(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return type(obj)(_clean(x) for x in obj)
    return obj

def jsonify(data=None, **kw):
    """Drop-in safe jsonify — never crashes on surrogate chars."""
    payload = data if data is not None else kw
    try:
        clean = _clean(payload)
        # Verify it serialises cleanly
        _json.dumps(clean, ensure_ascii=False)
        return _flask_jsonify(clean)
    except Exception:
        try:
            safe = _json.dumps(_clean(payload), ensure_ascii=True,
                               default=lambda x: str(x))
            return Response(safe, mimetype='application/json')
        except Exception as e2:
            return Response(_json.dumps({'error': str(e2)}), mimetype='application/json')
from flask_cors import CORS
from flask_socketio import SocketIO, emit
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from urllib.parse import quote_plus
import csv, feedparser, io, json, math, os, re, requests, threading, time
try:
    import websocket as _websocket_client
except ImportError:
    _websocket_client = None
import xml.etree.ElementTree as ET
import yfinance as yf
import pandas as pd

APP_PORT        = int(os.getenv("PORT", os.getenv("APP_PORT", "5050")))
SEC_USER_AGENT  = os.getenv("SEC_USER_AGENT", "InsiderSignalTerminal/4.0 local@example.com")
MIN_TRADE_VALUE = float(os.getenv("MIN_INSIDER_TRADE_VALUE", "30000"))

# ── Embedded Templates ──────────────────────────────────────
_TEMPLATES = {
    "commodity.html": "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"UTF-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\n<title>Commodity · IST</title>\n<script src=\"https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.7.5/socket.io.min.js\"></script>\n<script src=\"https://cdn.plot.ly/plotly-2.32.0.min.js\"></script>\n<link href=\"https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600&family=Syne:wght@400;600;700;800&display=swap\" rel=\"stylesheet\">\n<style>\n\n*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}\n:root{\n  --bg:#080b0f;--bg2:#0d1117;--bg3:#111720;--bg4:#161d29;\n  --b:#1e2d3d;--b2:#243040;\n  --t:#c9d1d9;--t2:#8b949e;--t3:#484f58;\n  --gr:#00e5a0;--rd:#ff4d6d;--bl:#0095ff;\n  --yl:#f0c060;--pu:#a78bfa;--or:#ff8c42;\n  --fm:'JetBrains Mono',monospace;--fd:'Syne',sans-serif;\n}\nhtml,body{height:100%;background:var(--bg);color:var(--t);font-family:var(--fm);font-size:13px}\\n@keyframes spin{from{transform:rotate(0deg)}to{transform:rotate(360deg)}}\\n.chart-msg{display:flex;flex-direction:column;align-items:center;justify-content:center;height:100%;gap:12px;color:#484f58;font-family:var(--fd);font-size:13px;text-align:center;padding:20px}\\n.chart-retry{background:rgba(0,229,160,.1);color:#00e5a0;border:1px solid rgba(0,229,160,.25);font-family:var(--fd);font-size:11px;padding:6px 16px;border-radius:4px;cursor:pointer;transition:all .15s}\\n.chart-retry:hover{background:rgba(0,229,160,.2)}\n::-webkit-scrollbar{width:5px;height:5px}::-webkit-scrollbar-track{background:transparent}::-webkit-scrollbar-thumb{background:var(--b2);border-radius:3px}\n.up{color:var(--gr)}.dn{color:var(--rd)}.nc{color:var(--t2)}\n/* TAPE */\n#tape{height:26px;background:var(--bg2);border-bottom:1px solid var(--b);overflow:hidden;display:flex;align-items:center;flex-shrink:0}\n#tape-inner{display:flex;white-space:nowrap;animation:tape 90s linear infinite;user-select:none;-webkit-user-select:none;cursor:default;}\n/* tape runs continuously */\n@keyframes tape{from{transform:translateX(0)}to{transform:translateX(-50%)}}\n.ti{display:inline-flex;align-items:center;gap:5px;padding:0 16px;height:26px;border-right:1px solid var(--b);font-size:11px;user-select:none;-webkit-user-select:none;}\n.ts{color:var(--t2);font-weight:600}.tc{font-size:10px}\n/* NAV */\n#nav{height:40px;background:var(--bg2);border-bottom:1px solid var(--b);display:flex;align-items:center;padding:0 10px;gap:2px;flex-shrink:0}\n.nl{display:flex;align-items:center;gap:5px;padding:5px 10px;border-radius:4px;font-size:11px;font-family:var(--fd);font-weight:700;letter-spacing:.04em;text-transform:uppercase;color:var(--t2);text-decoration:none;transition:all .15s;white-space:nowrap}\n.nl:hover{color:var(--t);background:var(--bg3)}\n.nl.on{color:var(--gr);background:rgba(0,229,160,.07);border:1px solid rgba(0,229,160,.12)}\n.nl svg{flex-shrink:0;opacity:.7}.nl.on svg{opacity:1}\n#nav-logo{font-family:var(--fd);font-size:14px;font-weight:800;color:var(--gr);letter-spacing:.04em;margin-right:12px;text-decoration:none}\n/* TICKER BAR */\n#tkbar{height:38px;background:var(--bg2);border-bottom:1px solid var(--b);display:flex;align-items:center;padding:0 14px;gap:10px;flex-shrink:0}\n#tk-sym{font-family:var(--fd);font-size:15px;font-weight:800;color:var(--gr);flex-shrink:0}\n#tk-name{font-size:11px;color:var(--t2);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}\n#tk-px{font-family:var(--fd);font-size:15px;font-weight:700;flex-shrink:0}\n#tk-chg{font-size:11px;padding:2px 8px;border-radius:3px;font-weight:600;flex-shrink:0}\n#tk-sw{position:relative}\n#tk-si{background:var(--bg3);border:1px solid var(--b2);color:var(--t);font-family:var(--fm);font-size:11px;padding:4px 9px;border-radius:3px;outline:none;width:160px}\n#tk-si:focus{border-color:var(--bl)}\n#tk-dr{position:absolute;top:calc(100% + 2px);right:0;background:var(--bg3);border:1px solid var(--b2);border-radius:3px;z-index:999;display:none;min-width:200px;max-height:200px;overflow-y:auto}\n.tdr{padding:6px 10px;cursor:pointer;display:flex;justify-content:space-between;gap:8px;font-size:11px}.tdr:hover{background:var(--bg4)}\n.tds{color:var(--gr);font-weight:700}.tdn{color:var(--t2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:120px}\n/* PAGE BODY */\n.pb{display:flex;flex-direction:column;height:100vh;overflow:hidden}\n.pc{flex:1;overflow-y:auto;overflow-x:hidden}\n/* SKELETON */\n.sk{background:linear-gradient(90deg,var(--bg3) 25%,var(--bg4) 50%,var(--bg3) 75%);background-size:200% 100%;animation:sk 1.2s ease infinite;border-radius:3px}\n@keyframes sk{0%{background-position:200% 0}100%{background-position:-200% 0}}\n.sk-line{height:14px;margin-bottom:8px}.sk-val{height:24px;margin-bottom:4px}\n/* SPIN */\n.spin-svg{display:inline-block;vertical-align:middle;margin-right:8px;width:44px;height:20px}\n/* EMPTY */\n.empty{text-align:center;padding:40px 20px;color:var(--t3);font-size:12px}\n\n\n.pc{padding:16px 20px}\n.section{background:var(--bg2);border:1px solid var(--b);border-radius:6px;margin-bottom:10px;overflow:hidden}\n.shdr{padding:9px 14px;background:var(--bg3);border-bottom:1px solid var(--b);font-family:var(--fd);font-size:10px;font-weight:700;letter-spacing:.09em;text-transform:uppercase;color:var(--t2)}\n.srow{padding:7px 14px;border-bottom:1px solid rgba(255,255,255,.025);display:flex;justify-content:space-between;align-items:start}\n.srow:last-child{border-bottom:none}\n.slbl{font-size:11px;color:var(--t2)}.sval{font-size:12px;font-weight:600;color:var(--t);text-align:right;max-width:60%}\n.sval.up{color:var(--gr)}.sval.dn{color:var(--rd)}\n.hint{background:rgba(240,192,96,.07);border:1px solid rgba(240,192,96,.2);border-radius:4px;padding:10px 14px;margin-bottom:10px;font-size:11px;color:var(--t);line-height:1.6}\n.driver{display:flex;gap:8px;align-items:flex-start;padding:6px 0;border-bottom:1px solid rgba(255,255,255,.03)}\n.driver:last-child{border-bottom:none}\n.driver-dot{width:6px;height:6px;border-radius:50%;background:var(--bl);flex-shrink:0;margin-top:4px}\n.driver-txt{font-size:11px;color:var(--t);line-height:1.5}\n</style>\n</head>\n<body>\n<div class=\"pb\">\n  <div id=\"tape\"><div id=\"tape-inner\"></div></div>\n  \n<div id=\"nav\"><a href=\"/\" id=\"nav-logo\" style=\"display:flex;align-items:center;gap:7px;text-decoration:none\">\n      <svg width=\"22\" height=\"22\" viewBox=\"0 0 64 64\" fill=\"none\">\n        <rect width=\"64\" height=\"64\" rx=\"10\" fill=\"rgba(0,229,160,0.15)\" stroke=\"rgba(0,229,160,0.4)\" stroke-width=\"2\"/>\n        <polyline points=\"8,44 20,28 30,36 42,18 56,22\" stroke=\"#00e5a0\" stroke-width=\"5\" fill=\"none\" stroke-linecap=\"round\" stroke-linejoin=\"round\"/>\n        <circle cx=\"42\" cy=\"18\" r=\"5\" fill=\"#00e5a0\"/>\n      </svg>\n      <span style=\"font-family:var(--fd);font-size:14px;font-weight:800;color:var(--gr);letter-spacing:.04em\">IST</span>\n    </a>\n    <a href=\"/chart\" class=\"nl\"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"currentColor\"><rect x=\"1\" y=\"7\" width=\"2\" height=\"5\"/><rect x=\"5\" y=\"4\" width=\"2\" height=\"8\"/><rect x=\"9\" y=\"1\" width=\"2\" height=\"11\"/></svg>Chart</a><a href=\"/financials\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><polyline points=\"1,9 4,5 7,7 12,2\"/></svg>Financials</a><a href=\"/metrics\" class=\"nl on\"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"currentColor\"><circle cx=\"3\" cy=\"3\" r=\"1.5\"/><circle cx=\"10\" cy=\"3\" r=\"1.5\"/><circle cx=\"3\" cy=\"10\" r=\"1.5\"/><circle cx=\"10\" cy=\"10\" r=\"1.5\"/></svg>Metrics</a><a href=\"/insider\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><circle cx=\"6.5\" cy=\"4\" r=\"2.5\"/><path d=\"M1 12c0-3 2.5-5 5.5-5s5.5 2 5.5 5\"/></svg>Insider</a><a href=\"/congress\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><rect x=\"1\" y=\"5\" width=\"11\" height=\"7\" rx=\"1\"/><path d=\"M4 5V3a2.5 2.5 0 015 0v2\"/></svg>Congress</a><a href=\"/fairvalue\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><path d=\"M6.5 1L8 5h4l-3 2.5 1 4L6.5 9 3 11.5l1-4L1 5h4z\"/></svg>Fair Value</a><a href=\"/news\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><rect x=\"1\" y=\"1\" width=\"11\" height=\"11\" rx=\"1\"/><line x1=\"3\" y1=\"4\" x2=\"10\" y2=\"4\"/><line x1=\"3\" y1=\"7\" x2=\"10\" y2=\"7\"/></svg>News</a><a href=\"/livefeed\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"currentColor\"><circle cx=\"6.5\" cy=\"6.5\" r=\"2\"/><circle cx=\"6.5\" cy=\"6.5\" r=\"4\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1\" opacity=\".5\"/></svg>Live Feed</a></div>\n<script id=\"asset-nav-fix\">\ndocument.addEventListener('DOMContentLoaded', function(){\n  const t=(new URLSearchParams(window.location.search).get('t')||'').toUpperCase();\n  const isCrypto=t.endsWith('-USD')&&!['GLD','SLV','USO','TLT','HYG','LQD','GDX'].includes(t);\n  const isFuture=t.endsWith('=F');\n  const isIndex=t.startsWith('^')||t==='DX-Y.NYB';\n  if(!isCrypto&&!isFuture&&!isIndex) return;\n  const STOCK_ONLY=['/financials','/insider','/congress','/fairvalue'];\n  document.querySelectorAll('#nav a.nl').forEach(a=>{\n    const href=a.getAttribute('href')||'';\n    if(STOCK_ONLY.some(p=>href.includes(p))) a.style.display='none';\n  });\n});\n</script>\n\n  <style>\n  /* Hide stock-only nav items for crypto/commodity */\n  a.nl[href*=\"/insider\"], a.nl[href*=\"/congress\"], a.nl[href*=\"/fairvalue\"] {\n    opacity: 0.3; pointer-events: none; cursor: not-allowed;\n  }\n  </style>\n  <div id=\"tkbar\">\n    <span id=\"tk-sym\">—</span><span id=\"tk-name\">—</span>\n    <span id=\"tk-px\" style=\"color:var(--t2)\">—</span><span id=\"tk-chg\"></span>\n    <div id=\"tk-sw\"><input id=\"tk-si\" placeholder=\"Change ticker…\" autocomplete=\"off\" spellcheck=\"false\"><div id=\"tk-dr\"></div></div>\n  </div>\n  <div class=\"pc\"><div class=\"empty\"><svg class=\"spin-svg\" viewBox=\"0 0 44 20\" fill=\"none\"><polyline points=\"2,16 8,10 14,13 22,5 30,9 42,3\" stroke=\"#00e5a0\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-dasharray=\"120\" stroke-dashoffset=\"120\"><animate attributeName=\"stroke-dashoffset\" from=\"120\" to=\"0\" dur=\"1.2s\" repeatCount=\"indefinite\"/></polyline></svg>A carregar dados commodity…</div></div>\n</div>\n<script>\n\n\n\nconst TSYMS=['SPY','QQQ','^GSPC','^IXIC','^VIX','CL=F','BZ=F','GC=F','SI=F','DX-Y.NYB','^TNX','BTC-USD'];\nconst TLBL={SPY:'SPY',QQQ:'QQQ','^GSPC':'S&P500','^IXIC':'NASDAQ','^VIX':'VIX','CL=F':'WTI','BZ=F':'BRENT','GC=F':'GOLD','SI=F':'SILVER','DX-Y.NYB':'DXY','^TNX':'US10Y','BTC-USD':'BTC'};\nconst socket=io();\nfunction buildTape(){\n  document.getElementById('tape-inner').innerHTML=[...TSYMS,...TSYMS].map(t=>{\n    const id='ti_'+t.replace(/[^a-zA-Z0-9]/g,'_');\n    return`<div class=\"ti\" id=\"${id}\" onclick=\"navToTape('${t}')\" style=\"cursor:pointer;\"><span class=\"ts\">${TLBL[t]||t}</span>&nbsp;<span>—</span>&nbsp;<span class=\"tc nc\">—</span></div>`;\n  }).join('');\n  socket.emit('subscribe',{tickers:TSYMS});\n}\nsocket.on('price_update',({prices})=>{\n  prices.forEach(p=>{\n    const id='ti_'+p.ticker.replace(/[^a-zA-Z0-9]/g,'_');\n    document.querySelectorAll('#'+id).forEach(el=>{const cs=el.children;if(cs[1]&&p.price!=null)cs[1].textContent='$'+p.price.toFixed(2);if(cs[2]&&p.change_pct!=null){const s=p.change_pct>=0?'+':'';cs[2].textContent=s+p.change_pct.toFixed(2)+'%';cs[2].className='tc '+(p.change_pct>0?'up':p.change_pct<0?'dn':'nc');}});\n    if(p.ticker===window.TK)updateTkBar(p);\n  });\n});\nbuildTape();\nconst MEM={};const CACHE_TTL=300000;\nasync function api(url,ttl=CACHE_TTL,timeout=12000){\n  if(MEM[url]&&Date.now()-MEM[url].ts<ttl)return MEM[url].data;\n  try{\n    const ctrl=new AbortController();\n    const tid=setTimeout(()=>ctrl.abort(),timeout);\n    const r=await fetch(url,{signal:ctrl.signal});\n    clearTimeout(tid);\n    if(!r.ok)return null;\n    const d=await r.json();\n    MEM[url]={data:d,ts:Date.now()};\n    return d;\n  }catch{return null;}\n}\nfunction ssGet(k){try{const v=sessionStorage.getItem(k);if(!v)return null;const p=JSON.parse(v);if(Date.now()-p.ts>CACHE_TTL){sessionStorage.removeItem(k);return null;}return p.data;}catch{return null;}}\nfunction ssSet(k,d){try{sessionStorage.setItem(k,JSON.stringify({data:d,ts:Date.now()}));}catch{}}\nasync function getStockData(t){const k='full:'+t;const ss=ssGet(k);if(ss)return ss;const d=await api('/api/full/'+t,CACHE_TTL);if(d)ssSet(k,d);return d;}\nfunction updateTkBar(p){const pxEl=document.getElementById('tk-px'),chEl=document.getElementById('tk-chg');if(!pxEl)return;if(p.price!=null){pxEl.textContent='$'+p.price.toFixed(2);pxEl.style.color=p.change_pct>0?'var(--gr)':p.change_pct<0?'var(--rd)':'var(--t)';}if(p.change_pct!=null){const s=p.change_pct>=0?'+':'';chEl.textContent=s+p.change_pct.toFixed(2)+'%';chEl.style.cssText=`background:${p.change_pct>=0?'rgba(0,229,160,.12)':'rgba(255,77,109,.12)'};color:${p.change_pct>=0?'var(--gr)':'var(--rd)'};padding:2px 8px;border-radius:3px;`;}}\nasync function initTkBar(t){document.getElementById('tk-sym').textContent=t;const d=await api('/api/stock_fast/'+encodeURIComponent(t),6000);if(d){document.getElementById('tk-name').textContent=d.name||t;updateTkBar(d);}socket.emit('subscribe',{tickers:[t]});}\nlet sT;const siEl=document.getElementById('tk-si'),drEl=document.getElementById('tk-dr');\nif(siEl){siEl.addEventListener('input',function(){clearTimeout(sT);const v=this.value.trim();if(!v){drEl.style.display='none';return;}sT=setTimeout(async()=>{const d=await api('/api/universe?q='+encodeURIComponent(v)+'&limit=10',60000);if(!d?.results?.length){drEl.style.display='none';return;}drEl.innerHTML=d.results.map(x=>`<div class=\"tdr\" onclick=\"navTo('${x.ticker}')\"><span class=\"tds\">${x.ticker}</span><span class=\"tdn\">${x.name||''}</span></div>`).join('');drEl.style.display='block';},200);});document.addEventListener('click',e=>{if(!e.target.closest('#tk-sw'))drEl.style.display='none';});}\nfunction navTo(t){if(!t)return;if(siEl){siEl.value='';drEl.style.display='none';}window.location.href='/chart?t='+encodeURIComponent(t);}\nfunction navToTape(t){\n  if(!t)return;\n  // Route to appropriate page based on asset type\n  window.location.href='/chart?t='+encodeURIComponent(t);\n}\nfunction fixNavLinks(){const t=window.TK;if(!t)return;document.querySelectorAll('#nav a.nl').forEach(a=>{const href=a.getAttribute('href');if(href&&href!=='/'&&!href.startsWith('javascript')){const u=new URL(href,window.location.origin);u.searchParams.set('t',t);a.setAttribute('href',u.toString());}});}\nwindow.TK=(new URLSearchParams(window.location.search).get('t')||'GC=F').toUpperCase();\nconst COMM_FULL={\n  'GC=F':{sym:'GOLD',name:'Gold · USD/oz'},\n  'SI=F':{sym:'SILVER',name:'Silver · USD/oz'},\n  'CL=F':{sym:'WTI',name:'WTI Crude Oil · USD/bbl'},\n  'BZ=F':{sym:'BRENT',name:'Brent Crude Oil · USD/bbl'},\n  'NG=F':{sym:'GAS',name:'Natural Gas · USD/MMBtu'},\n  'HG=F':{sym:'COPPER',name:'Copper · USD/lb'},\n  'ZC=F':{sym:'CORN',name:'Corn · USD/bushel'},\n  'ZW=F':{sym:'WHEAT',name:'Wheat · USD/bushel'},\n  'PL=F':{sym:'PLAT',name:'Platinum · USD/oz'},\n  'PA=F':{sym:'PALL',name:'Palladium · USD/oz'},\n  'ZS=F':{sym:'SOY',name:'Soybeans · USD/bushel'},\n};\n// Set proper sym/name in tkbar immediately\nconst _ci=COMM_FULL[window.TK]||{sym:window.TK.replace('=F',''),name:window.TK};\ndocument.addEventListener('DOMContentLoaded',()=>{\n  const symEl=document.getElementById('tk-sym');\n  const nmEl=document.getElementById('tk-name');\n  if(symEl) symEl.textContent=_ci.sym;\n  if(nmEl) nmEl.textContent=_ci.name;\n});\n// Update tkbar name for commodities\nconst COMM_NAMES={'GC=F':'Gold Futures','SI=F':'Silver Futures','CL=F':'WTI Crude Oil',\n  'BZ=F':'Brent Crude Oil','NG=F':'Natural Gas','HG=F':'Copper','ZC=F':'Corn',\n  'ZW=F':'Wheat','PL=F':'Platinum','PA=F':'Palladium','ZS=F':'Soybeans'};\ndocument.addEventListener('DOMContentLoaded',()=>{\n  const nm=COMM_NAMES[window.TK];\n  if(nm&&document.getElementById('tk-name'))document.getElementById('tk-name').textContent=nm;\n  if(document.getElementById('tk-sym'))document.getElementById('tk-sym').textContent=window.TK;\n});\nconst fmtB=v=>{if(v==null)return'—';const a=Math.abs(v),s=v<0?'-':'';if(a>=1e12)return s+'$'+(a/1e12).toFixed(2)+'T';if(a>=1e9)return s+'$'+(a/1e9).toFixed(2)+'B';if(a>=1e6)return s+'$'+(a/1e6).toFixed(1)+'M';return s+'$'+a.toLocaleString('en-US',{maximumFractionDigits:0});};\nconst p1=v=>v==null?'—':(v*100).toFixed(1)+'%';\nconst fn=(v,d=2)=>v==null?'—':Number(v).toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d});\n\nconst COMM_TICKERS=['GC=F','SI=F','CL=F','BZ=F','NG=F','HG=F','ZC=F','ZW=F','PL=F'];\nconst COMM_LABELS={'GC=F':'OURO','SI=F':'PRATA','CL=F':'WTI','BZ=F':'BRENT','NG=F':'GÁS','HG=F':'COBRE','ZC=F':'MILHO','ZW=F':'TRIGO','PL=F':'PLATINA'};\n\nasync function loadComm(t){\n  const el=document.querySelector('.pc');\n  el.innerHTML='<div class=\"empty\"><svg class=\"spin-svg\" viewBox=\"0 0 44 20\" fill=\"none\"><polyline points=\"2,16 8,10 14,13 22,5 30,9 42,3\" stroke=\"#00e5a0\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-dasharray=\"120\" stroke-dashoffset=\"120\"><animate attributeName=\"stroke-dashoffset\" from=\"120\" to=\"0\" dur=\"1.2s\" repeatCount=\"indefinite\"/></polyline></svg>A carregar…</div>';\n  const [ci, px] = await Promise.all([api('/api/commodity_info/'+encodeURIComponent(t),300000), api('/api/stock_fast/'+encodeURIComponent(t),6000)]);\n  let html='';\n\n  // Quick selector\n  html+=`<div style=\"display:flex;gap:6px;flex-wrap:wrap;margin-bottom:14px\">\n    ${COMM_TICKERS.map(tk=>`<button onclick=\"navTo('${tk}')\" style=\"padding:4px 10px;border-radius:4px;font-size:10px;font-family:var(--fd);font-weight:700;cursor:pointer;border:1px solid ${tk===t?'var(--gr)':'var(--b2)'};background:${tk===t?'rgba(0,229,160,.1)':'var(--bg3)'};color:${tk===t?'var(--gr)':'var(--t2)'}\">${COMM_LABELS[tk]||tk}</button>`).join('')}\n  </div>`;\n\n  if(!ci){html+='<div class=\"empty\">Sem dados para esta commodity</div>';el.innerHTML=html;return;}\n\n  // Price hero\n  if(px?.price){\n    const chg=px.change_pct;\n    html+=`<div style=\"background:var(--bg2);border:1px solid var(--b);border-radius:6px;padding:16px 20px;margin-bottom:14px;display:flex;justify-content:space-between;align-items:center\">\n      <div>\n        <div style=\"font-size:10px;color:var(--t2);margin-bottom:4px;font-family:var(--fd);font-weight:700;letter-spacing:.08em;text-transform:uppercase\">${ci.name||t} · ${ci.unit||'USD'}</div>\n        <div style=\"font-family:var(--fd);font-size:32px;font-weight:800;color:${chg>0?'var(--gr)':chg<0?'var(--rd)':'var(--t)'}\">$${px.price.toFixed(2)}</div>\n      </div>\n      <div style=\"text-align:right\">\n        <div style=\"font-size:16px;font-weight:700;color:${chg>0?'var(--gr)':chg<0?'var(--rd)':'var(--t2)'}\">\n          ${chg!=null?(chg>=0?'+':'')+chg.toFixed(2)+'%':'—'}\n        </div>\n        <div style=\"font-size:10px;color:var(--t3);margin-top:4px\">variação hoje</div>\n      </div>\n    </div>`;\n  }\n\n  // Seasonal hint\n  if(ci.seasonal){\n    html+=`<div class=\"hint\"> <b>Sazonalidade:</b> ${ci.seasonal}</div>`;\n  }\n\n  // Key drivers\n  if(ci.drivers?.length){\n    html+=`<div class=\"section\"><div class=\"shdr\">Factores que movem o preço</div>\n    <div style=\"padding:8px 14px\">`;\n    ci.drivers.forEach(d=>{html+=`<div class=\"driver\"><div class=\"driver-dot\"></div><div class=\"driver-txt\">${d}</div></div>`;});\n    html+=`</div></div>`;\n  }\n\n  // Trading tips based on commodity type\n  const tips = {\n    'GC=F': ['Dollar Index (DXY) fraco = Ouro sobe. Correlação negativa forte.','Juros reais (TIPS) negativos = bullish para ouro.','Tensões geopolíticas = procura de safe haven. Compra de bancos centrais (China, Rússia, Índia) suporta preço.','RSI acima de 70 = zona de sobrecompra, cautela.'],\n    'SI=F': ['Combina características de ouro (safe haven) e cobre (industrial).','Ratio ouro/prata > 80 = prata potencialmente barata vs ouro.','Procura solar e baterias crescente = driver de longo prazo.'],\n    'CL=F': ['Reuniões OPEC+ movem o preço. Mantém-te actualizado.','EIA Inventory Report (quarta-feira 15:30 ET) = volatilidade garantida.','Spread WTI-Brent > $5 = mercado norte-americano abundante.','VIX alto + dólar forte = pressão baixista no petróleo.'],\n    'BZ=F': ['Referência global (Europa, Ásia). Mais afectado por tensões Médio Oriente.','Contango (futuro > spot) = mercado bem abastecido. Backwardation = oferta escassa.'],\n    'NG=F': ['Mercado altamente sazonal. Inverno = procura de aquecimento.','Storage report (quinta-feira 14:30 ET) é o relatório mais impactante.','LNG exports crescentes ligam preço US a mercado global.'],\n    'HG=F': ['PMI Manufatureiro China > 50 = bullish para cobre.','Estoques LME baixos = oferta escassa = preço sobe.','Transição energética (EVs, eólica, solar) = procura estrutural crescente.'],\n  };\n  const ctips = tips[t] || ['Analisa os drivers de oferta e procura específicos desta commodity.','Consulta relatórios de agências como EIA, USDA, LME.','Usa análise técnica: médias móveis, RSI, volume.'];\n  html+=`<div class=\"section\"><div class=\"shdr\"> Dicas de Decisão</div>\n  <div style=\"padding:8px 14px\">`;\n  ctips.forEach((tip,i)=>{html+=`<div class=\"driver\"><div style=\"width:18px;height:18px;border-radius:50%;background:var(--bg4);border:1px solid var(--b2);display:flex;align-items:center;justify-content:center;flex-shrink:0;font-size:9px;font-weight:700;color:var(--bl)\">${i+1}</div><div class=\"driver-txt\">${tip}</div></div>`;});\n  html+=`</div></div>`;\n\n  // Chart button\n  html+=`<div style=\"text-align:center;padding:10px 0\">\n    <button onclick=\"window.location.href='/chart?t=${encodeURIComponent(t)}'\" style=\"background:var(--gr);color:var(--bg);font-family:var(--fd);font-size:12px;font-weight:800;padding:10px 24px;border-radius:5px;border:none;cursor:pointer\">Ver Gráfico →</button>\n  </div>`;\n\n  el.innerHTML=html;\n}\n\nfunction onTickerChange(t){\n  // If it's a commodity, load commodity view; otherwise go to chart\n  if(t.endsWith('=F')||t.endsWith('=F')){loadComm(t);}\n  else{window.location.href='/chart?t='+encodeURIComponent(t);}\n}\nloadComm(window.TK);\n\ninitTkBar(window.TK);fixNavLinks();\n</script>\n</body>\n</html>",
    "congress.html": "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"UTF-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\n<title>Congress · IST</title>\n<script src=\"https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.7.5/socket.io.min.js\"></script>\n<link href=\"https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600&family=Syne:wght@400;600;700;800&display=swap\" rel=\"stylesheet\">\n<style>\n*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}\n:root{\n  --bg:#080b0f;--bg2:#0d1117;--bg3:#111720;--bg4:#161d29;\n  --b:#1e2d3d;--b2:#243040;\n  --t:#c9d1d9;--t2:#8b949e;--t3:#484f58;\n  --gr:#00e5a0;--rd:#ff4d6d;--bl:#0095ff;\n  --yl:#f0c060;--pu:#a78bfa;--or:#ff8c42;\n  --fm:'JetBrains Mono',monospace;--fd:'Syne',sans-serif;\n}\nhtml,body{height:100%;background:var(--bg);color:var(--t);font-family:var(--fm);font-size:13px}\n::-webkit-scrollbar{width:5px;height:5px}::-webkit-scrollbar-track{background:transparent}::-webkit-scrollbar-thumb{background:var(--b2);border-radius:3px}\n.up{color:var(--gr)}.dn{color:var(--rd)}.nc{color:var(--t2)}\n/* TAPE */\n#tape{height:26px;background:var(--bg2);border-bottom:1px solid var(--b);overflow:hidden;display:flex;align-items:center;flex-shrink:0}\n#tape-inner{display:flex;white-space:nowrap;animation:tape 90s linear infinite;user-select:none;-webkit-user-select:none;cursor:default;}\n/* tape runs continuously */\n@keyframes tape{from{transform:translateX(0)}to{transform:translateX(-50%)}}\n.ti{display:inline-flex;align-items:center;gap:5px;padding:0 16px;height:26px;border-right:1px solid var(--b);font-size:11px;user-select:none;-webkit-user-select:none;}\n.ts{color:var(--t2);font-weight:600}.tc{font-size:10px}\n/* NAV */\n#nav{height:40px;background:var(--bg2);border-bottom:1px solid var(--b);display:flex;align-items:center;padding:0 10px;gap:2px;flex-shrink:0}\n.nl{display:flex;align-items:center;gap:5px;padding:5px 10px;border-radius:4px;font-size:11px;font-family:var(--fd);font-weight:700;letter-spacing:.04em;text-transform:uppercase;color:var(--t2);text-decoration:none;transition:all .15s;white-space:nowrap}\n.nl:hover{color:var(--t);background:var(--bg3)}\n.nl.on{color:var(--gr);background:rgba(0,229,160,.07);border:1px solid rgba(0,229,160,.12)}\n.nl svg{flex-shrink:0;opacity:.7}.nl.on svg{opacity:1}\n#nav-logo{font-family:var(--fd);font-size:14px;font-weight:800;color:var(--gr);letter-spacing:.04em;margin-right:12px;text-decoration:none}\n/* TICKER BAR */\n#tkbar{height:38px;background:var(--bg2);border-bottom:1px solid var(--b);display:flex;align-items:center;padding:0 14px;gap:10px;flex-shrink:0}\n#tk-sym{font-family:var(--fd);font-size:15px;font-weight:800;color:var(--gr);flex-shrink:0}\n#tk-name{font-size:11px;color:var(--t2);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}\n#tk-px{font-family:var(--fd);font-size:15px;font-weight:700;flex-shrink:0}\n#tk-chg{font-size:11px;padding:2px 8px;border-radius:3px;font-weight:600;flex-shrink:0}\n#tk-sw{position:relative}\n#tk-si{background:var(--bg3);border:1px solid var(--b2);color:var(--t);font-family:var(--fm);font-size:11px;padding:4px 9px;border-radius:3px;outline:none;width:160px}\n#tk-si:focus{border-color:var(--bl)}\n#tk-dr{position:absolute;top:calc(100% + 2px);right:0;background:var(--bg3);border:1px solid var(--b2);border-radius:3px;z-index:999;display:none;min-width:200px;max-height:200px;overflow-y:auto}\n.tdr{padding:6px 10px;cursor:pointer;display:flex;justify-content:space-between;gap:8px;font-size:11px}.tdr:hover{background:var(--bg4)}\n.tds{color:var(--gr);font-weight:700}.tdn{color:var(--t2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:120px}\n/* PAGE BODY */\n.pb{display:flex;flex-direction:column;height:100vh;overflow:hidden}\n.pc{flex:1;overflow-y:auto;overflow-x:hidden}\n/* SKELETON */\n.sk{background:linear-gradient(90deg,var(--bg3) 25%,var(--bg4) 50%,var(--bg3) 75%);background-size:200% 100%;animation:sk 1.2s ease infinite;border-radius:3px}\n@keyframes sk{0%{background-position:200% 0}100%{background-position:-200% 0}}\n.sk-line{height:14px;margin-bottom:8px}.sk-val{height:24px;margin-bottom:4px}\n/* SPIN */\n.spin-svg{display:inline-block;vertical-align:middle;margin-right:8px;width:44px;height:20px}\n/* EMPTY */\n.empty{text-align:center;padding:40px 20px;color:var(--t3);font-size:12px}\n\n.pc{padding:16px 20px;max-width:1000px}\n.stats{display:flex;gap:10px;margin-bottom:14px;flex-wrap:wrap}\n.stat{background:var(--bg2);border:1px solid var(--b);border-radius:4px;padding:8px 16px;text-align:center}\n.stat-v{font-family:var(--fd);font-size:22px;font-weight:800}.stat-l{font-size:9px;color:var(--t3);font-family:var(--fd);font-weight:700;text-transform:uppercase;letter-spacing:.07em;margin-top:2px}\n/* Member cards */\n.members-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:10px;margin-bottom:18px}\n.mcard{background:var(--bg2);border:1px solid var(--b);border-radius:6px;padding:12px;display:flex;flex-direction:column;align-items:center;gap:7px;cursor:pointer;transition:border .15s}\n.mcard:hover{border-color:var(--gr)}\n.mphoto{width:52px;height:52px;border-radius:50%;object-fit:cover;border:2px solid var(--b2);background:var(--bg3)}\n.mname{font-size:11px;font-weight:700;color:var(--t);text-align:center;line-height:1.3}\n.mparty{font-size:10px;color:var(--t2)}\n.mtots{display:flex;gap:8px;font-size:10px}\n.tbl{width:100%;border-collapse:collapse;font-size:12px}\n.tbl th{text-align:left;color:var(--t3);font-size:9px;letter-spacing:.07em;text-transform:uppercase;font-family:var(--fd);font-weight:700;padding:8px 10px;border-bottom:1px solid var(--b);background:var(--bg2);position:sticky;top:0}\n.tbl td{padding:8px 10px;border-bottom:1px solid rgba(255,255,255,.025);color:var(--t)}\n.tbl tr:hover td{background:rgba(255,255,255,.02)}\n.pty{width:22px;height:22px;border-radius:50%;display:inline-flex;align-items:center;justify-content:center;font-size:9px;font-weight:800;vertical-align:middle}\n.D{background:rgba(0,100,255,.2);color:#4d94ff}.R{background:rgba(255,50,50,.2);color:#ff6b6b}.I{background:rgba(150,150,150,.2);color:#aaa}\n.avatar-sm{width:28px;height:28px;border-radius:50%;border:1px solid var(--b2);vertical-align:middle;margin-right:7px;object-fit:cover}\n</style>\n</head>\n<body>\n<div class=\"pb\">\n<div id=\"tape\"><div id=\"tape-inner\"></div></div>\n<div id=\"nav\"><a href=\"/\" id=\"nav-logo\" style=\"display:flex;align-items:center;gap:7px;text-decoration:none\">\n      <svg width=\"22\" height=\"22\" viewBox=\"0 0 64 64\" fill=\"none\">\n        <rect width=\"64\" height=\"64\" rx=\"10\" fill=\"rgba(0,229,160,0.15)\" stroke=\"rgba(0,229,160,0.4)\" stroke-width=\"2\"/>\n        <polyline points=\"8,44 20,28 30,36 42,18 56,22\" stroke=\"#00e5a0\" stroke-width=\"5\" fill=\"none\" stroke-linecap=\"round\" stroke-linejoin=\"round\"/>\n        <circle cx=\"42\" cy=\"18\" r=\"5\" fill=\"#00e5a0\"/>\n      </svg>\n      <span style=\"font-family:var(--fd);font-size:14px;font-weight:800;color:var(--gr);letter-spacing:.04em\">IST</span>\n    </a>\n    <a href=\"/chart\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"currentColor\"><rect x=\"1\" y=\"7\" width=\"2\" height=\"5\"/><rect x=\"5\" y=\"4\" width=\"2\" height=\"8\"/><rect x=\"9\" y=\"1\" width=\"2\" height=\"11\"/></svg>Chart</a><a href=\"/financials\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><polyline points=\"1,9 4,5 7,7 12,2\"/></svg>Financials</a><a href=\"/metrics\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"currentColor\"><circle cx=\"3\" cy=\"3\" r=\"1.5\"/><circle cx=\"10\" cy=\"3\" r=\"1.5\"/><circle cx=\"3\" cy=\"10\" r=\"1.5\"/><circle cx=\"10\" cy=\"10\" r=\"1.5\"/></svg>Metrics</a><a href=\"/insider\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><circle cx=\"6.5\" cy=\"4\" r=\"2.5\"/><path d=\"M1 12c0-3 2.5-5 5.5-5s5.5 2 5.5 5\"/></svg>Insider</a><a href=\"/congress\" class=\"nl on\"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><rect x=\"1\" y=\"5\" width=\"11\" height=\"7\" rx=\"1\"/><path d=\"M4 5V3a2.5 2.5 0 015 0v2\"/></svg>Congress</a><a href=\"/fairvalue\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><path d=\"M6.5 1L8 5h4l-3 2.5 1 4L6.5 9 3 11.5l1-4L1 5h4z\"/></svg>Fair Value</a><a href=\"/news\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><rect x=\"1\" y=\"1\" width=\"11\" height=\"11\" rx=\"1\"/><line x1=\"3\" y1=\"4\" x2=\"10\" y2=\"4\"/><line x1=\"3\" y1=\"7\" x2=\"10\" y2=\"7\"/></svg>News</a><a href=\"/livefeed\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"currentColor\"><circle cx=\"6.5\" cy=\"6.5\" r=\"2\"/><circle cx=\"6.5\" cy=\"6.5\" r=\"4\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1\" opacity=\".5\"/></svg>Live Feed</a></div>\n<script id=\"asset-nav-fix\">\ndocument.addEventListener('DOMContentLoaded', function(){\n  const t=(new URLSearchParams(window.location.search).get('t')||'').toUpperCase();\n  const isCrypto=t.endsWith('-USD')&&!['GLD','SLV','USO','TLT','HYG','LQD','GDX'].includes(t);\n  const isFuture=t.endsWith('=F');\n  const isIndex=t.startsWith('^')||t==='DX-Y.NYB';\n  if(!isCrypto&&!isFuture&&!isIndex) return;\n  const STOCK_ONLY=['/financials','/insider','/congress','/fairvalue'];\n  document.querySelectorAll('#nav a.nl').forEach(a=>{\n    const href=a.getAttribute('href')||'';\n    if(STOCK_ONLY.some(p=>href.includes(p))) a.style.display='none';\n  });\n});\n</script>\n<div id=\"tkbar\">\n  <span id=\"tk-sym\">—</span><span id=\"tk-name\">—</span>\n  <span id=\"tk-px\" style=\"color:var(--t2)\">—</span><span id=\"tk-chg\"></span>\n  <div id=\"tk-sw\"><input id=\"tk-si\" placeholder=\"Change ticker…\" autocomplete=\"off\" spellcheck=\"false\"><div id=\"tk-dr\"></div></div>\n</div>\n<div class=\"pc\"><div class=\"empty\"><svg class=\"spin-svg\" viewBox=\"0 0 44 20\" fill=\"none\"><polyline points=\"2,16 8,10 14,13 22,5 30,9 42,3\" stroke=\"#00e5a0\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-dasharray=\"120\" stroke-dashoffset=\"120\"><animate attributeName=\"stroke-dashoffset\" from=\"120\" to=\"0\" dur=\"1.2s\" repeatCount=\"indefinite\"/></polyline></svg>Loading…</div></div>\n</div>\n<script>\nconst TSYMS=['SPY','QQQ','^GSPC','^IXIC','^VIX','CL=F','BZ=F','GC=F','SI=F','DX-Y.NYB','^TNX','BTC-USD'];\nconst TLBL={SPY:'SPY',QQQ:'QQQ','^GSPC':'S&P500','^IXIC':'NASDAQ','^VIX':'VIX','CL=F':'WTI','BZ=F':'BRENT','GC=F':'GOLD','SI=F':'SILVER','DX-Y.NYB':'DXY','^TNX':'US10Y','BTC-USD':'BTC'};\nconst socket=io();\nfunction buildTape(){\n  document.getElementById('tape-inner').innerHTML=[...TSYMS,...TSYMS].map(t=>{\n    const id='ti_'+t.replace(/[^a-zA-Z0-9]/g,'_');\n    return`<div class=\"ti\" id=\"${id}\" onclick=\"navToTape('${t}')\" style=\"cursor:pointer;\"><span class=\"ts\">${TLBL[t]||t}</span>&nbsp;<span>—</span>&nbsp;<span class=\"tc nc\">—</span></div>`;\n  }).join('');\n  socket.emit('subscribe',{tickers:TSYMS});\n}\nsocket.on('price_update',({prices})=>{\n  prices.forEach(p=>{\n    const id='ti_'+p.ticker.replace(/[^a-zA-Z0-9]/g,'_');\n    document.querySelectorAll('#'+id).forEach(el=>{\n      const cs=el.children;\n      if(cs[1]&&p.price!=null)cs[1].textContent='$'+p.price.toFixed(2);\n      if(cs[2]&&p.change_pct!=null){const s=p.change_pct>=0?'+':'';cs[2].textContent=s+p.change_pct.toFixed(2)+'%';cs[2].className='tc '+(p.change_pct>0?'up':p.change_pct<0?'dn':'nc');}\n    });\n    if(p.ticker===window.TK)updateTkBar(p);\n    if(typeof onPriceUpdate==='function')onPriceUpdate(p);\n  });\n});\nbuildTape();\nconst MEM={};const CACHE_TTL=300000;\nasync function api(url,ttl=CACHE_TTL,timeout=12000){\n  if(MEM[url]&&Date.now()-MEM[url].ts<ttl)return MEM[url].data;\n  try{\n    const ctrl=new AbortController();\n    const tid=setTimeout(()=>ctrl.abort(),timeout);\n    const r=await fetch(url,{signal:ctrl.signal});\n    clearTimeout(tid);\n    if(!r.ok)return null;\n    const d=await r.json();\n    MEM[url]={data:d,ts:Date.now()};\n    return d;\n  }catch{return null;}\n}\nfunction ssGet(k){try{const v=sessionStorage.getItem(k);if(!v)return null;const p=JSON.parse(v);if(Date.now()-p.ts>CACHE_TTL){sessionStorage.removeItem(k);return null;}return p.data;}catch{return null;}}\nfunction ssSet(k,d){try{sessionStorage.setItem(k,JSON.stringify({data:d,ts:Date.now()}));}catch{}}\nasync function getStockData(t){const k='full:'+t;const ss=ssGet(k);if(ss)return ss;const mc=MEM['/api/full/'+t];if(mc&&Date.now()-mc.ts<CACHE_TTL)return mc.data;const d=await api('/api/full/'+t,CACHE_TTL);if(d)ssSet(k,d);return d;}\nfunction updateTkBar(p){const pxEl=document.getElementById('tk-px'),chEl=document.getElementById('tk-chg');if(!pxEl)return;if(p.price!=null){pxEl.textContent='$'+p.price.toFixed(2);pxEl.style.color=p.change_pct>0?'var(--gr)':p.change_pct<0?'var(--rd)':'var(--t)';}if(p.change_pct!=null){const s=p.change_pct>=0?'+':'';chEl.textContent=s+p.change_pct.toFixed(2)+'%';chEl.style.cssText=`background:${p.change_pct>=0?'rgba(0,229,160,.12)':'rgba(255,77,109,.12)'};color:${p.change_pct>=0?'var(--gr)':'var(--rd)'};padding:2px 8px;border-radius:3px;`;}}\nasync function initTkBar(t){document.getElementById('tk-sym').textContent=t;const d=await api('/api/stock_fast/'+encodeURIComponent(t),6000);if(d){document.getElementById('tk-name').textContent=d.name||t;updateTkBar(d);}socket.emit('subscribe',{tickers:[t]});}\nlet sT;const siEl=document.getElementById('tk-si'),drEl=document.getElementById('tk-dr');\nif(siEl){siEl.addEventListener('input',function(){clearTimeout(sT);const v=this.value.trim();if(!v){drEl.style.display='none';return;}sT=setTimeout(async()=>{const d=await api('/api/universe?q='+encodeURIComponent(v)+'&limit=10',60000);if(!d?.results?.length){drEl.style.display='none';return;}drEl.innerHTML=d.results.map(x=>`<div class=\"tdr\" onclick=\"navTo('${x.ticker}')\"><span class=\"tds\">${x.ticker}</span><span class=\"tdn\">${x.name||''}</span></div>`).join('');drEl.style.display='block';},200);});document.addEventListener('click',e=>{if(!e.target.closest('#tk-sw'))drEl.style.display='none';});}\nfunction navTo(t){if(!t)return;if(siEl){siEl.value='';drEl.style.display='none';}window.location.href='/chart?t='+encodeURIComponent(t);}\n// navToTape: always go to chart page with the ticker\nfunction navToTape(t){\n  if(!t)return;\n  // Route to appropriate page based on asset type\n  window.location.href='/chart?t='+encodeURIComponent(t);\n}\nwindow.TK=(new URLSearchParams(window.location.search).get('t')||'NVDA').toUpperCase();\nconst fmtB=v=>{if(v==null)return'—';const a=Math.abs(v),s=v<0?'-':'';if(a>=1e12)return s+'$'+(a/1e12).toFixed(2)+'T';if(a>=1e9)return s+'$'+(a/1e9).toFixed(2)+'B';if(a>=1e6)return s+'$'+(a/1e6).toFixed(1)+'M';return s+'$'+a.toLocaleString('en-US',{maximumFractionDigits:0});};\nconst fmtPx=v=>v==null?'—':'$'+Number(v).toFixed(2);\nconst p1=v=>v==null?'—':(v*100).toFixed(1)+'%';\nconst fn=(v,d=2)=>v==null?'—':Number(v).toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d});\n\nconst AM={'$1,001 - $15,000':'$1K–15K','$15,001 - $50,000':'$15K–50K','$50,001 - $100,000':'$50K–100K','$100,001 - $250,000':'$100K–250K','$250,001 - $500,000':'$250K–500K','Over $1,000,000':'>$1M'};\nconst fmtAmt=a=>AM[a]||a||'—';\n\nasync function loadCong(t){\n  const _isCr=t.endsWith('-USD')&&!['GLD','SLV','USO','TLT'].includes(t);\n  const _isFu=t.endsWith('=F')||t.startsWith('^')||t==='DX-Y.NYB';\n  if(_isCr){window.location.href='/crypto?t='+encodeURIComponent(t);return;}\n  if(_isFu){window.location.href='/commodity?t='+encodeURIComponent(t);return;}\n  // Redirect non-stocks to their page immediately\n  const el=document.querySelector('.pc');\n  el.innerHTML='<div class=\"empty\"><svg class=\"spin-svg\" viewBox=\"0 0 44 20\" fill=\"none\"><polyline points=\"2,16 8,10 14,13 22,5 30,9 42,3\" stroke=\"#00e5a0\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-dasharray=\"120\" stroke-dashoffset=\"120\"><animate attributeName=\"stroke-dashoffset\" from=\"120\" to=\"0\" dur=\"1.2s\" repeatCount=\"indefinite\"/></polyline></svg>A carregar trades congressistas…</div>';\n  const d=await api('/api/congress/'+t,300000);\n  const trades=d?.trades||[];\n  if(!trades.length){el.innerHTML=`<div class=\"empty\">Sem trades congressistas para ${t}<br><small style=\"color:var(--t3)\">Dados do House Stock Watcher e Senate Stock Watcher</small></div>`;return;}\n\n  const members=d.members_detail||[];\n  const partyCol={D:'var(--bl)',R:'var(--rd)',I:'var(--pu)'};\n\n  let html=`<div class=\"stats\">\n    <div class=\"stat\"><div class=\"stat-v\">${trades.length}</div><div class=\"stat-l\">Trades</div></div>\n    <div class=\"stat\"><div class=\"stat-v\" style=\"color:var(--gr)\">${d.buy_count||0}</div><div class=\"stat-l\">Compras</div></div>\n    <div class=\"stat\"><div class=\"stat-v\" style=\"color:var(--rd)\">${d.sell_count||0}</div><div class=\"stat-l\">Vendas</div></div>\n    <div class=\"stat\"><div class=\"stat-v\" style=\"color:var(--yl)\">${members.length}</div><div class=\"stat-l\">Congressistas</div></div>\n  </div>`;\n\n  // Member cards with photos\n  if(members.length){\n    html+=`<div style=\"font-family:var(--fd);font-size:9px;font-weight:700;letter-spacing:.09em;text-transform:uppercase;color:var(--t3);margin-bottom:8px\">Congressistas com posições em ${t}</div>\n    <div class=\"members-grid\">`;\n    members.forEach(m=>{\n      const pt=(m.party||'').charAt(0).toUpperCase()||'I';\n      const pc=partyCol[pt]||'var(--t2)';\n      const av=m.avatar||`https://ui-avatars.com/api/?name=${encodeURIComponent(m.name)}&size=104&background=161d29&color=00e5a0&bold=true&format=svg`;\n      html+=`<div class=\"mcard\" onclick=\"filterMember('${m.name.replace(/'/g,\"\\'\")}')\">\n        <img class=\"mphoto\" src=\"${av}\" alt=\"${m.name}\" onerror=\"this.src='${av}'\">\n        <div class=\"mname\">${m.name}</div>\n        <div class=\"mparty\" style=\"color:${pc}\">${m.party||'—'} · ${m.chamber||'—'}</div>\n        <div class=\"mtots\">\n          ${m.buy_count>0?`<span style=\"color:var(--gr)\">▲${m.buy_count}</span>`:''}\n          ${m.sell_count>0?`<span style=\"color:var(--rd)\">▼${m.sell_count}</span>`:''}\n        </div>\n      </div>`;\n    });\n    html+=`</div>`;\n    // Async Wikipedia photo enrichment\n    setTimeout(()=>{\n      members.forEach(m=>{\n        fetch('/api/insider_photo/'+encodeURIComponent(m.name)).then(r=>r.json()).then(ph=>{\n          if(ph?.url&&ph.source==='wikipedia'){\n            document.querySelectorAll('.mphoto').forEach(img=>{if(img.alt===m.name)img.src=ph.url;});\n          }\n        }).catch(()=>{});\n      });\n    },500);\n  }\n\n  // Trade table with avatar\n  html+=`<div style=\"font-family:var(--fd);font-size:9px;font-weight:700;letter-spacing:.09em;text-transform:uppercase;color:var(--t3);margin-bottom:8px\" id=\"tbl-label\">Todos os trades</div>\n  <table class=\"tbl\" id=\"trades-tbl\"><thead><tr>\n    <th>Congressista</th><th>Câmara</th><th>Tipo</th><th>Montante</th><th>Data</th><th>Activo</th>\n  </tr></thead><tbody id=\"trades-body\">`;\n  trades.forEach(tr=>{\n    const iB=/purchase|buy/i.test(tr.type||'');const iS=/sale|sell/i.test(tr.type||'');\n    const pt=(tr.party||'').charAt(0).toUpperCase()||'I';\n    const av=tr.avatar||`https://ui-avatars.com/api/?name=${encodeURIComponent(tr.name||'?')}&size=56&background=161d29&color=00e5a0&bold=true&format=svg`;\n    html+=`<tr data-name=\"${tr.name||''}\">\n      <td><img class=\"avatar-sm\" src=\"${av}\" alt=\"${tr.name||''}\" onerror=\"this.src='${av}'\">\n          <span style=\"font-weight:600\">${tr.name||'—'}</span>\n          <span class=\"pty ${pt}\" style=\"margin-left:6px\">${pt}</span></td>\n      <td style=\"color:var(--t2)\">${tr.chamber||'—'}</td>\n      <td style=\"font-weight:700;color:${iB?'var(--gr)':iS?'var(--rd)':'var(--t2)'}\">${iB?'▲ BUY':iS?'▼ SELL':'TRADE'}</td>\n      <td style=\"font-weight:600\">${fmtAmt(tr.amount)}</td>\n      <td style=\"color:var(--t2)\">${tr.date||'—'}</td>\n      <td style=\"color:var(--t3);font-size:10px\">${(tr.asset||'').slice(0,45)}</td>\n    </tr>`;\n  });\n  html+=`</tbody></table>`;\n  el.innerHTML=html;\n}\n\nlet memberFilter=null;\nfunction filterMember(name){\n  memberFilter = memberFilter===name ? null : name;\n  const rows=document.querySelectorAll('#trades-body tr');\n  rows.forEach(r=>{r.style.display=(!memberFilter||r.dataset.name===memberFilter)?'':'none';});\n  const lbl=document.getElementById('tbl-label');\n  if(lbl)lbl.textContent=memberFilter?`Trades de ${memberFilter} (clica novamente para mostrar todos)`:'Todos os trades';\n}\n\nfunction onTickerChange(t){loadCong(t);}\nloadCong(window.TK);\n\ninitTkBar(window.TK);\n\n// Fix nav links to carry current ticker\nfunction fixNavLinks(){\n  const t=window.TK;\n  if(!t)return;\n  document.querySelectorAll('#nav a.nl').forEach(a=>{\n    const href=a.getAttribute('href');\n    if(href&&href!=='/'&&!href.startsWith('javascript')){\n      const u=new URL(href,window.location.origin);\n      u.searchParams.set('t',t);\n      a.setAttribute('href',u.toString());\n    }\n  });\n}\nfixNavLinks();\n// Also update nav links whenever ticker changes\n// navTo already calls fixNavLinks()\n</script>\n</body>\n</html>",
    "crypto.html": "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"UTF-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\n<title>Crypto · IST</title>\n<script src=\"https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.7.5/socket.io.min.js\"></script>\n<script src=\"https://cdn.plot.ly/plotly-2.32.0.min.js\"></script>\n<link href=\"https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600&family=Syne:wght@400;600;700;800&display=swap\" rel=\"stylesheet\">\n<style>\n\n*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}\n:root{\n  --bg:#080b0f;--bg2:#0d1117;--bg3:#111720;--bg4:#161d29;\n  --b:#1e2d3d;--b2:#243040;\n  --t:#c9d1d9;--t2:#8b949e;--t3:#484f58;\n  --gr:#00e5a0;--rd:#ff4d6d;--bl:#0095ff;\n  --yl:#f0c060;--pu:#a78bfa;--or:#ff8c42;\n  --fm:'JetBrains Mono',monospace;--fd:'Syne',sans-serif;\n}\nhtml,body{height:100%;background:var(--bg);color:var(--t);font-family:var(--fm);font-size:13px}\n::-webkit-scrollbar{width:5px;height:5px}::-webkit-scrollbar-track{background:transparent}::-webkit-scrollbar-thumb{background:var(--b2);border-radius:3px}\n.up{color:var(--gr)}.dn{color:var(--rd)}.nc{color:var(--t2)}\n/* TAPE */\n#tape{height:26px;background:var(--bg2);border-bottom:1px solid var(--b);overflow:hidden;display:flex;align-items:center;flex-shrink:0}\n#tape-inner{display:flex;white-space:nowrap;animation:tape 90s linear infinite;user-select:none;-webkit-user-select:none;cursor:default;}\n/* tape runs continuously */\n@keyframes tape{from{transform:translateX(0)}to{transform:translateX(-50%)}}\n.ti{display:inline-flex;align-items:center;gap:5px;padding:0 16px;height:26px;border-right:1px solid var(--b);font-size:11px;user-select:none;-webkit-user-select:none;}\n.ts{color:var(--t2);font-weight:600}.tc{font-size:10px}\n/* NAV */\n#nav{height:40px;background:var(--bg2);border-bottom:1px solid var(--b);display:flex;align-items:center;padding:0 10px;gap:2px;flex-shrink:0}\n.nl{display:flex;align-items:center;gap:5px;padding:5px 10px;border-radius:4px;font-size:11px;font-family:var(--fd);font-weight:700;letter-spacing:.04em;text-transform:uppercase;color:var(--t2);text-decoration:none;transition:all .15s;white-space:nowrap}\n.nl:hover{color:var(--t);background:var(--bg3)}\n.nl.on{color:var(--gr);background:rgba(0,229,160,.07);border:1px solid rgba(0,229,160,.12)}\n.nl svg{flex-shrink:0;opacity:.7}.nl.on svg{opacity:1}\n#nav-logo{font-family:var(--fd);font-size:14px;font-weight:800;color:var(--gr);letter-spacing:.04em;margin-right:12px;text-decoration:none}\n/* TICKER BAR */\n#tkbar{height:38px;background:var(--bg2);border-bottom:1px solid var(--b);display:flex;align-items:center;padding:0 14px;gap:10px;flex-shrink:0}\n#tk-sym{font-family:var(--fd);font-size:15px;font-weight:800;color:var(--gr);flex-shrink:0}\n#tk-name{font-size:11px;color:var(--t2);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}\n#tk-px{font-family:var(--fd);font-size:15px;font-weight:700;flex-shrink:0}\n#tk-chg{font-size:11px;padding:2px 8px;border-radius:3px;font-weight:600;flex-shrink:0}\n#tk-sw{position:relative}\n#tk-si{background:var(--bg3);border:1px solid var(--b2);color:var(--t);font-family:var(--fm);font-size:11px;padding:4px 9px;border-radius:3px;outline:none;width:160px}\n#tk-si:focus{border-color:var(--bl)}\n#tk-dr{position:absolute;top:calc(100% + 2px);right:0;background:var(--bg3);border:1px solid var(--b2);border-radius:3px;z-index:999;display:none;min-width:200px;max-height:200px;overflow-y:auto}\n.tdr{padding:6px 10px;cursor:pointer;display:flex;justify-content:space-between;gap:8px;font-size:11px}.tdr:hover{background:var(--bg4)}\n.tds{color:var(--gr);font-weight:700}.tdn{color:var(--t2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:120px}\n/* PAGE BODY */\n.pb{display:flex;flex-direction:column;height:100vh;overflow:hidden}\n.pc{flex:1;overflow-y:auto;overflow-x:hidden}\n/* SKELETON */\n.sk{background:linear-gradient(90deg,var(--bg3) 25%,var(--bg4) 50%,var(--bg3) 75%);background-size:200% 100%;animation:sk 1.2s ease infinite;border-radius:3px}\n@keyframes sk{0%{background-position:200% 0}100%{background-position:-200% 0}}\n.sk-line{height:14px;margin-bottom:8px}.sk-val{height:24px;margin-bottom:4px}\n/* SPIN */\n.spin-svg{display:inline-block;vertical-align:middle;margin-right:8px;width:44px;height:20px}\n/* EMPTY */\n.empty{text-align:center;padding:40px 20px;color:var(--t3);font-size:12px}\n\n\n.pc{padding:16px 20px}\n.fg-meter{background:var(--bg2);border:1px solid var(--b);border-radius:8px;padding:20px;margin-bottom:14px;display:flex;align-items:center;gap:20px}\n.fg-dial{width:90px;height:90px;border-radius:50%;display:flex;flex-direction:column;align-items:center;justify-content:center;flex-shrink:0;border:4px solid}\n.fg-val{font-family:var(--fd);font-size:28px;font-weight:800}\n.fg-lbl{font-size:10px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;margin-top:2px}\n.section{background:var(--bg2);border:1px solid var(--b);border-radius:6px;margin-bottom:10px;overflow:hidden}\n.shdr{padding:9px 14px;background:var(--bg3);border-bottom:1px solid var(--b);font-family:var(--fd);font-size:10px;font-weight:700;letter-spacing:.09em;text-transform:uppercase;color:var(--t2)}\n.sgrid{display:grid;grid-template-columns:1fr 1fr;gap:0}\n.srow{padding:8px 14px;border-bottom:1px solid rgba(255,255,255,.025);display:flex;justify-content:space-between;align-items:center}\n.srow:last-child{border-bottom:none}\n.slbl{font-size:11px;color:var(--t2)}.sval{font-size:12px;font-weight:600;color:var(--t)}\n.sval.up{color:var(--gr)}.sval.dn{color:var(--rd)}.sval.yl{color:var(--yl)}\n.halving-bar{height:8px;background:var(--bg4);border-radius:4px;overflow:hidden;margin:8px 0}\n.halving-fill{height:100%;border-radius:4px;background:linear-gradient(90deg,var(--gr),var(--yl))}\n.hint{background:rgba(0,149,255,.07);border:1px solid rgba(0,149,255,.2);border-radius:4px;padding:10px 14px;margin-bottom:10px;font-size:11px;color:var(--t);line-height:1.6}\n.hint b{color:var(--bl)}\n.fg-hist{display:flex;flex-direction:column;gap:2px;margin-top:12px}\n.fg-hbar{flex:1;border-radius:2px 2px 0 0;cursor:default;position:relative}\n.fg-hbar:hover::after{content:attr(data-tip);position:absolute;bottom:110%;left:50%;transform:translateX(-50%);background:var(--bg);border:1px solid var(--b2);color:var(--t);font-size:9px;padding:3px 6px;border-radius:3px;white-space:nowrap;pointer-events:none;z-index:10}\n</style>\n</head>\n<body>\n<div class=\"pb\">\n  <div id=\"tape\"><div id=\"tape-inner\"></div></div>\n  \n<div id=\"nav\"><a href=\"/\" id=\"nav-logo\" style=\"display:flex;align-items:center;gap:7px;text-decoration:none\">\n      <svg width=\"22\" height=\"22\" viewBox=\"0 0 64 64\" fill=\"none\">\n        <rect width=\"64\" height=\"64\" rx=\"10\" fill=\"rgba(0,229,160,0.15)\" stroke=\"rgba(0,229,160,0.4)\" stroke-width=\"2\"/>\n        <polyline points=\"8,44 20,28 30,36 42,18 56,22\" stroke=\"#00e5a0\" stroke-width=\"5\" fill=\"none\" stroke-linecap=\"round\" stroke-linejoin=\"round\"/>\n        <circle cx=\"42\" cy=\"18\" r=\"5\" fill=\"#00e5a0\"/>\n      </svg>\n      <span style=\"font-family:var(--fd);font-size:14px;font-weight:800;color:var(--gr);letter-spacing:.04em\">IST</span>\n    </a>\n    <a href=\"/chart\" class=\"nl\"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"currentColor\"><rect x=\"1\" y=\"7\" width=\"2\" height=\"5\"/><rect x=\"5\" y=\"4\" width=\"2\" height=\"8\"/><rect x=\"9\" y=\"1\" width=\"2\" height=\"11\"/></svg>Chart</a><a href=\"/financials\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><polyline points=\"1,9 4,5 7,7 12,2\"/></svg>Financials</a><a href=\"/metrics\" class=\"nl on\"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"currentColor\"><circle cx=\"3\" cy=\"3\" r=\"1.5\"/><circle cx=\"10\" cy=\"3\" r=\"1.5\"/><circle cx=\"3\" cy=\"10\" r=\"1.5\"/><circle cx=\"10\" cy=\"10\" r=\"1.5\"/></svg>Metrics</a><a href=\"/insider\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><circle cx=\"6.5\" cy=\"4\" r=\"2.5\"/><path d=\"M1 12c0-3 2.5-5 5.5-5s5.5 2 5.5 5\"/></svg>Insider</a><a href=\"/congress\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><rect x=\"1\" y=\"5\" width=\"11\" height=\"7\" rx=\"1\"/><path d=\"M4 5V3a2.5 2.5 0 015 0v2\"/></svg>Congress</a><a href=\"/fairvalue\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><path d=\"M6.5 1L8 5h4l-3 2.5 1 4L6.5 9 3 11.5l1-4L1 5h4z\"/></svg>Fair Value</a><a href=\"/news\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><rect x=\"1\" y=\"1\" width=\"11\" height=\"11\" rx=\"1\"/><line x1=\"3\" y1=\"4\" x2=\"10\" y2=\"4\"/><line x1=\"3\" y1=\"7\" x2=\"10\" y2=\"7\"/></svg>News</a><a href=\"/livefeed\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"currentColor\"><circle cx=\"6.5\" cy=\"6.5\" r=\"2\"/><circle cx=\"6.5\" cy=\"6.5\" r=\"4\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1\" opacity=\".5\"/></svg>Live Feed</a></div>\n<script id=\"asset-nav-fix\">\ndocument.addEventListener('DOMContentLoaded', function(){\n  const t=(new URLSearchParams(window.location.search).get('t')||'').toUpperCase();\n  const isCrypto=t.endsWith('-USD')&&!['GLD','SLV','USO','TLT','HYG','LQD','GDX'].includes(t);\n  const isFuture=t.endsWith('=F');\n  const isIndex=t.startsWith('^')||t==='DX-Y.NYB';\n  if(!isCrypto&&!isFuture&&!isIndex) return;\n  const STOCK_ONLY=['/financials','/insider','/congress','/fairvalue'];\n  document.querySelectorAll('#nav a.nl').forEach(a=>{\n    const href=a.getAttribute('href')||'';\n    if(STOCK_ONLY.some(p=>href.includes(p))) a.style.display='none';\n  });\n});\n</script>\n\n  <style>\n  /* Hide stock-only nav items for crypto/commodity */\n  a.nl[href*=\"/insider\"], a.nl[href*=\"/congress\"], a.nl[href*=\"/fairvalue\"] {\n    opacity: 0.3; pointer-events: none; cursor: not-allowed;\n  }\n  </style>\n  <div id=\"tkbar\">\n    <span id=\"tk-sym\">—</span><span id=\"tk-name\">—</span>\n    <span id=\"tk-px\" style=\"color:var(--t2)\">—</span><span id=\"tk-chg\"></span>\n    <div id=\"tk-sw\"><input id=\"tk-si\" placeholder=\"Change ticker…\" autocomplete=\"off\" spellcheck=\"false\"><div id=\"tk-dr\"></div></div>\n  </div>\n  <div class=\"pc\"><div class=\"empty\"><svg class=\"spin-svg\" viewBox=\"0 0 44 20\" fill=\"none\"><polyline points=\"2,16 8,10 14,13 22,5 30,9 42,3\" stroke=\"#00e5a0\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-dasharray=\"120\" stroke-dashoffset=\"120\"><animate attributeName=\"stroke-dashoffset\" from=\"120\" to=\"0\" dur=\"1.2s\" repeatCount=\"indefinite\"/></polyline></svg>A carregar dados crypto…</div></div>\n</div>\n<script>\n\n\n\nconst TSYMS=['SPY','QQQ','^GSPC','^IXIC','^VIX','CL=F','BZ=F','GC=F','SI=F','DX-Y.NYB','^TNX','BTC-USD'];\nconst TLBL={SPY:'SPY',QQQ:'QQQ','^GSPC':'S&P500','^IXIC':'NASDAQ','^VIX':'VIX','CL=F':'WTI','BZ=F':'BRENT','GC=F':'GOLD','SI=F':'SILVER','DX-Y.NYB':'DXY','^TNX':'US10Y','BTC-USD':'BTC'};\nconst socket=io();\nfunction buildTape(){\n  document.getElementById('tape-inner').innerHTML=[...TSYMS,...TSYMS].map(t=>{\n    const id='ti_'+t.replace(/[^a-zA-Z0-9]/g,'_');\n    return`<div class=\"ti\" id=\"${id}\" onclick=\"navToTape('${t}')\" style=\"cursor:pointer;\"><span class=\"ts\">${TLBL[t]||t}</span>&nbsp;<span>—</span>&nbsp;<span class=\"tc nc\">—</span></div>`;\n  }).join('');\n  socket.emit('subscribe',{tickers:TSYMS});\n}\nsocket.on('price_update',({prices})=>{\n  prices.forEach(p=>{\n    const id='ti_'+p.ticker.replace(/[^a-zA-Z0-9]/g,'_');\n    document.querySelectorAll('#'+id).forEach(el=>{const cs=el.children;if(cs[1]&&p.price!=null)cs[1].textContent='$'+p.price.toFixed(2);if(cs[2]&&p.change_pct!=null){const s=p.change_pct>=0?'+':'';cs[2].textContent=s+p.change_pct.toFixed(2)+'%';cs[2].className='tc '+(p.change_pct>0?'up':p.change_pct<0?'dn':'nc');}});\n    if(p.ticker===window.TK)updateTkBar(p);\n  });\n});\nbuildTape();\nconst MEM={};const CACHE_TTL=300000;\nasync function api(url,ttl=CACHE_TTL,timeout=12000){\n  if(MEM[url]&&Date.now()-MEM[url].ts<ttl)return MEM[url].data;\n  try{\n    const ctrl=new AbortController();\n    const tid=setTimeout(()=>ctrl.abort(),timeout);\n    const r=await fetch(url,{signal:ctrl.signal});\n    clearTimeout(tid);\n    if(!r.ok)return null;\n    const d=await r.json();\n    MEM[url]={data:d,ts:Date.now()};\n    return d;\n  }catch{return null;}\n}\nfunction ssGet(k){try{const v=sessionStorage.getItem(k);if(!v)return null;const p=JSON.parse(v);if(Date.now()-p.ts>CACHE_TTL){sessionStorage.removeItem(k);return null;}return p.data;}catch{return null;}}\nfunction ssSet(k,d){try{sessionStorage.setItem(k,JSON.stringify({data:d,ts:Date.now()}));}catch{}}\nasync function getStockData(t){const k='full:'+t;const ss=ssGet(k);if(ss)return ss;const d=await api('/api/full/'+t,CACHE_TTL);if(d)ssSet(k,d);return d;}\nfunction updateTkBar(p){const pxEl=document.getElementById('tk-px'),chEl=document.getElementById('tk-chg');if(!pxEl)return;if(p.price!=null){pxEl.textContent='$'+p.price.toFixed(2);pxEl.style.color=p.change_pct>0?'var(--gr)':p.change_pct<0?'var(--rd)':'var(--t)';}if(p.change_pct!=null){const s=p.change_pct>=0?'+':'';chEl.textContent=s+p.change_pct.toFixed(2)+'%';chEl.style.cssText=`background:${p.change_pct>=0?'rgba(0,229,160,.12)':'rgba(255,77,109,.12)'};color:${p.change_pct>=0?'var(--gr)':'var(--rd)'};padding:2px 8px;border-radius:3px;`;}}\nasync function initTkBar(t){document.getElementById('tk-sym').textContent=t;const d=await api('/api/stock_fast/'+encodeURIComponent(t),6000);if(d){document.getElementById('tk-name').textContent=d.name||t;updateTkBar(d);}socket.emit('subscribe',{tickers:[t]});}\nlet sT;const siEl=document.getElementById('tk-si'),drEl=document.getElementById('tk-dr');\nif(siEl){siEl.addEventListener('input',function(){clearTimeout(sT);const v=this.value.trim();if(!v){drEl.style.display='none';return;}sT=setTimeout(async()=>{const d=await api('/api/universe?q='+encodeURIComponent(v)+'&limit=10',60000);if(!d?.results?.length){drEl.style.display='none';return;}drEl.innerHTML=d.results.map(x=>`<div class=\"tdr\" onclick=\"navTo('${x.ticker}')\"><span class=\"tds\">${x.ticker}</span><span class=\"tdn\">${x.name||''}</span></div>`).join('');drEl.style.display='block';},200);});document.addEventListener('click',e=>{if(!e.target.closest('#tk-sw'))drEl.style.display='none';});}\nfunction navTo(t){if(!t)return;if(siEl){siEl.value='';drEl.style.display='none';}window.location.href='/chart?t='+encodeURIComponent(t);}\nfunction navToTape(t){\n  if(!t)return;\n  // Route to appropriate page based on asset type\n  window.location.href='/chart?t='+encodeURIComponent(t);\n}\nfunction fixNavLinks(){const t=window.TK;if(!t)return;document.querySelectorAll('#nav a.nl').forEach(a=>{const href=a.getAttribute('href');if(href&&href!=='/'&&!href.startsWith('javascript')){const u=new URL(href,window.location.origin);u.searchParams.set('t',t);a.setAttribute('href',u.toString());}});}\nwindow.TK=(new URLSearchParams(window.location.search).get('t')||'BTC-USD').toUpperCase();\nconst fmtB=v=>{if(v==null)return'—';const a=Math.abs(v),s=v<0?'-':'';if(a>=1e12)return s+'$'+(a/1e12).toFixed(2)+'T';if(a>=1e9)return s+'$'+(a/1e9).toFixed(2)+'B';if(a>=1e6)return s+'$'+(a/1e6).toFixed(1)+'M';return s+'$'+a.toLocaleString('en-US',{maximumFractionDigits:0});};\nconst p1=v=>v==null?'—':(v*100).toFixed(1)+'%';\nconst fn=(v,d=2)=>v==null?'—':Number(v).toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d});\n\nconst CRYPTO_TICKERS=['BTC-USD','ETH-USD','SOL-USD','BNB-USD','XRP-USD','ADA-USD','DOGE-USD','AVAX-USD'];\n\nfunction fgColor(v){\n  if(v>=75)return{bg:'rgba(0,229,160,.2)',border:'var(--gr)',text:'var(--gr)'};\n  if(v>=55)return{bg:'rgba(0,229,160,.1)',border:'#80e0a0',text:'#80e0a0'};\n  if(v>=45)return{bg:'rgba(240,192,96,.15)',border:'var(--yl)',text:'var(--yl)'};\n  if(v>=25)return{bg:'rgba(255,140,66,.15)',border:'var(--or)',text:'var(--or)'};\n  return{bg:'rgba(255,77,109,.2)',border:'var(--rd)',text:'var(--rd)'};\n}\n\nasync function loadCrypto(t){\n  const el=document.querySelector('.pc');\n  el.innerHTML='<div class=\"empty\"><svg class=\"spin-svg\" viewBox=\"0 0 44 20\" fill=\"none\"><polyline points=\"2,16 8,10 14,13 22,5 30,9 42,3\" stroke=\"#00e5a0\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-dasharray=\"120\" stroke-dashoffset=\"120\"><animate attributeName=\"stroke-dashoffset\" from=\"120\" to=\"0\" dur=\"1.2s\" repeatCount=\"indefinite\"/></polyline></svg>A carregar…</div>';\n  const [ci,px] = await Promise.all([api('/api/crypto_info/'+t,60000), api('/api/stock_fast/'+encodeURIComponent(t),6000)]);\n  let html='';\n\n  // Quick ticker selector\n  html+=`<div style=\"display:flex;gap:6px;flex-wrap:wrap;margin-bottom:14px\">\n    ${CRYPTO_TICKERS.map(tk=>`<button onclick=\"navTo('${tk}')\" style=\"padding:4px 10px;border-radius:4px;font-size:10px;font-family:var(--fd);font-weight:700;cursor:pointer;border:1px solid ${tk===t?'var(--gr)':'var(--b2)'};background:${tk===t?'rgba(0,229,160,.1)':'var(--bg3)'};color:${tk===t?'var(--gr)':'var(--t2)'}\">${tk.replace('-USD','')}</button>`).join('')}\n  </div>`;\n\n  // Fear & Greed\n  const fg=ci?.fear_greed;\n  if(fg){\n    const c=fgColor(fg.value);\n    html+=`<div class=\"fg-meter\">\n      <div class=\"fg-dial\" style=\"background:${c.bg};border-color:${c.border}\">\n        <div class=\"fg-val\" style=\"color:${c.text}\">${fg.value}</div>\n        <div class=\"fg-lbl\" style=\"color:${c.text};font-size:8px\">/100</div>\n      </div>\n      <div style=\"flex:1\">\n        <div style=\"font-family:var(--fd);font-size:18px;font-weight:800;color:${c.text}\">${fg.label}</div>\n        <div style=\"font-size:11px;color:var(--t2);margin-top:4px\">Fear & Greed Index · Crypto Market Sentiment</div>\n        <div style=\"font-size:10px;color:var(--t3);margin-top:2px\">Fonte: Alternative.me · Atualizado diariamente</div>\n        <div class=\"fg-hist\" style=\"flex-direction:column;height:auto;gap:0\">${(()=>{\n          const days=7;\n          const hist=(fg.history||[]).slice(0,days).reverse();\n          const WDAYS=['Dom','Seg','Ter','Qua','Qui','Sex','Sáb'];\n          return hist.map((h,i)=>{\n            const hc=fgColor(h.value);\n            const pct=Math.round(h.value/100*140);\n            // Parse timestamp to get weekday\n            const dt=new Date(parseInt(h.date)*1000);\n            const wd=WDAYS[dt.getDay()];\n            const dd=dt.getDate()+'/'+(dt.getMonth()+1);\n            return`<div style=\"display:flex;align-items:center;gap:8px;margin-bottom:4px\">\n              <div style=\"width:28px;font-size:9px;color:var(--t3);text-align:right;flex-shrink:0\">${wd}</div>\n              <div style=\"width:${pct}px;height:14px;border-radius:2px;background:${hc.border};opacity:.85;min-width:10px;transition:width .3s\"></div>\n              <div style=\"font-size:10px;font-weight:700;color:${hc.text}\">${h.value}</div>\n              <div style=\"font-size:9px;color:var(--t3)\">${h.label}</div>\n            </div>`;\n          }).join('');\n        })()}</div>\n      </div>\n    </div>`;\n\n    // Interpretation hints\n    const hints={\n      extreme_fear:' <b>Medo Extremo</b> — historicamente bom momento para comprar. Mercado sobrevende por pânico.',\n      fear:' <b>Medo</b> — potencial oportunidade. Considerar DCA (compras regulares).',\n      neutral:'⚪ <b>Neutro</b> — mercado equilibrado. Sem sinal claro de entrada/saída.',\n      greed:' <b>Ganância</b> — mercado aquecido. Considerar reduzir posições ou aguardar correção.',\n      extreme_greed:' <b>Ganância Extrema</b> — historicamente mau momento para entrar. Alta probabilidade de correção.',\n    };\n    const fgl=fg.label.toLowerCase().replace(' ','_');\n    const hint=hints[fgl]||hints.neutral;\n    html+=`<div class=\"hint\">${hint}<br><span style=\"color:var(--t3);font-size:10px\">Este índice combina volatilidade, momentum, volume, dominância e trends sociais.</span></div>`;\n  }\n\n  // ── Bull/Bear signal metrics ──\n  if(ci?.market||ci?.fear_greed||ci?.coin){\n    const fg_val = ci?.fear_greed?.value;\n    const btc_dom = ci?.market?.btc_dominance;\n    const price_7d = ci?.coin?.price_change_7d;\n    const price_30d = ci?.coin?.price_change_30d;\n    const price_1y = ci?.coin?.price_change_1y;\n    const ath_pct = ci?.coin?.ath_change_pct;\n\n    // Composite signal\n    let bullScore = 0, bearScore = 0;\n    if(fg_val!=null){ if(fg_val<25)bullScore+=2; else if(fg_val<45)bullScore+=1; else if(fg_val>75)bearScore+=2; else if(fg_val>55)bearScore+=1; }\n    if(btc_dom!=null){ if(btc_dom>55)bearScore+=1; else if(btc_dom<45)bullScore+=1; } // high BTC dom = alt season not started\n    if(price_7d!=null){ if(price_7d>10)bullScore+=1; else if(price_7d<-10)bearScore+=1; }\n    if(price_30d!=null){ if(price_30d>20)bullScore+=1; else if(price_30d<-20)bearScore+=1; }\n    const totalSignal = bullScore - bearScore;\n    const signalLabel = totalSignal>=3?'BULL FORTE':totalSignal>=1?'BULL FRACO':totalSignal<=-3?'BEAR FORTE':totalSignal<=-1?'BEAR FRACO':'NEUTRO';\n    const signalColor = totalSignal>0?'var(--gr)':totalSignal<0?'var(--rd)':'var(--t2)';\n\n    html+=`<div style=\"background:var(--bg3);border:1px solid ${signalColor};border-radius:6px;padding:14px 16px;margin-bottom:14px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px\">\n      <div>\n        <div style=\"font-size:10px;color:var(--t2);font-family:var(--fd);font-weight:700;letter-spacing:.08em;text-transform:uppercase;margin-bottom:6px\">Sinal de Mercado Composto</div>\n        <div style=\"font-family:var(--fd);font-size:26px;font-weight:800;color:${signalColor}\">${signalLabel}</div>\n        <div style=\"font-size:10px;color:var(--t3);margin-top:3px\">Baseado em F&G, dominância BTC, momentum 7D/30D</div>\n      </div>\n      <div style=\"display:flex;gap:8px;flex-wrap:wrap\">\n        ${fg_val!=null?`<div style=\"background:var(--bg4);border-radius:4px;padding:6px 10px;text-align:center\"><div style=\"font-size:18px;font-weight:800;color:${fg_val<40?'var(--gr)':fg_val>60?'var(--rd)':'var(--yl)'}\">${fg_val}</div><div style=\"font-size:9px;color:var(--t3)\">Fear&Greed</div></div>`:''}\n        ${btc_dom!=null?`<div style=\"background:var(--bg4);border-radius:4px;padding:6px 10px;text-align:center\"><div style=\"font-size:18px;font-weight:800;color:var(--bl)\">${btc_dom}%</div><div style=\"font-size:9px;color:var(--t3)\">BTC Dom.</div></div>`:''}\n        ${price_7d!=null?`<div style=\"background:var(--bg4);border-radius:4px;padding:6px 10px;text-align:center\"><div style=\"font-size:18px;font-weight:800;color:${price_7d>0?'var(--gr)':'var(--rd)'}\">${price_7d>0?'+':''}${price_7d}%</div><div style=\"font-size:9px;color:var(--t3)\">7 Dias</div></div>`:''}\n        ${price_30d!=null?`<div style=\"background:var(--bg4);border-radius:4px;padding:6px 10px;text-align:center\"><div style=\"font-size:18px;font-weight:800;color:${price_30d>0?'var(--gr)':'var(--rd)'}\">${price_30d>0?'+':''}${price_30d}%</div><div style=\"font-size:9px;color:var(--t3)\">30 Dias</div></div>`:''}\n        ${ath_pct!=null?`<div style=\"background:var(--bg4);border-radius:4px;padding:6px 10px;text-align:center\"><div style=\"font-size:18px;font-weight:800;color:var(--yl)\">${ath_pct.toFixed(0)}%</div><div style=\"font-size:9px;color:var(--t3)\">vs ATH</div></div>`:''}\n      </div>\n    </div>`;\n\n    // Key metrics table for bull/bear decisions\n    const metrics=[\n      ['Fear & Greed Index', fg_val!=null?fg_val+'/100':'—', fg_val!=null?(fg_val<25?'Comprar (pânico)':fg_val<45?'Oportunidade':fg_val>75?'Vender (euforia)':fg_val>55?'Cuidado':'Neutro'):'—', fg_val!=null?(fg_val<40?'var(--gr)':fg_val>60?'var(--rd)':'var(--yl)'):'var(--t2)'],\n      ['BTC Dominância', btc_dom!=null?btc_dom+'%':'—', btc_dom!=null?(btc_dom>60?'BTC season (alts fracas)':btc_dom<40?'Alt season activa':'Transição'):'—', 'var(--t2)'],\n      ['Momentum 7D', price_7d!=null?(price_7d>0?'+':'')+price_7d+'%':'—', price_7d!=null?(price_7d>15?'Momentum forte':price_7d>5?'Positivo':price_7d<-15?'Queda forte':'Negativo'):'—', price_7d!=null?(price_7d>0?'var(--gr)':'var(--rd)'):'var(--t2)'],\n      ['Momentum 30D', price_30d!=null?(price_30d>0?'+':'')+price_30d+'%':'—', price_30d!=null?(price_30d>30?'Bull trend':price_30d>10?'Alta moderada':price_30d<-30?'Bear trend':'Queda moderada'):'—', price_30d!=null?(price_30d>0?'var(--gr)':'var(--rd)'):'var(--t2)'],\n      ['Performance 1 Ano', price_1y!=null?(price_1y>0?'+':'')+price_1y+'%':'—', price_1y!=null?(price_1y>100?'Ciclo bull activo':price_1y>0?'Positivo anual':price_1y<-50?'Bear severo':'Abaixo de 0'):'—', price_1y!=null?(price_1y>0?'var(--gr)':'var(--rd)'):'var(--t2)'],\n      ['vs ATH', ath_pct!=null?ath_pct.toFixed(1)+'%':'—', ath_pct!=null?(ath_pct>-20?'Próximo do ATH':ath_pct>-50?'Meio ciclo':ath_pct>-80?'Território de acumulação':'Capitulação'):'—', ath_pct!=null?(ath_pct>-30?'var(--yl)':'var(--gr)'):'var(--t2)'],\n    ];\n    html+=`<div style=\"background:var(--bg2);border:1px solid var(--b);border-radius:6px;margin-bottom:14px;overflow:hidden\">\n      <div style=\"padding:9px 14px;background:var(--bg3);border-bottom:1px solid var(--b);font-family:var(--fd);font-size:10px;font-weight:700;letter-spacing:.09em;text-transform:uppercase;color:var(--t2)\">Metricas Bull/Bear</div>\n      <table style=\"width:100%;border-collapse:collapse;font-size:11px\">\n        <thead><tr>\n          <th style=\"text-align:left;padding:6px 12px;color:var(--t3);font-size:9px;letter-spacing:.07em;text-transform:uppercase;border-bottom:1px solid var(--b)\">Indicador</th>\n          <th style=\"text-align:center;padding:6px 12px;color:var(--t3);font-size:9px;letter-spacing:.07em;text-transform:uppercase;border-bottom:1px solid var(--b)\">Valor</th>\n          <th style=\"text-align:left;padding:6px 12px;color:var(--t3);font-size:9px;letter-spacing:.07em;text-transform:uppercase;border-bottom:1px solid var(--b)\">Interpretacao</th>\n        </tr></thead>\n        <tbody>${metrics.map(([lbl,val,interp,col])=>`<tr style=\"border-bottom:1px solid rgba(255,255,255,.03)\">\n          <td style=\"padding:7px 12px;color:var(--t2)\">${lbl}</td>\n          <td style=\"padding:7px 12px;text-align:center;font-weight:700;color:${col}\">${val}</td>\n          <td style=\"padding:7px 12px;color:var(--t)\">${interp}</td>\n        </tr>`).join('')}</tbody>\n      </table>\n    </div>`;\n  }\n\n  // Market overview\n  const mkt=ci?.market;\n  if(mkt){\n    html+=`<div class=\"section\"><div class=\"shdr\">Mercado Crypto Global</div>\n    <div class=\"sgrid\">\n      <div class=\"srow\"><span class=\"slbl\">BTC Dominância</span><span class=\"sval ${mkt.btc_dominance>50?'up':'dn'}\">${mkt.btc_dominance}%</span></div>\n      <div class=\"srow\"><span class=\"slbl\">ETH Dominância</span><span class=\"sval\">${mkt.eth_dominance}%</span></div>\n      <div class=\"srow\"><span class=\"slbl\">Market Cap Total</span><span class=\"sval\">${fmtB(mkt.total_market_cap_usd)}</span></div>\n      <div class=\"srow\"><span class=\"slbl\">Variação 24h</span><span class=\"sval ${mkt.market_cap_change_24h>0?'up':'dn'}\">${mkt.market_cap_change_24h>0?'+':''}${mkt.market_cap_change_24h}%</span></div>\n      <div class=\"srow\"><span class=\"slbl\">Cryptos Activas</span><span class=\"sval\">${mkt.active_cryptos?.toLocaleString()||'—'}</span></div>\n    </div></div>`;\n  }\n\n  // BTC Halving\n  const hv=ci?.halving;\n  if(hv){\n    const pct=Math.round((hv.current_block-(hv.next_halving_block-210000))/210000*100);\n    html+=`<div class=\"section\"><div class=\"shdr\">Bitcoin Halving</div>\n    <div style=\"padding:12px 14px\">\n      <div style=\"display:flex;justify-content:space-between;font-size:11px;color:var(--t2);margin-bottom:4px\">\n        <span>Bloco ${hv.next_halving_block-210000}</span><span>Bloco actual: ${hv.current_block?.toLocaleString()}</span><span>Bloco ${hv.next_halving_block}</span>\n      </div>\n      <div class=\"halving-bar\"><div class=\"halving-fill\" style=\"width:${Math.min(pct,100)}%\"></div></div>\n      <div style=\"display:flex;justify-content:space-between;flex-wrap:wrap;gap:10px;margin-top:10px\">\n        <div><div class=\"slbl\">Blocos restantes</div><div style=\"font-family:var(--fd);font-size:18px;font-weight:800;color:var(--yl)\">${hv.blocks_remaining?.toLocaleString()||'—'}</div></div>\n        <div><div class=\"slbl\">Data estimada</div><div style=\"font-family:var(--fd);font-size:16px;font-weight:800;color:var(--gr)\">${hv.next_date_est||'—'}</div></div>\n        <div><div class=\"slbl\">Dias estimados</div><div style=\"font-family:var(--fd);font-size:18px;font-weight:800;color:var(--bl)\">${hv.days_estimate||'—'}</div></div>\n        <div><div class=\"slbl\">Recompensa após</div><div style=\"font-size:12px;font-weight:700;color:var(--t)\">${hv.reward_after}</div></div>\n      </div>\n      <div style=\"font-size:10px;color:var(--t3);margin-top:8px\">${hv.note}</div>\n    </div></div>\n    <div class=\"hint\">⚡ <b>Halving</b> — A cada ~210.000 blocos, a recompensa dos mineiros é cortada a metade. Historicamente os maiores bull runs ocorreram 6-18 meses após o halving.<br>\n    <span style=\"color:var(--t3);font-size:10px\">Últimos halvings: Nov 2012 → $12, Jul 2016 → $650, Mai 2020 → $8.700, Abr 2024 → $63.000</span></div>`;\n  }\n\n  // Coin-specific data\n  const coin=ci?.coin;\n  if(coin){\n    html+=`<div class=\"section\"><div class=\"shdr\">${coin.name||t} — Dados de Mercado</div>\n    <div class=\"sgrid\">\n      <div class=\"srow\"><span class=\"slbl\">Rank</span><span class=\"sval\">#${coin.market_cap_rank||'—'}</span></div>\n      <div class=\"srow\"><span class=\"slbl\">Market Cap</span><span class=\"sval\">${fmtB(coin.market_cap)}</span></div>\n      <div class=\"srow\"><span class=\"slbl\">Volume 24h</span><span class=\"sval\">${fmtB(coin.volume_24h)}</span></div>\n      <div class=\"srow\"><span class=\"slbl\">ATH</span><span class=\"sval\">${coin.ath?'$'+coin.ath.toLocaleString('en-US',{maximumFractionDigits:2}):'—'}</span></div>\n      <div class=\"srow\"><span class=\"slbl\">vs ATH</span><span class=\"sval ${coin.ath_change_pct>-20?'yl':'dn'}\">${coin.ath_change_pct!=null?coin.ath_change_pct.toFixed(1)+'%':'—'}</span></div>\n      <div class=\"srow\"><span class=\"slbl\">ATH Data</span><span class=\"sval\" style=\"font-size:10px\">${coin.ath_date||'—'}</span></div>\n      <div class=\"srow\"><span class=\"slbl\">Var. 7D</span><span class=\"sval ${coin.price_change_7d>0?'up':'dn'}\">${coin.price_change_7d!=null?(coin.price_change_7d>=0?'+':'')+coin.price_change_7d.toFixed(1)+'%':'—'}</span></div>\n      <div class=\"srow\"><span class=\"slbl\">Var. 30D</span><span class=\"sval ${coin.price_change_30d>0?'up':'dn'}\">${coin.price_change_30d!=null?(coin.price_change_30d>=0?'+':'')+coin.price_change_30d.toFixed(1)+'%':'—'}</span></div>\n      <div class=\"srow\"><span class=\"slbl\">Var. 1 Ano</span><span class=\"sval ${coin.price_change_1y>0?'up':'dn'}\">${coin.price_change_1y!=null?(coin.price_change_1y>=0?'+':'')+coin.price_change_1y.toFixed(1)+'%':'—'}</span></div>\n      <div class=\"srow\"><span class=\"slbl\">Supply Circulante</span><span class=\"sval\">${coin.circulating_supply?coin.circulating_supply.toLocaleString('en-US',{maximumFractionDigits:0}):'—'}</span></div>\n      <div class=\"srow\"><span class=\"slbl\">Supply Máximo</span><span class=\"sval\">${coin.max_supply?coin.max_supply.toLocaleString('en-US',{maximumFractionDigits:0}):'∞'}</span></div>\n      <div class=\"srow\"><span class=\"slbl\">% Emitido</span><span class=\"sval\">${coin.supply_pct!=null?coin.supply_pct+'%':'—'}</span></div>\n    </div>\n    ${coin.description?`<div style=\"padding:10px 14px;font-size:11px;color:var(--t2);line-height:1.6;border-top:1px solid var(--b)\">${coin.description}</div>`:''}\n    </div>`;\n  }\n\n  if(!ci&&!px){html='<div class=\"empty\">Sem dados disponíveis para '+t+'</div>';}\n  el.innerHTML=html;\n}\n\nfunction onTickerChange(t){loadCrypto(t);}\nloadCrypto(window.TK);\n\ninitTkBar(window.TK);fixNavLinks();\n</script>\n</body>\n</html>",
    "fairvalue.html": "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"UTF-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\n<title>Fair Value · IST</title>\n<script src=\"https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.7.5/socket.io.min.js\"></script>\n<link href=\"https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600&family=Syne:wght@400;600;700;800&family=Inter:wght@300;400;600;700;800&display=swap\" rel=\"stylesheet\">\n<style>\n*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}\n:root{\n  --bg:#080b0f;--bg2:#0d1117;--bg3:#111720;--bg4:#161d29;\n  --b:#1e2d3d;--b2:#243040;\n  --t:#c9d1d9;--t2:#8b949e;--t3:#484f58;\n  --gr:#00e5a0;--rd:#ff4d6d;--bl:#0095ff;\n  --yl:#f0c060;--pu:#a78bfa;--or:#ff8c42;\n  --fm:'JetBrains Mono',monospace;--fd:'Syne',sans-serif;\n}\nhtml,body{height:100%;background:var(--bg);color:var(--t);font-family:var(--fm);font-size:13px}\n::-webkit-scrollbar{width:5px;height:5px}::-webkit-scrollbar-track{background:transparent}::-webkit-scrollbar-thumb{background:var(--b2);border-radius:3px}\n.up{color:var(--gr)}.dn{color:var(--rd)}.nc{color:var(--t2)}\n/* TAPE */\n#tape{height:26px;background:var(--bg2);border-bottom:1px solid var(--b);overflow:hidden;display:flex;align-items:center;flex-shrink:0}\n#tape-inner{display:flex;white-space:nowrap;animation:tape 90s linear infinite;user-select:none;-webkit-user-select:none;cursor:default;}\n/* tape runs continuously */\n@keyframes tape{from{transform:translateX(0)}to{transform:translateX(-50%)}}\n.ti{display:inline-flex;align-items:center;gap:5px;padding:0 16px;height:26px;border-right:1px solid var(--b);font-size:11px;user-select:none;-webkit-user-select:none;}\n.ts{color:var(--t2);font-weight:600}.tc{font-size:10px}\n/* NAV */\n#nav{height:40px;background:var(--bg2);border-bottom:1px solid var(--b);display:flex;align-items:center;padding:0 10px;gap:2px;flex-shrink:0}\n.nl{display:flex;align-items:center;gap:5px;padding:5px 10px;border-radius:4px;font-size:11px;font-family:var(--fd);font-weight:700;letter-spacing:.04em;text-transform:uppercase;color:var(--t2);text-decoration:none;transition:all .15s;white-space:nowrap}\n.nl:hover{color:var(--t);background:var(--bg3)}\n.nl.on{color:var(--gr);background:rgba(0,229,160,.07);border:1px solid rgba(0,229,160,.12)}\n.nl svg{flex-shrink:0;opacity:.7}.nl.on svg{opacity:1}\n#nav-logo{font-family:var(--fd);font-size:14px;font-weight:800;color:var(--gr);letter-spacing:.04em;margin-right:12px;text-decoration:none}\n/* TICKER BAR */\n#tkbar{height:38px;background:var(--bg2);border-bottom:1px solid var(--b);display:flex;align-items:center;padding:0 14px;gap:10px;flex-shrink:0}\n#tk-sym{font-family:var(--fd);font-size:15px;font-weight:800;color:var(--gr);flex-shrink:0}\n#tk-name{font-size:11px;color:var(--t2);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}\n#tk-px{font-family:var(--fd);font-size:15px;font-weight:700;flex-shrink:0}\n#tk-chg{font-size:11px;padding:2px 8px;border-radius:3px;font-weight:600;flex-shrink:0}\n#tk-sw{position:relative}\n#tk-si{background:var(--bg3);border:1px solid var(--b2);color:var(--t);font-family:var(--fm);font-size:11px;padding:4px 9px;border-radius:3px;outline:none;width:160px}\n#tk-si:focus{border-color:var(--bl)}\n#tk-dr{position:absolute;top:calc(100% + 2px);right:0;background:var(--bg3);border:1px solid var(--b2);border-radius:3px;z-index:999;display:none;min-width:200px;max-height:200px;overflow-y:auto}\n.tdr{padding:6px 10px;cursor:pointer;display:flex;justify-content:space-between;gap:8px;font-size:11px}.tdr:hover{background:var(--bg4)}\n.tds{color:var(--gr);font-weight:700}.tdn{color:var(--t2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:120px}\n/* PAGE BODY */\n.pb{display:flex;flex-direction:column;height:100vh;overflow:hidden}\n.pc{flex:1;overflow-y:auto;overflow-x:hidden}\n/* SKELETON */\n.sk{background:linear-gradient(90deg,var(--bg3) 25%,var(--bg4) 50%,var(--bg3) 75%);background-size:200% 100%;animation:sk 1.2s ease infinite;border-radius:3px}\n@keyframes sk{0%{background-position:200% 0}100%{background-position:-200% 0}}\n.sk-line{height:14px;margin-bottom:8px}.sk-val{height:24px;margin-bottom:4px}\n/* SPIN */\n.spin-svg{display:inline-block;vertical-align:middle;margin-right:8px;width:44px;height:20px}\n/* EMPTY */\n.empty{text-align:center;padding:40px 20px;color:var(--t3);font-size:12px}\n\n.pc{padding:16px 20px;max-width:800px}\n.fv-hero{background:var(--bg2);border:1px solid var(--bl);border-radius:8px;padding:22px;margin-bottom:16px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:14px}\n.fv-v{font-family:'Inter',system-ui,-apple-system,sans-serif;font-size:44px;font-weight:800;color:var(--bl);letter-spacing:-.03em}\n.mos{padding:10px 22px;border-radius:8px;font-family:'Inter',system-ui,-apple-system,sans-serif;font-size:20px;font-weight:700;letter-spacing:-.01em}\n.models{display:grid;grid-template-columns:1fr 1fr;gap:10px}\n.mc{background:var(--bg2);border:1px solid var(--b);border-radius:6px;padding:16px}\n.mc-n{font-family:'Inter',system-ui,-apple-system,sans-serif;font-size:13px;font-weight:700;color:var(--t);margin-bottom:2px;letter-spacing:-.01em}\n.mc-l{font-size:10px;color:var(--t3);margin-bottom:10px}\n.mc-v{font-family:'Inter',system-ui,-apple-system,sans-serif;font-size:30px;font-weight:800;color:var(--bl);margin-bottom:6px;letter-spacing:-.02em}\n.mc-bar{height:5px;background:var(--bg4);border-radius:2px;overflow:hidden;margin-bottom:5px}\n.mc-fill{height:100%;border-radius:2px;background:var(--bl)}\n.mc-d{font-size:11px;font-weight:600}\n</style>\n</head>\n<body>\n<div class=\"pb\">\n<div id=\"tape\"><div id=\"tape-inner\"></div></div>\n<div id=\"nav\"><a href=\"/\" id=\"nav-logo\" style=\"display:flex;align-items:center;gap:7px;text-decoration:none\">\n      <svg width=\"22\" height=\"22\" viewBox=\"0 0 64 64\" fill=\"none\">\n        <rect width=\"64\" height=\"64\" rx=\"10\" fill=\"rgba(0,229,160,0.15)\" stroke=\"rgba(0,229,160,0.4)\" stroke-width=\"2\"/>\n        <polyline points=\"8,44 20,28 30,36 42,18 56,22\" stroke=\"#00e5a0\" stroke-width=\"5\" fill=\"none\" stroke-linecap=\"round\" stroke-linejoin=\"round\"/>\n        <circle cx=\"42\" cy=\"18\" r=\"5\" fill=\"#00e5a0\"/>\n      </svg>\n      <span style=\"font-family:var(--fd);font-size:14px;font-weight:800;color:var(--gr);letter-spacing:.04em\">IST</span>\n    </a>\n    <a href=\"/chart\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"currentColor\"><rect x=\"1\" y=\"7\" width=\"2\" height=\"5\"/><rect x=\"5\" y=\"4\" width=\"2\" height=\"8\"/><rect x=\"9\" y=\"1\" width=\"2\" height=\"11\"/></svg>Chart</a><a href=\"/financials\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><polyline points=\"1,9 4,5 7,7 12,2\"/></svg>Financials</a><a href=\"/metrics\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"currentColor\"><circle cx=\"3\" cy=\"3\" r=\"1.5\"/><circle cx=\"10\" cy=\"3\" r=\"1.5\"/><circle cx=\"3\" cy=\"10\" r=\"1.5\"/><circle cx=\"10\" cy=\"10\" r=\"1.5\"/></svg>Metrics</a><a href=\"/insider\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><circle cx=\"6.5\" cy=\"4\" r=\"2.5\"/><path d=\"M1 12c0-3 2.5-5 5.5-5s5.5 2 5.5 5\"/></svg>Insider</a><a href=\"/congress\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><rect x=\"1\" y=\"5\" width=\"11\" height=\"7\" rx=\"1\"/><path d=\"M4 5V3a2.5 2.5 0 015 0v2\"/></svg>Congress</a><a href=\"/fairvalue\" class=\"nl on\"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><path d=\"M6.5 1L8 5h4l-3 2.5 1 4L6.5 9 3 11.5l1-4L1 5h4z\"/></svg>Fair Value</a><a href=\"/news\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><rect x=\"1\" y=\"1\" width=\"11\" height=\"11\" rx=\"1\"/><line x1=\"3\" y1=\"4\" x2=\"10\" y2=\"4\"/><line x1=\"3\" y1=\"7\" x2=\"10\" y2=\"7\"/></svg>News</a><a href=\"/livefeed\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"currentColor\"><circle cx=\"6.5\" cy=\"6.5\" r=\"2\"/><circle cx=\"6.5\" cy=\"6.5\" r=\"4\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1\" opacity=\".5\"/></svg>Live Feed</a></div>\n<script id=\"asset-nav-fix\">\ndocument.addEventListener('DOMContentLoaded', function(){\n  const t=(new URLSearchParams(window.location.search).get('t')||'').toUpperCase();\n  const isCrypto=t.endsWith('-USD')&&!['GLD','SLV','USO','TLT','HYG','LQD','GDX'].includes(t);\n  const isFuture=t.endsWith('=F');\n  const isIndex=t.startsWith('^')||t==='DX-Y.NYB';\n  if(!isCrypto&&!isFuture&&!isIndex) return;\n  const STOCK_ONLY=['/financials','/insider','/congress','/fairvalue'];\n  document.querySelectorAll('#nav a.nl').forEach(a=>{\n    const href=a.getAttribute('href')||'';\n    if(STOCK_ONLY.some(p=>href.includes(p))) a.style.display='none';\n  });\n});\n</script>\n<div id=\"tkbar\">\n  <span id=\"tk-sym\">—</span><span id=\"tk-name\">—</span>\n  <span id=\"tk-px\" style=\"color:var(--t2)\">—</span><span id=\"tk-chg\"></span>\n  <div id=\"tk-sw\"><input id=\"tk-si\" placeholder=\"Change ticker…\" autocomplete=\"off\" spellcheck=\"false\"><div id=\"tk-dr\"></div></div>\n</div>\n<div class=\"pc\"><div class=\"empty\"><svg class=\"spin-svg\" viewBox=\"0 0 44 20\" fill=\"none\"><polyline points=\"2,16 8,10 14,13 22,5 30,9 42,3\" stroke=\"#00e5a0\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-dasharray=\"120\" stroke-dashoffset=\"120\"><animate attributeName=\"stroke-dashoffset\" from=\"120\" to=\"0\" dur=\"1.2s\" repeatCount=\"indefinite\"/></polyline></svg>A calcular fair value…</div></div>\n</div>\n<script>\nconst TSYMS=['SPY','QQQ','^GSPC','^IXIC','^VIX','CL=F','BZ=F','GC=F','SI=F','DX-Y.NYB','^TNX','BTC-USD'];\nconst TLBL={SPY:'SPY',QQQ:'QQQ','^GSPC':'S&P500','^IXIC':'NASDAQ','^VIX':'VIX','CL=F':'WTI','BZ=F':'BRENT','GC=F':'GOLD','SI=F':'SILVER','DX-Y.NYB':'DXY','^TNX':'US10Y','BTC-USD':'BTC'};\nconst socket=io();\nfunction buildTape(){\n  document.getElementById('tape-inner').innerHTML=[...TSYMS,...TSYMS].map(t=>{\n    const id='ti_'+t.replace(/[^a-zA-Z0-9]/g,'_');\n    return`<div class=\"ti\" id=\"${id}\" onclick=\"navToTape('${t}')\" style=\"cursor:pointer;\"><span class=\"ts\">${TLBL[t]||t}</span>&nbsp;<span>—</span>&nbsp;<span class=\"tc nc\">—</span></div>`;\n  }).join('');\n  socket.emit('subscribe',{tickers:TSYMS});\n}\nsocket.on('price_update',({prices})=>{\n  prices.forEach(p=>{\n    const id='ti_'+p.ticker.replace(/[^a-zA-Z0-9]/g,'_');\n    document.querySelectorAll('#'+id).forEach(el=>{\n      const cs=el.children;\n      if(cs[1]&&p.price!=null)cs[1].textContent='$'+p.price.toFixed(2);\n      if(cs[2]&&p.change_pct!=null){const s=p.change_pct>=0?'+':'';cs[2].textContent=s+p.change_pct.toFixed(2)+'%';cs[2].className='tc '+(p.change_pct>0?'up':p.change_pct<0?'dn':'nc');}\n    });\n    if(p.ticker===window.TK)updateTkBar(p);\n    if(typeof onPriceUpdate==='function')onPriceUpdate(p);\n  });\n});\nbuildTape();\nconst MEM={};const CACHE_TTL=300000;\nasync function api(url,ttl=CACHE_TTL,timeout=12000){\n  if(MEM[url]&&Date.now()-MEM[url].ts<ttl)return MEM[url].data;\n  try{\n    const ctrl=new AbortController();\n    const tid=setTimeout(()=>ctrl.abort(),timeout);\n    const r=await fetch(url,{signal:ctrl.signal});\n    clearTimeout(tid);\n    if(!r.ok)return null;\n    const d=await r.json();\n    MEM[url]={data:d,ts:Date.now()};\n    return d;\n  }catch{return null;}\n}\nfunction ssGet(k){try{const v=sessionStorage.getItem(k);if(!v)return null;const p=JSON.parse(v);if(Date.now()-p.ts>CACHE_TTL){sessionStorage.removeItem(k);return null;}return p.data;}catch{return null;}}\nfunction ssSet(k,d){try{sessionStorage.setItem(k,JSON.stringify({data:d,ts:Date.now()}));}catch{}}\nasync function getStockData(t){const k='full:'+t;const ss=ssGet(k);if(ss)return ss;const mc=MEM['/api/full/'+t];if(mc&&Date.now()-mc.ts<CACHE_TTL)return mc.data;const d=await api('/api/full/'+t,CACHE_TTL);if(d)ssSet(k,d);return d;}\nfunction updateTkBar(p){const pxEl=document.getElementById('tk-px'),chEl=document.getElementById('tk-chg');if(!pxEl)return;if(p.price!=null){pxEl.textContent='$'+p.price.toFixed(2);pxEl.style.color=p.change_pct>0?'var(--gr)':p.change_pct<0?'var(--rd)':'var(--t)';}if(p.change_pct!=null){const s=p.change_pct>=0?'+':'';chEl.textContent=s+p.change_pct.toFixed(2)+'%';chEl.style.cssText=`background:${p.change_pct>=0?'rgba(0,229,160,.12)':'rgba(255,77,109,.12)'};color:${p.change_pct>=0?'var(--gr)':'var(--rd)'};padding:2px 8px;border-radius:3px;`;}}\nasync function initTkBar(t){document.getElementById('tk-sym').textContent=t;const d=await api('/api/stock_fast/'+encodeURIComponent(t),6000);if(d){document.getElementById('tk-name').textContent=d.name||t;updateTkBar(d);}socket.emit('subscribe',{tickers:[t]});}\nlet sT;const siEl=document.getElementById('tk-si'),drEl=document.getElementById('tk-dr');\nif(siEl){siEl.addEventListener('input',function(){clearTimeout(sT);const v=this.value.trim();if(!v){drEl.style.display='none';return;}sT=setTimeout(async()=>{const d=await api('/api/universe?q='+encodeURIComponent(v)+'&limit=10',60000);if(!d?.results?.length){drEl.style.display='none';return;}drEl.innerHTML=d.results.map(x=>`<div class=\"tdr\" onclick=\"navTo('${x.ticker}')\"><span class=\"tds\">${x.ticker}</span><span class=\"tdn\">${x.name||''}</span></div>`).join('');drEl.style.display='block';},200);});document.addEventListener('click',e=>{if(!e.target.closest('#tk-sw'))drEl.style.display='none';});}\nfunction navTo(t){if(!t)return;if(siEl){siEl.value='';drEl.style.display='none';}window.location.href='/chart?t='+encodeURIComponent(t);}\n// navToTape: always go to chart page with the ticker\nfunction navToTape(t){\n  if(!t)return;\n  // Route to appropriate page based on asset type\n  window.location.href='/chart?t='+encodeURIComponent(t);\n}\nwindow.TK=(new URLSearchParams(window.location.search).get('t')||'NVDA').toUpperCase();\nconst fmtB=v=>{if(v==null)return'—';const a=Math.abs(v),s=v<0?'-':'';if(a>=1e12)return s+'$'+(a/1e12).toFixed(2)+'T';if(a>=1e9)return s+'$'+(a/1e9).toFixed(2)+'B';if(a>=1e6)return s+'$'+(a/1e6).toFixed(1)+'M';return s+'$'+a.toLocaleString('en-US',{maximumFractionDigits:0});};\nconst fmtPx=v=>v==null?'—':'$'+Number(v).toFixed(2);\nconst p1=v=>v==null?'—':(v*100).toFixed(1)+'%';\nconst fn=(v,d=2)=>v==null?'—':Number(v).toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d});\n\nasync function loadFV(t){\n  const _isCr=t.endsWith('-USD')&&!['GLD','SLV','USO','TLT'].includes(t);\n  const _isFu=t.endsWith('=F')||t.startsWith('^')||t==='DX-Y.NYB';\n  if(_isCr){window.location.href='/crypto?t='+encodeURIComponent(t);return;}\n  if(_isFu){window.location.href='/commodity?t='+encodeURIComponent(t);return;}\n  // Redirect non-stocks to their page immediately\n  const el=document.querySelector('.pc');\n  el.innerHTML='<div class=\"empty\"><svg class=\"spin-svg\" viewBox=\"0 0 44 20\" fill=\"none\"><polyline points=\"2,16 8,10 14,13 22,5 30,9 42,3\" stroke=\"#00e5a0\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-dasharray=\"120\" stroke-dashoffset=\"120\"><animate attributeName=\"stroke-dashoffset\" from=\"120\" to=\"0\" dur=\"1.2s\" repeatCount=\"indefinite\"/></polyline></svg>A calcular…</div>';\n  const d=await getStockData(t);\n  if(!d){el.innerHTML='<div class=\"empty\">Sem dados</div>';return;}\n  const fv=d.fair_value,models=d.fair_value_models||{},price=d.price;\n  let html='';\n  if(fv&&price){\n    const mos=Math.round((fv-price)/price*100);\n    const rat=mos>25?'Muito Barata':mos>10?'Barata':mos>-5?'Justa':mos>-20?'Cara':'Muito Cara';\n    const mc=mos>10?'rgba(0,229,160,.15)':'rgba(255,77,109,.15)';\n    const tc=mos>10?'var(--gr)':mos<-10?'var(--rd)':'var(--yl)';\n    html+=`<div class=\"fv-hero\">\n      <div>\n        <div style=\"font-size:10px;color:var(--t2);margin-bottom:6px;font-family:var(--fd);font-weight:700;letter-spacing:.08em;text-transform:uppercase\">FAIR VALUE COMPOSTO · ${t}</div>\n        <div class=\"fv-v\">$${fv.toFixed(2)}</div>\n        <div style=\"font-size:11px;color:var(--t2);margin-top:4px\">Preço actual: $${price.toFixed(2)} · Classificação: <b>${rat}</b></div>\n      </div>\n      <div style=\"text-align:right\">\n        <div class=\"mos\" style=\"background:${mc};color:${tc}\">${(mos>=0?'+':'')+mos}% MoS</div>\n        <div style=\"font-size:10px;color:var(--t2);margin-top:6px\">Margem de Segurança</div>\n      </div>\n    </div>`;\n  }\n  const maxV=Math.max(...Object.values(models).map(m=>m.value||0),price||0,1);\n  html+='<div class=\"models\">';\n  for(const[name,m] of Object.entries(models)){\n    if(!m.value)continue;\n    const pct=Math.round(m.value/maxV*100);\n    const diff=price?Math.round((m.value-price)/price*100):null;\n    html+=`<div class=\"mc\">\n      <div class=\"mc-n\">${name}</div><div class=\"mc-l\">${m.label||''}</div>\n      <div class=\"mc-v\">$${m.value.toFixed(2)}</div>\n      <div class=\"mc-bar\"><div class=\"mc-fill\" style=\"width:${pct}%\"></div></div>\n      ${diff!=null?`<div class=\"mc-d\" style=\"color:${diff>=0?'var(--gr)':'var(--rd)'}\">${diff>=0?'+':''}${diff}% vs preço actual</div>`:''}\n      ${m.low&&m.high?`<div style=\"font-size:10px;color:var(--t3);margin-top:3px\">Range: $${m.low.toFixed(2)} – $${m.high.toFixed(2)}</div>`:''}\n    </div>`;\n  }\n  html+='</div>';\n  if(!Object.keys(models).length)html='<div class=\"empty\">Dados insuficientes para calcular modelos</div>';\n  el.innerHTML=html;\n}\nfunction onTickerChange(t){loadFV(t);}\nloadFV(window.TK);\n\ninitTkBar(window.TK);\n\n// Fix nav links to carry current ticker\nfunction fixNavLinks(){\n  const t=window.TK;\n  if(!t)return;\n  document.querySelectorAll('#nav a.nl').forEach(a=>{\n    const href=a.getAttribute('href');\n    if(href&&href!=='/'&&!href.startsWith('javascript')){\n      const u=new URL(href,window.location.origin);\n      u.searchParams.set('t',t);\n      a.setAttribute('href',u.toString());\n    }\n  });\n}\nfixNavLinks();\n// Also update nav links whenever ticker changes\n// navTo already calls fixNavLinks()\n</script>\n</body>\n</html>",
    "financials.html": "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"UTF-8\">\n<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\n<title>Financials · IST</title>\n<script src=\"https://cdn.plot.ly/plotly-2.27.0.min.js\"></script>\n<script src=\"https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.7.5/socket.io.min.js\"></script>\n<link href=\"https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;700&family=Cabinet+Grotesk:wght@400;600;700;800&display=swap\" rel=\"stylesheet\">\n<style>\n*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}\n:root{\n  --bg:#040608;--bg2:#0a0f14;--bg3:#0e141b;\n  --b:#162030;--b2:#1c2c3e;\n  --t:#d4dde6;--t2:#7a8fa0;--t3:#364860;\n  --gr:#00e5a0;--rd:#ff3d5a;--bl:#0088ee;\n  --fd:'Cabinet Grotesk',sans-serif;--fm:'JetBrains Mono',monospace;\n}\nhtml,body{height:100%;overflow:hidden}\nbody{background:var(--bg);color:var(--t);font-family:var(--fd);font-size:13px;display:flex;flex-direction:column}\n\n/* ── TAPE ── */\n#tape{height:26px;background:rgba(7,11,15,.98);border-bottom:1px solid var(--b);overflow:hidden;flex-shrink:0;display:flex;align-items:center}\n#tape-inner{display:flex;animation:scroll 60s linear infinite;white-space:nowrap}\n@keyframes scroll{0%{transform:translateX(0)}100%{transform:translateX(-50%)}}\n.ti{display:inline-flex;align-items:center;gap:5px;padding:0 16px;height:26px;border-right:1px solid var(--b);font-size:10px;font-family:var(--fm);cursor:pointer;flex-shrink:0}\n.ti:hover{background:var(--bg2)}.ts{color:var(--t3)}.tv{color:var(--t)}.tc{font-size:9px}\n.up{color:var(--gr)}.dn{color:var(--rd)}.nc{color:var(--t2)}\n\n/* ── NAV ── */\n#nav{height:46px;background:var(--bg2);border-bottom:1px solid var(--b);display:flex;align-items:center;padding:0 16px;gap:4px;flex-shrink:0;overflow-x:auto}\n.nav-logo{display:flex;align-items:center;gap:7px;text-decoration:none;margin-right:12px;flex-shrink:0}\n.logo-box{width:22px;height:22px;background:var(--gr);border-radius:4px;display:flex;align-items:center;justify-content:center}\n.logo-box svg{width:12px;height:12px}\n.logo-txt{font-size:17px;font-weight:800;color:var(--t);letter-spacing:.05em;font-family:var(--fd)}\n.nl{display:inline-flex;align-items:center;gap:5px;padding:5px 10px;border-radius:4px;font-size:11px;font-weight:600;color:var(--t2);text-decoration:none;white-space:nowrap;transition:all .15s;flex-shrink:0}\n.nl:hover{color:var(--t);background:rgba(255,255,255,.06)}\n.nl.on{color:var(--gr);background:rgba(0,229,160,.08)}\n.nl-icon{font-size:12px;opacity:.7}\n\n/* ── TKBAR ── */\n#tkbar{height:36px;background:var(--bg3);border-bottom:1px solid var(--b);display:flex;align-items:center;padding:0 14px;gap:10px;flex-shrink:0}\n#tk-sym{font-size:15px;font-weight:800;color:var(--gr);font-family:var(--fd);flex-shrink:0}\n#tk-name{font-size:11px;color:var(--t2);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}\n#tk-px{font-size:14px;font-weight:600;color:var(--t);font-family:var(--fm)}\n#tk-ch{font-size:11px;font-weight:600;font-family:var(--fm)}\n#tk-chg-btn{margin-left:auto;padding:4px 10px;border-radius:4px;font-size:10px;font-weight:700;color:var(--t2);background:var(--bg2);border:1px solid var(--b);cursor:pointer;font-family:var(--fd);transition:all .15s;flex-shrink:0}\n#tk-chg-btn:hover{color:var(--t);border-color:var(--t3)}\n#tk-search-wrap{position:relative;display:none;flex-shrink:0}\n#tk-si{width:180px;background:var(--bg2);border:1px solid var(--gr);border-radius:4px;color:var(--t);font-family:var(--fd);font-size:12px;padding:4px 8px;outline:none}\n#tk-dr{position:absolute;top:calc(100% + 4px);left:0;width:260px;background:var(--bg2);border:1px solid var(--b);border-radius:6px;z-index:999;max-height:240px;overflow-y:auto;display:none;box-shadow:0 8px 24px rgba(0,0,0,.5)}\n.dr-item{padding:8px 12px;cursor:pointer;display:flex;align-items:center;gap:8px;border-bottom:1px solid rgba(255,255,255,.04)}\n.dr-item:hover{background:var(--bg3)}.dr-sym{font-weight:700;color:var(--t);font-size:12px;min-width:50px}.dr-nm{font-size:11px;color:var(--t2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}\n\n/* ── TABS ── */\n#tabs{height:34px;background:var(--bg2);border-bottom:1px solid var(--b);display:flex;align-items:stretch;padding:0 14px;gap:0;flex-shrink:0}\n.tab{display:inline-flex;align-items:center;padding:0 14px;font-size:11px;font-weight:600;color:var(--t3);cursor:pointer;border-bottom:2px solid transparent;transition:all .15s;white-space:nowrap;font-family:var(--fd);letter-spacing:.02em}\n.tab:hover{color:var(--t2)}\n.tab.on{color:var(--gr);border-bottom-color:var(--gr)}\n\n/* ── MAIN ── */\n#main{flex:1;overflow-y:auto;padding:14px}\n\n/* ── LOADING / ERROR ── */\n.centered{display:flex;flex-direction:column;align-items:center;justify-content:center;height:60vh;gap:16px;text-align:center}\n.spin{display:block;width:40px;height:40px;border:2px solid var(--b2);border-top-color:var(--gr);border-radius:50%;animation:spin .8s linear infinite}\n@keyframes spin{to{transform:rotate(360deg)}}\n.load-msg{font-size:13px;color:var(--t2);font-family:var(--fm)}\n.err-box{background:rgba(255,61,90,.08);border:1px solid rgba(255,61,90,.2);border-radius:8px;padding:20px 28px;max-width:480px}\n.err-title{font-size:15px;font-weight:700;color:var(--rd);margin-bottom:8px}\n.err-sub{font-size:12px;color:var(--t2);line-height:1.6;margin-bottom:14px}\n.btn-retry{padding:7px 18px;border-radius:4px;font-size:12px;font-weight:700;background:var(--bl);color:#fff;border:none;cursor:pointer;font-family:var(--fd)}\n\n/* ── CARDS (original) ── */\n.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:12px;margin-bottom:12px}\n.card{background:var(--bg2);border:1px solid var(--b);border-radius:8px;overflow:hidden}\n.card.full{grid-column:1/-1}\n.card-hdr{padding:10px 14px;border-bottom:1px solid var(--b);display:flex;align-items:center;justify-content:space-between}\n.card-title{font-size:10px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:var(--t3);font-family:var(--fd)}\n.card-val{font-size:12px;font-weight:700;color:var(--t);font-family:var(--fm)}\n.card-body{padding:0}\n.plot{width:100%;height:200px}\n.plot-tall{width:100%;height:300px}\n.plot-wf{width:100%;height:280px}\n\n/* ── EARNINGS (original) ── */\n.eps-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:0;border-bottom:1px solid var(--b)}\n.ep{padding:12px 16px;border-right:1px solid var(--b)}\n.ep:last-child{border-right:none}\n.ep-l{font-size:9px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:var(--t3);margin-bottom:6px}\n.ep-v{font-size:16px;font-weight:600;font-family:var(--fm)}\n.eps-table{width:100%;border-collapse:collapse;font-size:11px;font-family:var(--fm)}\n.eps-table th{text-align:left;color:var(--t3);font-size:9px;padding:6px 12px;border-bottom:1px solid var(--b);letter-spacing:.08em;text-transform:uppercase;font-weight:700}\n.eps-table td{padding:7px 12px;border-bottom:1px solid rgba(255,255,255,.025)}\n.eps-table tr:hover td{background:rgba(255,255,255,.02)}\n\n/* ── TAB PANELS ── */\n.tpanel{display:none}\n.tpanel.on{display:block}\n</style>\n</head>\n<body>\n\n<!-- TAPE -->\n<div id=\"tape\"><div id=\"tape-inner\"></div></div>\n\n<!-- NAV — identical to original -->\n<nav id=\"nav\">\n  <a href=\"/\" id=\"nav-logo\" style=\"display:flex;align-items:center;gap:7px;text-decoration:none;margin-right:12px;flex-shrink:0\">\n      <svg width=\"22\" height=\"22\" viewBox=\"0 0 64 64\" fill=\"none\">\n        <rect width=\"64\" height=\"64\" rx=\"10\" fill=\"rgba(0,229,160,0.15)\" stroke=\"rgba(0,229,160,0.4)\" stroke-width=\"2\"/>\n        <polyline points=\"8,44 20,28 30,36 42,18 56,22\" stroke=\"#00e5a0\" stroke-width=\"5\" fill=\"none\" stroke-linecap=\"round\" stroke-linejoin=\"round\"/>\n        <circle cx=\"42\" cy=\"18\" r=\"5\" fill=\"#00e5a0\"/>\n      </svg>\n      <span style=\"font-size:17px;font-weight:800;color:var(--t);letter-spacing:.05em;font-family:var(--fd)\">IST</span>\n    </a>\n  <a href=\"/chart\" class=\"nl\"><span class=\"nl-icon\">&#9641;</span>Chart</a>\n  <a href=\"/financials\" class=\"nl on\"><span class=\"nl-icon\">&#128200;</span>Financials</a>\n  <a href=\"/metrics\" class=\"nl\"><span class=\"nl-icon\">&#8759;</span>Metrics</a>\n  <a href=\"/insider\" class=\"nl\"><span class=\"nl-icon\">&#128100;</span>Insider</a>\n  <a href=\"/congress\" class=\"nl\"><span class=\"nl-icon\">&#128274;</span>Congress</a>\n  <a href=\"/fairvalue\" class=\"nl\"><span class=\"nl-icon\">&#9733;</span>Fair Value</a>\n  <a href=\"/news\" class=\"nl\"><span class=\"nl-icon\">&#128196;</span>News</a>\n  <a href=\"/livefeed\" class=\"nl\"><span class=\"nl-icon\">&#9899;</span>Live Feed</a>\n</nav>\n\n<!-- TKBAR — identical to original -->\n<div id=\"tkbar\">\n  <span id=\"tk-sym\">--</span>\n  <span id=\"tk-name\">--</span>\n  <span id=\"tk-px\"></span>\n  <span id=\"tk-ch\" class=\"nc\"></span>\n  <button id=\"tk-chg-btn\" onclick=\"toggleSearch()\">Change ticker...</button>\n  <div id=\"tk-search-wrap\">\n    <input id=\"tk-si\" placeholder=\"Search ticker...\" autocomplete=\"off\"\n      oninput=\"onSearch(this.value)\" onkeydown=\"onKey(event)\"/>\n    <div id=\"tk-dr\"></div>\n  </div>\n</div>\n\n<!-- TABS -->\n<div id=\"tabs\">\n  <div class=\"tab on\"  onclick=\"switchTab('overview')\">Overview</div>\n  <div class=\"tab\"     onclick=\"switchTab('waterfall')\">Waterfall</div>\n  <div class=\"tab\"     onclick=\"switchTab('flow')\">Flow (Sankey)</div>\n  <div class=\"tab\"     onclick=\"switchTab('earnings')\">Earnings</div>\n</div>\n\n<!-- MAIN -->\n<div id=\"main\">\n  <div class=\"centered\" id=\"loading-state\">\n    <span class=\"spin\"></span>\n    <span class=\"load-msg\" id=\"load-msg\">Loading financial data...</span>\n  </div>\n  <div id=\"content\" style=\"display:none\"></div>\n</div>\n\n<script>\n// ── CONFIG — identical to original ──\nconst TK = (new URLSearchParams(location.search).get('t') || 'NVDA').toUpperCase();\nconst PLO = {\n  paper_bgcolor:'transparent',plot_bgcolor:'transparent',\n  margin:{l:40,r:10,t:10,b:40},\n  font:{family:'JetBrains Mono',color:'#7a8fa0',size:10},\n  xaxis:{gridcolor:'#162030',linecolor:'#162030',tickcolor:'#364860'},\n  yaxis:{gridcolor:'#162030',linecolor:'#162030',tickcolor:'#364860'},\n  hovermode:'x unified',\n  hoverlabel:{bgcolor:'#0e141b',bordercolor:'#1c2c3e',font:{color:'#d4dde6',family:'JetBrains Mono',size:11}}\n};\n\n// ── TAPE — identical to original ──\n(function(){\n  const SYMS=['^GSPC','^IXIC','BTC-USD','GC=F','CL=F','^VIX','^TNX','NVDA','AAPL','MSFT','TSLA','SPY','QQQ'];\n  const LBL={'^GSPC':'SP500','^IXIC':'NDX','BTC-USD':'BTC','GC=F':'GOLD','CL=F':'OIL','^VIX':'VIX','^TNX':'US10Y','NVDA':'NVDA','AAPL':'AAPL','MSFT':'MSFT','TSLA':'TSLA','SPY':'SPY','QQQ':'QQQ'};\n  const tid=t=>'TT_'+t.replace(/[^a-zA-Z0-9]/g,'_');\n  const inner=document.getElementById('tape-inner');\n  inner.innerHTML=[...SYMS,...SYMS].map(t=>`<div class=\"ti\" id=\"${tid(t)}\" onclick=\"location.href='/chart?t=${encodeURIComponent(t)}'\"><span class=\"ts\">${LBL[t]||t}</span><span class=\"tv\">--</span><span class=\"tc nc\">--</span></div>`).join('');\n  function updTape(p){\n    if(!p.ticker||p.price==null)return;\n    const el=document.getElementById(tid(p.ticker));if(!el)return;\n    const cs=el.children;\n    const px=p.price>10000?'$'+Math.round(p.price).toLocaleString():'$'+p.price.toFixed(2);\n    const pct=p.change_pct==null?'--':(p.change_pct>=0?'+':'')+p.change_pct.toFixed(2)+'%';\n    if(cs[1])cs[1].textContent=px;\n    if(cs[2]){cs[2].textContent=pct;cs[2].className='tc '+(p.change_pct>0?'up':p.change_pct<0?'dn':'nc');}\n  }\n  fetch('/api/watchlist?tickers='+SYMS.join(',')).then(r=>r.json()).then(d=>{(d.stocks||[]).forEach(updTape);}).catch(()=>{});\n  try{const s=io();s.on('connect',()=>s.emit('subscribe',{tickers:SYMS}));s.on('price_update',({prices})=>{(prices||[]).forEach(updTape);});}catch(e){}\n})();\n\n// ── NAV LINKS — identical to original ──\ndocument.querySelectorAll('.nl').forEach(a=>{\n  const u=new URL(a.href,location.origin);\n  u.searchParams.set('t',TK);\n  a.href=u.toString();\n});\n\n// ── TICKER BAR — identical to original ──\ndocument.getElementById('tk-sym').textContent=TK;\nlet searchTimer;\nfunction toggleSearch(){\n  const w=document.getElementById('tk-search-wrap');\n  const shown=w.style.display==='block';\n  w.style.display=shown?'none':'block';\n  if(!shown)document.getElementById('tk-si').focus();\n}\nfunction onSearch(v){\n  clearTimeout(searchTimer);\n  const dr=document.getElementById('tk-dr');\n  if(!v.trim()){dr.style.display='none';return;}\n  searchTimer=setTimeout(()=>{\n    fetch('/api/universe?q='+encodeURIComponent(v.trim())+'&limit=8')\n      .then(r=>r.json()).then(d=>{\n        const items=d.results||[];\n        if(!items.length){dr.style.display='none';return;}\n        dr.innerHTML=items.map(x=>`<div class=\"dr-item\" onclick=\"goTicker('${x.ticker}')\"><span class=\"dr-sym\">${x.ticker}</span><span class=\"dr-nm\">${x.name||''}</span></div>`).join('');\n        dr.style.display='block';\n      }).catch(()=>{});\n  },200);\n}\nfunction onKey(e){if(e.key==='Escape')document.getElementById('tk-search-wrap').style.display='none';}\nfunction goTicker(t){location.href='/financials?t='+encodeURIComponent(t);}\n\n// Load price for tkbar — identical to original\nfetch('/api/stock_fast/'+encodeURIComponent(TK))\n  .then(r=>r.json()).then(d=>{\n    if(d.name)document.getElementById('tk-name').textContent=d.name;\n    if(d.price!=null){\n      document.getElementById('tk-px').textContent='$'+d.price.toFixed(2);\n      const pct=d.change_pct;\n      if(pct!=null){\n        const el=document.getElementById('tk-ch');\n        el.textContent=(pct>=0?'+':'')+pct.toFixed(2)+'%';\n        el.className=pct>0?'up':pct<0?'dn':'nc';\n      }\n    }\n  }).catch(()=>{});\n\n// ── FORMAT HELPERS — identical to original ──\nfunction fmtB(v){if(v==null||isNaN(v))return'—';const a=Math.abs(v),s=v<0?'-':'';if(a>=1e12)return s+'$'+(a/1e12).toFixed(2)+'T';if(a>=1e9)return s+'$'+(a/1e9).toFixed(2)+'B';if(a>=1e6)return s+'$'+(a/1e6).toFixed(1)+'M';return s+'$'+Math.round(a).toLocaleString();}\nfunction fmtPct(v){return v==null?'—':(v*100).toFixed(1)+'%';}\nfunction bar_color(vals){return vals.map(v=>v>=0?'rgba(0,229,160,.75)':'rgba(255,61,90,.75)');}\nfunction last(arr){return arr&&arr.length?arr[arr.length-1].value:null;}\nfunction dates(arr){return arr.map(d=>d.date);}\nfunction vals(arr){return arr.map(d=>d.value);}\n\n// ── TABS ──\nlet _activeTab='overview';\nfunction switchTab(name){\n  _activeTab=name;\n  document.querySelectorAll('.tab').forEach((t,i)=>{\n    const names=['overview','waterfall','flow','earnings'];\n    t.classList.toggle('on',names[i]===name);\n  });\n  document.querySelectorAll('.tpanel').forEach(p=>p.classList.toggle('on',p.id==='panel-'+name));\n  // Force Plotly resize on tab switch (panels were hidden during initial render)\n  setTimeout(()=>{\n    document.querySelectorAll('#panel-'+name+' .plot,#panel-'+name+' .plot-tall,#panel-'+name+' .plot-wf').forEach(el=>{\n      try{Plotly.relayout(el,{autosize:true});}catch(e){}\n    });\n  },60);\n}\n\n// ── LOADING STATE — identical to original ──\nconst msgs=['Loading financial data...','Fetching income statement...','Running yfinance (may take ~5s)...','Almost there...'];\nlet mi=0;\nconst mt=setInterval(()=>{const el=document.getElementById('load-msg');if(el&&mi<msgs.length-1){mi++;el.textContent=msgs[mi];}},4000);\n\n// ── MAIN LOAD — fetch statements + earnings in parallel ──\nasync function load(){\n  try{\n    const [stmtRes,earnRes]=await Promise.all([\n      fetch('/api/statements/'+encodeURIComponent(TK)),\n      fetch('/api/earnings/'+encodeURIComponent(TK))\n    ]);\n    const d=await stmtRes.json();\n    clearInterval(mt);\n    if(!stmtRes.ok||(!d.income?.length&&!d.charts?.revenue?.length)){\n      showError(d.error||'No financial data available for '+TK+'. Try NVDA, AAPL, MSFT.');\n      return;\n    }\n    try{\n      if(earnRes.ok){const earn=await earnRes.json();if(earn&&(earn.history?.length||earn.next_earnings_date))d.earnings=earn;}\n    }catch(e){}\n    render(d);\n  }catch(e){\n    clearInterval(mt);\n    showError('Request failed: '+e.message);\n  }\n}\n\nfunction showError(msg){\n  document.getElementById('loading-state').style.display='none';\n  document.getElementById('content').style.display='block';\n  document.getElementById('content').innerHTML=`<div class=\"centered\"><div class=\"err-box\"><div class=\"err-title\">Could not load financials</div><div class=\"err-sub\">${msg}</div><button class=\"btn-retry\" onclick=\"location.reload()\">Try Again</button></div></div>`;\n}\n\n// ── RENDER — builds 4 tab panels ──\nfunction render(d){\n  document.getElementById('loading-state').style.display='none';\n  const con=document.getElementById('content');\n  con.style.display='block';\n  const ch=d.charts||{};\n\n  const rev=last(ch.revenue), gp=last(ch.gross_profit), ni=last(ch.net_income);\n  const fcf=last(ch.fcf), ocf=last(ch.operating_cf), capex=last(ch.capex);\n  const cogs=rev!=null&&gp!=null?Math.max(0,rev-gp):null;\n  const opex=gp!=null&&ni!=null?Math.max(0,gp-Math.max(0,ni)):null;\n\n  // ── BUILD HTML ──\n  let html='';\n\n  // ── PANEL: OVERVIEW ──\n  html+=`<div class=\"tpanel on\" id=\"panel-overview\">`;\n  const bars=[\n    ['revenue','Revenue','#0095ff'],\n    ['gross_profit','Gross Profit','#a78bfa'],\n    ['net_income','Net Income',null],\n    ['fcf','Free Cash Flow',null],\n    ['operating_cf','Operating CF',null],\n    ['cash','Cash','#22d3ee'],\n    ['debt','Total Debt','#ff4d6d'],\n  ];\n  html+=`<div class=\"grid\">`;\n  bars.forEach(([k,lbl,clr])=>{\n    if(!ch[k]?.length)return;\n    html+=`<div class=\"card\"><div class=\"card-hdr\"><span class=\"card-title\">${lbl}</span><span class=\"card-val\">${fmtB(last(ch[k]))}</span></div><div class=\"plot\" id=\"bc_${k}\"></div></div>`;\n  });\n  html+=`</div>`;\n  html+=`</div>`;\n\n  // ── PANEL: WATERFALL ──\n  html+=`<div class=\"tpanel\" id=\"panel-waterfall\">`;\n  if(rev!=null){\n    html+=`<div class=\"card full\" style=\"margin-bottom:12px\">\n      <div class=\"card-hdr\">\n        <span class=\"card-title\">Income Waterfall — ${TK}</span>\n        <span class=\"card-val\" style=\"font-size:10px;color:var(--t2)\">Revenue → COGS → Gross Profit → OpEx → Net Income</span>\n      </div>\n      <div class=\"plot-wf\" id=\"wf-main\"></div>\n    </div>`;\n  }\n  // YoY bars for revenue + net income side by side\n  html+=`<div class=\"grid\">`;\n  if(ch.revenue?.length) html+=`<div class=\"card\"><div class=\"card-hdr\"><span class=\"card-title\">Revenue YoY</span><span class=\"card-val\">${fmtB(rev)}</span></div><div class=\"plot\" id=\"wf_rev\"></div></div>`;\n  if(ch.gross_profit?.length) html+=`<div class=\"card\"><div class=\"card-hdr\"><span class=\"card-title\">Gross Profit YoY</span><span class=\"card-val\">${fmtB(gp)}</span></div><div class=\"plot\" id=\"wf_gp\"></div></div>`;\n  if(ch.net_income?.length) html+=`<div class=\"card\"><div class=\"card-hdr\"><span class=\"card-title\">Net Income YoY</span><span class=\"card-val\">${fmtB(ni)}</span></div><div class=\"plot\" id=\"wf_ni\"></div></div>`;\n  if(ch.fcf?.length) html+=`<div class=\"card\"><div class=\"card-hdr\"><span class=\"card-title\">Free Cash Flow YoY</span><span class=\"card-val\">${fmtB(fcf)}</span></div><div class=\"plot\" id=\"wf_fcf\"></div></div>`;\n  html+=`</div>`;\n  html+=`</div>`;\n\n  // ── PANEL: FLOW (SANKEYS) ──\n  html+=`<div class=\"tpanel\" id=\"panel-flow\">`;\n  if(rev!=null&&gp!=null){\n    html+=`<div class=\"card full\" style=\"margin-bottom:12px\"><div class=\"card-hdr\"><span class=\"card-title\">Income Flow (Sankey)</span></div><div class=\"plot-tall\" id=\"sankey1\"></div></div>`;\n  }\n  if(ocf!=null&&ocf>0){\n    html+=`<div class=\"card full\" style=\"margin-bottom:12px\"><div class=\"card-hdr\"><span class=\"card-title\">Capital Allocation</span></div><div class=\"plot-tall\" id=\"sankey2\"></div></div>`;\n  }\n  if(rev==null&&ocf==null){\n    html+=`<div class=\"centered\"><span style=\"color:var(--t2);font-family:var(--fm)\">No flow data available.</span></div>`;\n  }\n  html+=`</div>`;\n\n  // ── PANEL: EARNINGS ──\n  html+=`<div class=\"tpanel\" id=\"panel-earnings\">`;\n  const e=d.earnings||d.earn;\n  if(e){\n    const beat=e.last_eps_actual!=null&&e.last_eps_estimate!=null&&e.last_eps_actual>=e.last_eps_estimate;\n    const nextDate=e.next_earnings_date||'—';\n    const epsEst=e.last_eps_estimate!=null?'$'+e.last_eps_estimate.toFixed(2):'—';\n    const epsAct=e.last_eps_actual!=null?'$'+e.last_eps_actual.toFixed(2):'—';\n    const epsActColor=e.last_eps_actual>0?'up':'dn';\n    const epsSur=e.last_eps_surprise!=null?(e.last_eps_surprise>=0?'+':'')+e.last_eps_surprise.toFixed(1)+'%':'—';\n    const epsSurColor=e.last_eps_surprise>0?'up':'dn';\n    const beatTxt=e.last_eps_actual!=null?(beat?'✓ BEAT':'✗ MISS'):'—';\n    const beatColor=beat?'var(--gr)':'var(--rd)';\n    html+=`<div class=\"card full\" style=\"margin-bottom:12px\">\n      <div class=\"card-hdr\"><span class=\"card-title\">Earnings</span></div>\n      <div class=\"eps-grid\">\n        <div class=\"ep\"><div class=\"ep-l\">Next Earnings</div><div class=\"ep-v\" style=\"color:var(--bl);font-size:14px\">${nextDate}</div></div>\n        <div class=\"ep\"><div class=\"ep-l\">EPS Estimate</div><div class=\"ep-v\">${epsEst}</div></div>\n        <div class=\"ep\"><div class=\"ep-l\">EPS Actual</div><div class=\"ep-v ${epsActColor}\">${epsAct}</div></div>\n        <div class=\"ep\"><div class=\"ep-l\">Surprise</div><div class=\"ep-v ${epsSurColor}\">${epsSur}<div style=\"font-size:11px;margin-top:2px;color:${beatColor}\">${beatTxt}</div></div></div>\n      </div>`;\n    if(e.history?.length){\n      html+=`<div style=\"overflow-x:auto;padding:0 0 4px\"><table class=\"eps-table\">\n        <thead><tr><th>Date</th><th>Estimate</th><th>Actual</th><th>Surprise</th><th>Result</th></tr></thead>\n        <tbody id=\"eps-rows\"></tbody>\n      </table></div>`;\n    }\n    html+=`</div>`;\n    window._epsHistory=e.history||[];\n  }else{\n    html+=`<div class=\"centered\"><span style=\"color:var(--t2);font-family:var(--fm)\">No earnings data available.</span></div>`;\n  }\n  html+=`</div>`;\n\n  // ── INJECT ──\n  con.innerHTML=html;\n\n  // ── EPS TABLE ROWS ──\n  const epsRowsEl=document.getElementById('eps-rows');\n  if(epsRowsEl&&window._epsHistory){\n    epsRowsEl.innerHTML=window._epsHistory.map(r=>{\n      const estStr=r.estimate!=null?'$'+r.estimate.toFixed(2):'—';\n      const actStr=r.actual!=null?'$'+r.actual.toFixed(2):'—';\n      const actColor=r.actual>0?'var(--gr)':'var(--rd)';\n      const surStr=r.surprise!=null?(r.surprise>=0?'+':'')+r.surprise.toFixed(1)+'%':'—';\n      const surColor=r.surprise>0?'var(--gr)':r.surprise<0?'var(--rd)':'var(--t2)';\n      const beatStr=r.beat===true?'✓ BEAT':r.beat===false?'✗ MISS':'—';\n      const beatColor=r.beat===true?'var(--gr)':'var(--rd)';\n      return`<tr><td style=\"color:var(--t2)\">${r.date}</td><td>${estStr}</td><td style=\"color:${actColor};font-weight:700\">${actStr}</td><td style=\"color:${surColor}\">${surStr}</td><td style=\"color:${beatColor};font-weight:700\">${beatStr}</td></tr>`;\n    }).join('');\n  }\n\n  // ── OVERVIEW BAR CHARTS ──\n  bars.forEach(([k,lbl,clr])=>{\n    const el=document.getElementById('bc_'+k);\n    if(!el||!ch[k]?.length)return;\n    const v=vals(ch[k]),x=dates(ch[k]);\n    const colors=clr?v.map(()=>clr):bar_color(v);\n    Plotly.newPlot(el,[{type:'bar',x,y:v,marker:{color:colors,opacity:.85},hovertemplate:`<b>${lbl}</b><br>%{x}<br><b>%{customdata}</b><extra></extra>`,customdata:v.map(fmtB)}],{...PLO,showlegend:false},{responsive:true,displayModeBar:false});\n  });\n\n  // ── WATERFALL MAIN ──\n  const wfEl=document.getElementById('wf-main');\n  if(wfEl&&rev!=null){\n    const wfCogs=cogs??0, wfOpex=opex??0, wfNi=ni??0;\n    Plotly.newPlot(wfEl,[{\n      type:'waterfall',orientation:'v',\n      measure:['absolute','relative','total','relative','total'],\n      x:['Revenue','COGS','Gross Profit','OpEx','Net Income'],\n      y:[rev,-wfCogs,0,-wfOpex,0],\n      text:[fmtB(rev),fmtB(wfCogs),fmtB(rev-wfCogs),fmtB(wfOpex),fmtB(wfNi)],\n      textposition:'outside',\n      textfont:{color:'#d4dde6',size:10,family:'JetBrains Mono'},\n      connector:{line:{color:'#1c2c3e',width:1,dash:'dot'}},\n      increasing:{marker:{color:'rgba(0,229,160,.75)'}},\n      decreasing:{marker:{color:'rgba(255,61,90,.75)'}},\n      totals:{marker:{color:wfNi>=0?'rgba(0,229,160,.85)':'rgba(255,61,90,.85)'}},\n      hovertemplate:'<b>%{x}</b><br>%{text}<extra></extra>',\n    }],{\n      ...PLO,\n      margin:{l:50,r:20,t:30,b:40},\n      xaxis:{...PLO.xaxis,fixedrange:true},\n      yaxis:{...PLO.yaxis,fixedrange:true,tickprefix:'$',tickformat:',.3s'},\n      showlegend:false,\n    },{responsive:true,displayModeBar:false});\n  }\n\n  // ── WATERFALL YoY BARS ──\n  [['wf_rev',ch.revenue,'#0095ff'],['wf_gp',ch.gross_profit,'#a78bfa'],['wf_ni',ch.net_income,null],['wf_fcf',ch.fcf,null]].forEach(([id,arr,clr])=>{\n    const el=document.getElementById(id);\n    if(!el||!arr?.length)return;\n    const v=vals(arr),x=dates(arr);\n    const colors=clr?v.map(()=>clr):bar_color(v);\n    Plotly.newPlot(el,[{type:'bar',x,y:v,marker:{color:colors,opacity:.85},customdata:v.map(fmtB),hovertemplate:'%{x}<br><b>%{customdata}</b><extra></extra>'}],{...PLO,showlegend:false},{responsive:true,displayModeBar:false});\n  });\n\n  // ── SANKEY 1: Income Flow — identical to original ──\n  const sk1=document.getElementById('sankey1');\n  if(sk1&&rev!=null&&gp!=null){\n    const cogs2=Math.max(0,rev-gp);\n    const opex2=ni!=null?Math.max(0,gp-Math.max(0,ni)):null;\n    const lbls=['Revenue','Cost of Revenue','Gross Profit'];\n    const src=[],tgt=[],val=[],col=[];\n    src.push(0);tgt.push(1);val.push(cogs2);col.push('rgba(255,77,109,.5)');\n    src.push(0);tgt.push(2);val.push(gp);col.push('rgba(0,149,255,.4)');\n    if(opex2!=null&&ni!=null){\n      lbls.push('OpEx','Net Income');\n      src.push(2);tgt.push(3);val.push(opex2);col.push('rgba(255,140,66,.4)');\n      src.push(2);tgt.push(4);val.push(Math.max(0,ni));col.push('rgba(0,229,160,.5)');\n    }\n    if(fcf!=null&&fcf>0){\n      lbls.push('Free Cash Flow');\n      const fromIdx=lbls.indexOf('Net Income');\n      if(fromIdx>=0){src.push(fromIdx);tgt.push(lbls.length-1);val.push(fcf);col.push('rgba(0,229,160,.7)');}\n    }\n    Plotly.newPlot(sk1,[{type:'sankey',orientation:'h',\n      node:{pad:16,thickness:20,line:{width:0},label:lbls,color:['#0095ff','#ff4d6d','#0095ff','#ff8c42','#00e5a0','#00e5a0']},\n      link:{source:src,target:tgt,value:val,color:col,customdata:val.map(fmtB),hovertemplate:'%{source.label} → %{target.label}<br><b>%{customdata}</b><extra></extra>'}\n    }],{...PLO,margin:{l:10,r:10,t:10,b:10}},{responsive:true,displayModeBar:false});\n  }\n\n  // ── SANKEY 2: Capital Allocation — identical to original ──\n  const sk2=document.getElementById('sankey2');\n  if(sk2&&ocf!=null&&ocf>0){\n    const div=last(ch.dividends_paid),bb=last(ch.buybacks),cx=capex?Math.abs(capex):0;\n    const lbls2=['Operating CF','CapEx','Dividends','Buybacks','Free Cash'];\n    const src2=[],tgt2=[],val2=[],col2=[];\n    let rem=ocf;\n    if(cx>0&&cx<rem){src2.push(0);tgt2.push(1);val2.push(cx);col2.push('rgba(255,77,109,.5)');rem-=cx;}\n    if(div&&Math.abs(div)>0&&Math.abs(div)<rem){src2.push(0);tgt2.push(2);val2.push(Math.abs(div));col2.push('rgba(245,185,66,.5)');rem-=Math.abs(div);}\n    if(bb&&Math.abs(bb)>0&&Math.abs(bb)<rem){src2.push(0);tgt2.push(3);val2.push(Math.abs(bb));col2.push('rgba(167,139,250,.5)');rem-=Math.abs(bb);}\n    if(rem>0){src2.push(0);tgt2.push(4);val2.push(rem);col2.push('rgba(0,229,160,.6)');}\n    if(src2.length>0){\n      Plotly.newPlot(sk2,[{type:'sankey',orientation:'h',\n        node:{pad:16,thickness:20,line:{width:0},label:lbls2,color:['#0095ff','#ff4d6d','#f5b942','#a78bfa','#00e5a0']},\n        link:{source:src2,target:tgt2,value:val2,color:col2,customdata:val2.map(fmtB),hovertemplate:'%{source.label} → %{target.label}<br><b>%{customdata}</b><extra></extra>'}\n      }],{...PLO,margin:{l:10,r:10,t:10,b:10}},{responsive:true,displayModeBar:false});\n    }\n  }\n}\n\nload();\n</script>\n</body>\n</html>\n",
    "index.html": "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"UTF-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\n<title>Chart · IST</title>\n<script src=\"https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.7.5/socket.io.min.js\"></script>\n<script src=\"https://cdn.plot.ly/plotly-2.32.0.min.js\"></script><link href=\"https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600&family=Syne:wght@400;600;700;800&display=swap\" rel=\"stylesheet\">\n<style>\n*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}\n:root{\n  --bg:#080b0f;--bg2:#0d1117;--bg3:#111720;--bg4:#161d29;\n  --b:#1e2d3d;--b2:#243040;\n  --t:#c9d1d9;--t2:#8b949e;--t3:#484f58;\n  --gr:#00e5a0;--rd:#ff4d6d;--bl:#0095ff;\n  --yl:#f0c060;--pu:#a78bfa;--or:#ff8c42;\n  --fm:'JetBrains Mono',monospace;--fd:'Syne',sans-serif;\n}\nhtml,body{height:100%;background:var(--bg);color:var(--t);font-family:var(--fm);font-size:13px}\n::-webkit-scrollbar{width:5px;height:5px}::-webkit-scrollbar-track{background:transparent}::-webkit-scrollbar-thumb{background:var(--b2);border-radius:3px}\n.up{color:var(--gr)}.dn{color:var(--rd)}.nc{color:var(--t2)}\n/* TAPE */\n#tape{height:26px;background:var(--bg2);border-bottom:1px solid var(--b);overflow:hidden;display:flex;align-items:center;flex-shrink:0}\n#tape-inner{display:flex;white-space:nowrap;animation:tape 90s linear infinite;user-select:none;-webkit-user-select:none;cursor:default;}\n/* tape runs continuously */\n@keyframes tape{from{transform:translateX(0)}to{transform:translateX(-50%)}}\n.ti{display:inline-flex;align-items:center;gap:5px;padding:0 16px;height:26px;border-right:1px solid var(--b);font-size:11px;user-select:none;-webkit-user-select:none;}\n.ts{color:var(--t2);font-weight:600}.tc{font-size:10px}\n/* NAV */\n#nav{height:40px;background:var(--bg2);border-bottom:1px solid var(--b);display:flex;align-items:center;padding:0 10px;gap:2px;flex-shrink:0}\n.nl{display:flex;align-items:center;gap:5px;padding:5px 10px;border-radius:4px;font-size:11px;font-family:var(--fd);font-weight:700;letter-spacing:.04em;text-transform:uppercase;color:var(--t2);text-decoration:none;transition:all .15s;white-space:nowrap}\n.nl:hover{color:var(--t);background:var(--bg3)}\n.nl.on{color:var(--gr);background:rgba(0,229,160,.07);border:1px solid rgba(0,229,160,.12)}\n.nl svg{flex-shrink:0;opacity:.7}.nl.on svg{opacity:1}\n#nav-logo{font-family:var(--fd);font-size:14px;font-weight:800;color:var(--gr);letter-spacing:.04em;margin-right:12px;text-decoration:none}\n/* TICKER BAR */\n#tkbar{height:38px;background:var(--bg2);border-bottom:1px solid var(--b);display:flex;align-items:center;padding:0 14px;gap:10px;flex-shrink:0}\n#tk-sym{font-family:var(--fd);font-size:15px;font-weight:800;color:var(--gr);flex-shrink:0}\n#tk-name{font-size:11px;color:var(--t2);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}\n#tk-px{font-family:var(--fd);font-size:15px;font-weight:700;flex-shrink:0}\n#tk-chg{font-size:11px;padding:2px 8px;border-radius:3px;font-weight:600;flex-shrink:0}\n#tk-sw{position:relative}\n#tk-si{background:var(--bg3);border:1px solid var(--b2);color:var(--t);font-family:var(--fm);font-size:11px;padding:4px 9px;border-radius:3px;outline:none;width:160px}\n#tk-si:focus{border-color:var(--bl)}\n#tk-dr{position:absolute;top:calc(100% + 2px);right:0;background:var(--bg3);border:1px solid var(--b2);border-radius:3px;z-index:999;display:none;min-width:200px;max-height:200px;overflow-y:auto}\n.tdr{padding:6px 10px;cursor:pointer;display:flex;justify-content:space-between;gap:8px;font-size:11px}.tdr:hover{background:var(--bg4)}\n.tds{color:var(--gr);font-weight:700}.tdn{color:var(--t2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:120px}\n/* PAGE BODY */\n.pb{display:flex;flex-direction:column;height:100vh;overflow:hidden}\n.pc{flex:1;overflow-y:auto;overflow-x:hidden}\n/* SKELETON */\n.sk{background:linear-gradient(90deg,var(--bg3) 25%,var(--bg4) 50%,var(--bg3) 75%);background-size:200% 100%;animation:sk 1.2s ease infinite;border-radius:3px}\n@keyframes sk{0%{background-position:200% 0}100%{background-position:-200% 0}}\n.sk-line{height:14px;margin-bottom:8px}.sk-val{height:24px;margin-bottom:4px}\n/* SPIN */\n.spin-svg{display:inline-block;vertical-align:middle;margin-right:8px;width:44px;height:20px}\n/* EMPTY */\n.empty{text-align:center;padding:40px 20px;color:var(--t3);font-size:12px}\n\nbody{overflow:hidden}\n.chart-outer{display:flex;flex:1;min-height:0;overflow:hidden}\n#wl-sb{width:188px;flex-shrink:0;background:var(--bg2);border-right:1px solid var(--b);display:flex;flex-direction:column;overflow:hidden}\n#wl-h{padding:6px 10px;border-bottom:1px solid var(--b);font-family:var(--fd);font-size:10px;font-weight:700;color:var(--t2);letter-spacing:.07em;text-transform:uppercase;display:flex;justify-content:space-between}\n#wl-l{flex:1;overflow-y:auto}\n.wr{display:flex;gap:6px;align-items:center;padding:6px 10px;cursor:pointer;border-bottom:1px solid rgba(255,255,255,.025);transition:background .1s}\n.wr:hover{background:var(--bg3)}.wr.on{background:var(--bg3);border-left:2px solid var(--gr)}.wr.on .ws{color:var(--gr)}\n.ws{font-size:12px;font-weight:600;color:var(--t)}.wrr{text-align:right}.wpx{font-size:11px}.wch{font-size:10px}\n#cm{flex:1;display:flex;flex-direction:column;min-width:0;overflow:hidden}\n#cc{height:36px;background:var(--bg2);border-bottom:1px solid var(--b);display:flex;align-items:center;padding:0 10px;gap:5px;flex-shrink:0}\n.cb{background:var(--bg3);border:1px solid var(--b2);color:var(--t2);font-family:var(--fm);font-size:10px;padding:3px 8px;cursor:pointer;border-radius:2px;transition:all .12s}\n.cb:hover{color:var(--t)}.cb.on{background:var(--gr);color:var(--bg);border-color:var(--gr);font-weight:700}\n.cb.ov.on{background:transparent;color:var(--bl);border-color:var(--bl)}\n.csep{width:1px;height:16px;background:var(--b);margin:0 3px}\n#mkt-status{font-size:10px;font-family:var(--fd);font-weight:700;padding:4px 10px;border-radius:20px;cursor:default;position:relative;display:flex;align-items:center;transition:all .2s;white-space:nowrap}\n/* mkt panel handled by JS */\n#cw{flex:1;min-height:0}\n#tc{width:100%;height:100%}\n#ss{height:64px;background:var(--bg2);border-top:1px solid var(--b);display:flex;flex-shrink:0}\n.sc{flex:1;padding:7px 11px;border-right:1px solid var(--b);display:flex;flex-direction:column;justify-content:center;overflow:hidden;min-width:0}\n.sc:last-child{border-right:none}\n.sl{font-family:var(--fd);font-size:9px;font-weight:700;letter-spacing:.09em;text-transform:uppercase;color:var(--t3);margin-bottom:2px}\n.sv{font-family:var(--fd);font-size:17px;font-weight:800;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}\n.sb2{font-size:9px;color:var(--t2);white-space:nowrap}\n.vg{color:var(--gr)}.vr{color:var(--rd)}.vb{color:var(--bl)}.vy{color:var(--yl)}.vd{color:var(--t2)}\n</style>\n</head>\n<body>\n<div class=\"pb\">\n<div id=\"tape\"><div id=\"tape-inner\"></div></div>\n<div id=\"nav\"><a href=\"/\" id=\"nav-logo\" style=\"display:flex;align-items:center;gap:7px;text-decoration:none\">\n      <svg width=\"22\" height=\"22\" viewBox=\"0 0 64 64\" fill=\"none\">\n        <rect width=\"64\" height=\"64\" rx=\"10\" fill=\"rgba(0,229,160,0.15)\" stroke=\"rgba(0,229,160,0.4)\" stroke-width=\"2\"/>\n        <polyline points=\"8,44 20,28 30,36 42,18 56,22\" stroke=\"#00e5a0\" stroke-width=\"5\" fill=\"none\" stroke-linecap=\"round\" stroke-linejoin=\"round\"/>\n        <circle cx=\"42\" cy=\"18\" r=\"5\" fill=\"#00e5a0\"/>\n      </svg>\n      <span style=\"font-family:var(--fd);font-size:14px;font-weight:800;color:var(--gr);letter-spacing:.04em\">IST</span>\n    </a>\n    <a href=\"/chart\" class=\"nl on\"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"currentColor\"><rect x=\"1\" y=\"7\" width=\"2\" height=\"5\"/><rect x=\"5\" y=\"4\" width=\"2\" height=\"8\"/><rect x=\"9\" y=\"1\" width=\"2\" height=\"11\"/></svg>Chart</a><a href=\"/financials\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><polyline points=\"1,9 4,5 7,7 12,2\"/></svg>Financials</a><a href=\"/metrics\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"currentColor\"><circle cx=\"3\" cy=\"3\" r=\"1.5\"/><circle cx=\"10\" cy=\"3\" r=\"1.5\"/><circle cx=\"3\" cy=\"10\" r=\"1.5\"/><circle cx=\"10\" cy=\"10\" r=\"1.5\"/></svg>Metrics</a><a href=\"/insider\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><circle cx=\"6.5\" cy=\"4\" r=\"2.5\"/><path d=\"M1 12c0-3 2.5-5 5.5-5s5.5 2 5.5 5\"/></svg>Insider</a><a href=\"/congress\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><rect x=\"1\" y=\"5\" width=\"11\" height=\"7\" rx=\"1\"/><path d=\"M4 5V3a2.5 2.5 0 015 0v2\"/></svg>Congress</a><a href=\"/fairvalue\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><path d=\"M6.5 1L8 5h4l-3 2.5 1 4L6.5 9 3 11.5l1-4L1 5h4z\"/></svg>Fair Value</a><a href=\"/news\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><rect x=\"1\" y=\"1\" width=\"11\" height=\"11\" rx=\"1\"/><line x1=\"3\" y1=\"4\" x2=\"10\" y2=\"4\"/><line x1=\"3\" y1=\"7\" x2=\"10\" y2=\"7\"/></svg>News</a><a href=\"/livefeed\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"currentColor\"><circle cx=\"6.5\" cy=\"6.5\" r=\"2\"/><circle cx=\"6.5\" cy=\"6.5\" r=\"4\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1\" opacity=\".5\"/></svg>Live Feed</a></div>\n<script id=\"asset-nav-fix\">\ndocument.addEventListener('DOMContentLoaded', function(){\n  const t=(new URLSearchParams(window.location.search).get('t')||'').toUpperCase();\n  const isCrypto=t.endsWith('-USD')&&!['GLD','SLV','USO','TLT','HYG','LQD','GDX'].includes(t);\n  const isFuture=t.endsWith('=F');\n  const isIndex=t.startsWith('^')||t==='DX-Y.NYB';\n  if(!isCrypto&&!isFuture&&!isIndex) return;\n  const STOCK_ONLY=['/financials','/insider','/congress','/fairvalue'];\n  document.querySelectorAll('#nav a.nl').forEach(a=>{\n    const href=a.getAttribute('href')||'';\n    if(STOCK_ONLY.some(p=>href.includes(p))) a.style.display='none';\n  });\n});\n</script>\n<div id=\"tkbar\">\n  <span id=\"tk-sym\">—</span><span id=\"tk-name\">—</span>\n  <span id=\"tk-px\" style=\"color:var(--t2)\">—</span><span id=\"tk-chg\"></span>\n  <div id=\"tk-sw\"><input id=\"tk-si\" placeholder=\"Change ticker…\" autocomplete=\"off\" spellcheck=\"false\"><div id=\"tk-dr\"></div></div>\n</div>\n<div class=\"chart-outer\">\n  <div id=\"wl-sb\"><div id=\"wl-h\"><span>Watchlist</span><span id=\"wl-cnt\" style=\"color:var(--t3)\">0</span></div><div id=\"wl-l\"></div></div>\n  <div id=\"cm\">\n    <div id=\"cc\">\n      <div style=\"display:flex;gap:2px\" id=\"pbtns\">\n        <button class=\"cb\" data-p=\"5d\">5D</button><button class=\"cb\" data-p=\"1mo\">1M</button>\n        <button class=\"cb\" data-p=\"3mo\">3M</button><button class=\"cb on\" data-p=\"1y\">1Y</button>\n        <button class=\"cb\" data-p=\"2y\">2Y</button><button class=\"cb\" data-p=\"5y\">5Y</button>\n      </div>\n      <div class=\"csep\"></div>\n      <button class=\"cb on\" id=\"bm-c\" onclick=\"setMode('candle')\">Candles</button>\n      <button class=\"cb\" id=\"bm-l\" onclick=\"setMode('line')\">Linha</button>\n      <button class=\"cb\" id=\"bm-r\" onclick=\"setMode('relative')\">Rel%</button>\n      <div class=\"csep\"></div>\n      <button class=\"cb ov on\" data-ov=\"SP500\">S&amp;P500</button>\n      <button class=\"cb ov\" data-ov=\"QQQ\">QQQ</button>\n      <button class=\"cb ov\" data-ov=\"M2\">M2</button>\n      <div class=\"csep\"></div>\n      <div id=\"mkt-wrap\" style=\"position:relative\">\n      <div id=\"mkt-status\" style=\"font-size:10px;font-family:var(--fd);font-weight:700;padding:4px 11px;border-radius:20px;cursor:pointer;display:flex;align-items:center;gap:5px;white-space:nowrap;transition:all .2s\" onmouseenter=\"showMktPanel()\" onmouseleave=\"hideMktPanelDelayed()\"></div>\n      <div id=\"mkt-panel\" onmouseenter=\"cancelHideMktPanel()\" onmouseleave=\"hideMktPanel()\" style=\"display:none;position:absolute;top:calc(100%+10px);right:0;background:#0d1117;border:1px solid #1e2d3d;border-radius:10px;padding:20px;width:380px;z-index:9999;box-shadow:0 12px 40px rgba(0,0,0,.7)\">\n        <div style=\"font-family:var(--fd);font-size:11px;font-weight:700;color:#8b949e;letter-spacing:.1em;text-transform:uppercase;margin-bottom:16px\">Sessões de Mercado</div>\n        <!-- T212-style timeline -->\n        <div style=\"position:relative;margin-bottom:20px\">\n          <!-- Timeline bar -->\n          <div style=\"position:relative;height:8px;background:#161d29;border-radius:4px;overflow:visible\">\n            <!-- Pre-market: 04:00-09:30 ET = 16.7%-39.6% of day -->\n            <div style=\"position:absolute;left:16.7%;width:22.9%;height:100%;background:linear-gradient(90deg,#ff8c42,#f0c060);border-radius:4px 0 0 4px\" title=\"Pre-Market\"></div>\n            <!-- Market open: 09:30-16:00 ET = 39.6%-66.7% -->\n            <div style=\"position:absolute;left:39.6%;width:27.1%;height:100%;background:linear-gradient(90deg,#00c896,#00e5a0)\" title=\"Regular Hours\"></div>\n            <!-- After-hours: 16:00-20:00 ET = 66.7%-83.3% -->\n            <div style=\"position:absolute;left:66.7%;width:16.6%;height:100%;background:linear-gradient(90deg,#e91e8c,#a855f7);border-radius:0 4px 4px 0\" title=\"After-Hours\"></div>\n            <!-- Current time dot -->\n            <div id=\"mkt-dot\" style=\"position:absolute;top:50%;transform:translate(-50%,-50%);width:14px;height:14px;border-radius:50%;background:#fff;border:2px solid #080b0f;box-shadow:0 0 8px rgba(255,255,255,.6);z-index:2;transition:left .5s ease\"></div>\n          </div>\n          <!-- Labels below timeline -->\n          <div style=\"display:flex;justify-content:space-between;margin-top:10px;font-size:9px;font-family:var(--fd);font-weight:700;letter-spacing:.06em;color:#484f58\">\n            <span>00:00</span><span style=\"color:#ff8c42\">04:00</span><span style=\"color:#00e5a0\">09:30</span><span style=\"color:#e91e8c\">16:00</span><span style=\"color:#a855f7\">20:00</span><span>24:00</span>\n          </div>\n        </div>\n        <!-- Session cards -->\n        <div style=\"display:grid;grid-template-columns:1fr 1fr;gap:8px\">\n          <div style=\"background:#111720;border-radius:6px;padding:10px 12px\">\n            <div style=\"display:flex;align-items:center;gap:6px;margin-bottom:4px\">\n              <div style=\"width:10px;height:10px;border-radius:50%;background:linear-gradient(135deg,#ff8c42,#f0c060)\"></div>\n              <span style=\"font-family:var(--fd);font-size:10px;font-weight:700;color:#f0c060;letter-spacing:.06em;text-transform:uppercase\">Pre-Market</span>\n            </div>\n            <div style=\"font-size:13px;font-weight:700;color:#c9d1d9\">04:00 – 09:30</div>\n            <div style=\"font-size:10px;color:#484f58;margin-top:2px\">Nova Iorque ET</div>\n          </div>\n          <div style=\"background:#111720;border-radius:6px;padding:10px 12px\">\n            <div style=\"display:flex;align-items:center;gap:6px;margin-bottom:4px\">\n              <div style=\"width:10px;height:10px;border-radius:50%;background:linear-gradient(135deg,#00c896,#00e5a0);animation:pulse 1.4s ease infinite\"></div>\n              <span style=\"font-family:var(--fd);font-size:10px;font-weight:700;color:#00e5a0;letter-spacing:.06em;text-transform:uppercase\">NYSE Open</span>\n            </div>\n            <div style=\"font-size:13px;font-weight:700;color:#c9d1d9\">09:30 – 16:00</div>\n            <div style=\"font-size:10px;color:#484f58;margin-top:2px\">Nova Iorque ET</div>\n          </div>\n          <div style=\"background:#111720;border-radius:6px;padding:10px 12px\">\n            <div style=\"display:flex;align-items:center;gap:6px;margin-bottom:4px\">\n              <div style=\"width:10px;height:10px;border-radius:50%;background:linear-gradient(135deg,#e91e8c,#a855f7)\"></div>\n              <span style=\"font-family:var(--fd);font-size:10px;font-weight:700;color:#e91e8c;letter-spacing:.06em;text-transform:uppercase\">After-Hours</span>\n            </div>\n            <div style=\"font-size:13px;font-weight:700;color:#c9d1d9\">16:00 – 20:00</div>\n            <div style=\"font-size:10px;color:#484f58;margin-top:2px\">Nova Iorque ET</div>\n          </div>\n          <div style=\"background:#111720;border-radius:6px;padding:10px 12px\">\n            <div style=\"display:flex;align-items:center;gap:6px;margin-bottom:4px\">\n              <div style=\"width:10px;height:10px;border-radius:50%;background:#243040\"></div>\n              <span style=\"font-family:var(--fd);font-size:10px;font-weight:700;color:#484f58;letter-spacing:.06em;text-transform:uppercase\">Overnight</span>\n            </div>\n            <div style=\"font-size:13px;font-weight:700;color:#c9d1d9\">20:00 – 04:00</div>\n            <div style=\"font-size:10px;color:#484f58;margin-top:2px\">Nova Iorque ET</div>\n          </div>\n        </div>\n        <div id=\"mkt-et-time\" style=\"margin-top:12px;text-align:center;font-size:10px;color:#484f58;font-family:var(--fd)\"></div>\n      </div>\n    </div>\n    </div>\n    <div id=\"cw\"><div id=\"tc\"></div></div>\n    <div id=\"ss\">\n      <div class=\"sc\"><div class=\"sl\">Signal</div><div class=\"sv vd\" id=\"s0\">—</div><div class=\"sb2\">/100</div></div>\n      <div class=\"sc\"><div class=\"sl\">Upside</div><div class=\"sv vd\" id=\"s1\">—</div><div class=\"sb2\" id=\"s1b\">target</div></div>\n      <div class=\"sc\"><div class=\"sl\">Fair Value</div><div class=\"sv vb\" id=\"s2\">—</div><div class=\"sb2\" id=\"s2b\"></div></div>\n      <div class=\"sc\"><div class=\"sl\">Insider</div><div class=\"sv vd\" id=\"s3\">—</div><div class=\"sb2\">&gt;$30k·180d</div></div>\n      <div class=\"sc\"><div class=\"sl\">Congress</div><div class=\"sv vd\" id=\"s4\">—</div><div class=\"sb2\" id=\"s4b\"></div></div>\n      <div class=\"sc\" style=\"border:none\"><div class=\"sl\">Earnings</div><div class=\"sv vb\" id=\"s5\" style=\"font-size:12px\">—</div><div class=\"sb2\" id=\"s5b\"></div></div>\n    </div>\n  </div>\n</div>\n</div>\n<script>\nconst TSYMS=['SPY','QQQ','^GSPC','^IXIC','^VIX','CL=F','BZ=F','GC=F','SI=F','DX-Y.NYB','^TNX','BTC-USD'];\nconst TLBL={SPY:'SPY',QQQ:'QQQ','^GSPC':'S&P500','^IXIC':'NASDAQ','^VIX':'VIX','CL=F':'WTI','BZ=F':'BRENT','GC=F':'GOLD','SI=F':'SILVER','DX-Y.NYB':'DXY','^TNX':'US10Y','BTC-USD':'BTC'};\nconst socket=io();\nfunction buildTape(){\n  document.getElementById('tape-inner').innerHTML=[...TSYMS,...TSYMS].map(t=>{\n    const id='ti_'+t.replace(/[^a-zA-Z0-9]/g,'_');\n    // ALL tape items are clickable — everything is navigable\n    return`<div class=\"ti\" id=\"${id}\" onclick=\"navToTape('${t}')\" style=\"cursor:pointer;\"><span class=\"ts\">${TLBL[t]||t}</span>&nbsp;<span>—</span>&nbsp;<span class=\"tc nc\">—</span></div>`;\n  }).join('');\n  socket.emit('subscribe',{tickers:TSYMS});\n}\nsocket.on('price_update',({prices})=>{\n  prices.forEach(p=>{\n    const id='ti_'+p.ticker.replace(/[^a-zA-Z0-9]/g,'_');\n    document.querySelectorAll('#'+id).forEach(el=>{\n      const cs=el.children;\n      if(cs[1]&&p.price!=null)cs[1].textContent='$'+p.price.toFixed(2);\n      if(cs[2]&&p.change_pct!=null){const s=p.change_pct>=0?'+':'';cs[2].textContent=s+p.change_pct.toFixed(2)+'%';cs[2].className='tc '+(p.change_pct>0?'up':p.change_pct<0?'dn':'nc');}\n    });\n    if(p.ticker===window.TK)updateTkBar(p);\n    if(typeof onPriceUpdate==='function')onPriceUpdate(p);\n  });\n});\nbuildTape();\nconst MEM={};const CACHE_TTL=300000;\nasync function api(url,ttl=CACHE_TTL,timeout=12000){\n  if(MEM[url]&&Date.now()-MEM[url].ts<ttl)return MEM[url].data;\n  try{\n    const ctrl=new AbortController();\n    const tid=setTimeout(()=>ctrl.abort(),timeout);\n    const r=await fetch(url,{signal:ctrl.signal});\n    clearTimeout(tid);\n    if(!r.ok)return null;\n    const d=await r.json();\n    MEM[url]={data:d,ts:Date.now()};\n    return d;\n  }catch{return null;}\n}\nfunction ssGet(k){try{const v=sessionStorage.getItem(k);if(!v)return null;const p=JSON.parse(v);if(Date.now()-p.ts>CACHE_TTL){sessionStorage.removeItem(k);return null;}return p.data;}catch{return null;}}\nfunction ssSet(k,d){try{sessionStorage.setItem(k,JSON.stringify({data:d,ts:Date.now()}));}catch{}}\nasync function getStockData(t){const k='full:'+t;const ss=ssGet(k);if(ss)return ss;const mc=MEM['/api/full/'+t];if(mc&&Date.now()-mc.ts<CACHE_TTL)return mc.data;const d=await api('/api/full/'+encodeURIComponent(t),CACHE_TTL);if(d)ssSet(k,d);return d;}\nfunction updateTkBar(p){const pxEl=document.getElementById('tk-px'),chEl=document.getElementById('tk-chg');if(!pxEl)return;if(p.price!=null){pxEl.textContent='$'+p.price.toFixed(2);pxEl.style.color=p.change_pct>0?'var(--gr)':p.change_pct<0?'var(--rd)':'var(--t)';}if(p.change_pct!=null){const s=p.change_pct>=0?'+':'';chEl.textContent=s+p.change_pct.toFixed(2)+'%';chEl.style.cssText=`background:${p.change_pct>=0?'rgba(0,229,160,.12)':'rgba(255,77,109,.12)'};color:${p.change_pct>=0?'var(--gr)':'var(--rd)'};padding:2px 8px;border-radius:3px;`;}}\nasync function initTkBar(t){document.getElementById('tk-sym').textContent=t;const d=await api('/api/stock_fast/'+encodeURIComponent(t),6000);if(d){document.getElementById('tk-name').textContent=d.name||t;updateTkBar(d);}socket.emit('subscribe',{tickers:[t]});}\nlet sT;const siEl=document.getElementById('tk-si'),drEl=document.getElementById('tk-dr');\nif(siEl){siEl.addEventListener('input',function(){clearTimeout(sT);const v=this.value.trim();if(!v){drEl.style.display='none';return;}sT=setTimeout(async()=>{const d=await api('/api/universe?q='+encodeURIComponent(v)+'&limit=10',60000);if(!d?.results?.length){drEl.style.display='none';return;}drEl.innerHTML=d.results.map(x=>`<div class=\"tdr\" onclick=\"navTo('${x.ticker}')\"><span class=\"tds\">${x.ticker}</span><span class=\"tdn\">${x.name||''}</span></div>`).join('');drEl.style.display='block';},200);});document.addEventListener('click',e=>{if(!e.target.closest('#tk-sw'))drEl.style.display='none';});}\nfunction navTo(t){if(!t)return;if(siEl){siEl.value='';drEl.style.display='none';}const url=new URL(window.location);url.searchParams.set('t',t);window.history.pushState({},'',url);window.TK=t;initTkBar(t);fixNavLinks();if(typeof onTickerChange==='function')onTickerChange(t);}\n// navToTape: always go to chart page with the ticker\nfunction navToTape(t){\n  if(!t)return;\n  // Route to appropriate page based on asset type\n  // Always open chart first - user can navigate to crypto/commodity tab\n  window.location.href='/chart?t='+encodeURIComponent(t);\n}\nwindow.TK=(new URLSearchParams(window.location.search).get('t')||'NVDA').toUpperCase();\nconst fmtB=v=>{if(v==null)return'—';const a=Math.abs(v),s=v<0?'-':'';if(a>=1e12)return s+'$'+(a/1e12).toFixed(2)+'T';if(a>=1e9)return s+'$'+(a/1e9).toFixed(2)+'B';if(a>=1e6)return s+'$'+(a/1e6).toFixed(1)+'M';return s+'$'+a.toLocaleString('en-US',{maximumFractionDigits:0});};\nconst fmtPx=v=>v==null?'—':'$'+Number(v).toFixed(2);\nconst p1=v=>v==null?'—':(v*100).toFixed(1)+'%';\nconst fn=(v,d=2)=>v==null?'—':Number(v).toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d});\n\n\nfunction getLogoUrl(ticker) {\n  const domains = {\n    NVDA:'nvidia.com',AAPL:'apple.com',MSFT:'microsoft.com',GOOGL:'google.com',\n    META:'meta.com',AMZN:'amazon.com',TSLA:'tesla.com',AVGO:'broadcom.com',\n    AMD:'amd.com',INTC:'intel.com',JPM:'jpmorganchase.com',BAC:'bankofamerica.com',\n    GS:'goldmansachs.com',MS:'morganstanley.com',V:'visa.com',MA:'mastercard.com',\n    LLY:'lilly.com',UNH:'unitedhealthgroup.com',JNJ:'jnj.com',ABBV:'abbvie.com',\n    MRK:'merck.com',XOM:'exxonmobil.com',CVX:'chevron.com',WMT:'walmart.com',\n    COST:'costco.com',HD:'homedepot.com',NKE:'nike.com',MCD:'mcdonalds.com',\n    PLTR:'palantir.com',CRWD:'crowdstrike.com',NET:'cloudflare.com',SNOW:'snowflake.com',\n    ARM:'arm.com',ASML:'asml.com',TSM:'tsmc.com',ORCL:'oracle.com',CRM:'salesforce.com',\n    NFLX:'netflix.com',MSTR:'microstrategy.com',COIN:'coinbase.com',HOOD:'robinhood.com',\n    SPY:'ssga.com',QQQ:'invesco.com',BABA:'alibaba.com',NIO:'nio.com',\n    GLD:'spdrgoldshares.com',RIOT:'riotplatforms.com',MARA:'marathondh.com',\n    BP:'bp.com',BIDU:'baidu.com',JNJ:'jnj.com',WMT:'walmart.com',\n  };\n  const d = domains[ticker.replace('-USD','').replace('-B','').toUpperCase()];\n  return d ? 'https://logo.clearbit.com/'+d : null;\n}\n\nconst DEFWL=['NVDA','AAPL','MSFT','GOOGL','META','AMZN','TSLA','AVGO','AMD','INTC','JPM','BAC','GS','MS','V','MA','BRK-B','LLY','UNH','JNJ','ABBV','MRK','XOM','CVX','BP','WMT','COST','HD','NKE','MCD','PLTR','CRWD','NET','SNOW','ARM','ASML','TSM','ORCL','CRM','NFLX','MSTR','COIN','HOOD','RIOT','MARA','SPY','QQQ','GLD','BABA','NIO','BIDU','BRK-B','BTC-USD','ETH-USD'];\nlet WL=JSON.parse(localStorage.getItem('ist_wl')||'null')||[...DEFWL];\nconst pm={};let mode='candle',period='1y',ovs=['SP500'];\nfunction saveWl(){localStorage.setItem('ist_wl',JSON.stringify(WL));}\nfunction renderWl(){\n  document.getElementById('wl-cnt').textContent=WL.length;\n  document.getElementById('wl-l').innerHTML=WL.map(t=>{\n    const p=pm[t];const pxt=p?.price!=null?'$'+p.price.toFixed(2):'—';\n    const cht=p?.change_pct!=null?(p.change_pct>=0?'+':'')+p.change_pct.toFixed(2)+'%':'—';\n    const chc=p?.change_pct>0?'up':p?.change_pct<0?'dn':'nc';\n    const logo=getLogoUrl(t);\n    return`<div class=\"wr ${t===window.TK?'on':''}\" onclick=\"navTo('${t}')\">\n      ${logo?`<img src=\"${logo}\" style=\"width:18px;height:18px;border-radius:3px;object-fit:contain;flex-shrink:0;background:#fff;padding:1px;\" onerror=\"this.style.display='none'\">`:'<div style=\"width:18px;height:18px;flex-shrink:0\"></div>'}\n      <span class=\"ws\" style=\"flex:1\">${t}</span>\n      <div class=\"wrr\"><div class=\"wpx\" id=\"wp_${t}\">${pxt}</div><div class=\"wch ${chc}\" id=\"wc_${t}\">${cht}</div></div>\n    </div>`;\n  }).join('');\n  socket.emit('subscribe',{tickers:WL});\n}\nfunction onPriceUpdate(p){pm[p.ticker]=p;const pe=document.getElementById('wp_'+p.ticker),ce=document.getElementById('wc_'+p.ticker);if(pe&&p.price!=null)pe.textContent='$'+p.price.toFixed(2);if(ce&&p.change_pct!=null){ce.textContent=(p.change_pct>=0?'+':'')+p.change_pct.toFixed(2)+'%';ce.className='wch '+(p.change_pct>0?'up':p.change_pct<0?'dn':'nc');}}\nasync function initWl(){const d=await api('/api/watchlist?tickers='+WL.slice(0,80).join(','),15000);if(d?.stocks){d.stocks.forEach(p=>{if(p?.ticker)pm[p.ticker]=p;});renderWl();}}\nrenderWl();initWl();setInterval(initWl,15000);\nasync function drawChart(t){\n  const el=document.getElementById('tc');\n  const norm=mode==='relative'?1:0;\n  const ov_p=mode==='candle'?'':ovs.join(',');\n  const d=await api('/api/chart/'+encodeURIComponent(t)+'?period='+period+'&overlays='+ov_p+'&normalized='+norm,120000,20000);\n  if(!d?.series)return;\n  const traces=[];let ci=0;\n  for(const[name,pts] of Object.entries(d.series)){\n    if(!pts?.length){ci++;continue;}\n    const isM=name===t;const cols=['#00e5a0','#0095ff','#f0c060','#ff8c42','#a78bfa'];\n    const col=isM?cols[0]:cols[(ci%4)+1];\n    if(mode==='candle'&&isM){\n      traces.push({type:'candlestick',name,x:pts.map(p=>p.date),open:pts.map(p=>p.open),high:pts.map(p=>p.high),low:pts.map(p=>p.low),close:pts.map(p=>p.close||p.value),increasing:{line:{color:'#00e5a0',width:1},fillcolor:'rgba(0,229,160,.85)'},decreasing:{line:{color:'#ff4d6d',width:1},fillcolor:'rgba(255,77,109,.85)'},whiskerwidth:0});\n      const vols=pts.map(p=>p.volume||0);\n      traces.push({type:'bar',name:'Volume',x:pts.map(p=>p.date),y:vols,marker:{color:pts.map((p,i)=>(!pts[i-1]||p.close>=pts[i-1].close)?'rgba(0,229,160,.2)':'rgba(255,77,109,.2)'),line:{width:0}},yaxis:'y2',showlegend:false,hovertemplate:'Vol: %{y:,.0f}<extra></extra>'});\n    } else {\n      traces.push({type:'scatter',mode:'lines',name,x:pts.map(p=>p.date),y:pts.map(p=>p.value??p.close),line:{color:col,width:isM?2:1.5,dash:isM?'solid':'dot'},hovertemplate:'<b>'+name+'</b>: %{y:.2f}<extra></extra>'});\n    }\n    ci++;\n  }\n  const layout={paper_bgcolor:'#080b0f',plot_bgcolor:'#080b0f',margin:{l:52,r:52,t:8,b:30},\n    xaxis:{gridcolor:'rgba(255,255,255,.04)',tickfont:{color:'#484f58',family:'JetBrains Mono',size:10},zeroline:false,rangeslider:{visible:false},showspikes:true,spikecolor:'#484f58',spikemode:'across',spikethickness:1,spikedash:'dot'},\n    yaxis:{gridcolor:'rgba(255,255,255,.04)',tickfont:{color:'#484f58',family:'JetBrains Mono',size:10},zeroline:false,autorange:true,side:'right',showspikes:true,spikecolor:'#484f58',spikemode:'across',spikethickness:1},\n    yaxis2:{domain:[0,.14],showticklabels:false,gridcolor:'rgba(0,0,0,0)',zeroline:false,fixedrange:true},\n    legend:{font:{color:'#8b949e',family:'JetBrains Mono',size:10},bgcolor:'rgba(8,11,15,.8)',x:0.01,y:0.99,bordercolor:'#1e2d3d',borderwidth:1},\n    hovermode:'x unified',hoverlabel:{bgcolor:'#0d1117',bordercolor:'#1e2d3d',font:{color:'#c9d1d9',family:'JetBrains Mono',size:11}},\n    modebar:{bgcolor:'transparent',color:'#484f58',activecolor:'#00e5a0',remove:['pan2d','zoom2d']},dragmode:'pan'};\n  Plotly.react(el,traces,layout,{\n    responsive:true,displayModeBar:true,\n    modeBarButtonsToRemove:['autoScale2d','lasso2d','select2d','toggleSpikelines','pan2d'],\n    scrollZoom:true\n  }).then(()=>{\n    // Force zoom mode after render — Plotly sometimes resets to pan\n    Plotly.relayout(el,{dragmode:'pan'});\n    // Remove pan cursor from the chart div\n    const plotDiv=document.getElementById('tc');\n    if(plotDiv){plotDiv.style.cursor='crosshair';}\n  });\n}\nasync function loadSignals(t){\n  const d=await getStockData(t);if(!d)return;\n  const sc=d.signal_score;const si=id=>document.getElementById(id);\n  si('s0').textContent=sc!=null?sc:'—';si('s0').className='sv '+(sc>=60?'vg':sc>=40?'vy':sc!=null?'vr':'vd');\n  const up=d.upside;si('s1').textContent=up!=null?(up>=0?'+':'')+up+'%':'—';si('s1').className='sv '+(up>0?'vg':up<0?'vr':'vd');\n  if(d.target_mean)si('s1b').textContent='target $'+d.target_mean.toFixed(2);\n  si('s2').textContent=d.fair_value!=null?'$'+d.fair_value.toFixed(2):'—';\n  if(d.fair_value&&d.price){const m=Math.round((d.fair_value-d.price)/d.price*100);si('s2b').textContent=(m>=0?'+':'')+m+'% vs preço';}\n  si('s3').textContent=d.insider_trade_count!=null?d.insider_trade_count:'—';si('s3').className='sv '+(d.insider_trade_count>0?'vy':'vd');\n  si('s4').textContent=d.congress_buys||0;si('s4').className='sv '+(d.congress_buys>0?'vg':'vd');\n  if(d.congress_members?.length)si('s4b').textContent=d.congress_members.slice(0,2).join(', ');\n  si('s5').textContent=d.earnings_date||'—';\n  api('/api/earnings/'+encodeURIComponent(t),300000).then(e=>{if(e?.eps_estimate!=null&&si('s5b'))si('s5b').textContent='EPS est. $'+e.eps_estimate.toFixed(2);});\n}\nfunction setMode(m){\n  mode=m;['c','l','r'].forEach(x=>{const b=document.getElementById('bm-'+x);if(b)b.classList.toggle('on',x===m[0]);});\n  document.querySelectorAll('.cb.ov').forEach(b=>b.style.opacity=m==='candle'?'.35':'1');\n  drawChart(window.TK);\n}\ndocument.querySelectorAll('#pbtns .cb').forEach(b=>{b.addEventListener('click',()=>{document.querySelectorAll('#pbtns .cb').forEach(x=>x.classList.remove('on'));b.classList.add('on');period=b.dataset.p;drawChart(window.TK);});});\ndocument.querySelectorAll('.cb.ov').forEach(b=>{b.addEventListener('click',()=>{if(mode==='candle')return;const ov=b.dataset.ov;if(ovs.includes(ov))ovs=ovs.filter(x=>x!==ov);else ovs.push(ov);b.classList.toggle('on',ovs.includes(ov));drawChart(window.TK);});});\nfunction onTickerChange(t){\n  renderWl(); fixNavLinks();\n  drawChart(t); loadSignals(t);\n}\nfixNavLinks();\ndrawChart(window.TK);loadSignals(window.TK);\n\n// Market status\nfunction updateMarketStatus(){\n  const el=document.getElementById('mkt-status');\n  if(!el)return;\n  const t=window.TK||'';\n  const isCrypto=t.endsWith('-USD')&&!['GLD','SLV','USO','TLT'].includes(t);\n  const isFuture=t.endsWith('=F');\n  if(isCrypto){\n    el.innerHTML='<span style=\"width:7px;height:7px;border-radius:50%;background:#00e5a0;animation:pulse 1.4s ease infinite;flex-shrink:0\"></span>24/7 OPEN';\n    el.style.cssText='font-size:10px;font-family:var(--fd);font-weight:700;padding:4px 11px;border-radius:20px;cursor:default;display:flex;align-items:center;gap:5px;background:rgba(0,229,160,.1);color:#00e5a0;border:1px solid rgba(0,229,160,.25)';\n    return;\n  }\n  if(isFuture){\n    el.innerHTML='<span style=\"width:7px;height:7px;border-radius:50%;background:#0095ff;flex-shrink:0\"></span>FUTURES';\n    el.style.cssText='font-size:10px;font-family:var(--fd);font-weight:700;padding:4px 11px;border-radius:20px;cursor:default;display:flex;align-items:center;gap:5px;background:rgba(0,149,255,.1);color:#0095ff;border:1px solid rgba(0,149,255,.25)';\n    return;\n  }\n  let h,m,dow;\n  try{\n    const now=new Date();\n    const fmt=new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',hour:'2-digit',minute:'2-digit',weekday:'short',hour12:false});\n    const parts=Object.fromEntries(fmt.formatToParts(now).map(x=>[x.type,x.value]));\n    h=parseInt(parts.hour);m=parseInt(parts.minute);dow=parts.weekday;\n  }catch(e){return;}\n  const totalMin=h*60+m;\n  const isWeekday=['Mon','Tue','Wed','Thu','Fri'].includes(dow);\n  let label,color,bg,border,dot,dotAnim='';\n  if(!isWeekday||totalMin<240||totalMin>=1200){\n    label='CLOSED';color='#484f58';bg='rgba(255,255,255,.04)';border='rgba(255,255,255,.08)';dot='#243040';\n  } else if(totalMin<570){\n    label='PRE-MARKET';color='#f0c060';bg='rgba(240,192,96,.08)';border='rgba(240,192,96,.2)';dot='#f0c060';\n  } else if(totalMin<960){\n    label='NYSE OPEN';color='#00e5a0';bg='rgba(0,229,160,.1)';border='rgba(0,229,160,.25)';dot='#00e5a0';dotAnim='animation:pulse 1.4s ease infinite';\n  } else {\n    label='AFTER-HOURS';color='#e91e8c';bg='rgba(233,30,140,.08)';border='rgba(233,30,140,.2)';dot='#e91e8c';\n  }\n  el.innerHTML=`<span style=\"width:7px;height:7px;border-radius:50%;background:${dot};${dotAnim};flex-shrink:0\"></span>${label}`;\n  el.style.cssText=`font-size:10px;font-family:var(--fd);font-weight:700;padding:4px 11px;border-radius:20px;cursor:pointer;display:flex;align-items:center;gap:5px;background:${bg};color:${color};border:1px solid ${border}`;\n}\nupdateMarketStatus();\nsetInterval(updateMarketStatus,30000);\n\nlet _mktT=null;\nfunction showMktPanel(){clearTimeout(_mktT);const p=document.getElementById('mkt-panel');if(p){p.style.display='block';updateTimeMarker();}}\nfunction hideMktPanel(){const p=document.getElementById('mkt-panel');if(p)p.style.display='none';}\nfunction hideMktPanelDelayed(){_mktT=setTimeout(hideMktPanel,250);}\nfunction cancelHideMktPanel(){clearTimeout(_mktT);}\nfunction updateTimeMarker(){\n  try{\n    const now=new Date();\n    const fmt=new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',hour:'numeric',minute:'numeric',hour12:false});\n    const parts=fmt.formatToParts(now);\n    const p2=Object.fromEntries(parts.map(x=>[x.type,x.value]));\n    const h=parseInt(p2.hour),m=parseInt(p2.minute);\n    const totalMin=h*60+m;\n    const pct=Math.min(100,Math.max(0,(totalMin/1440)*100));\n    const marker=document.getElementById('mkt-time-marker');\n    if(marker)marker.style.left=pct+'%';\n    const etEl=document.getElementById('mkt-et-time');\n    if(etEl)etEl.textContent=`Hora actual em Nova Iorque: ${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')} ET`;\n  }catch(e){}\n}\n\ninitTkBar(window.TK);\n\n// Fix nav links to carry current ticker\nfunction fixNavLinks(){\n  const t=window.TK;\n  if(!t)return;\n  const isCrypto=t.endsWith('-USD')&&!['GLD','SLV','USO','TLT'].includes(t);\n  const isFuture=t.endsWith('=F')||t.startsWith('^')||t==='DX-Y.NYB';\n  const STOCK_ONLY=['/financials','/insider','/congress','/fairvalue']; // metrics/news/livefeed available for all\n  document.querySelectorAll('#nav a.nl').forEach(a=>{\n    const href=a.getAttribute('href')||'';\n    // Update href to carry ticker\n    if(href&&href!=='/'&&!href.startsWith('javascript')&&!href.startsWith('#')){\n      try{const u=new URL(href,window.location.origin);u.searchParams.set('t',t);a.setAttribute('href',u.toString());}catch(e){}\n    }\n    // Hide stock-only tabs completely for non-stock assets\n    const isStockOnly=STOCK_ONLY.some(p=>href.includes(p));\n    if(isStockOnly&&(isCrypto||isFuture)){\n      a.style.display='none';\n    } else {\n      a.style.display='';\n    }\n  });\n  // Update display name in tkbar symbol\n  const DISPLAY={'GC=F':'GOLD','SI=F':'SILVER','CL=F':'WTI','BZ=F':'BRENT','NG=F':'GAS',\n    'HG=F':'COPPER','ZC=F':'CORN','ZW=F':'WHEAT','PL=F':'PLATINUM','PA=F':'PALLADIUM',\n    '^GSPC':'SP500','^IXIC':'NASDAQ','^DJI':'DOW','^VIX':'VIX','^TNX':'US10Y',\n    'DX-Y.NYB':'DXY','BTC-USD':'BTC','ETH-USD':'ETH','SOL-USD':'SOL','BNB-USD':'BNB',\n    'XRP-USD':'XRP','ADA-USD':'ADA','DOGE-USD':'DOGE','AVAX-USD':'AVAX',\n  };\n  const symEl=document.getElementById('tk-sym');\n  if(symEl&&DISPLAY[t])symEl.textContent=DISPLAY[t];\n}\nfixNavLinks();\n</script>\n</body>\n</html>",
    "insider.html": "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"UTF-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\n<title>Insider · IST</title>\n<script src=\"https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.7.5/socket.io.min.js\"></script>\n<link href=\"https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600&family=Syne:wght@400;600;700;800&display=swap\" rel=\"stylesheet\">\n<style>\n*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}\n:root{\n  --bg:#080b0f;--bg2:#0d1117;--bg3:#111720;--bg4:#161d29;\n  --b:#1e2d3d;--b2:#243040;\n  --t:#c9d1d9;--t2:#8b949e;--t3:#484f58;\n  --gr:#00e5a0;--rd:#ff4d6d;--bl:#0095ff;\n  --yl:#f0c060;--pu:#a78bfa;--or:#ff8c42;\n  --fm:'JetBrains Mono',monospace;--fd:'Syne',sans-serif;\n}\nhtml,body{height:100%;background:var(--bg);color:var(--t);font-family:var(--fm);font-size:13px}\n::-webkit-scrollbar{width:5px;height:5px}::-webkit-scrollbar-track{background:transparent}::-webkit-scrollbar-thumb{background:var(--b2);border-radius:3px}\n.up{color:var(--gr)}.dn{color:var(--rd)}.nc{color:var(--t2)}\n/* TAPE */\n#tape{height:26px;background:var(--bg2);border-bottom:1px solid var(--b);overflow:hidden;display:flex;align-items:center;flex-shrink:0}\n#tape-inner{display:flex;white-space:nowrap;animation:tape 90s linear infinite;user-select:none;-webkit-user-select:none;cursor:default;}\n/* tape runs continuously */\n@keyframes tape{from{transform:translateX(0)}to{transform:translateX(-50%)}}\n.ti{display:inline-flex;align-items:center;gap:5px;padding:0 16px;height:26px;border-right:1px solid var(--b);font-size:11px;user-select:none;-webkit-user-select:none;}\n.ts{color:var(--t2);font-weight:600}.tc{font-size:10px}\n/* NAV */\n#nav{height:40px;background:var(--bg2);border-bottom:1px solid var(--b);display:flex;align-items:center;padding:0 10px;gap:2px;flex-shrink:0}\n.nl{display:flex;align-items:center;gap:5px;padding:5px 10px;border-radius:4px;font-size:11px;font-family:var(--fd);font-weight:700;letter-spacing:.04em;text-transform:uppercase;color:var(--t2);text-decoration:none;transition:all .15s;white-space:nowrap}\n.nl:hover{color:var(--t);background:var(--bg3)}\n.nl.on{color:var(--gr);background:rgba(0,229,160,.07);border:1px solid rgba(0,229,160,.12)}\n.nl svg{flex-shrink:0;opacity:.7}.nl.on svg{opacity:1}\n#nav-logo{font-family:var(--fd);font-size:14px;font-weight:800;color:var(--gr);letter-spacing:.04em;margin-right:12px;text-decoration:none}\n/* TICKER BAR */\n#tkbar{height:38px;background:var(--bg2);border-bottom:1px solid var(--b);display:flex;align-items:center;padding:0 14px;gap:10px;flex-shrink:0}\n#tk-sym{font-family:var(--fd);font-size:15px;font-weight:800;color:var(--gr);flex-shrink:0}\n#tk-name{font-size:11px;color:var(--t2);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}\n#tk-px{font-family:var(--fd);font-size:15px;font-weight:700;flex-shrink:0}\n#tk-chg{font-size:11px;padding:2px 8px;border-radius:3px;font-weight:600;flex-shrink:0}\n#tk-sw{position:relative}\n#tk-si{background:var(--bg3);border:1px solid var(--b2);color:var(--t);font-family:var(--fm);font-size:11px;padding:4px 9px;border-radius:3px;outline:none;width:160px}\n#tk-si:focus{border-color:var(--bl)}\n#tk-dr{position:absolute;top:calc(100% + 2px);right:0;background:var(--bg3);border:1px solid var(--b2);border-radius:3px;z-index:999;display:none;min-width:200px;max-height:200px;overflow-y:auto}\n.tdr{padding:6px 10px;cursor:pointer;display:flex;justify-content:space-between;gap:8px;font-size:11px}.tdr:hover{background:var(--bg4)}\n.tds{color:var(--gr);font-weight:700}.tdn{color:var(--t2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:120px}\n/* PAGE BODY */\n.pb{display:flex;flex-direction:column;height:100vh;overflow:hidden}\n.pc{flex:1;overflow-y:auto;overflow-x:hidden}\n/* SKELETON */\n.sk{background:linear-gradient(90deg,var(--bg3) 25%,var(--bg4) 50%,var(--bg3) 75%);background-size:200% 100%;animation:sk 1.2s ease infinite;border-radius:3px}\n@keyframes sk{0%{background-position:200% 0}100%{background-position:-200% 0}}\n.sk-line{height:14px;margin-bottom:8px}.sk-val{height:24px;margin-bottom:4px}\n/* SPIN */\n.spin-svg{display:inline-block;vertical-align:middle;margin-right:8px;width:44px;height:20px}\n/* EMPTY */\n.empty{text-align:center;padding:40px 20px;color:var(--t3);font-size:12px}\n\n.pc{padding:16px 20px;max-width:1000px;position:relative}\n.ins-card{background:var(--bg2);border:1px solid var(--b);border-radius:6px;padding:14px;margin-bottom:12px}\n.ins-head{display:flex;gap:12px;align-items:center;margin-bottom:10px}\n.ins-photo{width:46px;height:46px;border-radius:50%;object-fit:cover;border:2px solid var(--b2);background:var(--bg3);flex-shrink:0}\n.ins-nm{font-size:14px;font-weight:700;color:var(--t)}\n.ins-role{font-size:11px;color:var(--t2);margin-top:2px}\n.ins-tots{display:flex;gap:10px;font-size:11px;margin-top:4px}\n.tbl{width:100%;border-collapse:collapse;font-size:11px}\n.ins-card{overflow-x:auto}\n.tbl th{text-align:left;color:var(--t3);font-size:9px;letter-spacing:.07em;text-transform:uppercase;font-family:var(--fd);font-weight:700;padding:6px 8px;border-bottom:1px solid var(--b);background:var(--bg3)}\n.tbl td{padding:7px 8px;border-bottom:1px solid rgba(255,255,255,.025);color:var(--t)}\n.tbl tr:hover td{background:rgba(255,255,255,.02)}\n.ab{font-size:9px;font-weight:700;padding:2px 7px;border-radius:2px}\n.ab-BUY{background:rgba(0,229,160,.15);color:var(--gr)}.ab-SELL{background:rgba(255,77,109,.15);color:var(--rd)}.ab-FILING{background:rgba(0,149,255,.1);color:var(--bl)}\n</style>\n</head>\n<body>\n<div class=\"pb\">\n<div id=\"tape\"><div id=\"tape-inner\"></div></div>\n<div id=\"nav\"><a href=\"/\" id=\"nav-logo\" style=\"display:flex;align-items:center;gap:7px;text-decoration:none\">\n      <svg width=\"22\" height=\"22\" viewBox=\"0 0 64 64\" fill=\"none\">\n        <rect width=\"64\" height=\"64\" rx=\"10\" fill=\"rgba(0,229,160,0.15)\" stroke=\"rgba(0,229,160,0.4)\" stroke-width=\"2\"/>\n        <polyline points=\"8,44 20,28 30,36 42,18 56,22\" stroke=\"#00e5a0\" stroke-width=\"5\" fill=\"none\" stroke-linecap=\"round\" stroke-linejoin=\"round\"/>\n        <circle cx=\"42\" cy=\"18\" r=\"5\" fill=\"#00e5a0\"/>\n      </svg>\n      <span style=\"font-family:var(--fd);font-size:14px;font-weight:800;color:var(--gr);letter-spacing:.04em\">IST</span>\n    </a>\n    <a href=\"/chart\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"currentColor\"><rect x=\"1\" y=\"7\" width=\"2\" height=\"5\"/><rect x=\"5\" y=\"4\" width=\"2\" height=\"8\"/><rect x=\"9\" y=\"1\" width=\"2\" height=\"11\"/></svg>Chart</a><a href=\"/financials\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><polyline points=\"1,9 4,5 7,7 12,2\"/></svg>Financials</a><a href=\"/metrics\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"currentColor\"><circle cx=\"3\" cy=\"3\" r=\"1.5\"/><circle cx=\"10\" cy=\"3\" r=\"1.5\"/><circle cx=\"3\" cy=\"10\" r=\"1.5\"/><circle cx=\"10\" cy=\"10\" r=\"1.5\"/></svg>Metrics</a><a href=\"/insider\" class=\"nl on\"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><circle cx=\"6.5\" cy=\"4\" r=\"2.5\"/><path d=\"M1 12c0-3 2.5-5 5.5-5s5.5 2 5.5 5\"/></svg>Insider</a><a href=\"/congress\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><rect x=\"1\" y=\"5\" width=\"11\" height=\"7\" rx=\"1\"/><path d=\"M4 5V3a2.5 2.5 0 015 0v2\"/></svg>Congress</a><a href=\"/fairvalue\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><path d=\"M6.5 1L8 5h4l-3 2.5 1 4L6.5 9 3 11.5l1-4L1 5h4z\"/></svg>Fair Value</a><a href=\"/news\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><rect x=\"1\" y=\"1\" width=\"11\" height=\"11\" rx=\"1\"/><line x1=\"3\" y1=\"4\" x2=\"10\" y2=\"4\"/><line x1=\"3\" y1=\"7\" x2=\"10\" y2=\"7\"/></svg>News</a><a href=\"/livefeed\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"currentColor\"><circle cx=\"6.5\" cy=\"6.5\" r=\"2\"/><circle cx=\"6.5\" cy=\"6.5\" r=\"4\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1\" opacity=\".5\"/></svg>Live Feed</a></div>\n<script id=\"asset-nav-fix\">\ndocument.addEventListener('DOMContentLoaded', function(){\n  const t=(new URLSearchParams(window.location.search).get('t')||'').toUpperCase();\n  const isCrypto=t.endsWith('-USD')&&!['GLD','SLV','USO','TLT','HYG','LQD','GDX'].includes(t);\n  const isFuture=t.endsWith('=F');\n  const isIndex=t.startsWith('^')||t==='DX-Y.NYB';\n  if(!isCrypto&&!isFuture&&!isIndex) return;\n  const STOCK_ONLY=['/financials','/insider','/congress','/fairvalue'];\n  document.querySelectorAll('#nav a.nl').forEach(a=>{\n    const href=a.getAttribute('href')||'';\n    if(STOCK_ONLY.some(p=>href.includes(p))) a.style.display='none';\n  });\n});\n</script>\n<div id=\"tkbar\">\n  <span id=\"tk-sym\">—</span><span id=\"tk-name\">—</span>\n  <span id=\"tk-px\" style=\"color:var(--t2)\">—</span><span id=\"tk-chg\"></span>\n  <div id=\"tk-sw\"><input id=\"tk-si\" placeholder=\"Change ticker…\" autocomplete=\"off\" spellcheck=\"false\"><div id=\"tk-dr\"></div></div>\n</div>\n<div class=\"pc\"><div class=\"empty\"><svg class=\"spin-svg\" viewBox=\"0 0 44 20\" fill=\"none\"><polyline points=\"2,16 8,10 14,13 22,5 30,9 42,3\" stroke=\"#00e5a0\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-dasharray=\"120\" stroke-dashoffset=\"120\"><animate attributeName=\"stroke-dashoffset\" from=\"120\" to=\"0\" dur=\"1.2s\" repeatCount=\"indefinite\"/></polyline></svg>Loading SEC Form 4…</div></div>\n</div>\n<script>\nconst TSYMS=['SPY','QQQ','^GSPC','^IXIC','^VIX','CL=F','BZ=F','GC=F','SI=F','DX-Y.NYB','^TNX','BTC-USD'];\nconst TLBL={SPY:'SPY',QQQ:'QQQ','^GSPC':'S&P500','^IXIC':'NASDAQ','^VIX':'VIX','CL=F':'WTI','BZ=F':'BRENT','GC=F':'GOLD','SI=F':'SILVER','DX-Y.NYB':'DXY','^TNX':'US10Y','BTC-USD':'BTC'};\nconst socket=io();\nfunction buildTape(){\n  document.getElementById('tape-inner').innerHTML=[...TSYMS,...TSYMS].map(t=>{\n    const id='ti_'+t.replace(/[^a-zA-Z0-9]/g,'_');\n    return`<div class=\"ti\" id=\"${id}\" onclick=\"navToTape('${t}')\" style=\"cursor:pointer;\"><span class=\"ts\">${TLBL[t]||t}</span>&nbsp;<span>—</span>&nbsp;<span class=\"tc nc\">—</span></div>`;\n  }).join('');\n  socket.emit('subscribe',{tickers:TSYMS});\n}\nsocket.on('price_update',({prices})=>{\n  prices.forEach(p=>{\n    const id='ti_'+p.ticker.replace(/[^a-zA-Z0-9]/g,'_');\n    document.querySelectorAll('#'+id).forEach(el=>{\n      const cs=el.children;\n      if(cs[1]&&p.price!=null)cs[1].textContent='$'+p.price.toFixed(2);\n      if(cs[2]&&p.change_pct!=null){const s=p.change_pct>=0?'+':'';cs[2].textContent=s+p.change_pct.toFixed(2)+'%';cs[2].className='tc '+(p.change_pct>0?'up':p.change_pct<0?'dn':'nc');}\n    });\n    if(p.ticker===window.TK)updateTkBar(p);\n    if(typeof onPriceUpdate==='function')onPriceUpdate(p);\n  });\n});\nbuildTape();\nconst MEM={};const CACHE_TTL=300000;\nasync function api(url,ttl=CACHE_TTL,timeout=12000){\n  if(MEM[url]&&Date.now()-MEM[url].ts<ttl)return MEM[url].data;\n  try{\n    const ctrl=new AbortController();\n    const tid=setTimeout(()=>ctrl.abort(),timeout);\n    const r=await fetch(url,{signal:ctrl.signal});\n    clearTimeout(tid);\n    if(!r.ok)return null;\n    const d=await r.json();\n    MEM[url]={data:d,ts:Date.now()};\n    return d;\n  }catch{return null;}\n}\nfunction ssGet(k){try{const v=sessionStorage.getItem(k);if(!v)return null;const p=JSON.parse(v);if(Date.now()-p.ts>CACHE_TTL){sessionStorage.removeItem(k);return null;}return p.data;}catch{return null;}}\nfunction ssSet(k,d){try{sessionStorage.setItem(k,JSON.stringify({data:d,ts:Date.now()}));}catch{}}\nasync function getStockData(t){const k='full:'+t;const ss=ssGet(k);if(ss)return ss;const mc=MEM['/api/full/'+t];if(mc&&Date.now()-mc.ts<CACHE_TTL)return mc.data;const d=await api('/api/full/'+t,CACHE_TTL);if(d)ssSet(k,d);return d;}\nfunction updateTkBar(p){const pxEl=document.getElementById('tk-px'),chEl=document.getElementById('tk-chg');if(!pxEl)return;if(p.price!=null){pxEl.textContent='$'+p.price.toFixed(2);pxEl.style.color=p.change_pct>0?'var(--gr)':p.change_pct<0?'var(--rd)':'var(--t)';}if(p.change_pct!=null){const s=p.change_pct>=0?'+':'';chEl.textContent=s+p.change_pct.toFixed(2)+'%';chEl.style.cssText=`background:${p.change_pct>=0?'rgba(0,229,160,.12)':'rgba(255,77,109,.12)'};color:${p.change_pct>=0?'var(--gr)':'var(--rd)'};padding:2px 8px;border-radius:3px;`;}}\nasync function initTkBar(t){document.getElementById('tk-sym').textContent=t;const d=await api('/api/stock_fast/'+encodeURIComponent(t),6000);if(d){document.getElementById('tk-name').textContent=d.name||t;updateTkBar(d);}socket.emit('subscribe',{tickers:[t]});}\nlet sT;const siEl=document.getElementById('tk-si'),drEl=document.getElementById('tk-dr');\nif(siEl){siEl.addEventListener('input',function(){clearTimeout(sT);const v=this.value.trim();if(!v){drEl.style.display='none';return;}sT=setTimeout(async()=>{const d=await api('/api/universe?q='+encodeURIComponent(v)+'&limit=10',60000);if(!d?.results?.length){drEl.style.display='none';return;}drEl.innerHTML=d.results.map(x=>`<div class=\"tdr\" onclick=\"navTo('${x.ticker}')\"><span class=\"tds\">${x.ticker}</span><span class=\"tdn\">${x.name||''}</span></div>`).join('');drEl.style.display='block';},200);});document.addEventListener('click',e=>{if(!e.target.closest('#tk-sw'))drEl.style.display='none';});}\nfunction navTo(t){if(!t)return;if(siEl){siEl.value='';drEl.style.display='none';}window.location.href='/chart?t='+encodeURIComponent(t);}\n// navToTape: always go to chart page with the ticker\nfunction navToTape(t){\n  if(!t)return;\n  // Route to appropriate page based on asset type\n  window.location.href='/chart?t='+encodeURIComponent(t);\n}\nwindow.TK=(new URLSearchParams(window.location.search).get('t')||'NVDA').toUpperCase();\nconst fmtB=v=>{if(v==null)return'—';const a=Math.abs(v),s=v<0?'-':'';if(a>=1e12)return s+'$'+(a/1e12).toFixed(2)+'T';if(a>=1e9)return s+'$'+(a/1e9).toFixed(2)+'B';if(a>=1e6)return s+'$'+(a/1e6).toFixed(1)+'M';return s+'$'+a.toLocaleString('en-US',{maximumFractionDigits:0});};\nconst fmtPx=v=>v==null?'—':'$'+Number(v).toFixed(2);\nconst p1=v=>v==null?'—':(v*100).toFixed(1)+'%';\nconst fn=(v,d=2)=>v==null?'—':Number(v).toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d});\n\nasync function loadIns(t){\n  const _isCr=t.endsWith('-USD')&&!['GLD','SLV','USO','TLT'].includes(t);\n  const _isFu=t.endsWith('=F')||t.startsWith('^')||t==='DX-Y.NYB';\n  if(_isCr){window.location.href='/crypto?t='+encodeURIComponent(t);return;}\n  if(_isFu){window.location.href='/commodity?t='+encodeURIComponent(t);return;}\n  // Redirect non-stocks to their page immediately\n  const el=document.querySelector('.pc');\n  el.innerHTML='<div class=\"empty\"><svg class=\"spin-svg\" viewBox=\"0 0 44 20\" fill=\"none\"><polyline points=\"2,16 8,10 14,13 22,5 30,9 42,3\" stroke=\"#00e5a0\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-dasharray=\"120\" stroke-dashoffset=\"120\"><animate attributeName=\"stroke-dashoffset\" from=\"120\" to=\"0\" dur=\"1.2s\" repeatCount=\"indefinite\"/></polyline></svg>A carregar Form 4 do SEC…</div>';\n  const d=await api('/api/insider/'+t, 120000, 25000);\n  const trades=d?.insider_trades||[];\n\n  if(!trades.length){\n    let msg=`<div class=\"empty\">Sem Form 4 para ${t}`;\n    if(d?.sec_error) msg+=`<br><small style=\"color:var(--t3)\">${d.sec_error}</small>`;\n    else msg+=`<br><small style=\"color:var(--t3)\">ADRs e empresas estrangeiras podem não ter filings no SEC</small>`;\n    if(d?.sec_url) msg+=`<br><a href=\"${d.sec_url}\" target=\"_blank\" style=\"color:var(--bl);font-size:11px;text-decoration:none;margin-top:8px;display:inline-block\">Ver SEC →</a>`;\n    el.innerHTML=msg+'</div>';\n    return;\n  }\n\n  // Group by person using insider_profiles if available, else build from trades\n  const profiles = d.insider_profiles || {};\n  const byP = Object.keys(profiles).length ? profiles : {};\n  if(!Object.keys(byP).length){\n    trades.forEach(tr=>{\n      const n=tr.owner||'?';\n      if(!byP[n]) byP[n]={name:n,relation:tr.relation||'Insider',owner_cik:tr.owner_cik||'',trades:[],total_bought:0,total_sold:0,shares_held:null};\n      byP[n].trades=byP[n].trades||[];\n      byP[n].trades.push(tr);\n      if(tr.action==='BUY'  && tr.value) byP[n].total_bought=(byP[n].total_bought||0)+tr.value;\n      if(tr.action==='SELL' && tr.value) byP[n].total_sold  =(byP[n].total_sold||0)+tr.value;\n      if(tr.shares_after) byP[n].shares_held=tr.shares_after;\n    });\n  }\n\n  // Count meaningful trades\n  const meaningful = trades.filter(tr=>tr.action==='BUY'||tr.action==='SELL');\n  const totalBuys  = meaningful.filter(tr=>tr.action==='BUY').length;\n  const totalSells = meaningful.filter(tr=>tr.action==='SELL').length;\n\n  let html=`<div style=\"display:flex;gap:10px;margin-bottom:16px;flex-wrap:wrap\">\n    <div style=\"background:var(--bg2);border:1px solid var(--b);border-radius:4px;padding:8px 16px;text-align:center\">\n      <div style=\"font-family:var(--fd);font-size:20px;font-weight:800\">${trades.length}</div>\n      <div style=\"font-size:9px;color:var(--t3);font-family:var(--fd);font-weight:700;text-transform:uppercase;letter-spacing:.07em\">Total</div>\n    </div>\n    <div style=\"background:var(--bg2);border:1px solid var(--b);border-radius:4px;padding:8px 16px;text-align:center\">\n      <div style=\"font-family:var(--fd);font-size:20px;font-weight:800;color:var(--gr)\">${totalBuys}</div>\n      <div style=\"font-size:9px;color:var(--t3);font-family:var(--fd);font-weight:700;text-transform:uppercase;letter-spacing:.07em\">Compras</div>\n    </div>\n    <div style=\"background:var(--bg2);border:1px solid var(--b);border-radius:4px;padding:8px 16px;text-align:center\">\n      <div style=\"font-family:var(--fd);font-size:20px;font-weight:800;color:var(--rd)\">${totalSells}</div>\n      <div style=\"font-size:9px;color:var(--t3);font-family:var(--fd);font-weight:700;text-transform:uppercase;letter-spacing:.07em\">Vendas</div>\n    </div>\n    <div style=\"background:var(--bg2);border:1px solid rgba(240,192,96,.2);border-radius:4px;padding:8px 16px;text-align:center\">\n      <div style=\"font-family:var(--fd);font-size:11px;font-weight:700;color:var(--yl)\">⚠ Até 2 dias</div>\n      <div style=\"font-size:9px;color:var(--t3)\">para reportar ao SEC</div>\n    </div>\n  </div>`;\n\n  for(const per of Object.values(byP)){\n    const perTrades = per.trades||[];\n    if(!perTrades.length) continue;\n    const name     = per.name||per.owner||'?';\n    const relation = per.relation||'Insider/Executive';\n    const cik      = per.owner_cik||per.cik||'';\n    const bought   = per.total_bought||0;\n    const sold     = per.total_sold||0;\n    const held     = per.shares_held||null;\n    const av       = `https://ui-avatars.com/api/?name=${encodeURIComponent(name)}&size=96&background=161d29&color=00e5a0&bold=true&format=svg`;\n\n    // Estimate portfolio value: shares held × current price\n    const currentPx = (await api('/api/stock_fast/'+encodeURIComponent(t),6000))?.price;\n    const portVal   = (held && currentPx) ? held * currentPx : null;\n\n    html+=`<div class=\"ins-card\">\n      <div class=\"ins-head\">\n        <img class=\"ins-photo\" src=\"${av}\" alt=\"${name}\" onerror=\"this.src='${av}'\">\n        <div style=\"flex:1;min-width:0\">\n          <div class=\"ins-nm\">${name}</div>\n          <div class=\"ins-role\">${relation}</div>\n          <div class=\"ins-tots\">\n            ${bought>0?`<span class=\"up\">▲ Comprou: ${fmtB(bought)}</span>`:''}\n            ${sold>0?`<span style=\"color:var(--rd)\">▼ Vendeu: ${fmtB(sold)}</span>`:''}\n          </div>\n          ${held!=null?`<div style=\"font-size:10px;color:var(--t2);margin-top:3px\">\n            Posição actual: <b style=\"color:var(--t)\">${Math.round(held).toLocaleString()} acções</b>\n            ${portVal?`· Est. Portfolio: <b style=\"color:var(--gr)\">${fmtB(portVal)}</b>`:''}\n          </div>`:''}\n        </div>\n        <div style=\"display:flex;gap:6px;flex-shrink:0;align-items:flex-start\">\n          ${cik?`<a href=\"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=${cik}&type=4&owner=include&count=40\" target=\"_blank\" style=\"font-size:10px;color:var(--bl);border:1px solid var(--bl);padding:4px 9px;border-radius:3px;text-decoration:none\">SEC →</a>`:''}\n        </div>\n      </div>\n      <table class=\"tbl\">\n        <thead><tr>\n          <th>Acção</th><th>Título</th><th>Data</th><th>Acções</th><th>Preço</th><th>Valor</th><th>Posição Após</th><th>Form</th>\n        </tr></thead>\n        <tbody>`;\n\n    perTrades.forEach(tr=>{\n      const isDerivative = tr.derivative || tr.source_type?.includes('Derivative');\n      const actionColor  = tr.action==='BUY'?'var(--gr)':tr.action==='SELL'?'var(--rd)':'var(--t2)';\n      const note         = tr.note ? `<div style=\"font-size:9px;color:var(--t3)\">${tr.note}</div>` : '';\n      html+=`<tr>\n        <td><span class=\"ab ab-${tr.action||'FILING'}\">${tr.action||'FILING'}</span>\n            ${isDerivative?`<span style=\"font-size:8px;color:var(--pu);margin-left:3px\">OPT</span>`:''}\n        </td>\n        <td style=\"color:var(--t2);font-size:10px\">${tr.security||'Common Stock'}</td>\n        <td style=\"color:var(--t2)\">${tr.date||'—'}</td>\n        <td style=\"color:var(--t)\">${tr.shares!=null?Math.round(tr.shares).toLocaleString():'—'}</td>\n        <td style=\"color:var(--t)\">${tr.price!=null&&tr.price>0?'$'+tr.price.toFixed(2):'—'}</td>\n        <td style=\"font-weight:700;color:${actionColor}\">${tr.value!=null?fmtB(tr.value):'—'}${note}</td>\n        <td style=\"color:${tr.shares_after?'var(--t2)':'var(--t3)'}\">${tr.shares_after!=null?Math.round(tr.shares_after).toLocaleString()+' sh':'—'}</td>\n        <td>${tr.filing_url?`<a href=\"${tr.filing_url}\" target=\"_blank\" style=\"color:var(--bl);text-decoration:none;font-size:10px\">Form 4 →</a>`:'—'}</td>\n      </tr>`;\n    });\n    html+=`</tbody></table></div>`;\n  }\n\n  if(d?.sec_url) html+=`<div style=\"text-align:center;padding:12px 0\"><a href=\"${d.sec_url}\" target=\"_blank\" style=\"font-size:11px;color:var(--bl);text-decoration:none\">Ver todos os filings no SEC →</a></div>`;\n  el.innerHTML=html;\n\n  // Async: try to get real photos from Wikipedia\n  for(const per of Object.values(byP)){\n    const name=per.name||per.owner||'';\n    if(!name) continue;\n    fetch('/api/insider_photo/'+encodeURIComponent(name))\n      .then(r=>r.json())\n      .then(ph=>{\n        if(ph?.url && ph.source==='wikipedia'){\n          document.querySelectorAll('.ins-photo').forEach(img=>{\n            if(img.alt===name) img.src=ph.url;\n          });\n        }\n      }).catch(()=>{});\n  }\n}\nfunction onTickerChange(t){loadIns(t);}\nloadIns(window.TK);\n\ninitTkBar(window.TK);\n\n// Fix nav links to carry current ticker\nfunction fixNavLinks(){\n  const t=window.TK;\n  if(!t)return;\n  document.querySelectorAll('#nav a.nl').forEach(a=>{\n    const href=a.getAttribute('href');\n    if(href&&href!=='/'&&!href.startsWith('javascript')){\n      const u=new URL(href,window.location.origin);\n      u.searchParams.set('t',t);\n      a.setAttribute('href',u.toString());\n    }\n  });\n}\nfixNavLinks();\n// Also update nav links whenever ticker changes\n// navTo already calls fixNavLinks()\n</script>\n</body>\n</html>",
    "terminal.html": "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"UTF-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\n<title>IST · Insider Signal Terminal</title>\n<script src=\"https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.7.5/socket.io.min.js\"></script>\n<link href=\"https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600&family=Syne:wght@400;700;800&display=swap\" rel=\"stylesheet\">\n<style>\n*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}\n:root{--bg:#060a0e;--bg2:#0a0f14;--bg3:#0d1219;--bg4:#111820;--b:#192230;--b2:#1e2d3d;--t:#c9d1d9;--t2:#8b949e;--t3:#3d4f61;--gr:#00e5a0;--rd:#ff4d6d;--bl:#0095ff;--yl:#f0c060;--pu:#a78bfa;--or:#ff8c42;--fm:'JetBrains Mono',monospace;--fd:'Syne',sans-serif;}\nhtml,body{height:100%;background:var(--bg);color:var(--t);font-family:var(--fm);font-size:12px;overflow:hidden;}\n.up{color:var(--gr)}.dn{color:var(--rd)}.nc{color:var(--t2)}\n\n/* ── TOP BAR ─────────────────────────────────── */\n#tape-bar{height:26px;background:var(--bg2);border-bottom:1px solid var(--b);overflow:hidden;display:flex;align-items:center;flex-shrink:0;}#topbar{height:38px;background:var(--bg2);border-bottom:1px solid var(--b);display:flex;align-items:center;padding:0 12px;gap:6px;flex-shrink:0;}.topbar-tape{height:26px;overflow:hidden;border-bottom:1px solid rgba(255,255,255,.04);display:flex;}.topbar-nav{height:38px;display:flex;align-items:center;padding:0 12px;gap:4px;}\n.logo{display:flex;align-items:center;gap:7px;text-decoration:none;padding-right:16px;border-right:1px solid var(--b);margin-right:8px;flex-shrink:0;}\n.logo-txt{font-family:var(--fd);font-size:14px;font-weight:800;color:var(--gr);}\n#tape-inner{display:flex;white-space:nowrap;animation:tape 60s linear infinite;user-select:none;flex:1;overflow:hidden;}\n@keyframes tape{from{transform:translateX(0)}to{transform:translateX(-50%)}}\n.ti{display:inline-flex;align-items:center;gap:5px;padding:0 14px;height:32px;border-right:1px solid var(--b);font-size:11px;cursor:pointer;flex-shrink:0;}\n.ti:hover{background:var(--bg3)}.ts{color:var(--t2);font-weight:600;font-size:10px}.tc{font-size:10px}\n.top-actions{display:flex;align-items:center;gap:6px;padding-left:12px;border-left:1px solid var(--b);flex-shrink:0;}\n.ta-btn{padding:3px 10px;border-radius:3px;font-size:10px;font-family:var(--fd);font-weight:700;letter-spacing:.04em;text-transform:uppercase;color:var(--t2);text-decoration:none;cursor:pointer;transition:all .12s;border:1px solid transparent;}\n.ta-btn:hover{color:var(--gr);border-color:rgba(0,229,160,.2);background:rgba(0,229,160,.05);}\n\n/* ── MAIN LAYOUT ─────────────────────────────── */\n#main{height:calc(100vh - 64px);display:grid;grid-template-columns:220px 1fr 260px;grid-template-rows:1fr;overflow:hidden;}\n\n/* ── LEFT PANEL: Watchlist + indices ─────────── */\n#left{background:var(--bg2);border-right:1px solid var(--b);display:flex;flex-direction:column;overflow:hidden;}\n.panel-hdr{padding:8px 12px;border-bottom:1px solid var(--b);display:flex;align-items:center;justify-content:space-between;}\n.panel-title{font-family:var(--fd);font-size:9px;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:var(--t3);}\n.live-dot{width:6px;height:6px;border-radius:50%;background:var(--gr);animation:blink 1.4s ease infinite;flex-shrink:0;}\n@keyframes blink{0%,100%{opacity:1}50%{opacity:.3}}\n.panel-scroll{flex:1;overflow-y:auto;}\n.panel-scroll::-webkit-scrollbar{width:3px}.panel-scroll::-webkit-scrollbar-thumb{background:var(--b2);border-radius:2px}\n/* Ticker rows */\n.tk-row{display:flex;justify-content:space-between;align-items:center;padding:7px 12px;cursor:pointer;border-bottom:1px solid rgba(255,255,255,.03);transition:background .1s;}\n.tk-row:hover{background:var(--bg3)}.tk-sym{font-family:var(--fd);font-size:12px;font-weight:700;color:var(--gr);}\n.tk-name{font-size:9px;color:var(--t3);margin-top:1px;}\n.tk-right{text-align:right;}.tk-px{font-size:12px;font-weight:600;color:var(--t);}\n.tk-ch{font-size:10px;margin-top:1px;}\n/* Section divider */\n.sec-div{padding:5px 12px 3px;font-family:var(--fd);font-size:8px;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:var(--t3);background:var(--bg);border-top:1px solid var(--b);border-bottom:1px solid var(--b);}\n\n/* ── CENTER: Search + Market overview ────────── */\n#center{display:flex;flex-direction:column;overflow:hidden;background:var(--bg);}\n/* Search bar */\n#search-bar{padding:10px 16px;border-bottom:1px solid var(--b);background:var(--bg2);display:flex;align-items:center;gap:10px;flex-shrink:0;}\n.search-wrap{position:relative;flex:1;max-width:500px;}\n#srch{width:100%;background:var(--bg3);border:1px solid var(--b2);color:var(--t);font-family:var(--fm);font-size:12px;padding:7px 12px 7px 32px;border-radius:5px;outline:none;transition:border .15s;}\n#srch:focus{border-color:var(--gr);}\n.srch-icon{position:absolute;left:10px;top:50%;transform:translateY(-50%);color:var(--t3);font-size:12px;pointer-events:none;}\n#srch-drop{position:absolute;top:calc(100%+4px);left:0;right:0;background:var(--bg3);border:1px solid var(--b2);border-radius:6px;z-index:999;display:none;max-height:240px;overflow-y:auto;box-shadow:0 8px 32px rgba(0,0,0,.6);}\n.dr{padding:8px 12px;cursor:pointer;display:flex;justify-content:space-between;gap:8px;font-size:11px;border-bottom:1px solid rgba(255,255,255,.03);}\n.dr:hover{background:var(--bg4)}.dr-sym{color:var(--gr);font-weight:700;flex-shrink:0;min-width:60px;}.dr-name{color:var(--t2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}\n.mkt-status-chip{padding:4px 10px;border-radius:3px;font-size:9px;font-family:var(--fd);font-weight:700;display:flex;align-items:center;gap:5px;flex-shrink:0;}\n\n/* Market overview grid */\n#center-scroll{flex:1;overflow-y:auto;padding:12px;}\n#center-scroll::-webkit-scrollbar{width:3px}#center-scroll::-webkit-scrollbar-thumb{background:var(--b2);border-radius:2px}\n.grid-label{font-family:var(--fd);font-size:8px;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:var(--t3);margin-bottom:8px;}\n/* Big feature cards */\n.big-grid{display:grid;grid-template-columns:1fr 1fr 1fr;gap:6px;margin-bottom:14px;}\n.big-card{background:var(--bg2);border:1px solid var(--b);border-radius:6px;padding:12px 14px;cursor:pointer;transition:all .15s;}\n.big-card:hover{border-color:var(--gr);background:var(--bg3);transform:translateY(-1px);}\n.bc-sym{font-family:var(--fd);font-size:10px;font-weight:700;color:var(--t2);margin-bottom:6px;}\n.bc-val{font-family:var(--fd);font-size:20px;font-weight:800;color:var(--t);line-height:1;}\n.bc-chg{font-size:11px;font-weight:600;margin-top:4px;}\n/* Stocks movers */\n.movers-grid{display:grid;grid-template-columns:1fr 1fr;gap:5px;margin-bottom:14px;}\n.mv{background:var(--bg2);border:1px solid var(--b);border-radius:5px;padding:8px 10px;cursor:pointer;transition:all .15s;display:flex;justify-content:space-between;align-items:center;}\n.mv:hover{border-color:rgba(0,229,160,.2);background:var(--bg3);}\n.mv-l .sym{font-family:var(--fd);font-size:12px;font-weight:800;color:var(--gr);}\n.mv-l .nm{font-size:9px;color:var(--t3);margin-top:1px;}\n.mv-r{text-align:right;}.mv-r .px{font-size:12px;font-weight:700;color:var(--t);}\n.mv-r .ch{font-size:10px;font-weight:600;margin-top:1px;}\n/* Crypto strip */\n.crypto-row{display:flex;gap:5px;margin-bottom:14px;overflow-x:auto;padding-bottom:2px;}\n.crypto-row::-webkit-scrollbar{height:2px}\n.cy{background:var(--bg2);border:1px solid var(--b);border-radius:5px;padding:8px 10px;cursor:pointer;transition:all .15s;flex-shrink:0;min-width:90px;}\n.cy:hover{border-color:var(--yl);}.cy .sym{font-family:var(--fd);font-size:11px;font-weight:800;color:var(--yl);}\n.cy .px{font-size:11px;font-weight:600;color:var(--t);margin-top:3px;}.cy .ch{font-size:9px;margin-top:1px;}\n/* Tools grid */\n.tools-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:5px;}\n.tool{background:var(--bg2);border:1px solid var(--b);border-radius:5px;padding:10px;cursor:pointer;text-decoration:none;transition:all .15s;text-align:center;}\n.tool:hover{border-color:rgba(0,229,160,.25);background:var(--bg3);}\n.tool-ic{font-size:18px;margin-bottom:4px;}\n.tool-nm{font-family:var(--fd);font-size:9px;font-weight:700;color:var(--t);letter-spacing:.05em;}\n\n/* ── RIGHT PANEL: Commodities + hot ─────────── */\n#right{background:var(--bg2);border-left:1px solid var(--b);display:flex;flex-direction:column;overflow:hidden;}\n.comm-card{display:flex;justify-content:space-between;align-items:center;padding:7px 12px;cursor:pointer;border-bottom:1px solid rgba(255,255,255,.03);transition:background .1s;}\n.comm-card:hover{background:var(--bg3)}.cc-sym{font-family:var(--fd);font-size:11px;font-weight:700;color:var(--or);}\n.cc-name{font-size:9px;color:var(--t3);margin-top:1px;}.cc-right{text-align:right;}\n.cc-px{font-size:11px;font-weight:600;color:var(--t);}.cc-ch{font-size:9px;margin-top:1px;}\n/* ETF section */\n.etf-row{display:flex;justify-content:space-between;align-items:center;padding:6px 12px;cursor:pointer;border-bottom:1px solid rgba(255,255,255,.03);transition:background .1s;}\n.etf-row:hover{background:var(--bg3)}.ef-sym{font-family:var(--fd);font-size:11px;font-weight:700;color:var(--pu);}\n.ef-name{font-size:9px;color:var(--t3);margin-top:1px;}.ef-right{text-align:right;}\n.ef-px{font-size:11px;font-weight:600;color:var(--t);}.ef-ch{font-size:9px;margin-top:1px;}\n</style>\n</head>\n<body>\n<!-- TAPE BAR -->\n<div id=\"tape-bar\"><div id=\"tape-inner\"></div></div>\n<!-- NAV BAR -->\n<div id=\"topbar\">\n  <a href=\"/\" class=\"logo\">\n    <svg width=\"18\" height=\"18\" viewBox=\"0 0 64 64\" fill=\"none\"><rect width=\"64\" height=\"64\" rx=\"8\" fill=\"rgba(0,229,160,0.15)\" stroke=\"rgba(0,229,160,0.4)\" stroke-width=\"2\"/><polyline points=\"8,44 20,28 30,36 42,18 56,22\" stroke=\"#00e5a0\" stroke-width=\"5\" fill=\"none\" stroke-linecap=\"round\" stroke-linejoin=\"round\"/><circle cx=\"42\" cy=\"18\" r=\"4\" fill=\"#00e5a0\"/></svg>\n    <span class=\"logo-txt\">IST</span>\n  </a>\n  <div class=\"top-actions\">\n    <a href=\"/chart?t=NVDA\" class=\"ta-btn\">Chart</a>\n    <a href=\"/livefeed\" class=\"ta-btn\">Live Feed</a>\n    <a href=\"/crypto?t=BTC-USD\" class=\"ta-btn\">Crypto</a>\n  </div>\n</div>\n\n<!-- MAIN 3-COLUMN LAYOUT -->\n<div id=\"main\">\n\n  <!-- LEFT: Indices + Watchlist -->\n  <div id=\"left\">\n    <div class=\"panel-hdr\">\n      <span class=\"panel-title\">&#205;ndices &amp; Watchlist</span>\n      <div class=\"live-dot\"></div>\n    </div>\n    <div class=\"panel-scroll\">\n      <div class=\"sec-div\">&#205;ndices</div>\n      <div class=\"tk-row\" onclick=\"go('^GSPC')\"><div><div class=\"tk-sym\">SP500</div></div><div class=\"tk-right\"><div class=\"tk-px\" id=\"p_GSPC\">—</div><div class=\"tk-ch nc\" id=\"c_GSPC\">—</div></div></div>\n      <div class=\"tk-row\" onclick=\"go('^IXIC')\"><div><div class=\"tk-sym\">NASDAQ</div></div><div class=\"tk-right\"><div class=\"tk-px\" id=\"p_IXIC\">—</div><div class=\"tk-ch nc\" id=\"c_IXIC\">—</div></div></div>\n      <div class=\"tk-row\" onclick=\"go('^DJI')\"><div><div class=\"tk-sym\">DOW</div></div><div class=\"tk-right\"><div class=\"tk-px\" id=\"p_DJI\">—</div><div class=\"tk-ch nc\" id=\"c_DJI\">—</div></div></div>\n      <div class=\"tk-row\" onclick=\"go('^RUT')\"><div><div class=\"tk-sym\">RUSSELL</div></div><div class=\"tk-right\"><div class=\"tk-px\" id=\"p_RUT\">—</div><div class=\"tk-ch nc\" id=\"c_RUT\">—</div></div></div>\n      <div class=\"tk-row\" onclick=\"go('^VIX')\"><div><div class=\"tk-sym\">VIX</div></div><div class=\"tk-right\"><div class=\"tk-px\" id=\"p_VIX\">—</div><div class=\"tk-ch nc\" id=\"c_VIX\">—</div></div></div>\n      <div class=\"tk-row\" onclick=\"go('^TNX')\"><div><div class=\"tk-sym\">US 10Y</div></div><div class=\"tk-right\"><div class=\"tk-px\" id=\"p_TNX\">—</div><div class=\"tk-ch nc\" id=\"c_TNX\">—</div></div></div>\n      <div class=\"tk-row\" onclick=\"go('^TYX')\"><div><div class=\"tk-sym\">US 30Y</div></div><div class=\"tk-right\"><div class=\"tk-px\" id=\"p_TYX\">—</div><div class=\"tk-ch nc\" id=\"c_TYX\">—</div></div></div>\n      <div class=\"tk-row\" onclick=\"go('DX-Y.NYB')\"><div><div class=\"tk-sym\">DXY</div></div><div class=\"tk-right\"><div class=\"tk-px\" id=\"p_DXYNB\">—</div><div class=\"tk-ch nc\" id=\"c_DXYNB\">—</div></div></div>\n\n      <div class=\"sec-div\">Top Stocks</div>\n      <div id=\"wl-list\"></div>\n    </div>\n  </div>\n\n  <!-- CENTER: Search + overview -->\n  <div id=\"center\">\n    <div id=\"search-bar\">\n      <div class=\"search-wrap\">\n        <span class=\"srch-icon\">&#128269;</span>\n        <input id=\"srch\" placeholder=\"Pesquisa ticker ou empresa... (Enter para abrir)\" autocomplete=\"off\" spellcheck=\"false\">\n        <div id=\"srch-drop\"></div>\n      </div>\n      <div class=\"mkt-status-chip\" id=\"mkt-chip\" style=\"background:rgba(255,255,255,.04);color:#484f58;border:1px solid rgba(255,255,255,.06);cursor:pointer\" onclick=\"toggleMktTip()\">\n        <span id=\"mkt-dot\" style=\"width:6px;height:6px;border-radius:50%;background:#484f58\"></span>\n        <span id=\"mkt-lbl\">—</span>\n      </div>\n      <span style=\"font-size:9px;color:var(--t3);flex-shrink:0\" id=\"mkt-time\"></span>\n    </div>\n    <div id=\"center-scroll\">\n      <div class=\"grid-label\">Mercados Principais</div>\n      <div class=\"big-grid\">\n        <div class=\"big-card\" onclick=\"go('^GSPC')\"><div class=\"bc-sym\">S&amp;P 500</div><div class=\"bc-val\" id=\"bv_GSPC\">—</div><div class=\"bc-chg nc\" id=\"bc_GSPC\">—</div></div>\n        <div class=\"big-card\" onclick=\"go('^IXIC')\"><div class=\"bc-sym\">NASDAQ</div><div class=\"bc-val\" id=\"bv_IXIC\">—</div><div class=\"bc-chg nc\" id=\"bc_IXIC\">—</div></div>\n        <div class=\"big-card\" onclick=\"go('^VIX')\"><div class=\"bc-sym\">VIX</div><div class=\"bc-val\" id=\"bv_VIX\">—</div><div class=\"bc-chg nc\" id=\"bc_VIX\">—</div></div>\n        <div class=\"big-card\" onclick=\"go('GC=F')\"><div class=\"bc-sym\">GOLD</div><div class=\"bc-val\" id=\"bv_GCF\">—</div><div class=\"bc-chg nc\" id=\"bc_GCF\">—</div></div>\n        <div class=\"big-card\" onclick=\"go('BTC-USD')\"><div class=\"bc-sym\">BITCOIN</div><div class=\"bc-val\" id=\"bv_BTCUSD\">—</div><div class=\"bc-chg nc\" id=\"bc_BTCUSD\">—</div></div>\n        <div class=\"big-card\" onclick=\"go('^TNX')\"><div class=\"bc-sym\">US 10Y</div><div class=\"bc-val\" id=\"bv_TNX\">—</div><div class=\"bc-chg nc\" id=\"bc_TNX\">—</div></div>\n      </div>\n\n      <div class=\"grid-label\">Ac&ccedil;&otilde;es EUA</div>\n      <div class=\"movers-grid\" id=\"stocks-grid\"></div>\n\n      <div class=\"grid-label\">Crypto</div>\n      <div class=\"crypto-row\" id=\"crypto-row\"></div>\n\n      <div class=\"grid-label\">Ferramentas</div>\n      <div class=\"tools-grid\">\n        <a href=\"/financials?t=NVDA\" class=\"tool\"><div class=\"tool-ic\">📈</div><div class=\"tool-nm\">Financials</div></a>\n        <a href=\"/metrics?t=NVDA\" class=\"tool\"><div class=\"tool-ic\">⚡</div><div class=\"tool-nm\">Metrics</div></a>\n        <a href=\"/insider?t=NVDA\" class=\"tool\"><div class=\"tool-ic\">🔍</div><div class=\"tool-nm\">Insider</div></a>\n        <a href=\"/congress?t=NVDA\" class=\"tool\"><div class=\"tool-ic\">🏛</div><div class=\"tool-nm\">Congress</div></a>\n        <a href=\"/fairvalue?t=NVDA\" class=\"tool\"><div class=\"tool-ic\">⭐</div><div class=\"tool-nm\">Fair Value</div></a>\n        <a href=\"/news?t=NVDA\" class=\"tool\"><div class=\"tool-ic\">📰</div><div class=\"tool-nm\">News</div></a>\n        <a href=\"/livefeed\" class=\"tool\"><div class=\"tool-ic\">🔴</div><div class=\"tool-nm\">Live Feed</div></a>\n        \n      </div>\n    </div>\n  </div>\n\n  <!-- RIGHT: Commodities + ETFs -->\n  <div id=\"right\">\n    <div class=\"panel-hdr\">\n      <span class=\"panel-title\">Commodities &amp; ETFs</span>\n      <div class=\"live-dot\"></div>\n    </div>\n    <div class=\"panel-scroll\">\n      <div class=\"sec-div\">Commodities</div>\n      <div class=\"comm-card\" onclick=\"go('GC=F')\"><div><div class=\"cc-sym\">GOLD</div><div class=\"cc-name\">Ouro</div></div><div class=\"cc-right\"><div class=\"cc-px\" id=\"p_GCF\">—</div><div class=\"cc-ch nc\" id=\"c_GCF\">—</div></div></div>\n      <div class=\"comm-card\" onclick=\"go('SI=F')\"><div><div class=\"cc-sym\">SILVER</div><div class=\"cc-name\">Prata</div></div><div class=\"cc-right\"><div class=\"cc-px\" id=\"p_SIF\">—</div><div class=\"cc-ch nc\" id=\"c_SIF\">—</div></div></div>\n      <div class=\"comm-card\" onclick=\"go('CL=F')\"><div><div class=\"cc-sym\">WTI</div><div class=\"cc-name\">Petr&#243;leo WTI</div></div><div class=\"cc-right\"><div class=\"cc-px\" id=\"p_CLF\">—</div><div class=\"cc-ch nc\" id=\"c_CLF\">—</div></div></div>\n      <div class=\"comm-card\" onclick=\"go('BZ=F')\"><div><div class=\"cc-sym\">BRENT</div><div class=\"cc-name\">Petr&#243;leo Brent</div></div><div class=\"cc-right\"><div class=\"cc-px\" id=\"p_BZF\">—</div><div class=\"cc-ch nc\" id=\"c_BZF\">—</div></div></div>\n      <div class=\"comm-card\" onclick=\"go('NG=F')\"><div><div class=\"cc-sym\">GAS</div><div class=\"cc-name\">G&#225;s Natural</div></div><div class=\"cc-right\"><div class=\"cc-px\" id=\"p_NGF\">—</div><div class=\"cc-ch nc\" id=\"c_NGF\">—</div></div></div>\n      <div class=\"comm-card\" onclick=\"go('HG=F')\"><div><div class=\"cc-sym\">COPPER</div><div class=\"cc-name\">Cobre</div></div><div class=\"cc-right\"><div class=\"cc-px\" id=\"p_HGF\">—</div><div class=\"cc-ch nc\" id=\"c_HGF\">—</div></div></div>\n      <div class=\"comm-card\" onclick=\"go('ZC=F')\"><div><div class=\"cc-sym\">CORN</div><div class=\"cc-name\">Milho</div></div><div class=\"cc-right\"><div class=\"cc-px\" id=\"p_ZCF\">—</div><div class=\"cc-ch nc\" id=\"c_ZCF\">—</div></div></div>\n      <div class=\"comm-card\" onclick=\"go('ZW=F')\"><div><div class=\"cc-sym\">WHEAT</div><div class=\"cc-name\">Trigo</div></div><div class=\"cc-right\"><div class=\"cc-px\" id=\"p_ZWF\">—</div><div class=\"cc-ch nc\" id=\"c_ZWF\">—</div></div></div>\n\n      <div class=\"sec-div\">ETFs</div>\n      <div class=\"etf-row\" onclick=\"go('SPY')\"><div><div class=\"ef-sym\">SPY</div><div class=\"ef-name\">S&amp;P 500 ETF</div></div><div class=\"ef-right\"><div class=\"ef-px\" id=\"p_SPY\">—</div><div class=\"ef-ch nc\" id=\"c_SPY\">—</div></div></div>\n      <div class=\"etf-row\" onclick=\"go('QQQ')\"><div><div class=\"ef-sym\">QQQ</div><div class=\"ef-name\">NASDAQ 100 ETF</div></div><div class=\"ef-right\"><div class=\"ef-px\" id=\"p_QQQ\">—</div><div class=\"ef-ch nc\" id=\"c_QQQ\">—</div></div></div>\n      <div class=\"etf-row\" onclick=\"go('IWM')\"><div><div class=\"ef-sym\">IWM</div><div class=\"ef-name\">Russell 2000</div></div><div class=\"ef-right\"><div class=\"ef-px\" id=\"p_IWM\">—</div><div class=\"ef-ch nc\" id=\"c_IWM\">—</div></div></div>\n      <div class=\"etf-row\" onclick=\"go('GLD')\"><div><div class=\"ef-sym\">GLD</div><div class=\"ef-name\">Gold ETF</div></div><div class=\"ef-right\"><div class=\"ef-px\" id=\"p_GLD\">—</div><div class=\"ef-ch nc\" id=\"c_GLD\">—</div></div></div>\n      <div class=\"etf-row\" onclick=\"go('TLT')\"><div><div class=\"ef-sym\">TLT</div><div class=\"ef-name\">20Y Treasury ETF</div></div><div class=\"ef-right\"><div class=\"ef-px\" id=\"p_TLT\">—</div><div class=\"ef-ch nc\" id=\"c_TLT\">—</div></div></div>\n      <div class=\"etf-row\" onclick=\"go('ARKK')\"><div><div class=\"ef-sym\">ARKK</div><div class=\"ef-name\">ARK Innovation</div></div><div class=\"ef-right\"><div class=\"ef-px\" id=\"p_ARKK\">—</div><div class=\"ef-ch nc\" id=\"c_ARKK\">—</div></div></div>\n      <div class=\"etf-row\" onclick=\"go('XLE')\"><div><div class=\"ef-sym\">XLE</div><div class=\"ef-name\">Energy ETF</div></div><div class=\"ef-right\"><div class=\"ef-px\" id=\"p_XLE\">—</div><div class=\"ef-ch nc\" id=\"c_XLE\">—</div></div></div>\n      <div class=\"etf-row\" onclick=\"go('XLK')\"><div><div class=\"ef-sym\">XLK</div><div class=\"ef-name\">Tech ETF</div></div><div class=\"ef-right\"><div class=\"ef-px\" id=\"p_XLK\">—</div><div class=\"ef-ch nc\" id=\"c_XLK\">—</div></div></div>\n      <div class=\"etf-row\" onclick=\"go('TQQQ')\"><div><div class=\"ef-sym\">TQQQ</div><div class=\"ef-name\">3x NASDAQ Bull</div></div><div class=\"ef-right\"><div class=\"ef-px\" id=\"p_TQQQ\">—</div><div class=\"ef-ch nc\" id=\"c_TQQQ\">—</div></div></div>\n    </div>\n  </div>\n</div>\n\n<script>\nfunction go(t){window.location.href='/chart?t='+encodeURIComponent(t);}\nfunction fmtPx(v){if(v==null)return'—';if(v>=10000)return'$'+Math.round(v).toLocaleString('en-US');if(v>=1000)return'$'+Number(v).toFixed(0);if(v>=100)return'$'+Number(v).toFixed(2);if(v>=1)return'$'+Number(v).toFixed(2);return'$'+Number(v).toFixed(4);}\nfunction fmtPct(v){return v==null?'':(v>=0?'+':'')+Number(v).toFixed(2)+'%';}\nfunction ccls(v){return v==null?'nc':v>0?'up':'dn';}\n\n// ID mapping ticker -> element ID suffix\nconst ID={'^GSPC':'GSPC','^IXIC':'IXIC','^DJI':'DJI','^RUT':'RUT','^VIX':'VIX','^TNX':'TNX','^TYX':'TYX','DX-Y.NYB':'DXYNB','GC=F':'GCF','SI=F':'SIF','CL=F':'CLF','BZ=F':'BZF','NG=F':'NGF','HG=F':'HGF','ZC=F':'ZCF','ZW=F':'ZWF','BTC-USD':'BTCUSD'};\nfunction eid(t){return ID[t]||t.replace(/[^a-zA-Z0-9]/g,'_').replace(/-USD$/,'').replace(/\\^/,'');}\n\n// Stock watchlist\nconst STOCKS=['NVDA','AAPL','MSFT','GOOGL','META','AMZN','TSLA','AVGO','AMD','PLTR','CRWD','NFLX','JPM','V','LLY','XOM'];\nconst STOCK_NAMES={NVDA:'NVIDIA',AAPL:'Apple',MSFT:'Microsoft',GOOGL:'Alphabet',META:'Meta',AMZN:'Amazon',TSLA:'Tesla',AVGO:'Broadcom',AMD:'AMD',PLTR:'Palantir',CRWD:'CrowdStrike',NFLX:'Netflix',JPM:'JPMorgan',V:'Visa',LLY:'Eli Lilly',XOM:'ExxonMobil'};\nconst CRYPTO=['BTC-USD','ETH-USD','SOL-USD','BNB-USD','XRP-USD','DOGE-USD','ADA-USD'];\nconst TAPE_S=['^GSPC','^IXIC','^VIX','GC=F','CL=F','^TNX','DX-Y.NYB','BTC-USD','SPY','QQQ'];\nconst TAPE_L={'^GSPC':'SP500','^IXIC':'NASDAQ','^VIX':'VIX','GC=F':'GOLD','CL=F':'WTI','^TNX':'US10Y','DX-Y.NYB':'DXY','BTC-USD':'BTC',SPY:'SPY',QQQ:'QQQ'};\n\n// Render stocks\ndocument.getElementById('wl-list').innerHTML=STOCKS.map(t=>`\n  <div class=\"tk-row\" onclick=\"go('${t}')\">\n    <div><div class=\"tk-sym\">${t}</div><div class=\"tk-name\">${STOCK_NAMES[t]||''}</div></div>\n    <div class=\"tk-right\"><div class=\"tk-px\" id=\"p_${t}\">—</div><div class=\"tk-ch nc\" id=\"c_${t}\"></div></div>\n  </div>`).join('');\n\ndocument.getElementById('stocks-grid').innerHTML=STOCKS.slice(0,12).map(t=>`\n  <div class=\"mv\" onclick=\"go('${t}')\">\n    <div class=\"mv-l\"><div class=\"sym\">${t}</div><div class=\"nm\">${STOCK_NAMES[t]||''}</div></div>\n    <div class=\"mv-r\"><div class=\"px\" id=\"bp_${t}\">—</div><div class=\"ch nc\" id=\"bc2_${t}\"></div></div>\n  </div>`).join('');\n\ndocument.getElementById('crypto-row').innerHTML=CRYPTO.map(t=>{\n  const s=t.replace('-USD','');\n  return`<div class=\"cy\" onclick=\"go('${t}')\">\n    <div class=\"sym\">${s}</div>\n    <div class=\"px\" id=\"p_${eid(t)}\">—</div>\n    <div class=\"ch nc\" id=\"c_${eid(t)}\"></div>\n  </div>`;}).join('');\n\n// Tape\ndocument.getElementById('tape-inner').innerHTML=[...TAPE_S,...TAPE_S].map(t=>{\n  const id='ti_'+eid(t);\n  return`<div class=\"ti\" id=\"${id}\" onclick=\"go('${t}')\"><span class=\"ts\">${TAPE_L[t]||t}</span>&nbsp;<span>—</span>&nbsp;<span class=\"tc nc\">—</span></div>`;\n}).join('');\n\n// Market status\nfunction updateMktStatus(){\n  try{\n    const now=new Date();\n    const fmt=new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',hour:'2-digit',minute:'2-digit',weekday:'short',hour12:false});\n    const parts=Object.fromEntries(fmt.formatToParts(now).map(x=>[x.type,x.value]));\n    const h=parseInt(parts.hour),m=parseInt(parts.minute),tot=h*60+m;\n    const wd=['Mon','Tue','Wed','Thu','Fri'].includes(parts.weekday);\n    let lbl,col,bg;\n    if(!wd||tot<240||tot>=1200){lbl='FECHADO';col='#484f58';bg='rgba(255,255,255,.04)';}\n    else if(tot<570){lbl='PRE-MARKET';col='#f0c060';bg='rgba(240,192,96,.08)';}\n    else if(tot<960){lbl='NYSE ABERTO';col='#00e5a0';bg='rgba(0,229,160,.1)';}\n    else{lbl='AFTER-HOURS';col='#e91e8c';bg='rgba(233,30,140,.08)';}\n    const chip=document.getElementById('mkt-chip');\n    const dot=document.getElementById('mkt-dot');\n    const lbl_el=document.getElementById('mkt-lbl');\n    const time_el=document.getElementById('mkt-time');\n    if(chip)chip.style.cssText=`background:${bg};color:${col};border:1px solid ${col}33;padding:3px 8px;border-radius:3px;font-size:9px;font-family:var(--fd);font-weight:700;display:flex;align-items:center;gap:4px;flex-shrink:0`;\n    if(dot)dot.style.cssText=`width:5px;height:5px;border-radius:50%;background:${col}`;\n    if(lbl_el)lbl_el.textContent=lbl;\n    if(time_el)time_el.textContent=`NY: ${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')} ET`;\n  }catch(e){}\n}\nupdateMktStatus();\nsetInterval(updateMktStatus,30000);\n\n// Socket\nconst socket=io();\nconst ALL_TICKERS=[...new Set([...TAPE_S,...STOCKS,...CRYPTO,'^GSPC','^IXIC','^DJI','^RUT','^VIX','^TNX','^TYX','DX-Y.NYB','GC=F','SI=F','CL=F','BZ=F','NG=F','HG=F','ZC=F','ZW=F','SPY','QQQ','IWM','GLD','TLT','ARKK','XLE','XLK','TQQQ'])];\n\nsocket.on('connect',()=>socket.emit('subscribe',{tickers:ALL_TICKERS}));\n\nfunction upd(id,px,pct,prefix){\n  const pxEl=document.getElementById('p_'+id);\n  const chEl=document.getElementById('c_'+id);\n  if(pxEl&&px!=null)pxEl.textContent=(prefix||'')+fmtPx(px).replace('$',prefix?'':'$');\n  if(pxEl&&px!=null)pxEl.textContent=fmtPx(px);\n  if(chEl&&pct!=null){chEl.textContent=fmtPct(pct);chEl.className=chEl.className.replace(/up|dn|nc/g,'')+ccls(pct);}\n  // Big cards\n  const bvEl=document.getElementById('bv_'+id);\n  const bcEl=document.getElementById('bc_'+id);\n  if(bvEl&&px!=null)bvEl.textContent=fmtPx(px);\n  if(bcEl&&pct!=null){bcEl.textContent=fmtPct(pct);bcEl.className='bc-chg '+ccls(pct);}\n}\n\nsocket.on('price_update',({prices})=>{\n  prices.forEach(p=>{\n    if(!p.ticker)return;\n    const t=p.ticker,px=p.price,pct=p.change_pct;\n    const id=eid(t);\n\n    // Left panel + big cards + right panel\n    upd(id,px,pct);\n\n    // Stock rows in left panel\n    const sEl=document.getElementById('p_'+t);\n    const scEl=document.getElementById('c_'+t);\n    if(sEl&&px!=null)sEl.textContent=fmtPx(px);\n    if(scEl&&pct!=null){scEl.textContent=fmtPct(pct);scEl.className='tk-ch '+ccls(pct);}\n\n    // Stocks movers grid\n    const bpEl=document.getElementById('bp_'+t);\n    const bc2El=document.getElementById('bc2_'+t);\n    if(bpEl&&px!=null)bpEl.textContent=fmtPx(px);\n    if(bc2El&&pct!=null){bc2El.textContent=fmtPct(pct);bc2El.className='ch '+ccls(pct);}\n\n    // Tape\n    const tel=document.getElementById('ti_'+id);\n    if(tel){\n      const cs=tel.children;\n      if(cs[1]&&px!=null)cs[1].textContent=fmtPx(px);\n      if(cs[2]&&pct!=null){cs[2].textContent=fmtPct(pct);cs[2].className='tc '+ccls(pct);}\n    }\n  });\n});\n\n// REST initial prices\nfetch('/api/watchlist?tickers='+ALL_TICKERS.slice(0,80).join(','))\n  .then(r=>r.json()).then(d=>{\n    if(d?.stocks)d.stocks.forEach(p=>{\n      if(!p?.ticker)return;\n      const t=p.ticker,px=p.price,pct=p.change_pct;\n      const id=eid(t);\n      upd(id,px,pct);\n      const sEl=document.getElementById('p_'+t);if(sEl&&px!=null)sEl.textContent=fmtPx(px);\n      const scEl=document.getElementById('c_'+t);if(scEl&&pct!=null){scEl.textContent=fmtPct(pct);scEl.className='tk-ch '+ccls(pct);}\n      const bpEl=document.getElementById('bp_'+t);if(bpEl&&px!=null)bpEl.textContent=fmtPx(px);\n      const bc2El=document.getElementById('bc2_'+t);if(bc2El&&pct!=null){bc2El.textContent=fmtPct(pct);bc2El.className='ch '+ccls(pct);}\n      const tel=document.getElementById('ti_'+id);\n      if(tel){const cs=tel.children;if(cs[1]&&px!=null)cs[1].textContent=fmtPx(px);if(cs[2]&&pct!=null){cs[2].textContent=fmtPct(pct);cs[2].className='tc '+ccls(pct);}}\n    });\n  }).catch(()=>{});\n\n// Search\nlet sT;\nconst inp=document.getElementById('srch'),drop=document.getElementById('srch-drop');\ninp.addEventListener('input',function(){\n  clearTimeout(sT);const v=this.value.trim();\n  if(!v){drop.style.display='none';return;}\n  sT=setTimeout(async()=>{\n    try{\n      const r=await fetch('/api/universe?q='+encodeURIComponent(v)+'&limit=12');\n      const d=await r.json();\n      if(!d?.results?.length){drop.style.display='none';return;}\n      drop.innerHTML=d.results.map(x=>`<div class=\"dr\" onclick=\"go('${x.ticker}')\"><span class=\"dr-sym\">${x.ticker}</span><span class=\"dr-name\">${x.name||''}</span></div>`).join('');\n      drop.style.display='block';\n    }catch(e){drop.style.display='none';}\n  },200);\n});\ninp.addEventListener('keydown',e=>{if(e.key==='Enter'){const v=inp.value.trim().toUpperCase().replace('$','');if(v)go(v);}});\ndocument.addEventListener('click',e=>{if(!e.target.closest('.search-wrap'))drop.style.display='none';});\n\n// Fix tool links to use search ticker\ndocument.querySelectorAll('.tool[href]').forEach(a=>{\n  a.addEventListener('click',e=>{\n    const v=inp.value.trim().toUpperCase().replace('$','');\n    if(v){e.preventDefault();const url=new URL(a.href,location.origin);url.searchParams.set('t',v);location.href=url.toString();}\n  });\n});\n</script>\n\n<div id=\"mkt-panel\" style=\"display:none;position:fixed;top:70px;right:16px;background:#0d1117;border:1px solid #1e2d3d;border-radius:10px;padding:20px;width:380px;z-index:9999;box-shadow:0 12px 40px rgba(0,0,0,.6)\">\n  <div style=\"font-family:var(--fd);font-size:11px;font-weight:700;color:#8b949e;letter-spacing:.1em;text-transform:uppercase;margin-bottom:16px;display:flex;justify-content:space-between\"><span>Sess&otilde;es de Mercado</span><span id=\"mkt-ny-time\" style=\"color:#484f58\"></span></div>\n  <div id=\"mkt-timeline\" style=\"position:relative;height:8px;background:#1e2d3d;border-radius:4px;margin-bottom:20px;overflow:visible\">\n    <div id=\"mkt-time-marker\" style=\"position:absolute;top:-4px;width:2px;height:16px;background:#00e5a0;border-radius:2px;transition:left .5s\"></div>\n  </div>\n  <div style=\"display:grid;grid-template-columns:1fr 1fr;gap:10px\">\n    <div style=\"background:rgba(240,192,96,.08);border:1px solid rgba(240,192,96,.2);border-radius:8px;padding:14px\"><div style=\"display:flex;align-items:center;gap:6px;margin-bottom:8px\"><div style=\"width:8px;height:8px;border-radius:50%;background:#f0c060\"></div><div style=\"font-family:var(--fd);font-size:10px;font-weight:700;color:#f0c060\">PRE-MARKET</div></div><div style=\"font-family:var(--fd);font-size:15px;font-weight:800;color:#c9d1d9\">04:00 - 09:30</div><div style=\"font-size:10px;color:#484f58;margin-top:4px\">Nova Iorque ET</div></div>\n    <div style=\"background:rgba(0,229,160,.08);border:1px solid rgba(0,229,160,.2);border-radius:8px;padding:14px\"><div style=\"display:flex;align-items:center;gap:6px;margin-bottom:8px\"><div style=\"width:8px;height:8px;border-radius:50%;background:#00e5a0;animation:pulse 1.4s ease infinite\"></div><div style=\"font-family:var(--fd);font-size:10px;font-weight:700;color:#00e5a0\">NYSE OPEN</div></div><div style=\"font-family:var(--fd);font-size:15px;font-weight:800;color:#c9d1d9\">09:30 - 16:00</div><div style=\"font-size:10px;color:#484f58;margin-top:4px\">Nova Iorque ET</div></div>\n    <div style=\"background:rgba(233,30,140,.08);border:1px solid rgba(233,30,140,.2);border-radius:8px;padding:14px\"><div style=\"display:flex;align-items:center;gap:6px;margin-bottom:8px\"><div style=\"width:8px;height:8px;border-radius:50%;background:#e91e8c\"></div><div style=\"font-family:var(--fd);font-size:10px;font-weight:700;color:#e91e8c\">AFTER-HOURS</div></div><div style=\"font-family:var(--fd);font-size:15px;font-weight:800;color:#c9d1d9\">16:00 - 20:00</div><div style=\"font-size:10px;color:#484f58;margin-top:4px\">Nova Iorque ET</div></div>\n    <div style=\"background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08);border-radius:8px;padding:14px\"><div style=\"display:flex;align-items:center;gap:6px;margin-bottom:8px\"><div style=\"width:8px;height:8px;border-radius:50%;background:#243040\"></div><div style=\"font-family:var(--fd);font-size:10px;font-weight:700;color:#484f58\">OVERNIGHT</div></div><div style=\"font-family:var(--fd);font-size:15px;font-weight:800;color:#c9d1d9\">20:00 - 04:00</div><div style=\"font-size:10px;color:#484f58;margin-top:4px\">Nova Iorque ET</div></div>\n  </div>\n</div>\n<script>\nfunction toggleMktTip(){const p=document.getElementById('mkt-panel');if(p)p.style.display=p.style.display==='none'?'block':'none';}\ndocument.addEventListener('click',e=>{const p=document.getElementById('mkt-panel');const c=document.getElementById('mkt-chip');if(p&&c&&!p.contains(e.target)&&!c.contains(e.target))p.style.display='none';});\n</script>\n</body>\n</html>",
    "landing.html": "<!DOCTYPE html>\n<html lang=\"pt\">\n<head>\n<meta charset=\"UTF-8\">\n<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\n<title>IST · Insider Signal Terminal</title>\n<script src=\"https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.7.5/socket.io.min.js\"></script>\n<link rel=\"preconnect\" href=\"https://fonts.googleapis.com\">\n<link href=\"https://fonts.googleapis.com/css2?family=Space+Mono:ital,wght@0,400;0,700;1,400&family=Syne:wght@400;700;800;900&family=DM+Mono:wght@300;400;500&display=swap\" rel=\"stylesheet\">\n<style>\n*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}\n:root{\n  --bg:#03050a;\n  --bg1:#060a10;\n  --bg2:#090e16;\n  --bg3:#0d1520;\n  --b:#13202e;\n  --b2:#1a2d40;\n  --t:#c8d8e8;\n  --t2:#5a7a90;\n  --t3:#243040;\n  --gr:#00ff9d;\n  --gr2:#00cc7a;\n  --rd:#ff2d55;\n  --bl:#007aff;\n  --yl:#ffd60a;\n  --or:#ff6b2b;\n  --fmono:\"DM Mono\",monospace;\n  --fsyne:\"Syne\",sans-serif;\n  --fspace:\"Space Mono\",monospace;\n}\n\nhtml{scroll-behavior:smooth;overflow-x:hidden}\nbody{background:var(--bg);color:var(--t);font-family:var(--fmono);font-size:13px;overflow-x:hidden;cursor:none}\n\n/* CURSOR */\n#cur{position:fixed;width:8px;height:8px;background:var(--gr);border-radius:50%;pointer-events:none;z-index:9999;transform:translate(-50%,-50%);transition:transform .08s,width .2s,height .2s,background .2s;mix-blend-mode:difference}\n#cur2{position:fixed;width:32px;height:32px;border:1px solid rgba(0,255,157,.3);border-radius:50%;pointer-events:none;z-index:9998;transform:translate(-50%,-50%);transition:left .12s ease,top .12s ease,width .2s,height .2s}\nbody:hover #cur{opacity:1}\n\n/* SCROLLBAR */\n::-webkit-scrollbar{width:2px}\n::-webkit-scrollbar-thumb{background:var(--b2)}\n\n/* NOISE OVERLAY */\nbody::before{\n  content:\"\";position:fixed;inset:0;\n  background-image:url(\"data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)' opacity='0.03'/%3E%3C/svg%3E\");\n  pointer-events:none;z-index:1000;opacity:.4\n}\n\n/* TAPE */\n#tape{position:fixed;top:0;left:0;right:0;height:26px;background:rgba(3,5,10,.97);border-bottom:1px solid var(--b);z-index:500;overflow:hidden;display:flex;align-items:center}\n#tape-track{display:flex;white-space:nowrap;animation:tape 60s linear infinite}\n@keyframes tape{from{transform:translateX(0)}to{transform:translateX(-50%)}}\n.ti{display:inline-flex;align-items:center;gap:5px;padding:0 16px;height:26px;border-right:1px solid var(--b);font-size:9px;letter-spacing:.08em;cursor:pointer;transition:background .15s;flex-shrink:0}\n.ti:hover{background:var(--bg2)}\n.ts{color:var(--t3);font-family:var(--fspace)}.tv{color:var(--t)}.tc{font-size:8px}\n\n/* PROGRESS */\n#progress{position:fixed;top:26px;left:0;height:1px;background:var(--gr);z-index:501;width:0%;transition:width .06s linear;box-shadow:0 0 8px var(--gr)}\n\n/* NAV */\nnav{position:fixed;top:26px;left:0;right:0;height:50px;z-index:400;display:flex;align-items:center;padding:0 40px;transition:all .3s}\nnav.scrolled{background:rgba(3,5,10,.95);border-bottom:1px solid var(--b);backdrop-filter:blur(24px)}\n.nl{display:flex;align-items:center;gap:9px;text-decoration:none;margin-right:auto}\n.nl-mark{width:28px;height:28px;border:1px solid var(--gr);border-radius:4px;display:flex;align-items:center;justify-content:center;position:relative}\n.nl-mark::after{content:\"\";position:absolute;inset:3px;background:var(--gr);border-radius:2px;opacity:.15}\n.nl-mark svg{width:13px;height:13px;position:relative;z-index:1}\n.nl-txt{font-family:var(--fsyne);font-size:16px;font-weight:900;color:var(--t);letter-spacing:.15em}\n.nlinks{display:flex;gap:0}\n.nlink{padding:5px 12px;font-size:10px;font-family:var(--fspace);color:var(--t2);text-decoration:none;letter-spacing:.06em;transition:color .15s;border-radius:3px}\n.nlink:hover{color:var(--t)}\n.ncta{margin-left:12px;padding:7px 18px;border:1px solid var(--gr);border-radius:3px;font-size:10px;font-family:var(--fspace);font-weight:700;color:var(--gr);text-decoration:none;letter-spacing:.08em;transition:all .2s;position:relative;overflow:hidden}\n.ncta::before{content:\"\";position:absolute;inset:0;background:var(--gr);transform:translateX(-101%);transition:transform .25s cubic-bezier(.4,0,.2,1)}\n.ncta:hover{color:var(--bg)}.ncta:hover::before{transform:translateX(0)}\n.ncta span{position:relative;z-index:1}\n\n/* ── HERO ── */\n#hero{min-height:100vh;display:flex;align-items:center;position:relative;overflow:hidden;padding-top:76px}\n\n/* Animated grid */\n.hgrid{position:absolute;inset:0;background-image:linear-gradient(var(--b) 1px,transparent 1px),linear-gradient(90deg,var(--b) 1px,transparent 1px);background-size:80px 80px;opacity:.4;animation:gridpulse 4s ease-in-out infinite}\n@keyframes gridpulse{0%,100%{opacity:.4}50%{opacity:.25}}\n\n/* Scan line */\n.scanline{position:absolute;left:0;right:0;height:1px;background:linear-gradient(90deg,transparent,var(--gr),transparent);opacity:.4;animation:scan 6s linear infinite;top:0}\n@keyframes scan{from{top:0}to{top:100%}}\n\n/* Corner marks */\n.corner{position:absolute;width:24px;height:24px;opacity:.5}\n.corner.tl{top:80px;left:40px;border-top:1px solid var(--gr);border-left:1px solid var(--gr)}\n.corner.tr{top:80px;right:40px;border-top:1px solid var(--gr);border-right:1px solid var(--gr)}\n.corner.bl{bottom:40px;left:40px;border-bottom:1px solid var(--gr);border-left:1px solid var(--gr)}\n.corner.br{bottom:40px;right:40px;border-bottom:1px solid var(--gr);border-right:1px solid var(--gr)}\n\n.hero-inner{position:relative;z-index:2;max-width:1200px;margin:0 auto;padding:0 40px;width:100%}\n\n/* Eyebrow */\n.eyebrow{display:inline-flex;align-items:center;gap:10px;margin-bottom:48px;opacity:0;animation:fin .6s ease .1s forwards}\n.ebadge{font-size:9px;font-family:var(--fspace);letter-spacing:.15em;text-transform:uppercase;color:var(--gr);padding:4px 10px;border:1px solid rgba(0,255,157,.2);border-radius:2px}\n.edivider{width:40px;height:1px;background:var(--b2)}\n.etext{font-size:9px;color:var(--t2);font-family:var(--fspace);letter-spacing:.1em}\n\n/* Big title */\n.htitle{font-family:var(--fsyne);font-weight:900;font-size:clamp(48px,5.5vw,88px);line-height:.88;letter-spacing:-.01em;margin-bottom:0;overflow:hidden}\n.htitle-line{display:block;overflow:hidden}\n.htitle-word{display:inline-block;transform:translateY(105%);animation:wup .9s cubic-bezier(.16,1,.3,1) forwards}\n.htitle-word:nth-child(1){animation-delay:.15s}\n.htitle-word:nth-child(2){animation-delay:.25s}\n.haccent{color:var(--gr);font-style:normal;position:relative}\n.haccent::after{content:\"\";position:absolute;bottom:-2px;left:0;right:0;height:2px;background:var(--gr);transform:scaleX(0);animation:underln .6s cubic-bezier(.16,1,.3,1) 1.1s forwards}\n@keyframes underln{to{transform:scaleX(1)}}\n@keyframes wup{to{transform:translateY(0)}}\n@keyframes fin{to{opacity:1}}\n\n/* Subtitle row */\n.hero-sub-row{display:flex;align-items:flex-start;gap:48px;margin-top:32px;opacity:0;animation:fin .8s ease .8s forwards}\n.hero-desc{font-family:var(--fmono);font-size:14px;color:var(--t2);line-height:1.8;max-width:380px}\n.hero-desc strong{color:var(--t);font-weight:500}\n.hero-actions{display:flex;flex-direction:column;gap:12px;flex-shrink:0}\n.btn-main{padding:14px 32px;background:var(--gr);color:var(--bg);font-family:var(--fspace);font-size:11px;font-weight:700;letter-spacing:.1em;text-decoration:none;border-radius:3px;transition:all .2s;display:inline-flex;align-items:center;gap:10px}\n.btn-main:hover{background:#00ffb3;transform:translateY(-2px);box-shadow:0 12px 40px rgba(0,255,157,.3)}\n.btn-ghost{padding:14px 32px;border:1px solid var(--b2);color:var(--t2);font-family:var(--fspace);font-size:11px;letter-spacing:.1em;text-decoration:none;border-radius:3px;transition:all .2s;display:inline-flex;align-items:center;gap:10px}\n.btn-ghost:hover{border-color:var(--t3);color:var(--t)}\n\n/* PRICE STRIP */\n.price-strip{display:grid;grid-template-columns:repeat(6,1fr);border:1px solid var(--b);border-radius:4px;overflow:hidden;margin-top:72px;opacity:0;animation:fin .6s ease 1.1s forwards;background:var(--b)}\n.pc{background:var(--bg1);padding:14px 16px;cursor:pointer;transition:background .15s;position:relative;border-right:1px solid var(--b)}\n.pc:last-child{border-right:none}\n.pc::before{content:\"\";position:absolute;bottom:0;left:0;right:0;height:1px;background:var(--gr);transform:scaleX(0);transform-origin:left;transition:transform .3s}\n.pc:hover{background:var(--bg2)}.pc:hover::before{transform:scaleX(1)}\n.ps{font-family:var(--fspace);font-size:9px;letter-spacing:.12em;color:var(--t3);margin-bottom:6px;text-transform:uppercase}\n.pp{font-family:var(--fmono);font-size:15px;color:var(--t);margin-bottom:3px;transition:color .3s}\n.pch{font-family:var(--fspace);font-size:9px;letter-spacing:.04em;transition:color .3s}\n.up{color:var(--gr)}.dn{color:var(--rd)}.nc{color:var(--t2)}\n\n/* SECTION SHARED */\n.sw{max-width:1200px;margin:0 auto;padding:64px 40px}\n.slabel{font-size:9px;font-family:var(--fspace);letter-spacing:.2em;text-transform:uppercase;color:var(--gr);margin-bottom:20px;display:flex;align-items:center;gap:12px}\n.slabel::before{content:\"//\";color:var(--t3)}\n.stitle{font-family:var(--fsyne);font-weight:900;font-size:clamp(26px,2.8vw,40px);line-height:.95;letter-spacing:-.01em;color:var(--t);margin-bottom:14px}\n.stitle em{font-style:normal;color:var(--gr)}\n.sbody{font-family:var(--fmono);font-size:13px;color:var(--t2);line-height:1.85;max-width:440px}\n\n.reveal{opacity:0;transform:translateY(40px);transition:opacity .8s cubic-bezier(.16,1,.3,1),transform .8s cubic-bezier(.16,1,.3,1)}\n.reveal.on{opacity:1;transform:none}\n.d1{transition-delay:.1s}.d2{transition-delay:.2s}.d3{transition-delay:.3s}.d4{transition-delay:.4s}.d5{transition-delay:.5s}\n\n/* ── S1: FEATURES ── */\n#s1{border-top:1px solid var(--b)}\n.fgrid{display:grid;grid-template-columns:repeat(3,1fr);gap:1px;background:var(--b);border:1px solid var(--b);border-radius:6px;overflow:hidden;margin-top:32px}\n.fc{background:var(--bg1);padding:22px 20px;text-decoration:none;display:block;position:relative;overflow:hidden;transition:background .2s}\n.fc::after{content:\"\";position:absolute;top:0;left:0;right:0;height:1px;background:linear-gradient(90deg,transparent,var(--facc,var(--gr)),transparent);transform:scaleX(0);transition:transform .4s}\n.fc:hover{background:var(--bg2)}.fc:hover::after{transform:scaleX(1)}\n.fc-num{font-family:var(--fspace);font-size:8px;color:var(--t3);letter-spacing:.15em;margin-bottom:14px;display:block}\n.fc-ic{font-size:20px;margin-bottom:10px;display:block}\n.fc-ttl{font-family:var(--fsyne);font-size:14px;font-weight:800;color:var(--t);margin-bottom:6px}\n.fc-dsc{font-size:10px;color:var(--t2);line-height:1.65;margin-bottom:12px}\n.fc-tag{font-size:8px;font-family:var(--fspace);letter-spacing:.12em;text-transform:uppercase;color:var(--facc,var(--gr));padding:3px 8px;border:1px solid;border-color:color-mix(in srgb,var(--facc,var(--gr)) 30%,transparent);border-radius:2px;display:inline-block}\n\n/* ── S2: BIG STATS ── */\n#s2{border-top:1px solid var(--b);background:var(--bg1)}\n.stats{display:grid;grid-template-columns:repeat(4,1fr);border:1px solid var(--b);border-radius:6px;overflow:hidden;background:var(--b)}\n.stat{background:var(--bg1);padding:36px 16px;border-right:1px solid var(--b);text-align:center;position:relative;overflow:hidden}\n.stat:last-child{border-right:none}\n.stat::before{content:\"\";position:absolute;top:0;left:50%;transform:translateX(-50%);width:40%;height:1px;background:var(--gr);opacity:0;transition:opacity .4s}\n.stat:hover::before{opacity:.6}\n.sv{font-family:var(--fsyne);font-weight:900;font-size:clamp(28px,2.8vw,40px);color:var(--gr);line-height:1;display:block;margin-bottom:8px}\n.sl{font-family:var(--fspace);font-size:8px;letter-spacing:.1em;text-transform:uppercase;color:var(--t3);line-height:1.4}\n\n/* ── S3: COMO FUNCIONA (3 steps horizontais) ── */\n#s3{border-top:1px solid var(--b)}\n.how-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:1px;background:var(--b);border:1px solid var(--b);border-radius:6px;overflow:hidden;margin-top:32px}\n.how{background:var(--bg1);padding:24px 22px;position:relative;overflow:hidden}\n.how::before{content:\"\";position:absolute;left:0;top:0;bottom:0;width:1px;background:linear-gradient(180deg,transparent,var(--gr),transparent);opacity:0;transition:opacity .4s}\n.how:hover::before{opacity:.5}\n.how-n{font-family:var(--fsyne);font-weight:900;font-size:40px;color:var(--t3);line-height:1;margin-bottom:12px;opacity:.2}\n.how-ttl{font-family:var(--fsyne);font-size:15px;font-weight:800;color:var(--t);margin-bottom:8px}\n.how-body{font-size:10px;color:var(--t2);line-height:1.7}\n.how-arrow{position:absolute;right:20px;top:50%;transform:translateY(-50%);color:var(--t3);font-size:18px;opacity:.4}\n.how:last-child .how-arrow{display:none}\n\n/* ── S4: TERMINAL MOCKUP ── */\n#s4{border-top:1px solid var(--b);background:var(--bg)}\n.s4-inner{display:grid;grid-template-columns:1fr 1fr;gap:64px;align-items:center}\n.mock{border:1px solid var(--b);border-radius:8px;overflow:hidden;background:var(--bg1);box-shadow:0 40px 120px rgba(0,0,0,.8),0 0 0 1px rgba(0,255,157,.04)}\n.mock-bar{height:34px;background:var(--bg3);border-bottom:1px solid var(--b);display:flex;align-items:center;padding:0 12px;gap:6px}\n.mdot{width:9px;height:9px;border-radius:50%}\n.mock-tab{font-size:9px;font-family:var(--fspace);color:var(--t3);letter-spacing:.08em;margin-left:auto}\n.mock-body{padding:20px}\n\n/* Waterfall chart */\n.wf{display:flex;align-items:flex-end;gap:3px;height:160px;margin-bottom:16px}\n.wbar{border-radius:2px 2px 0 0;flex:1;position:relative;transition:height .8s cubic-bezier(.16,1,.3,1)}\n.wbar::after{content:attr(data-l);position:absolute;top:-16px;left:50%;transform:translateX(-50%);font-size:7px;font-family:var(--fspace);color:var(--t3);white-space:nowrap}\n\n/* insider mock */\n.imock-row{display:flex;align-items:center;gap:10px;padding:8px 0;border-bottom:1px solid rgba(255,255,255,.03)}\n.imock-row:last-child{border:none}\n.iav{width:30px;height:30px;border-radius:50%;background:var(--bg3);flex-shrink:0;display:flex;align-items:center;justify-content:center;font-size:9px;font-weight:700;color:var(--gr);font-family:var(--fspace);border:1px solid var(--b2)}\n.iname{font-size:11px;font-weight:500;color:var(--t);font-family:var(--fsyne)}\n.irole{font-size:9px;color:var(--t3);font-family:var(--fmono)}\n.ibadge{font-size:7px;font-weight:700;padding:2px 5px;border-radius:2px;font-family:var(--fspace);letter-spacing:.08em}\n.ibuy{background:rgba(0,255,157,.1);color:var(--gr);border:1px solid rgba(0,255,157,.2)}\n.isell{background:rgba(255,45,85,.1);color:var(--rd);border:1px solid rgba(255,45,85,.2)}\n.iamt{font-family:var(--fmono);font-size:13px;font-weight:500;margin-left:auto}\n\n/* Tabs */\n.mock-tabs{display:flex;gap:1px;background:var(--b);border-bottom:1px solid var(--b)}\n.mtab{padding:8px 14px;font-size:9px;font-family:var(--fspace);color:var(--t3);letter-spacing:.08em;cursor:pointer;transition:all .2s;background:var(--bg2)}\n.mtab.on{background:var(--bg1);color:var(--gr);border-bottom:1px solid var(--gr)}\n\n/* Mini chart sparkline */\n.spark{height:50px;display:flex;align-items:flex-end;gap:1.5px;padding:4px 0}\n.spk{border-radius:1px 1px 0 0;flex:1}\n.spk.u{background:rgba(0,255,157,.6)}.spk.d{background:rgba(255,45,85,.6)}\n\n/* ── S5: FINANCIALS PANEL (novo, sem scrollytelling) ── */\n#s5{border-top:1px solid var(--b);background:var(--bg1)}\n.fin-showcase{border:1px solid var(--b);border-radius:8px;overflow:hidden;background:var(--bg);margin-top:60px;box-shadow:0 40px 100px rgba(0,0,0,.6)}\n.fin-hdr{padding:12px 20px;border-bottom:1px solid var(--b);background:var(--bg2);display:flex;align-items:center;gap:12px}\n.fin-ticker{font-family:var(--fsyne);font-size:14px;font-weight:900;color:var(--t)}\n.fin-sub{font-size:9px;font-family:var(--fspace);color:var(--t3);letter-spacing:.1em}\n.fin-live{margin-left:auto;font-size:8px;font-family:var(--fspace);color:var(--gr);letter-spacing:.1em;display:flex;align-items:center;gap:5px}\n.fin-live::before{content:\"\";width:5px;height:5px;border-radius:50%;background:var(--gr);animation:blink 1.8s infinite}\n@keyframes blink{0%,100%{opacity:1}50%{opacity:.2}}\n\n.fin-grid{display:grid;grid-template-columns:1fr 1fr 1fr;gap:1px;background:var(--b)}\n\n.fin-box{background:var(--bg1);padding:14px 16px}\n.fin-box-label{font-size:8px;font-family:var(--fspace);letter-spacing:.15em;text-transform:uppercase;color:var(--t3);margin-bottom:12px}\n.fin-box-val{font-family:var(--fsyne);font-size:22px;font-weight:900;line-height:1}\n.fin-box-sub{font-size:10px;font-family:var(--fmono);color:var(--t2);margin-top:6px}\n\n/* Flow diagram */\n.flow{display:flex;align-items:center;gap:8px;padding:20px}\n.fnode{padding:10px 14px;border-radius:4px;font-size:11px;font-family:var(--fmono);font-weight:500;flex-shrink:0}\n.farr{color:var(--t3);font-size:16px;flex-shrink:0}\n.fcol{display:flex;flex-direction:column;gap:6px}\n.fnode-sm{padding:7px 10px;border-radius:3px;font-size:10px;font-family:var(--fmono)}\n\n/* Margin bars */\n.mbar-row{display:flex;flex-direction:column;gap:10px;padding:20px}\n.mbar{display:flex;flex-direction:column;gap:5px}\n.mbar-label{display:flex;justify-content:space-between;font-size:10px;font-family:var(--fmono)}\n.mbar-track{height:4px;background:var(--bg3);border-radius:2px;overflow:hidden}\n.mbar-fill{height:100%;border-radius:2px;transition:width 1.2s cubic-bezier(.16,1,.3,1)}\n\n/* ── S6: CONGRESS ── */\n#s6{border-top:1px solid var(--b)}\n.cong-grid{display:grid;grid-template-columns:1fr 1fr;gap:1px;background:var(--b);border:1px solid var(--b);border-radius:6px;overflow:hidden;margin-top:60px}\n.cong-side{background:var(--bg1);padding:20px 22px}\n.cong-label{font-size:9px;font-family:var(--fspace);letter-spacing:.15em;text-transform:uppercase;margin-bottom:20px;padding:4px 8px;border-radius:2px;display:inline-block}\n.dem{color:#4a9eff;background:rgba(74,158,255,.08);border:1px solid rgba(74,158,255,.2)}\n.rep{color:#ff453a;background:rgba(255,69,58,.08);border:1px solid rgba(255,69,58,.2)}\n.cong-row{display:flex;align-items:center;gap:10px;padding:9px 0;border-bottom:1px solid rgba(255,255,255,.03)}\n.cong-row:last-child{border:none}\n.cav{width:28px;height:28px;border-radius:3px;flex-shrink:0;display:flex;align-items:center;justify-content:center;font-size:9px;font-weight:700;font-family:var(--fspace)}\n.cname{font-size:11px;font-weight:700;color:var(--t);font-family:var(--fsyne)}\n.cstock{font-size:9px;color:var(--t2);font-family:var(--fmono)}\n.camt{font-family:var(--fmono);font-size:12px;font-weight:500;margin-left:auto}\n\n/* ── S7: CTA ── */\n#s7{border-top:1px solid var(--b);background:var(--bg);min-height:70vh;display:flex;align-items:center;justify-content:center;position:relative;overflow:hidden}\n.cta-bg{position:absolute;inset:0}\n.cta-grid{position:absolute;inset:0;background-image:linear-gradient(var(--b) 1px,transparent 1px),linear-gradient(90deg,var(--b) 1px,transparent 1px);background-size:60px 60px;opacity:.3}\n.cta-glow{position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);width:600px;height:400px;background:radial-gradient(ellipse,rgba(0,255,157,.06) 0%,transparent 70%)}\n.cta-inner{position:relative;z-index:2;text-align:center;padding:60px 40px}\n.cta-title{font-family:var(--fsyne);font-weight:900;font-size:clamp(32px,3.5vw,54px);line-height:.92;letter-spacing:-.01em;color:var(--t);margin-bottom:24px}\n.cta-title span{color:var(--gr)}\n.cta-sub{font-family:var(--fmono);font-size:14px;color:var(--t2);margin-bottom:48px;line-height:1.8}\n.cta-btns{display:flex;align-items:center;justify-content:center;gap:16px;flex-wrap:wrap}\n.cta-prices{display:flex;justify-content:center;gap:48px;margin-top:56px;flex-wrap:wrap;opacity:.7}\n.cp{text-align:center}\n.cps{font-family:var(--fspace);font-size:10px;letter-spacing:.15em;color:var(--t3);text-transform:uppercase}\n.cpp{font-family:var(--fmono);font-size:18px;color:var(--t);margin-top:4px}\n.cpc{font-family:var(--fspace);font-size:9px;margin-top:3px}\n\n/* FOOTER */\nfooter{border-top:1px solid var(--b);padding:20px 40px;display:flex;align-items:center;justify-content:space-between;background:var(--bg1)}\n.fl{font-family:var(--fsyne);font-size:16px;font-weight:900;color:var(--t3);letter-spacing:.15em}\n.fn{font-size:9px;color:var(--t3);font-family:var(--fspace);letter-spacing:.06em}\n</style>\n</head>\n<body>\n\n<!-- Custom cursor -->\n<div id=\"cur\"></div>\n<div id=\"cur2\"></div>\n\n<div id=\"progress\"></div>\n<div id=\"tape\"><div id=\"tape-track\"></div></div>\n\n<nav id=\"nav\">\n  <a href=\"/\" style=\"display:flex;align-items:center;gap:7px;text-decoration:none;margin-right:auto\">\n    <svg width=\"22\" height=\"22\" viewBox=\"0 0 64 64\" fill=\"none\">\n      <rect width=\"64\" height=\"64\" rx=\"10\" fill=\"rgba(0,229,160,0.15)\" stroke=\"rgba(0,229,160,0.4)\" stroke-width=\"2\"/>\n      <polyline points=\"8,44 20,28 30,36 42,18 56,22\" stroke=\"#00e5a0\" stroke-width=\"5\" fill=\"none\" stroke-linecap=\"round\" stroke-linejoin=\"round\"/>\n      <circle cx=\"42\" cy=\"18\" r=\"5\" fill=\"#00e5a0\"/>\n    </svg>\n    <span class=\"nl-txt\">IST</span>\n  </a>\n  <div class=\"nlinks\">\n    <a href=\"/chart\" class=\"nlink\">CHART</a>\n    <a href=\"/insider\" class=\"nlink\">INSIDER</a>\n    <a href=\"/congress\" class=\"nlink\">CONGRESS</a>\n    <a href=\"/financials\" class=\"nlink\">FINANCIALS</a>\n    <a href=\"/pricing\" class=\"nlink\">PRICING</a>\n  </div>\n  <a href=\"/auth\" class=\"ncta\"><span>ENTRAR &rarr;</span></a>\n</nav>\n\n<!-- ══ HERO ══ -->\n<section id=\"hero\">\n  <div class=\"hgrid\"></div>\n  <div class=\"scanline\"></div>\n  <div class=\"corner tl\"></div><div class=\"corner tr\"></div>\n  <div class=\"corner bl\"></div><div class=\"corner br\"></div>\n\n  <div class=\"hero-inner\">\n    <div class=\"eyebrow\">\n      <span class=\"ebadge\">v4.5 LIVE</span>\n      <div class=\"edivider\"></div>\n      <span class=\"etext\">Dados em tempo real &middot; Sem paywall</span>\n    </div>\n\n    <h1 class=\"htitle\">\n      <span class=\"htitle-line\">\n        <span class=\"htitle-word\">O&nbsp;MERCADO</span>\n      </span>\n      <span class=\"htitle-line\">\n        <span class=\"htitle-word\"><em class=\"haccent\">EM&nbsp;REAL</em></span>\n      </span>\n    </h1>\n\n    <div class=\"hero-sub-row\">\n      <p class=\"hero-desc\">\n        <strong>Insider trades</strong>, financials, fair value,<br>\n        congress trades e gr&aacute;ficos profissionais.<br><br>\n        Tudo num terminal. <strong>Gratu&iacute;to.</strong>\n      </p>\n      <div class=\"hero-actions\">\n        <a href=\"/auth\" class=\"btn-main\">\n          <svg width=\"12\" height=\"12\" viewBox=\"0 0 12 12\" fill=\"none\"><polyline points=\"1,9 4,5 6.5,7 9,2 11,3.5\" stroke=\"currentColor\" stroke-width=\"1.6\" stroke-linecap=\"round\"/></svg>\n          ABRIR TERMINAL\n        </a>\n        <a href=\"/chart\" class=\"btn-ghost\">\n          VER GR&Aacute;FICO &rarr;\n        </a>\n      </div>\n    </div>\n\n    <!-- Price strip -->\n    <div class=\"price-strip\">\n      <div class=\"pc\" onclick=\"go('^GSPC')\"><div class=\"ps\">SP500</div><div class=\"pp\" id=\"hp_GSPC\">&#8212;</div><div class=\"pch nc\" id=\"hc_GSPC\">&#8212;</div></div>\n      <div class=\"pc\" onclick=\"go('^IXIC')\"><div class=\"ps\">NASDAQ</div><div class=\"pp\" id=\"hp_IXIC\">&#8212;</div><div class=\"pch nc\" id=\"hc_IXIC\">&#8212;</div></div>\n      <div class=\"pc\" onclick=\"go('BTC-USD')\"><div class=\"ps\">BITCOIN</div><div class=\"pp\" id=\"hp_BTC\">&#8212;</div><div class=\"pch nc\" id=\"hc_BTC\">&#8212;</div></div>\n      <div class=\"pc\" onclick=\"go('GC=F')\"><div class=\"ps\">GOLD</div><div class=\"pp\" id=\"hp_GCF\">&#8212;</div><div class=\"pch nc\" id=\"hc_GCF\">&#8212;</div></div>\n      <div class=\"pc\" onclick=\"go('NVDA')\"><div class=\"ps\">NVDA</div><div class=\"pp\" id=\"hp_NVDA\">&#8212;</div><div class=\"pch nc\" id=\"hc_NVDA\">&#8212;</div></div>\n      <div class=\"pc\" onclick=\"go('^VIX')\"><div class=\"ps\">VIX</div><div class=\"pp\" id=\"hp_VIX\">&#8212;</div><div class=\"pch nc\" id=\"hc_VIX\">&#8212;</div></div>\n    </div>\n  </div>\n</section>\n\n<!-- ══ S1: FEATURES ══ -->\n<section id=\"s1\">\n  <div class=\"sw\">\n    <div class=\"slabel reveal\">Ferramentas</div>\n    <h2 class=\"stitle reveal d1\">Tudo o que precisas<br>para <em>decidir melhor</em>.</h2>\n    <div class=\"fgrid reveal d2\">\n      <a class=\"fc\" href=\"/chart\" style=\"--facc:#00ff9d\">\n        <span class=\"fc-num\">01 &mdash; CHART</span>\n        <span class=\"fc-ic\">&#128200;</span>\n        <div class=\"fc-ttl\">Gr&aacute;ficos Profissionais</div>\n        <div class=\"fc-dsc\">Candlesticks, linha, relativo. Overlays S&P500, QQQ, M2. Watchlist em tempo real com alertas.</div>\n        <span class=\"fc-tag\">CANDLES &middot; VOLUME &middot; OVERLAYS</span>\n      </a>\n      <a class=\"fc\" href=\"/insider\" style=\"--facc:#ffd60a\">\n        <span class=\"fc-num\">02 &mdash; INSIDER</span>\n        <span class=\"fc-ic\">&#128269;</span>\n        <div class=\"fc-ttl\">Insider Trades</div>\n        <div class=\"fc-dsc\">SEC Form 4 em tempo real. CEOs, CFOs e directores a comprar ou vender &mdash; at&eacute; 48h ap&oacute;s o trade.</div>\n        <span class=\"fc-tag\">FORM 4 &middot; SEC &middot; LIVE</span>\n      </a>\n      <a class=\"fc\" href=\"/congress\" style=\"--facc:#007aff\">\n        <span class=\"fc-num\">03 &mdash; CONGRESS</span>\n        <span class=\"fc-ic\">&#127963;</span>\n        <div class=\"fc-ttl\">Congress Trades</div>\n        <div class=\"fc-dsc\">Segue o dinheiro de quem faz as leis. Democratas e Republicanos, legalmente obrigados a reportar.</div>\n        <span class=\"fc-tag\">STOCK ACT &middot; C&Acirc;MARAS</span>\n      </a>\n      <a class=\"fc\" href=\"/financials\" style=\"--facc:#ff6b2b\">\n        <span class=\"fc-num\">04 &mdash; FINANCIALS</span>\n        <span class=\"fc-ic\">&#9650;</span>\n        <div class=\"fc-ttl\">Financials</div>\n        <div class=\"fc-dsc\">Waterfall de receita, Sankey de capital, EPS hist&oacute;rico. V&ecirc; para onde vai cada d&oacute;lar em segundos.</div>\n        <span class=\"fc-tag\">RECEITA &middot; COGS &middot; FCF</span>\n      </a>\n      <a class=\"fc\" href=\"/fairvalue\" style=\"--facc:#ff2d55\">\n        <span class=\"fc-num\">05 &mdash; FAIR VALUE</span>\n        <span class=\"fc-ic\">&#11088;</span>\n        <div class=\"fc-ttl\">Fair Value</div>\n        <div class=\"fc-dsc\">DCF, Graham Number, EV/EBITDA, Lynch. Margem de seguran&ccedil;a calculada automaticamente.</div>\n        <span class=\"fc-tag\">DCF &middot; GRAHAM &middot; P/E</span>\n      </a>\n      <a class=\"fc\" href=\"/livefeed\" style=\"--facc:#bf5af2\">\n        <span class=\"fc-num\">06 &mdash; LIVE FEED</span>\n        <span class=\"fc-ic\">&#128308;</span>\n        <div class=\"fc-ttl\">Live Feed</div>\n        <div class=\"fc-dsc\">Toda a bolsa americana em stream cont&iacute;nuo. Filtra por BUY, SELL, ticker ou valor m&iacute;nimo.</div>\n        <span class=\"fc-tag\">TODA A BOLSA &middot; 48H</span>\n      </a>\n    </div>\n  </div>\n</section>\n\n<!-- ══ S2: STATS ══ -->\n<section id=\"s2\">\n  <div class=\"sw\" style=\"padding-bottom:0;padding-top:0\">\n    <div class=\"stats\">\n      <div class=\"stat reveal\"><span class=\"sv\" data-target=\"10\">0</span><span class=\"sl\">M&oacute;dulos de an&aacute;lise</span></div>\n      <div class=\"stat reveal d1\"><span class=\"sv\" data-target=\"30000\" data-sfx=\"+\">0</span><span class=\"sl\">Stocks dispon&iacute;veis</span></div>\n      <div class=\"stat reveal d2\"><span class=\"sv\" data-txt=\"24/7\">&#8212;</span><span class=\"sl\">Crypto sem paragem</span></div>\n      <div class=\"stat reveal d3\"><span class=\"sv\" data-txt=\"$0\">&#8212;</span><span class=\"sl\">Sem paywall</span></div>\n    </div>\n  </div>\n</section>\n\n<!-- ══ S3: COMO FUNCIONA ══ -->\n<section id=\"s3\">\n  <div class=\"sw\">\n    <div class=\"slabel reveal\">Workflow</div>\n    <h2 class=\"stitle reveal d1\">Como funciona<br>o <em>terminal</em>.</h2>\n    <div class=\"how-grid reveal d2\">\n      <div class=\"how\">\n        <div class=\"how-n\">01</div>\n        <div class=\"how-ttl\">Escolhe o activo</div>\n        <div class=\"how-body\">Pesquisa qualquer ticker americano — NYSE, NASDAQ, ou crypto. Mais de 30,000 instrumentos dispon&iacute;veis instantaneamente.</div>\n        <div class=\"how-arrow\">&rarr;</div>\n      </div>\n      <div class=\"how\">\n        <div class=\"how-n\">02</div>\n        <div class=\"how-ttl\">An&aacute;lise completa</div>\n        <div class=\"how-body\">Gr&aacute;fico, insiders, financials, fair value e m&eacute;tricas num &uacute;nico ecr&atilde;. Sem tabs escondidos, sem paywall para os dados importantes.</div>\n        <div class=\"how-arrow\">&rarr;</div>\n      </div>\n      <div class=\"how\">\n        <div class=\"how-n\">03</div>\n        <div class=\"how-ttl\">Decide com contexto</div>\n        <div class=\"how-body\">Quando o CEO est&aacute; a comprar e o fair value est&aacute; 20% abaixo do pre&ccedil;o — isso &eacute; sinal. O IST coloca-te &agrave; frente da informa&ccedil;&atilde;o.</div>\n      </div>\n    </div>\n  </div>\n</section>\n\n<!-- ══ S4: TERMINAL MOCKUP ══ -->\n<section id=\"s4\">\n  <div class=\"sw\">\n    <div class=\"s4-inner\">\n      <div>\n        <div class=\"slabel reveal\">Terminal</div>\n        <h2 class=\"stitle reveal d1\">Um ecr&atilde; para<br>tudo o que<br><em>importa</em>.</h2>\n        <p class=\"sbody reveal d2\">Insiders, gr&aacute;fico e financials lado a lado. Sem saltar entre abas. Sem perder contexto. Profissional.</p>\n        <div class=\"reveal d3\" style=\"margin-top:28px\">\n          <a href=\"/auth\" class=\"btn-main\" style=\"display:inline-flex\">ABRIR TERMINAL &rarr;</a>\n        </div>\n      </div>\n      <div class=\"reveal d2\">\n        <div class=\"mock\">\n          <div class=\"mock-bar\">\n            <div class=\"mdot\" style=\"background:#ff5f57\"></div>\n            <div class=\"mdot\" style=\"background:#febc2e\"></div>\n            <div class=\"mdot\" style=\"background:#28c840\"></div>\n            <span class=\"mock-tab\">NVDA &middot; INSIDER SIGNAL TERMINAL</span>\n          </div>\n          <div class=\"mock-tabs\">\n            <div class=\"mtab on\">INSIDER</div>\n            <div class=\"mtab\">CHART</div>\n            <div class=\"mtab\">FINANCIALS</div>\n            <div class=\"mtab\">FAIR VALUE</div>\n          </div>\n          <div class=\"mock-body\">\n            <div class=\"imock-row\">\n              <div class=\"iav\">JH</div>\n              <div>\n                <div class=\"iname\">Jensen Huang</div>\n                <div class=\"irole\">CEO &middot; Director</div>\n              </div>\n              <div style=\"margin-left:auto;text-align:right\">\n                <span class=\"ibadge isell\">SELL</span>\n                <div class=\"iamt dn\">-$42.8M</div>\n              </div>\n            </div>\n            <div class=\"imock-row\">\n              <div class=\"iav\">CC</div>\n              <div>\n                <div class=\"iname\">Colette Kress</div>\n                <div class=\"irole\">CFO &middot; EVP</div>\n              </div>\n              <div style=\"margin-left:auto;text-align:right\">\n                <span class=\"ibadge isell\">SELL</span>\n                <div class=\"iamt dn\">-$18.3M</div>\n              </div>\n            </div>\n            <div class=\"imock-row\">\n              <div class=\"iav\" style=\"color:var(--gr)\">MS</div>\n              <div>\n                <div class=\"iname\">Mark Stevens</div>\n                <div class=\"irole\">Director</div>\n              </div>\n              <div style=\"margin-left:auto;text-align:right\">\n                <span class=\"ibadge ibuy\">BUY</span>\n                <div class=\"iamt up\">+$2.1M</div>\n              </div>\n            </div>\n            <div class=\"imock-row\">\n              <div class=\"iav\" style=\"color:var(--gr)\">TC</div>\n              <div>\n                <div class=\"iname\">Tench Coxe</div>\n                <div class=\"irole\">Director</div>\n              </div>\n              <div style=\"margin-left:auto;text-align:right\">\n                <span class=\"ibadge ibuy\">BUY</span>\n                <div class=\"iamt up\">+$890K</div>\n              </div>\n            </div>\n            <div style=\"margin-top:12px;padding-top:12px;border-top:1px solid var(--b)\">\n              <div style=\"font-size:8px;color:var(--t3);font-family:var(--fspace);letter-spacing:.1em;margin-bottom:6px\">NVDA &middot; 6M SPARKLINE</div>\n              <div class=\"spark\" id=\"spark-mock\"></div>\n            </div>\n          </div>\n        </div>\n      </div>\n    </div>\n  </div>\n</section>\n\n<!-- ══ S5: FINANCIALS ══ -->\n<section id=\"s5\">\n  <div class=\"sw\">\n    <div class=\"slabel reveal\">Financials</div>\n    <h2 class=\"stitle reveal d1\">Receita que<br>se <em>v&ecirc;</em>.</h2>\n    <p class=\"sbody reveal d2\">Cada d&oacute;lar de receita mapeado. COGS, OpEx, resultado l&iacute;quido — visualizado em segundos.</p>\n\n    <div class=\"fin-showcase reveal d3\">\n      <div class=\"fin-hdr\">\n        <span class=\"fin-ticker\">NVDA</span>\n        <span class=\"fin-sub\">NVIDIA CORP &middot; FY2025 &middot; ANNUAL</span>\n        <span class=\"fin-live\">LIVE</span>\n      </div>\n      <div class=\"fin-grid\">\n        <!-- Box 1: Receita flow -->\n        <div class=\"fin-box\" style=\"grid-column:1/2\">\n          <div class=\"fin-box-label\">Fluxo de Receita</div>\n          <div class=\"flow\" style=\"padding:0;flex-wrap:nowrap;overflow:auto\">\n            <div class=\"fnode\" style=\"background:rgba(0,122,255,.12);border:1px solid rgba(0,122,255,.25);color:#007aff;font-size:10px\">\n              Receita<br><span style=\"font-size:18px;color:var(--t);font-family:var(--fsyne);font-weight:900\">$44.9B</span>\n            </div>\n            <div class=\"farr\">&#8594;</div>\n            <div class=\"fcol\">\n              <div class=\"fnode-sm\" style=\"background:rgba(255,45,85,.1);border:1px solid rgba(255,45,85,.18);color:var(--rd)\">COGS $5.6B</div>\n              <div class=\"fnode-sm\" style=\"background:rgba(0,255,157,.08);border:1px solid rgba(0,255,157,.2);color:var(--gr)\">Bruto $39.3B</div>\n            </div>\n            <div class=\"farr\">&#8594;</div>\n            <div class=\"fcol\">\n              <div class=\"fnode-sm\" style=\"background:rgba(255,107,43,.1);border:1px solid rgba(255,107,43,.18);color:var(--or)\">OpEx $4.7B</div>\n              <div class=\"fnode-sm\" style=\"background:rgba(0,255,157,.15);border:1px solid rgba(0,255,157,.35);color:var(--gr);font-weight:700\">Net $29.8B</div>\n            </div>\n          </div>\n        </div>\n        <!-- Box 2: KPIs -->\n        <div class=\"fin-box\" style=\"background:var(--bg2)\">\n          <div class=\"fin-box-label\">Margem Bruta</div>\n          <div class=\"fin-box-val\" style=\"color:var(--gr)\">87.5%</div>\n          <div class=\"fin-box-sub\">+4.2pp YoY &uarr;</div>\n        </div>\n        <div class=\"fin-box\">\n          <div class=\"fin-box-label\">Free Cash Flow</div>\n          <div class=\"fin-box-val\" style=\"color:var(--gr)\">$27.4B</div>\n          <div class=\"fin-box-sub\">FCF Yield 2.1%</div>\n        </div>\n        <!-- Box 3: Margin bars -->\n        <div class=\"fin-box\" style=\"grid-column:1/2;border-top:1px solid var(--b)\">\n          <div class=\"fin-box-label\">Margens</div>\n          <div class=\"mbar-row\" style=\"padding:0;margin-top:8px\">\n            <div class=\"mbar\">\n              <div class=\"mbar-label\"><span>Margem Bruta</span><span style=\"color:var(--gr)\">87.5%</span></div>\n              <div class=\"mbar-track\"><div class=\"mbar-fill\" data-w=\"87.5\" style=\"background:var(--gr);width:0%\"></div></div>\n            </div>\n            <div class=\"mbar\">\n              <div class=\"mbar-label\"><span>Margem Operacional</span><span style=\"color:var(--or)\">61.7%</span></div>\n              <div class=\"mbar-track\"><div class=\"mbar-fill\" data-w=\"61.7\" style=\"background:var(--or);width:0%\"></div></div>\n            </div>\n            <div class=\"mbar\">\n              <div class=\"mbar-label\"><span>Margem L&iacute;quida</span><span style=\"color:var(--gr)\">66.4%</span></div>\n              <div class=\"mbar-track\"><div class=\"mbar-fill\" data-w=\"66.4\" style=\"background:var(--gr);width:0%\"></div></div>\n            </div>\n          </div>\n        </div>\n        <div class=\"fin-box\" style=\"border-top:1px solid var(--b);background:var(--bg2)\">\n          <div class=\"fin-box-label\">Margem L&iacute;quida</div>\n          <div class=\"fin-box-val\" style=\"color:var(--gr)\">66.4%</div>\n          <div class=\"fin-box-sub\">Top 3% S&amp;P500</div>\n        </div>\n        <div class=\"fin-box\" style=\"border-top:1px solid var(--b)\">\n          <div class=\"fin-box-label\">EPS Diluído</div>\n          <div class=\"fin-box-val\" style=\"color:var(--gr)\">$1.19</div>\n          <div class=\"fin-box-sub\">+130% YoY &uarr;</div>\n        </div>\n      </div>\n    </div>\n  </div>\n</section>\n\n<!-- ══ S6: CONGRESS ══ -->\n<section id=\"s6\">\n  <div class=\"sw\">\n    <div class=\"slabel reveal\">Congress Trades</div>\n    <h2 class=\"stitle reveal d1\">Segue o dinheiro<br>de quem <em>legisla</em>.</h2>\n    <p class=\"sbody reveal d2\">Senadores e deputados s&atilde;o obrigados a reportar os seus trades. N&oacute;s mostramos-os em tempo real.</p>\n    <div class=\"cong-grid reveal d3\">\n      <div class=\"cong-side\">\n        <span class=\"cong-label dem\">&#9632; DEMOCRATAS</span>\n        <div class=\"cong-row\">\n          <div class=\"cav\" style=\"background:rgba(74,158,255,.1);color:#4a9eff;border:1px solid rgba(74,158,255,.2)\">NP</div>\n          <div><div class=\"cname\">Nancy Pelosi</div><div class=\"cstock\">NVDA &middot; CALLS</div></div>\n          <div class=\"camt up\">+$5.0M</div>\n        </div>\n        <div class=\"cong-row\">\n          <div class=\"cav\" style=\"background:rgba(74,158,255,.1);color:#4a9eff;border:1px solid rgba(74,158,255,.2)\">MS</div>\n          <div><div class=\"cname\">Mark Warner</div><div class=\"cstock\">MSFT &middot; BUY</div></div>\n          <div class=\"camt up\">+$250K</div>\n        </div>\n        <div class=\"cong-row\">\n          <div class=\"cav\" style=\"background:rgba(74,158,255,.1);color:#4a9eff;border:1px solid rgba(74,158,255,.2)\">RW</div>\n          <div><div class=\"cname\">Ron Wyden</div><div class=\"cstock\">AAPL &middot; SELL</div></div>\n          <div class=\"camt dn\">-$180K</div>\n        </div>\n      </div>\n      <div class=\"cong-side\" style=\"background:var(--bg2)\">\n        <span class=\"cong-label rep\">&#9632; REPUBLICANOS</span>\n        <div class=\"cong-row\">\n          <div class=\"cav\" style=\"background:rgba(255,69,58,.1);color:#ff453a;border:1px solid rgba(255,69,58,.2)\">TT</div>\n          <div><div class=\"cname\">Tommy Tuberville</div><div class=\"cstock\">GE &middot; BUY</div></div>\n          <div class=\"camt up\">+$1.2M</div>\n        </div>\n        <div class=\"cong-row\">\n          <div class=\"cav\" style=\"background:rgba(255,69,58,.1);color:#ff453a;border:1px solid rgba(255,69,58,.2)\">MC</div>\n          <div><div class=\"cname\">Mitch McConnell</div><div class=\"cstock\">LMT &middot; BUY</div></div>\n          <div class=\"camt up\">+$500K</div>\n        </div>\n        <div class=\"cong-row\">\n          <div class=\"cav\" style=\"background:rgba(255,69,58,.1);color:#ff453a;border:1px solid rgba(255,69,58,.2)\">DS</div>\n          <div><div class=\"cname\">Dan Sullivan</div><div class=\"cstock\">XOM &middot; SELL</div></div>\n          <div class=\"camt dn\">-$90K</div>\n        </div>\n      </div>\n    </div>\n  </div>\n</section>\n\n<!-- ══ S7: CTA ══ -->\n<section id=\"s7\">\n  <div class=\"cta-bg\">\n    <div class=\"cta-grid\"></div>\n    <div class=\"cta-glow\"></div>\n  </div>\n  <div class=\"cta-inner\">\n    <div class=\"slabel reveal\" style=\"justify-content:center\">Pronto</div>\n    <h2 class=\"cta-title reveal d1\">O TERMINAL<br>QUE <span>MERECES</span>.</h2>\n    <p class=\"cta-sub reveal d2\">Sem publicidade. Sem paywall.<br>Dados reais, an&aacute;lise real.</p>\n    <div class=\"cta-btns reveal d3\">\n      <a href=\"/auth\" class=\"btn-main\" style=\"font-size:12px;padding:16px 40px\">ABRIR TERMINAL &nearr;</a>\n      <a href=\"/pricing\" class=\"btn-ghost\" style=\"font-size:12px;padding:16px 40px\">VER PRICING</a>\n    </div>\n    <div class=\"cta-prices reveal d4\">\n      <div class=\"cp\"><div class=\"cps\">SP500</div><div class=\"cpp\" id=\"c_GSPC\">&#8212;</div><div class=\"cpc nc\" id=\"cc_GSPC\">&#8212;</div></div>\n      <div class=\"cp\"><div class=\"cps\">BTC</div><div class=\"cpp\" id=\"c_BTC\">&#8212;</div><div class=\"cpc nc\" id=\"cc_BTC\">&#8212;</div></div>\n      <div class=\"cp\"><div class=\"cps\">GOLD</div><div class=\"cpp\" id=\"c_GCF\">&#8212;</div><div class=\"cpc nc\" id=\"cc_GCF\">&#8212;</div></div>\n      <div class=\"cp\"><div class=\"cps\">NVDA</div><div class=\"cpp\" id=\"c_NVDA\">&#8212;</div><div class=\"cpc nc\" id=\"cc_NVDA\">&#8212;</div></div>\n    </div>\n  </div>\n</section>\n\n<footer>\n  <span class=\"fl\">IST</span>\n  <span class=\"fn\">INSIDER SIGNAL TERMINAL &middot; DADOS EM TEMPO REAL &middot; SEM PAYWALL</span>\n</footer>\n\n<script>\n// ── CURSOR ──\nconst cur=document.getElementById(\"cur\"),cur2=document.getElementById(\"cur2\");\nlet mx=0,my=0,cx=0,cy=0;\ndocument.addEventListener(\"mousemove\",e=>{mx=e.clientX;my=e.clientY;cur.style.left=mx+\"px\";cur.style.top=my+\"px\";});\nsetInterval(()=>{cx+=(mx-cx)*.15;cy+=(my-cy)*.15;cur2.style.left=cx+\"px\";cur2.style.top=cy+\"px\";},16);\ndocument.querySelectorAll(\"a,button,.pc,.fc,.how,.stat\").forEach(el=>{\n  el.addEventListener(\"mouseenter\",()=>{cur.style.width=\"16px\";cur.style.height=\"16px\";});\n  el.addEventListener(\"mouseleave\",()=>{cur.style.width=\"8px\";cur.style.height=\"8px\";});\n});\n\n// ── UTILS ──\nfunction go(t){window.location.href=\"/chart?t=\"+encodeURIComponent(t);}\nfunction fmtPx(v,t){if(v==null)return\"—\";t=t||\"\";if(t.includes(\"BTC\")||t.includes(\"ETH\")||v>10000)return\"$\"+Math.round(v).toLocaleString(\"en-US\");if(v>=1)return\"$\"+v.toFixed(2);return\"$\"+v.toFixed(4);}\nfunction fmtPct(v){return v==null?\"—\":(v>=0?\"+\":\"\")+v.toFixed(2)+\"%\";}\nfunction cls(v){return v==null?\"nc\":v>0?\"up\":\"dn\";}\n\n// ── NAV SCROLL ──\nwindow.addEventListener(\"scroll\",()=>{\n  const pct=(scrollY/(document.body.scrollHeight-innerHeight))*100;\n  document.getElementById(\"progress\").style.width=pct+\"%\";\n  document.getElementById(\"nav\").classList.toggle(\"scrolled\",scrollY>40);\n});\n\n// ── TAPE ──\nconst SYMS=[\"^GSPC\",\"^IXIC\",\"^VIX\",\"GC=F\",\"CL=F\",\"BTC-USD\",\"ETH-USD\",\"^TNX\",\"DX-Y.NYB\",\"SPY\",\"QQQ\",\"NVDA\",\"AAPL\",\"MSFT\",\"TSLA\",\"AMZN\",\"META\",\"GOOG\"];\nconst LBL={\"^GSPC\":\"SP500\",\"^IXIC\":\"NASDAQ\",\"^VIX\":\"VIX\",\"GC=F\":\"GOLD\",\"CL=F\":\"WTI\",\"BTC-USD\":\"BTC\",\"ETH-USD\":\"ETH\",\"^TNX\":\"US10Y\",\"DX-Y.NYB\":\"DXY\",\"SPY\":\"SPY\",\"QQQ\":\"QQQ\",\"NVDA\":\"NVDA\",\"AAPL\":\"AAPL\",\"MSFT\":\"MSFT\",\"TSLA\":\"TSLA\",\"AMZN\":\"AMZN\",\"META\":\"META\",\"GOOG\":\"GOOG\"};\nconst TID=t=>\"T_\"+t.replace(/[^a-zA-Z0-9]/g,\"_\");\ndocument.getElementById(\"tape-track\").innerHTML=[...SYMS,...SYMS].map(t=>\n  `<div class=\"ti\" id=\"${TID(t)}\" onclick=\"go('${t}')\"><span class=\"ts\">${LBL[t]||t}</span>&nbsp;<span class=\"tv\">—</span>&nbsp;<span class=\"tc nc\">—</span></div>`\n).join(\"\");\n\n// ── SPARKLINE ──\nfunction makeSpark(id,n){\n  const el=document.getElementById(id);if(!el)return;\n  let v=100;const vals=[];\n  for(let i=0;i<n;i++){v+=(Math.random()-.42)*5;vals.push(Math.max(20,v));}\n  const mx=Math.max(...vals);\n  el.innerHTML=vals.map((h,i)=>{\n    const up=i===0||h>=(vals[i-1]||h);\n    return`<div class=\"spk ${up?\"u\":\"d\"}\" style=\"height:${(h/mx*100)}%;transition-delay:${i*.015}s\"></div>`;\n  }).join(\"\");\n}\nsetTimeout(()=>makeSpark(\"spark-mock\",36),400);\n\n// ── PRICE UPDATE ──\nfunction upd(p){\n  const t=p.ticker,px=p.price,pct=p.change_pct;\n  if(!t||px==null)return;\n  const fm=fmtPx(px,t),pc=fmtPct(pct),c=cls(pct);\n  // Hero strip\n  const hmap={\"^GSPC\":\"GSPC\",\"^IXIC\":\"IXIC\",\"BTC-USD\":\"BTC\",\"GC=F\":\"GCF\",\"NVDA\":\"NVDA\",\"^VIX\":\"VIX\"};\n  const hk=hmap[t];\n  if(hk){\n    const pe=document.getElementById(\"hp_\"+hk),ce=document.getElementById(\"hc_\"+hk);\n    if(pe)pe.textContent=fm;\n    if(ce){ce.textContent=pc;ce.className=\"pch \"+c;}\n  }\n  // CTA strip\n  const ck={\"^GSPC\":\"GSPC\",\"BTC-USD\":\"BTC\",\"GC=F\":\"GCF\",\"NVDA\":\"NVDA\"}[t];\n  if(ck){\n    const pe=document.getElementById(\"c_\"+ck),ce=document.getElementById(\"cc_\"+ck);\n    if(pe)pe.textContent=fm;\n    if(ce){ce.textContent=pc;ce.className=\"cpc \"+c;}\n  }\n  // Tape\n  const te=document.getElementById(TID(t));\n  if(te){const cs=te.children;if(cs[1])cs[1].textContent=fm;if(cs[2]){cs[2].textContent=pc;cs[2].className=\"tc \"+c;}}\n}\ntry{const s=io();s.on(\"connect\",()=>s.emit(\"subscribe\",{tickers:SYMS}));s.on(\"price_update\",({prices})=>{if(Array.isArray(prices))prices.forEach(upd);});}catch(e){}\nfetch(\"/api/watchlist?tickers=\"+SYMS.join(\",\")).then(r=>r.json()).then(d=>{if(d?.stocks)d.stocks.forEach(upd);}).catch(()=>{});\n\n// ── SCROLL REVEAL ──\nconst revObs=new IntersectionObserver(entries=>{\n  entries.forEach(e=>{\n    if(!e.isIntersecting)return;\n    e.target.classList.add(\"on\");\n    // Counter animation\n    e.target.querySelectorAll(\"[data-target]\").forEach(el=>{\n      const tgt=parseInt(el.getAttribute(\"data-target\")),sfx=el.getAttribute(\"data-sfx\")||\"\";\n      let cur=0;const step=Math.ceil(tgt/50);\n      const iv=setInterval(()=>{cur=Math.min(cur+step,tgt);el.textContent=cur.toLocaleString()+sfx;if(cur>=tgt)clearInterval(iv);},18);\n    });\n    e.target.querySelectorAll(\"[data-txt]\").forEach(el=>{\n      setTimeout(()=>el.textContent=el.getAttribute(\"data-txt\"),200);\n    });\n    // Margin bars\n    e.target.querySelectorAll(\".mbar-fill[data-w]\").forEach(el=>{\n      setTimeout(()=>el.style.width=el.getAttribute(\"data-w\")+\"%\",300);\n    });\n  });\n},{threshold:0.12});\ndocument.querySelectorAll(\".reveal\").forEach(el=>revObs.observe(el));\n\n// Also observe fin-showcase separately for margin bars\nconst finObs=new IntersectionObserver(entries=>{\n  entries.forEach(e=>{\n    if(!e.isIntersecting)return;\n    e.target.querySelectorAll(\".mbar-fill[data-w]\").forEach(el=>{\n      setTimeout(()=>el.style.width=el.getAttribute(\"data-w\")+\"%\",300);\n    });\n  });\n},{threshold:0.15});\ndocument.querySelectorAll(\".fin-showcase\").forEach(el=>finObs.observe(el));\n</script>\n</body>\n</html>\n",
    "pricing.html": "<!DOCTYPE html>\n<html lang=\"pt\">\n<head>\n<meta charset=\"UTF-8\">\n<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\n<title>Pricing · IST</title>\n<script src=\"https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.7.5/socket.io.min.js\"></script>\n<link href=\"https://fonts.googleapis.com/css2?family=DM+Mono:wght@300;400;500&family=Bebas+Neue&family=Cabinet+Grotesk:wght@300;400;500;700;800;900&display=swap\" rel=\"stylesheet\">\n<style>\n*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}\n:root{\n  --bg:#040608;--bg1:#070b0f;--bg2:#0a0f14;--bg3:#0e141b;\n  --b:#162030;--b2:#1c2c3e;\n  --t:#d4dde6;--t2:#7a8fa0;--t3:#364860;\n  --gr:#00e5a0;--rd:#ff3d5a;--bl:#0088ee;--yl:#f5b942;--pu:#a78bfa;\n  --fm:\"DM Mono\",monospace;--fd:\"Cabinet Grotesk\",sans-serif;--fh:\"Bebas Neue\",sans-serif;\n}\nhtml{scroll-behavior:smooth;overflow-x:hidden}\nbody{background:var(--bg);color:var(--t);font-family:var(--fd);font-size:14px;overflow-x:hidden}\n::-webkit-scrollbar{width:3px}::-webkit-scrollbar-thumb{background:var(--b2);border-radius:2px}\n\n/* TAPE */\n#tape{position:fixed;top:0;left:0;right:0;height:28px;background:rgba(7,11,15,.96);border-bottom:1px solid var(--b);z-index:100;overflow:hidden;display:flex;align-items:center;backdrop-filter:blur(12px)}\n#tape-track{display:flex;white-space:nowrap;animation:tape 80s linear infinite}\n@keyframes tape{from{transform:translateX(0)}to{transform:translateX(-50%)}}\n.ti{display:inline-flex;align-items:center;gap:6px;padding:0 18px;height:28px;border-right:1px solid var(--b);font-size:10px;cursor:pointer;flex-shrink:0;font-family:var(--fm)}\n.ti:hover{background:var(--bg2)}.ts{color:var(--t3)}.tv{color:var(--t)}.tc{font-size:9px}\n.up{color:var(--gr)}.dn{color:var(--rd)}.nc{color:var(--t2)}\n\n/* NAV */\nnav{position:fixed;top:28px;left:0;right:0;height:52px;z-index:99;display:flex;align-items:center;padding:0 48px;background:rgba(4,6,8,.92);border-bottom:1px solid var(--b);backdrop-filter:blur(20px)}\n.nav-logo{display:flex;align-items:center;gap:10px;text-decoration:none;margin-right:auto}\n.nav-logo-mark{width:26px;height:26px;background:var(--gr);border-radius:5px;display:flex;align-items:center;justify-content:center}\n.nav-logo-mark svg{width:14px;height:14px}\n.nav-logo-txt{font-family:var(--fh);font-size:22px;color:var(--t);letter-spacing:.1em}\n.nav-links{display:flex;gap:2px}\n.nl{padding:6px 14px;border-radius:4px;font-size:12px;font-weight:500;color:var(--t2);text-decoration:none;transition:all .15s}\n.nl:hover{color:var(--t);background:rgba(255,255,255,.05)}\n.nl.on{color:var(--gr)}\n.nav-cta{margin-left:16px;padding:8px 20px;border-radius:4px;font-size:12px;font-weight:700;color:var(--bg);background:var(--gr);text-decoration:none;transition:all .2s;text-transform:uppercase;letter-spacing:.04em}\n.nav-cta:hover{background:#00ffb3;box-shadow:0 0 28px rgba(0,229,160,.4)}\n\n/* PAGE WRAP */\n.page{padding-top:80px}\n\n/* HERO */\n.hero{padding:80px 48px 60px;text-align:center;position:relative;overflow:hidden}\n.hero-bg{position:absolute;inset:0;background:radial-gradient(ellipse 800px 500px at 50% 60%,rgba(0,229,160,.04) 0%,transparent 70%);pointer-events:none}\n.hero-label{font-size:11px;font-weight:700;letter-spacing:.18em;text-transform:uppercase;color:var(--gr);margin-bottom:16px;display:flex;align-items:center;justify-content:center;gap:8px}\n.hero-label::before,.hero-label::after{content:\"\";width:24px;height:1px;background:var(--gr)}\n.hero-h{font-family:var(--fh);font-size:clamp(64px,8vw,100px);color:var(--t);line-height:.92;letter-spacing:.02em;margin-bottom:20px}\n.hero-h em{font-style:normal;color:var(--gr)}\n.hero-sub{font-size:16px;color:var(--t2);line-height:1.65;max-width:480px;margin:0 auto 48px}\n\n/* BILLING TOGGLE */\n.toggle-wrap{display:inline-flex;align-items:center;gap:14px;background:var(--bg2);border:1px solid var(--b);border-radius:40px;padding:6px 18px;margin-bottom:72px}\n.tog-lbl{font-size:12px;font-weight:600;color:var(--t3);cursor:pointer;transition:color .2s;letter-spacing:.03em;user-select:none}\n.tog-lbl.on{color:var(--t)}\n.tog-sw{width:40px;height:22px;background:var(--gr);border-radius:11px;position:relative;cursor:pointer;transition:background .2s;flex-shrink:0}\n.tog-sw::after{content:\"\";position:absolute;top:3px;left:3px;width:16px;height:16px;background:var(--bg);border-radius:50%;transition:transform .25s cubic-bezier(.16,1,.3,1)}\n.tog-sw.yr::after{transform:translateX(18px)}\n.save-pill{background:rgba(0,229,160,.1);border:1px solid rgba(0,229,160,.2);color:var(--gr);font-size:9px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;padding:2px 7px;border-radius:8px}\n\n/* PLANS GRID */\n.plans{max-width:1060px;margin:0 auto;padding:0 48px 100px;display:grid;grid-template-columns:repeat(3,1fr);gap:1px;background:var(--b);border:1px solid var(--b);border-radius:14px;overflow:hidden}\n\n.plan{background:var(--bg2);padding:44px 36px;display:flex;flex-direction:column;transition:background .2s,transform .3s cubic-bezier(.16,1,.3,1),box-shadow .3s;position:relative}\n.plan.featured{background:var(--bg3)}\n.plan:hover{background:#0c1219;transform:translateY(-6px);box-shadow:0 20px 60px rgba(0,0,0,.5)}\n.plan.featured:hover{background:#0f1520;transform:translateY(-8px);box-shadow:0 24px 70px rgba(0,229,160,.12)}\n.plan.elite:hover{transform:translateY(-6px);box-shadow:0 20px 60px rgba(167,139,250,.1)}\n\n/* accent top line */\n.plan::before{content:\"\";position:absolute;top:0;left:0;right:0;height:2px;opacity:0;transition:opacity .3s}\n.plan.free::before{background:var(--t3);opacity:.4}\n.plan.pro::before{background:var(--gr);opacity:1}\n.plan.elite::before{background:linear-gradient(90deg,#a78bfa,#0088ee);opacity:1}\n\n.plan-tag{font-size:10px;font-weight:700;letter-spacing:.12em;text-transform:uppercase;padding:3px 10px;border-radius:12px;display:inline-block;margin-bottom:20px;align-self:flex-start}\n.plan-name{font-family:var(--fh);font-size:40px;letter-spacing:.05em;margin-bottom:10px}\n.plan-desc{font-size:13px;color:var(--t2);line-height:1.6;margin-bottom:28px;min-height:40px}\n\n.plan-price-block{margin-bottom:28px}\n.plan-price{display:flex;align-items:flex-start;gap:3px;line-height:1}\n.plan-currency{font-size:20px;font-weight:700;color:var(--t2);margin-top:8px}\n.plan-amount{font-family:var(--fh);font-size:72px;letter-spacing:-.01em}\n.plan-cents{font-size:20px;font-weight:700;color:var(--t2);align-self:flex-end;margin-bottom:6px}\n.plan-period{font-size:11px;color:var(--t3);margin-top:8px;letter-spacing:.03em}\n.plan-annual-note{font-size:11px;color:var(--gr);margin-top:5px;min-height:18px}\n\n.plan-btn{display:block;width:100%;padding:13px;border-radius:6px;font-size:13px;font-weight:700;letter-spacing:.05em;text-transform:uppercase;text-align:center;text-decoration:none;cursor:pointer;transition:all .2s;border:none;margin-bottom:32px}\n.plan-btn.free-btn{background:transparent;color:var(--t2);border:1px solid var(--b2)}.plan-btn.free-btn:hover{border-color:var(--t3);color:var(--t)}\n.plan-btn.pro-btn{background:var(--gr);color:var(--bg)}.plan-btn.pro-btn:hover{background:#00ffb3;box-shadow:0 6px 28px rgba(0,229,160,.35);transform:translateY(-1px)}\n.plan-btn.elite-btn{background:linear-gradient(135deg,#a78bfa,#0088ee);color:#fff}.plan-btn.elite-btn:hover{box-shadow:0 6px 28px rgba(167,139,250,.4);transform:translateY(-1px)}\n\n.plan-divider{height:1px;background:var(--b);margin-bottom:24px}\n.plan-section{font-size:9px;font-weight:700;letter-spacing:.14em;text-transform:uppercase;color:var(--t3);margin-bottom:14px}\n\n.fi{display:flex;align-items:flex-start;gap:9px;margin-bottom:10px}\n.fi-icon{width:17px;height:17px;border-radius:50%;flex-shrink:0;display:flex;align-items:center;justify-content:center;font-size:8px;font-weight:700;margin-top:1px}\n.fi-y{background:rgba(0,229,160,.12);color:var(--gr)}\n.fi-n{background:rgba(255,255,255,.04);color:var(--t3)}\n.fi-s{background:rgba(167,139,250,.14);color:var(--pu)}\n.fi-txt{font-size:12px;color:var(--t2);line-height:1.55}\n.fi-txt strong{color:var(--t);font-weight:600}\n.fi-soon{font-size:8px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;background:rgba(245,185,66,.1);color:var(--yl);padding:1px 5px;border-radius:3px;margin-left:5px}\n.fi-new{font-size:8px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;background:rgba(0,229,160,.1);color:var(--gr);padding:1px 5px;border-radius:3px;margin-left:5px}\n\n/* COMPARISON TABLE */\n.comp-wrap{max-width:1060px;margin:0 auto;padding:0 48px 100px}\n.comp-title{font-family:var(--fh);font-size:clamp(52px,6vw,72px);color:var(--t);line-height:.93;letter-spacing:.02em;margin-bottom:48px;text-align:center}\n.comp-title em{font-style:normal;color:var(--gr)}\n.comp-table{border:1px solid var(--b);border-radius:10px;overflow:hidden}\n.comp-row{display:grid;grid-template-columns:2fr 1fr 1fr 1fr;border-bottom:1px solid var(--b)}\n.comp-row:last-child{border-bottom:none}\n.comp-row:hover:not(.comp-hdr):not(.comp-grp){background:rgba(255,255,255,.015)}\n.comp-hdr{background:var(--bg2);position:sticky;top:80px}\n.comp-grp{background:var(--bg3);grid-column:1/-1;padding:10px 20px;font-size:9px;font-weight:700;letter-spacing:.14em;text-transform:uppercase;color:var(--t3);border-top:1px solid var(--b)}\n.cc{padding:13px 20px;display:flex;align-items:center}\n.cc.fn{font-size:12px;color:var(--t2)}\n.cc.pn{font-family:var(--fh);font-size:18px;letter-spacing:.06em;justify-content:center;text-align:center}\n.cc.vl{justify-content:center;text-align:center}\n.ck{font-size:15px;color:var(--gr)}.cx{font-size:13px;color:var(--t3)}.ct{font-size:11px;color:var(--t2)}\n.ck-p{font-size:15px;color:var(--pu)}\n\n/* FAQ */\n.faq-wrap{max-width:760px;margin:0 auto;padding:0 48px 100px}\n.faq-title{font-family:var(--fh);font-size:clamp(52px,5vw,68px);color:var(--t);line-height:.93;margin-bottom:48px;text-align:center}\n.faq-item{border-bottom:1px solid var(--b)}\n.fq{padding:18px 0;font-size:15px;font-weight:600;color:var(--t);cursor:pointer;display:flex;justify-content:space-between;align-items:center;gap:16px;transition:color .2s;user-select:none}\n.fq:hover{color:var(--gr)}\n.fi-btn{width:20px;height:20px;border:1px solid var(--b2);border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:12px;color:var(--t3);flex-shrink:0;transition:all .3s}\n.faq-item.open .fi-btn{background:var(--gr);border-color:var(--gr);color:var(--bg);transform:rotate(45deg)}\n.fa{max-height:0;overflow:hidden;transition:max-height .4s cubic-bezier(.16,1,.3,1)}\n.faq-item.open .fa{max-height:200px}\n.fa-in{padding:0 0 18px;font-size:14px;color:var(--t2);line-height:1.72}\n\n/* BOTTOM CTA */\n.bcta{text-align:center;padding:80px 48px 100px;border-top:1px solid var(--b);position:relative;overflow:hidden}\n.bcta-glow{position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);width:600px;height:400px;background:radial-gradient(ellipse,rgba(0,229,160,.06) 0%,transparent 68%);pointer-events:none}\n.bcta-h{font-family:var(--fh);font-size:clamp(56px,7vw,88px);color:var(--t);line-height:.93;margin-bottom:20px;position:relative}\n.bcta-h span{color:var(--gr)}\n.bcta-sub{font-size:15px;color:var(--t2);margin-bottom:40px;line-height:1.6;position:relative}\n.bcta-btn{display:inline-flex;align-items:center;gap:8px;padding:15px 36px;border-radius:6px;font-size:14px;font-weight:700;letter-spacing:.05em;text-transform:uppercase;text-decoration:none;background:var(--gr);color:var(--bg);transition:all .2s;position:relative}\n.bcta-btn:hover{background:#00ffb3;transform:translateY(-2px);box-shadow:0 10px 40px rgba(0,229,160,.3)}\n\nfooter{border-top:1px solid var(--b);padding:22px 48px;display:flex;align-items:center;justify-content:space-between;background:var(--bg1)}\n.fl{font-family:var(--fh);font-size:18px;color:var(--t3);letter-spacing:.1em}\n.fn{font-size:10px;color:var(--t3)}\n\n.reveal{opacity:0;transform:translateY(32px);transition:opacity .8s cubic-bezier(.16,1,.3,1),transform .8s cubic-bezier(.16,1,.3,1)}\n.reveal.visible{opacity:1;transform:translateY(0)}\n.d1{transition-delay:.1s}.d2{transition-delay:.2s}.d3{transition-delay:.3s}\n</style>\n</head>\n<body>\n\n<div id=\"tape\"><div id=\"tape-track\"></div></div>\n\n<nav>\n  <a href=\"/\" class=\"nav-logo\">\n    <div class=\"nav-logo-mark\">\n      <svg viewBox=\"0 0 14 14\" fill=\"none\"><polyline points=\"1,11 4,6 7,8 10,3 13,4.5\" stroke=\"#040608\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\"/><circle cx=\"10\" cy=\"3\" r=\"1.4\" fill=\"#040608\"/></svg>\n    </div>\n    <span class=\"nav-logo-txt\">IST</span>\n  </a>\n  <div class=\"nav-links\">\n    <a href=\"/chart\" class=\"nl\">Chart</a>\n    <a href=\"/livefeed\" class=\"nl\">Live Feed</a>\n    <a href=\"/pricing\" class=\"nl on\">Pricing</a>\n    <a href=\"/terminal\" class=\"nl\">Terminal</a>\n  </div>\n  <a href=\"/terminal\" class=\"nav-cta\">Entrar &rarr;</a>\n</nav>\n\n<div class=\"page\">\n\n<!-- HERO -->\n<div class=\"hero\">\n  <div class=\"hero-bg\"></div>\n  <div class=\"hero-label reveal\">Planos e Pre&ccedil;os</div>\n  <h1 class=\"hero-h reveal d1\">SIMPLES.<br><em>SEM SURPRESAS.</em></h1>\n  <p class=\"hero-sub reveal d2\">Come&ccedil;a gr&aacute;tis. Faz upgrade quando precisares. Cancela a qualquer momento.</p>\n\n  <div class=\"toggle-wrap reveal d3\">\n    <span class=\"tog-lbl on\" id=\"lbl-m\" onclick=\"setBilling('m')\">Mensal</span>\n    <div class=\"tog-sw\" id=\"tog\" onclick=\"toggleBilling()\"></div>\n    <span class=\"tog-lbl\" id=\"lbl-y\" onclick=\"setBilling('y')\">Anual &nbsp;<span class=\"save-pill\">-20%</span></span>\n  </div>\n</div>\n\n<!-- PLANS -->\n<div class=\"plans\">\n\n  <!-- FREE -->\n  <div class=\"plan free reveal\">\n    <span class=\"plan-tag\" style=\"background:rgba(54,72,96,.35);color:var(--t3)\">Gratuito</span>\n    <div class=\"plan-name\" style=\"color:var(--t)\">FREE</div>\n    <div class=\"plan-desc\">Para explorar o terminal e mercados em tempo real, sem compromissos.</div>\n    <div class=\"plan-price-block\">\n      <div class=\"plan-price\">\n        <span class=\"plan-currency\">&euro;</span>\n        <span class=\"plan-amount\" style=\"color:var(--t)\">0</span>\n      </div>\n      <div class=\"plan-period\">Para sempre &middot; Sem cart&atilde;o</div>\n      <div class=\"plan-annual-note\">&nbsp;</div>\n    </div>\n    <a href=\"/terminal\" class=\"plan-btn free-btn\">Come&ccedil;ar agora</a>\n    <div class=\"plan-divider\"></div>\n    <div class=\"plan-section\">Inclu&iacute;do</div>\n    <div class=\"fi\"><div class=\"fi-icon fi-y\">&#10003;</div><div class=\"fi-txt\">Tape em tempo real &mdash; todos os mercados</div></div>\n    <div class=\"fi\"><div class=\"fi-icon fi-y\">&#10003;</div><div class=\"fi-txt\">Gr&aacute;ficos hist&oacute;ricos &mdash; 5D at&eacute; 5Y</div></div>\n    <div class=\"fi\"><div class=\"fi-icon fi-y\">&#10003;</div><div class=\"fi-txt\">Watchlist com <strong>50 tickers</strong></div></div>\n    <div class=\"fi\"><div class=\"fi-icon fi-y\">&#10003;</div><div class=\"fi-txt\">Financials b&aacute;sicos (receita, EPS)</div></div>\n    <div class=\"fi\"><div class=\"fi-icon fi-y\">&#10003;</div><div class=\"fi-txt\">M&eacute;tricas &mdash; P/E, P/S, ROE</div></div>\n    <div class=\"fi\"><div class=\"fi-icon fi-n\">&mdash;</div><div class=\"fi-txt\" style=\"opacity:.4\">Overlays S&amp;P, QQQ, M2</div></div>\n    <div class=\"fi\"><div class=\"fi-icon fi-n\">&mdash;</div><div class=\"fi-txt\" style=\"opacity:.4\">Insider Trades &amp; Congress</div></div>\n    <div class=\"fi\"><div class=\"fi-icon fi-n\">&mdash;</div><div class=\"fi-txt\" style=\"opacity:.4\">Fair Value (DCF, Graham)</div></div>\n  </div>\n\n  <!-- PRO -->\n  <div class=\"plan pro featured reveal d1\">\n    <span class=\"plan-tag\" style=\"background:rgba(0,229,160,.1);color:var(--gr)\">Mais popular</span>\n    <div class=\"plan-name\" style=\"color:var(--gr)\">PRO</div>\n    <div class=\"plan-desc\">Para traders e investidores que querem vantagem real sobre o mercado.</div>\n    <div class=\"plan-price-block\">\n      <div class=\"plan-price\">\n        <span class=\"plan-currency\">&euro;</span>\n        <span class=\"plan-amount\" id=\"pro-int\" style=\"color:var(--gr)\">15</span>\n        <span class=\"plan-cents\" id=\"pro-dec\">,99</span>\n      </div>\n      <div class=\"plan-period\" id=\"pro-period\">por m&ecirc;s</div>\n      <div class=\"plan-annual-note\" id=\"pro-note\">&nbsp;</div>\n    </div>\n    <div style=\"margin-bottom:8px;text-align:center\"><span style=\"background:rgba(0,229,160,.1);border:1px solid rgba(0,229,160,.2);color:var(--gr);font-size:9px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;padding:3px 10px;border-radius:10px\">7 dias gr&aacute;tis</span></div>\n    <button onclick=\"location.href='/terminal'\" class=\"plan-btn pro-btn\">Come&ccedil;ar Trial</button>\n    <div class=\"plan-divider\"></div>\n    <div class=\"plan-section\">Tudo do Free, mais:</div>\n    <div class=\"fi\"><div class=\"fi-icon fi-y\">&#10003;</div><div class=\"fi-txt\"><strong>Overlays</strong> S&amp;P500, QQQ, M2 nos gr&aacute;ficos</div></div>\n    <div class=\"fi\"><div class=\"fi-icon fi-y\">&#10003;</div><div class=\"fi-txt\">Watchlist <strong>ilimitada</strong></div></div>\n    <div class=\"fi\"><div class=\"fi-icon fi-y\">&#10003;</div><div class=\"fi-txt\"><strong>Sankey</strong> de capital allocation</div></div>\n    <div class=\"fi\"><div class=\"fi-icon fi-y\">&#10003;</div><div class=\"fi-txt\"><strong>Fair Value</strong> &mdash; DCF, Graham, EV/EBITDA, Lynch</div></div>\n    <div class=\"fi\"><div class=\"fi-icon fi-y\">&#10003;</div><div class=\"fi-txt\"><strong>Insider Trades</strong> &mdash; SEC Form 4 live</div></div>\n    <div class=\"fi\"><div class=\"fi-icon fi-y\">&#10003;</div><div class=\"fi-txt\"><strong>Congress Trades</strong> &mdash; STOCK Act</div></div>\n    <div class=\"fi\"><div class=\"fi-icon fi-y\">&#10003;</div><div class=\"fi-txt\"><strong>Live Feed</strong> &mdash; toda a bolsa americana</div></div>\n    <div class=\"fi\"><div class=\"fi-icon fi-y\">&#10003;</div><div class=\"fi-txt\">Signal Score por ticker <span class=\"fi-new\">NEW</span></div></div>\n  </div>\n\n  <!-- ELITE -->\n  <div class=\"plan elite reveal d2\">\n    <span class=\"plan-tag\" style=\"background:rgba(167,139,250,.1);color:var(--pu)\">Profissional</span>\n    <div class=\"plan-name\" style=\"background:linear-gradient(135deg,#a78bfa,#0088ee);-webkit-background-clip:text;-webkit-text-fill-color:transparent\">ELITE</div>\n    <div class=\"plan-desc\">Para profissionais e equipas com necessidades avan&ccedil;adas e acesso total.</div>\n    <div class=\"plan-price-block\">\n      <div class=\"plan-price\">\n        <span class=\"plan-currency\" style=\"-webkit-text-fill-color:var(--t2)\">&euro;</span>\n        <span class=\"plan-amount\" id=\"el-int\" style=\"background:linear-gradient(135deg,#a78bfa,#0088ee);-webkit-background-clip:text;-webkit-text-fill-color:transparent\">19</span>\n        <span class=\"plan-cents\" id=\"el-dec\" style=\"-webkit-text-fill-color:var(--t2)\">,99</span>\n      </div>\n      <div class=\"plan-period\" id=\"el-period\">por m&ecirc;s</div>\n      <div class=\"plan-annual-note\" id=\"el-note\">&nbsp;</div>\n    </div>\n    <div style=\"margin-bottom:8px;text-align:center\"><span style=\"background:rgba(167,139,250,.1);border:1px solid rgba(167,139,250,.2);color:var(--pu);font-size:9px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;padding:3px 10px;border-radius:10px\">7 dias gr&aacute;tis</span></div>\n    <button onclick=\"location.href='/terminal'\" class=\"plan-btn elite-btn\">Come&ccedil;ar Trial</button>\n    <div class=\"plan-divider\"></div>\n    <div class=\"plan-section\">Tudo do Pro, mais:</div>\n    <div class=\"fi\"><div class=\"fi-icon fi-s\">&#9733;</div><div class=\"fi-txt\"><strong>Alertas</strong> &mdash; email + push em tempo real</div></div>\n    <div class=\"fi\"><div class=\"fi-icon fi-s\">&#9733;</div><div class=\"fi-txt\"><strong>API Access</strong> &mdash; integra os teus sistemas</div></div>\n    <div class=\"fi\"><div class=\"fi-icon fi-s\">&#9733;</div><div class=\"fi-txt\"><strong>Screener</strong> &mdash; filtra por signal score, insiders <span class=\"fi-soon\">SOON</span></div></div>\n    <div class=\"fi\"><div class=\"fi-icon fi-s\">&#9733;</div><div class=\"fi-txt\"><strong>Portfolio tracker</strong> P&amp;L live <span class=\"fi-soon\">SOON</span></div></div>\n    <div class=\"fi\"><div class=\"fi-icon fi-s\">&#9733;</div><div class=\"fi-txt\"><strong>Exporta&ccedil;&atilde;o</strong> CSV / Excel de todos os dados</div></div>\n    <div class=\"fi\"><div class=\"fi-icon fi-s\">&#9733;</div><div class=\"fi-txt\"><strong>Suporte priorit&aacute;rio</strong> &mdash; resposta em 4h</div></div>\n    <div class=\"fi\"><div class=\"fi-icon fi-s\">&#9733;</div><div class=\"fi-txt\">Dashboard equipa &mdash; at&eacute; 5 utilizadores</div></div>\n  </div>\n\n</div>\n\n<!-- COMPARISON TABLE -->\n<div class=\"comp-wrap\">\n  <h2 class=\"comp-title reveal\">Compara&ccedil;&atilde;o <em>completa.</em></h2>\n  <div class=\"comp-table reveal d1\">\n    <div class=\"comp-row comp-hdr\">\n      <div class=\"cc fn\" style=\"font-size:10px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:var(--t3)\">Feature</div>\n      <div class=\"cc pn\" style=\"color:var(--t3)\">FREE</div>\n      <div class=\"cc pn\" style=\"color:var(--gr)\">PRO</div>\n      <div class=\"cc pn\" style=\"background:linear-gradient(135deg,#a78bfa,#0088ee);-webkit-background-clip:text;-webkit-text-fill-color:transparent\">ELITE</div>\n    </div>\n    <div class=\"comp-grp\">Mercados &amp; Gr&aacute;ficos</div>\n    <div class=\"comp-row\"><div class=\"cc fn\">Tape em tempo real</div><div class=\"cc vl\"><span class=\"ck\">&#10003;</span></div><div class=\"cc vl\"><span class=\"ck\">&#10003;</span></div><div class=\"cc vl\"><span class=\"ck\">&#10003;</span></div></div>\n    <div class=\"comp-row\"><div class=\"cc fn\">Gr&aacute;ficos hist&oacute;ricos</div><div class=\"cc vl\"><span class=\"ck\">&#10003;</span></div><div class=\"cc vl\"><span class=\"ck\">&#10003;</span></div><div class=\"cc vl\"><span class=\"ck\">&#10003;</span></div></div>\n    <div class=\"comp-row\"><div class=\"cc fn\">Overlays (S&amp;P, QQQ, M2)</div><div class=\"cc vl\"><span class=\"cx\">&#8212;</span></div><div class=\"cc vl\"><span class=\"ck\">&#10003;</span></div><div class=\"cc vl\"><span class=\"ck\">&#10003;</span></div></div>\n    <div class=\"comp-row\"><div class=\"cc fn\">Watchlist</div><div class=\"cc vl\"><span class=\"ct\">50 tickers</span></div><div class=\"cc vl\"><span class=\"ct\" style=\"color:var(--gr)\">Ilimitada</span></div><div class=\"cc vl\"><span class=\"ct\" style=\"color:var(--pu)\">Ilimitada</span></div></div>\n    <div class=\"comp-grp\">An&aacute;lise Financeira</div>\n    <div class=\"comp-row\"><div class=\"cc fn\">Financials b&aacute;sicos</div><div class=\"cc vl\"><span class=\"ck\">&#10003;</span></div><div class=\"cc vl\"><span class=\"ck\">&#10003;</span></div><div class=\"cc vl\"><span class=\"ck\">&#10003;</span></div></div>\n    <div class=\"comp-row\"><div class=\"cc fn\">Sankey capital allocation</div><div class=\"cc vl\"><span class=\"cx\">&#8212;</span></div><div class=\"cc vl\"><span class=\"ck\">&#10003;</span></div><div class=\"cc vl\"><span class=\"ck\">&#10003;</span></div></div>\n    <div class=\"comp-row\"><div class=\"cc fn\">Fair Value (DCF, Graham&hellip;)</div><div class=\"cc vl\"><span class=\"cx\">&#8212;</span></div><div class=\"cc vl\"><span class=\"ck\">&#10003;</span></div><div class=\"cc vl\"><span class=\"ck\">&#10003;</span></div></div>\n    <div class=\"comp-row\"><div class=\"cc fn\">Signal Score</div><div class=\"cc vl\"><span class=\"cx\">&#8212;</span></div><div class=\"cc vl\"><span class=\"ck\">&#10003;</span></div><div class=\"cc vl\"><span class=\"ck\">&#10003;</span></div></div>\n    <div class=\"comp-grp\">Insider &amp; Congress</div>\n    <div class=\"comp-row\"><div class=\"cc fn\">Insider Trades &mdash; SEC Form 4</div><div class=\"cc vl\"><span class=\"cx\">&#8212;</span></div><div class=\"cc vl\"><span class=\"ck\">&#10003;</span></div><div class=\"cc vl\"><span class=\"ck\">&#10003;</span></div></div>\n    <div class=\"comp-row\"><div class=\"cc fn\">Congress Trades</div><div class=\"cc vl\"><span class=\"cx\">&#8212;</span></div><div class=\"cc vl\"><span class=\"ck\">&#10003;</span></div><div class=\"cc vl\"><span class=\"ck\">&#10003;</span></div></div>\n    <div class=\"comp-row\"><div class=\"cc fn\">Live Feed &mdash; bolsa inteira</div><div class=\"cc vl\"><span class=\"cx\">&#8212;</span></div><div class=\"cc vl\"><span class=\"ck\">&#10003;</span></div><div class=\"cc vl\"><span class=\"ck\">&#10003;</span></div></div>\n    <div class=\"comp-grp\">Elite</div>\n    <div class=\"comp-row\"><div class=\"cc fn\">Alertas (email + push)</div><div class=\"cc vl\"><span class=\"cx\">&#8212;</span></div><div class=\"cc vl\"><span class=\"cx\">&#8212;</span></div><div class=\"cc vl\"><span class=\"ck-p\">&#10003;</span></div></div>\n    <div class=\"comp-row\"><div class=\"cc fn\">API Access</div><div class=\"cc vl\"><span class=\"cx\">&#8212;</span></div><div class=\"cc vl\"><span class=\"cx\">&#8212;</span></div><div class=\"cc vl\"><span class=\"ck-p\">&#10003;</span></div></div>\n    <div class=\"comp-row\"><div class=\"cc fn\">Exporta&ccedil;&atilde;o CSV/Excel</div><div class=\"cc vl\"><span class=\"cx\">&#8212;</span></div><div class=\"cc vl\"><span class=\"cx\">&#8212;</span></div><div class=\"cc vl\"><span class=\"ck-p\">&#10003;</span></div></div>\n    <div class=\"comp-row\"><div class=\"cc fn\">Suporte priorit&aacute;rio 4h</div><div class=\"cc vl\"><span class=\"cx\">&#8212;</span></div><div class=\"cc vl\"><span class=\"cx\">&#8212;</span></div><div class=\"cc vl\"><span class=\"ck-p\">&#10003;</span></div></div>\n    <div class=\"comp-row\"><div class=\"cc fn\">Dashboard equipa (5 users)</div><div class=\"cc vl\"><span class=\"cx\">&#8212;</span></div><div class=\"cc vl\"><span class=\"cx\">&#8212;</span></div><div class=\"cc vl\"><span class=\"ck-p\">&#10003;</span></div></div>\n  </div>\n</div>\n\n<!-- FAQ -->\n<div class=\"faq-wrap\">\n  <h2 class=\"faq-title reveal\">FAQ.</h2>\n  <div class=\"faq-item\">\n    <div class=\"fq\" onclick=\"faq(this)\">Posso cancelar quando quiser? <div class=\"fi-btn\">+</div></div>\n    <div class=\"fa\"><div class=\"fa-in\">Sim. Sem contratos. Cancelas a qualquer momento e tens acesso at&eacute; ao final do per&iacute;odo pago. Sem perguntas.</div></div>\n  </div>\n  <div class=\"faq-item\">\n    <div class=\"fq\" onclick=\"faq(this)\">Os dados s&atilde;o mesmo em tempo real? <div class=\"fi-btn\">+</div></div>\n    <div class=\"fa\"><div class=\"fa-in\">Sim. Usamos WebSocket com Socket.IO. Pre&ccedil;os chegam assim que s&atilde;o publicados. Insider trades t&ecirc;m o delay legal m&aacute;ximo de 48h (definido pelo SEC).</div></div>\n  </div>\n  <div class=\"faq-item\">\n    <div class=\"fq\" onclick=\"faq(this)\">O Free &eacute; mesmo gratuito para sempre? <div class=\"fi-btn\">+</div></div>\n    <div class=\"fa\"><div class=\"fa-in\">100% gratuito, sem trial. Sem cart&atilde;o de cr&eacute;dito. O Free d&aacute;-te gr&aacute;ficos, tape, financials b&aacute;sicos e m&eacute;tricas &mdash; mais do que suficiente para come&ccedil;ar.</div></div>\n  </div>\n  <div class=\"faq-item\">\n    <div class=\"fq\" onclick=\"faq(this)\">O que &eacute; o Signal Score? <div class=\"fi-btn\">+</div></div>\n    <div class=\"fa\"><div class=\"fa-in\">Score de 0-100 calculado com: fair value vs pre&ccedil;o actual, atividade insider, trades de congressistas, momentum t&eacute;cnico e analyst targets. Quanto maior, melhor o setup.</div></div>\n  </div>\n  <div class=\"faq-item\">\n    <div class=\"fq\" onclick=\"faq(this)\">Desconto para estudantes? <div class=\"fi-btn\">+</div></div>\n    <div class=\"fa\"><div class=\"fa-in\">Sim. Envia email com endere&ccedil;o institucional (.edu ou equivalente) e aplicamos 50% de desconto no Pro. Para universidades, contacta-nos para condi&ccedil;&otilde;es especiais.</div></div>\n  </div>\n  <div class=\"faq-item\">\n    <div class=\"fq\" onclick=\"faq(this)\">Como funciona a fatura&ccedil;&atilde;o anual? <div class=\"fi-btn\">+</div></div>\n    <div class=\"fa\"><div class=\"fa-in\">Pagas um ano adiantado com 20% de desconto. Pro anual: &euro;153,50/ano (poupa &euro;38,38). Elite anual: &euro;191,90/ano (poupa &euro;47,98). &Eacute; como ter 2-3 meses gr&aacute;tis.</div></div>\n  </div>\n</div>\n\n<!-- BOTTOM CTA -->\n<div class=\"bcta\">\n  <div class=\"bcta-glow\"></div>\n  <h2 class=\"bcta-h reveal\">COME&Ccedil;A <span>GR&Aacute;TIS</span>.<br>ESCALA DEPOIS.</h2>\n  <p class=\"bcta-sub reveal d1\">Sem publicidade. Sem paywall. Dados reais.</p>\n  <a href=\"/terminal\" class=\"bcta-btn reveal d2\">Abrir Terminal &nearr;</a>\n</div>\n\n<footer>\n  <span class=\"fl\">IST</span>\n  <span class=\"fn\">Insider Signal Terminal &middot; Pre&ccedil;os em EUR &middot; IVA inclu&iacute;do</span>\n</footer>\n\n</div><!-- /page -->\n\n<script>\n// Tape\nconst SYMS=[\"^GSPC\",\"^IXIC\",\"^VIX\",\"GC=F\",\"CL=F\",\"BTC-USD\",\"^TNX\",\"SPY\",\"QQQ\",\"NVDA\",\"AAPL\",\"MSFT\",\"TSLA\"];\nconst LBL={\"^GSPC\":\"SP500\",\"^IXIC\":\"NASDAQ\",\"^VIX\":\"VIX\",\"GC=F\":\"GOLD\",\"CL=F\":\"WTI\",\"BTC-USD\":\"BTC\",\"^TNX\":\"US10Y\",\"SPY\":\"SPY\",\"QQQ\":\"QQQ\",\"NVDA\":\"NVDA\",\"AAPL\":\"AAPL\",\"MSFT\":\"MSFT\",\"TSLA\":\"TSLA\"};\nconst TID=t=>\"T_\"+t.replace(/[^a-zA-Z0-9]/g,\"_\");\ndocument.getElementById(\"tape-track\").innerHTML=[...SYMS,...SYMS].map(t=>\n  `<div class=\"ti\" id=\"${TID(t)}\" onclick=\"location.href='/chart?t=${encodeURIComponent(t)}'\"><span class=\"ts\">${LBL[t]||t}</span>&nbsp;<span class=\"tv\">&#8212;</span>&nbsp;<span class=\"tc nc\">&#8212;</span></div>`\n).join(\"\");\nfunction fmtPx(v,t){if(v==null)return\"&#8212;\";t=t||\"\";if(t.includes(\"BTC\")||v>10000)return\"$\"+Math.round(v).toLocaleString(\"en-US\");return\"$\"+Number(v).toFixed(2);}\nfunction fmtPct(v){return v==null?\"&#8212;\":(v>=0?\"+\":\"\")+Number(v).toFixed(2)+\"%\";}\nfunction cls(v){return v==null?\"nc\":v>0?\"up\":\"dn\";}\nfunction upd(p){const t=p.ticker,px=p.price,pct=p.change_pct;if(!t||px==null)return;const te=document.getElementById(TID(t));if(te){const cs=te.children;if(cs[1])cs[1].innerHTML=fmtPx(px,t);if(cs[2]){cs[2].innerHTML=fmtPct(pct);cs[2].className=\"tc \"+cls(pct);}}}\ntry{const s=io();s.on(\"connect\",()=>s.emit(\"subscribe\",{tickers:SYMS}));s.on(\"price_update\",({prices})=>{if(Array.isArray(prices))prices.forEach(upd);});}catch(e){}\nfetch(\"/api/watchlist?tickers=\"+SYMS.join(\",\")).then(r=>r.json()).then(d=>{if(d?.stocks)d.stocks.forEach(upd);}).catch(()=>{});\n\n// Billing toggle\nlet billing=\"m\";\nconst PM=15.99,PA=PM*0.8,EM=19.99,EA=EM*0.8;\nfunction fmt(v){const i=Math.floor(v),d=Math.round((v-i)*100).toString().padStart(2,\"0\");return{i,d};}\nfunction setBilling(b){\n  billing=b;const isY=b===\"y\";\n  document.getElementById(\"tog\").classList.toggle(\"yr\",isY);\n  document.getElementById(\"lbl-m\").classList.toggle(\"on\",!isY);\n  document.getElementById(\"lbl-y\").classList.toggle(\"on\",isY);\n  const pm=isY?PA:PM,em=isY?EA:EM;\n  const pf=fmt(pm),ef=fmt(em);\n  document.getElementById(\"pro-int\").textContent=pf.i;\n  document.getElementById(\"pro-dec\").textContent=\",\"+pf.d;\n  document.getElementById(\"el-int\").textContent=ef.i;\n  document.getElementById(\"el-dec\").textContent=\",\"+ef.d;\n  document.getElementById(\"pro-period\").innerHTML=isY?\"por m&ecirc;s (faturado anualmente)\":\"por m&ecirc;s\";\n  document.getElementById(\"el-period\").innerHTML=isY?\"por m&ecirc;s (faturado anualmente)\":\"por m&ecirc;s\";\n  document.getElementById(\"pro-note\").innerHTML=isY?`&euro;${(PM*12*0.8).toFixed(2).replace(\".\",\",\")}/ano &mdash; poupa &euro;${(PM*12*0.2).toFixed(2).replace(\".\",\",\")}`:\"&nbsp;\";\n  document.getElementById(\"el-note\").innerHTML=isY?`&euro;${(EM*12*0.8).toFixed(2).replace(\".\",\",\")}/ano &mdash; poupa &euro;${(EM*12*0.2).toFixed(2).replace(\".\",\",\")}`:\"&nbsp;\";\n}\nfunction toggleBilling(){setBilling(billing===\"m\"?\"y\":\"m\");}\n\n// FAQ\nfunction faq(el){el.parentElement.classList.toggle(\"open\");}\n\n// Reveal\nconst obs=new IntersectionObserver(e=>{e.forEach(x=>{if(x.isIntersecting)x.target.classList.add(\"visible\");});},{threshold:0.12});\ndocument.querySelectorAll(\".reveal\").forEach(el=>obs.observe(el));\n</script>\n</body>\n</html>\n",
    "livefeed.html": "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"UTF-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\n<title>Live Feed · IST</title>\n<script src=\"https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.7.5/socket.io.min.js\"></script>\n<link href=\"https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600&family=Syne:wght@400;600;700;800&display=swap\" rel=\"stylesheet\">\n<style>\n*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}\n:root{\n  --bg:#080b0f;--bg2:#0d1117;--bg3:#111720;--bg4:#161d29;\n  --b:#1e2d3d;--b2:#243040;\n  --t:#c9d1d9;--t2:#8b949e;--t3:#484f58;\n  --gr:#00e5a0;--rd:#ff4d6d;--bl:#0095ff;\n  --yl:#f0c060;--pu:#a78bfa;--or:#ff8c42;\n  --fm:'JetBrains Mono',monospace;--fd:'Syne',sans-serif;\n}\nhtml,body{height:100%;background:var(--bg);color:var(--t);font-family:var(--fm);font-size:13px}\n::-webkit-scrollbar{width:5px;height:5px}::-webkit-scrollbar-track{background:transparent}::-webkit-scrollbar-thumb{background:var(--b2);border-radius:3px}\n.up{color:var(--gr)}.dn{color:var(--rd)}.nc{color:var(--t2)}\n/* TAPE */\n#tape{height:26px;background:var(--bg2);border-bottom:1px solid var(--b);overflow:hidden;display:flex;align-items:center;flex-shrink:0}\n#tape-inner{display:flex;white-space:nowrap;animation:tape 90s linear infinite;user-select:none;-webkit-user-select:none;cursor:default;}\n/* tape runs continuously */\n@keyframes tape{from{transform:translateX(0)}to{transform:translateX(-50%)}}\n.ti{display:inline-flex;align-items:center;gap:5px;padding:0 16px;height:26px;border-right:1px solid var(--b);font-size:11px;user-select:none;-webkit-user-select:none;}\n.ts{color:var(--t2);font-weight:600}.tc{font-size:10px}\n/* NAV */\n#nav{height:40px;background:var(--bg2);border-bottom:1px solid var(--b);display:flex;align-items:center;padding:0 10px;gap:2px;flex-shrink:0}\n.nl{display:flex;align-items:center;gap:5px;padding:5px 10px;border-radius:4px;font-size:11px;font-family:var(--fd);font-weight:700;letter-spacing:.04em;text-transform:uppercase;color:var(--t2);text-decoration:none;transition:all .15s;white-space:nowrap}\n.nl:hover{color:var(--t);background:var(--bg3)}\n.nl.on{color:var(--gr);background:rgba(0,229,160,.07);border:1px solid rgba(0,229,160,.12)}\n.nl svg{flex-shrink:0;opacity:.7}.nl.on svg{opacity:1}\n#nav-logo{font-family:var(--fd);font-size:14px;font-weight:800;color:var(--gr);letter-spacing:.04em;margin-right:12px;text-decoration:none}\n/* TICKER BAR */\n#tkbar{height:38px;background:var(--bg2);border-bottom:1px solid var(--b);display:flex;align-items:center;padding:0 14px;gap:10px;flex-shrink:0}\n#tk-sym{font-family:var(--fd);font-size:15px;font-weight:800;color:var(--gr);flex-shrink:0}\n#tk-name{font-size:11px;color:var(--t2);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}\n#tk-px{font-family:var(--fd);font-size:15px;font-weight:700;flex-shrink:0}\n#tk-chg{font-size:11px;padding:2px 8px;border-radius:3px;font-weight:600;flex-shrink:0}\n#tk-sw{position:relative}\n#tk-si{background:var(--bg3);border:1px solid var(--b2);color:var(--t);font-family:var(--fm);font-size:11px;padding:4px 9px;border-radius:3px;outline:none;width:160px}\n#tk-si:focus{border-color:var(--bl)}\n#tk-dr{position:absolute;top:calc(100% + 2px);right:0;background:var(--bg3);border:1px solid var(--b2);border-radius:3px;z-index:999;display:none;min-width:200px;max-height:200px;overflow-y:auto}\n.tdr{padding:6px 10px;cursor:pointer;display:flex;justify-content:space-between;gap:8px;font-size:11px}.tdr:hover{background:var(--bg4)}\n.tds{color:var(--gr);font-weight:700}.tdn{color:var(--t2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:120px}\n/* PAGE BODY */\n.pb{display:flex;flex-direction:column;height:100vh;overflow:hidden}\n.pc{flex:1;overflow-y:auto;overflow-x:hidden}\n/* SKELETON */\n.sk{background:linear-gradient(90deg,var(--bg3) 25%,var(--bg4) 50%,var(--bg3) 75%);background-size:200% 100%;animation:sk 1.2s ease infinite;border-radius:3px}\n@keyframes sk{0%{background-position:200% 0}100%{background-position:-200% 0}}\n.sk-line{height:14px;margin-bottom:8px}.sk-val{height:24px;margin-bottom:4px}\n/* SPIN */\n.spin-svg{display:inline-block;vertical-align:middle;margin-right:8px;width:44px;height:20px}\n/* EMPTY */\n.empty{text-align:center;padding:40px 20px;color:var(--t3);font-size:12px}\n\n.pc{padding:16px 20px}\n.lf-hdr{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;flex-wrap:wrap;gap:8px}\n.lf-note{font-size:10px;color:var(--yl);background:rgba(240,192,96,.07);border:1px solid rgba(240,192,96,.15);border-radius:4px;padding:7px 12px;margin-bottom:12px;line-height:1.6}\n.lff{display:flex;gap:6px}\n.lf-btn{padding:5px 14px;border-radius:4px;font-size:11px;font-family:var(--fd);font-weight:700;cursor:pointer;background:var(--bg2);border:1px solid var(--b);color:var(--t2);transition:all .15s}\n.lf-btn.on{background:var(--gr);color:var(--bg);border-color:var(--gr)}\n.pulse{display:inline-block;width:7px;height:7px;border-radius:50%;background:var(--gr);margin-right:5px;animation:pl 1.4s ease infinite;vertical-align:middle}\n@keyframes pl{0%,100%{opacity:1}50%{opacity:.25}}\n.tbl{width:100%;border-collapse:collapse;font-size:12px}\n.tbl th{text-align:left;color:var(--t3);font-size:9px;letter-spacing:.07em;text-transform:uppercase;font-family:var(--fd);font-weight:700;padding:8px 10px;border-bottom:1px solid var(--b);background:var(--bg2);position:sticky;top:0;z-index:2}\n.tbl td{padding:8px 10px;border-bottom:1px solid rgba(255,255,255,.025);vertical-align:middle}\n.tbl tr{cursor:pointer}.tbl tr:hover td{background:rgba(255,255,255,.03)}\n.av{width:30px;height:30px;border-radius:50%;border:1px solid var(--b2);background:var(--bg3);vertical-align:middle;margin-right:7px}\n.ab{font-size:9px;font-weight:700;padding:2px 7px;border-radius:2px}\n.ab-BUY{background:rgba(0,229,160,.15);color:var(--gr)}.ab-SELL{background:rgba(255,77,109,.15);color:var(--rd)}\n</style>\n</head>\n<body>\n<div class=\"pb\">\n<div id=\"tape\"><div id=\"tape-inner\"></div></div>\n<div id=\"nav\"><a href=\"/\" id=\"nav-logo\" style=\"display:flex;align-items:center;gap:7px;text-decoration:none\">\n      <svg width=\"22\" height=\"22\" viewBox=\"0 0 64 64\" fill=\"none\">\n        <rect width=\"64\" height=\"64\" rx=\"10\" fill=\"rgba(0,229,160,0.15)\" stroke=\"rgba(0,229,160,0.4)\" stroke-width=\"2\"/>\n        <polyline points=\"8,44 20,28 30,36 42,18 56,22\" stroke=\"#00e5a0\" stroke-width=\"5\" fill=\"none\" stroke-linecap=\"round\" stroke-linejoin=\"round\"/>\n        <circle cx=\"42\" cy=\"18\" r=\"5\" fill=\"#00e5a0\"/>\n      </svg>\n      <span style=\"font-family:var(--fd);font-size:14px;font-weight:800;color:var(--gr);letter-spacing:.04em\">IST</span>\n    </a>\n    <a href=\"/chart\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"currentColor\"><rect x=\"1\" y=\"7\" width=\"2\" height=\"5\"/><rect x=\"5\" y=\"4\" width=\"2\" height=\"8\"/><rect x=\"9\" y=\"1\" width=\"2\" height=\"11\"/></svg>Chart</a><a href=\"/financials\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><polyline points=\"1,9 4,5 7,7 12,2\"/></svg>Financials</a><a href=\"/metrics\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"currentColor\"><circle cx=\"3\" cy=\"3\" r=\"1.5\"/><circle cx=\"10\" cy=\"3\" r=\"1.5\"/><circle cx=\"3\" cy=\"10\" r=\"1.5\"/><circle cx=\"10\" cy=\"10\" r=\"1.5\"/></svg>Metrics</a><a href=\"/insider\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><circle cx=\"6.5\" cy=\"4\" r=\"2.5\"/><path d=\"M1 12c0-3 2.5-5 5.5-5s5.5 2 5.5 5\"/></svg>Insider</a><a href=\"/congress\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><rect x=\"1\" y=\"5\" width=\"11\" height=\"7\" rx=\"1\"/><path d=\"M4 5V3a2.5 2.5 0 015 0v2\"/></svg>Congress</a><a href=\"/fairvalue\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><path d=\"M6.5 1L8 5h4l-3 2.5 1 4L6.5 9 3 11.5l1-4L1 5h4z\"/></svg>Fair Value</a><a href=\"/news\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><rect x=\"1\" y=\"1\" width=\"11\" height=\"11\" rx=\"1\"/><line x1=\"3\" y1=\"4\" x2=\"10\" y2=\"4\"/><line x1=\"3\" y1=\"7\" x2=\"10\" y2=\"7\"/></svg>News</a><a href=\"/livefeed\" class=\"nl on\"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"currentColor\"><circle cx=\"6.5\" cy=\"6.5\" r=\"2\"/><circle cx=\"6.5\" cy=\"6.5\" r=\"4\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1\" opacity=\".5\"/></svg>Live Feed</a></div>\n<script id=\"asset-nav-fix\">\ndocument.addEventListener('DOMContentLoaded', function(){\n  const t=(new URLSearchParams(window.location.search).get('t')||'').toUpperCase();\n  const isCrypto=t.endsWith('-USD')&&!['GLD','SLV','USO','TLT','HYG','LQD','GDX'].includes(t);\n  const isFuture=t.endsWith('=F');\n  const isIndex=t.startsWith('^')||t==='DX-Y.NYB';\n  if(!isCrypto&&!isFuture&&!isIndex) return;\n  const STOCK_ONLY=['/financials','/insider','/congress','/fairvalue'];\n  document.querySelectorAll('#nav a.nl').forEach(a=>{\n    const href=a.getAttribute('href')||'';\n    if(STOCK_ONLY.some(p=>href.includes(p))) a.style.display='none';\n  });\n});\n</script>\n<div id=\"tkbar\">\n  <span id=\"tk-sym\">—</span><span id=\"tk-name\">—</span>\n  <span id=\"tk-px\" style=\"color:var(--t2)\">—</span><span id=\"tk-chg\"></span>\n  <div id=\"tk-sw\"><input id=\"tk-si\" placeholder=\"Change ticker…\" autocomplete=\"off\" spellcheck=\"false\"><div id=\"tk-dr\"></div></div>\n</div>\n<div class=\"pc\"><div class=\"empty\"><svg class=\"spin-svg\" viewBox=\"0 0 44 20\" fill=\"none\"><polyline points=\"2,16 8,10 14,13 22,5 30,9 42,3\" stroke=\"#00e5a0\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-dasharray=\"120\" stroke-dashoffset=\"120\"><animate attributeName=\"stroke-dashoffset\" from=\"120\" to=\"0\" dur=\"1.2s\" repeatCount=\"indefinite\"/></polyline></svg>A carregar live feed…</div></div>\n</div>\n<script>\nconst TSYMS=['SPY','QQQ','^GSPC','^IXIC','^VIX','CL=F','BZ=F','GC=F','SI=F','DX-Y.NYB','^TNX','BTC-USD'];\nconst TLBL={SPY:'SPY',QQQ:'QQQ','^GSPC':'S&P500','^IXIC':'NASDAQ','^VIX':'VIX','CL=F':'WTI','BZ=F':'BRENT','GC=F':'GOLD','SI=F':'SILVER','DX-Y.NYB':'DXY','^TNX':'US10Y','BTC-USD':'BTC'};\nconst socket=io();\nfunction buildTape(){\n  document.getElementById('tape-inner').innerHTML=[...TSYMS,...TSYMS].map(t=>{\n    const id='ti_'+t.replace(/[^a-zA-Z0-9]/g,'_');\n    return`<div class=\"ti\" id=\"${id}\" onclick=\"navToTape('${t}')\" style=\"cursor:pointer;\"><span class=\"ts\">${TLBL[t]||t}</span>&nbsp;<span>—</span>&nbsp;<span class=\"tc nc\">—</span></div>`;\n  }).join('');\n  socket.emit('subscribe',{tickers:TSYMS});\n}\nsocket.on('price_update',({prices})=>{\n  prices.forEach(p=>{\n    const id='ti_'+p.ticker.replace(/[^a-zA-Z0-9]/g,'_');\n    document.querySelectorAll('#'+id).forEach(el=>{\n      const cs=el.children;\n      if(cs[1]&&p.price!=null)cs[1].textContent='$'+p.price.toFixed(2);\n      if(cs[2]&&p.change_pct!=null){const s=p.change_pct>=0?'+':'';cs[2].textContent=s+p.change_pct.toFixed(2)+'%';cs[2].className='tc '+(p.change_pct>0?'up':p.change_pct<0?'dn':'nc');}\n    });\n    if(p.ticker===window.TK)updateTkBar(p);\n    if(typeof onPriceUpdate==='function')onPriceUpdate(p);\n  });\n});\nbuildTape();\nconst MEM={};const CACHE_TTL=300000;\nasync function api(url,ttl=CACHE_TTL,timeout=12000){\n  if(MEM[url]&&Date.now()-MEM[url].ts<ttl)return MEM[url].data;\n  try{\n    const ctrl=new AbortController();\n    const tid=setTimeout(()=>ctrl.abort(),timeout);\n    const r=await fetch(url,{signal:ctrl.signal});\n    clearTimeout(tid);\n    if(!r.ok)return null;\n    const d=await r.json();\n    MEM[url]={data:d,ts:Date.now()};\n    return d;\n  }catch{return null;}\n}\nfunction ssGet(k){try{const v=sessionStorage.getItem(k);if(!v)return null;const p=JSON.parse(v);if(Date.now()-p.ts>CACHE_TTL){sessionStorage.removeItem(k);return null;}return p.data;}catch{return null;}}\nfunction ssSet(k,d){try{sessionStorage.setItem(k,JSON.stringify({data:d,ts:Date.now()}));}catch{}}\nasync function getStockData(t){const k='full:'+t;const ss=ssGet(k);if(ss)return ss;const mc=MEM['/api/full/'+t];if(mc&&Date.now()-mc.ts<CACHE_TTL)return mc.data;const d=await api('/api/full/'+t,CACHE_TTL);if(d)ssSet(k,d);return d;}\nfunction updateTkBar(p){const pxEl=document.getElementById('tk-px'),chEl=document.getElementById('tk-chg');if(!pxEl)return;if(p.price!=null){pxEl.textContent='$'+p.price.toFixed(2);pxEl.style.color=p.change_pct>0?'var(--gr)':p.change_pct<0?'var(--rd)':'var(--t)';}if(p.change_pct!=null){const s=p.change_pct>=0?'+':'';chEl.textContent=s+p.change_pct.toFixed(2)+'%';chEl.style.cssText=`background:${p.change_pct>=0?'rgba(0,229,160,.12)':'rgba(255,77,109,.12)'};color:${p.change_pct>=0?'var(--gr)':'var(--rd)'};padding:2px 8px;border-radius:3px;`;}}\nasync function initTkBar(t){document.getElementById('tk-sym').textContent=t;const d=await api('/api/stock_fast/'+encodeURIComponent(t),6000);if(d){document.getElementById('tk-name').textContent=d.name||t;updateTkBar(d);}socket.emit('subscribe',{tickers:[t]});}\nlet sT;const siEl=document.getElementById('tk-si'),drEl=document.getElementById('tk-dr');\nif(siEl){siEl.addEventListener('input',function(){clearTimeout(sT);const v=this.value.trim();if(!v){drEl.style.display='none';return;}sT=setTimeout(async()=>{const d=await api('/api/universe?q='+encodeURIComponent(v)+'&limit=10',60000);if(!d?.results?.length){drEl.style.display='none';return;}drEl.innerHTML=d.results.map(x=>`<div class=\"tdr\" onclick=\"navTo('${x.ticker}')\"><span class=\"tds\">${x.ticker}</span><span class=\"tdn\">${x.name||''}</span></div>`).join('');drEl.style.display='block';},200);});document.addEventListener('click',e=>{if(!e.target.closest('#tk-sw'))drEl.style.display='none';});}\nfunction navTo(t){if(!t)return;if(siEl){siEl.value='';drEl.style.display='none';}window.location.href='/chart?t='+encodeURIComponent(t);}\n// navToTape: always go to chart page with the ticker\nfunction navToTape(t){\n  if(!t)return;\n  // Route to appropriate page based on asset type\n  window.location.href='/chart?t='+encodeURIComponent(t);\n}\nwindow.TK=(new URLSearchParams(window.location.search).get('t')||'NVDA').toUpperCase();\nconst fmtB=v=>{if(v==null)return'—';const a=Math.abs(v),s=v<0?'-':'';if(a>=1e12)return s+'$'+(a/1e12).toFixed(2)+'T';if(a>=1e9)return s+'$'+(a/1e9).toFixed(2)+'B';if(a>=1e6)return s+'$'+(a/1e6).toFixed(1)+'M';return s+'$'+a.toLocaleString('en-US',{maximumFractionDigits:0});};\nconst fmtPx=v=>v==null?'—':'$'+Number(v).toFixed(2);\nconst p1=v=>v==null?'—':(v*100).toFixed(1)+'%';\nconst fn=(v,d=2)=>v==null?'—':Number(v).toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d});\n\nlet LFF='ALL';\nasync function loadLF(){\n  const el=document.querySelector('.pc');\n  el.innerHTML='<div class=\"empty\"><svg class=\"spin-svg\" viewBox=\"0 0 44 20\" fill=\"none\"><polyline points=\"2,16 8,10 14,13 22,5 30,9 42,3\" stroke=\"#00e5a0\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-dasharray=\"120\" stroke-dashoffset=\"120\"><animate attributeName=\"stroke-dashoffset\" from=\"120\" to=\"0\" dur=\"1.2s\" repeatCount=\"indefinite\"/></polyline></svg>A carregar…</div>';\n  const act=LFF==='ALL'?'':'&action='+LFF;\n  const d=await api('/api/insider_realtime?limit=150'+act,60000);\n  if(!d){el.innerHTML=`<div class=\"empty\" style=\"padding:40px 20px;text-align:center\">\n  <div style=\"font-family:var(--fd);font-size:16px;font-weight:700;color:var(--t);margin-bottom:8px\">Feed SEC EDGAR</div>\n  <div style=\"font-size:12px;color:var(--t2);max-width:400px;margin:0 auto;line-height:1.7\">\n    O Live Feed mostra os Form 4 mais recentes de <b>todos</b> os insiders americanos — não apenas da empresa seleccionada.<br><br>\n    Se não há dados, o SEC pode estar temporariamente indisponível ou sem filings nas últimas 48h.\n  </div>\n  <div style=\"margin-top:16px;font-size:11px;color:var(--t3)\">⚠ Insiders têm até 2 dias úteis para reportar ao SEC.</div>\n</div>`;return;}\n  const trades=d.trades||[];\n  const ts=d.last_updated?new Date(d.last_updated).toLocaleTimeString():'—';\n  let html=`<div class=\"lf-note\">⚠ Insiders têm até 2 dias úteis para reportar ao SEC. Trades podem ter até 48h de atraso legal.</div>\n  <div style=\"font-size:11px;color:var(--t3);margin-bottom:10px\">\n    SEC Form 4 · Todos os insiders de todas as empresas americanas · Delay legal até 48h\n  </div>\n  <div class=\"lf-hdr\">\n    <span style=\"font-size:11px;color:var(--t2)\"><span class=\"pulse\"></span>Actualizado: ${ts} · ${trades.length} trades</span>\n    <div class=\"lff\"><button class=\"lf-btn ${LFF==='ALL'?'on':''}\" onclick=\"setLFF('ALL')\">TODOS</button><button class=\"lf-btn ${LFF==='BUY'?'on':''}\" onclick=\"setLFF('BUY')\">▲ BUY</button><button class=\"lf-btn ${LFF==='SELL'?'on':''}\" onclick=\"setLFF('SELL')\">▼ SELL</button></div>\n  </div>`;\n  if(!trades.length){el.innerHTML=html+'<div class=\"empty\">Sem trades para este filtro</div>';return;}\n  html+=`<table class=\"tbl\"><thead><tr><th>Insider</th><th>Cargo</th><th>Empresa</th><th>Acção</th><th>Acções</th><th>Preço</th><th>Valor</th><th>Posição</th><th>Data</th></tr></thead><tbody>`;\n  trades.forEach(t=>{\n    const av=`https://ui-avatars.com/api/?name=${encodeURIComponent(t.owner||'?')}&size=60&background=161d29&color=00e5a0&bold=true&format=svg`;\n    html+=`<tr onclick=\"navTo('${t.ticker||''}')\">\n      <td><img class=\"av\" src=\"${av}\" onerror=\"this.src='${av}'\"><span style=\"font-weight:600\">${t.owner||'—'}</span></td>\n      <td style=\"color:var(--t2);font-size:10px\">${t.relation||'Insider'}</td>\n      <td><span style=\"color:var(--bl);font-weight:600\">${t.ticker||''}</span><br><span style=\"font-size:10px;color:var(--t3)\">${(t.company||'').slice(0,28)}</span></td>\n      <td><span class=\"ab ab-${t.action||'FILING'}\">${t.action||'—'}</span></td>\n      <td>${t.shares!=null?Math.round(t.shares).toLocaleString():'—'}</td>\n      <td>${t.price!=null?'$'+t.price.toFixed(2):'—'}</td>\n      <td style=\"font-weight:700;color:${t.action==='BUY'?'var(--gr)':'var(--rd)'}\">${fmtB(t.value)}</td>\n      <td style=\"color:var(--t3);font-size:10px\">${t.shares_after!=null?Math.round(t.shares_after).toLocaleString()+' sh':'—'}</td>\n      <td style=\"color:var(--t2);font-size:10px\">${t.trade_date||t.filing_date||'—'}</td>\n    </tr>`;\n  });\n  html+='</tbody></table>';el.innerHTML=html;\n}\nfunction setLFF(f){LFF=f;loadLF();}\nfunction onTickerChange(t){/* live feed ignora ticker */}\nloadLF();setInterval(loadLF,90000);\n\ninitTkBar(window.TK);\n\n// Fix nav links to carry current ticker\nfunction fixNavLinks(){\n  const t=window.TK;\n  if(!t)return;\n  document.querySelectorAll('#nav a.nl').forEach(a=>{\n    const href=a.getAttribute('href');\n    if(href&&href!=='/'&&!href.startsWith('javascript')){\n      const u=new URL(href,window.location.origin);\n      u.searchParams.set('t',t);\n      a.setAttribute('href',u.toString());\n    }\n  });\n}\nfixNavLinks();\n// Also update nav links whenever ticker changes\n// navTo already calls fixNavLinks()\n</script>\n</body>\n</html>",
        "metrics.html": "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"UTF-8\">\n<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\n<title>Metrics · IST</title>\n<script src=\"https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.7.5/socket.io.min.js\"></script>\n\n<link href=\"https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600&family=Syne:wght@400;600;700;800&display=swap\" rel=\"stylesheet\">\n<style>\n*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}\n:root{\n  --bg:#080b0f;--bg2:#0d1117;--bg3:#111720;--bg4:#161d29;\n  --b:#1e2d3d;--b2:#243040;\n  --t:#c9d1d9;--t2:#8b949e;--t3:#484f58;\n  --gr:#00e5a0;--rd:#ff4d6d;--bl:#0095ff;\n  --yl:#f0c060;--pu:#a78bfa;--or:#ff8c42;\n  --fm:'JetBrains Mono',monospace;--fd:'Syne',sans-serif;\n}\nhtml,body{height:100%;background:var(--bg);color:var(--t);font-family:var(--fm);font-size:13px}\n::-webkit-scrollbar{width:5px;height:5px}::-webkit-scrollbar-track{background:transparent}::-webkit-scrollbar-thumb{background:var(--b2);border-radius:3px}\n.up{color:var(--gr)}.dn{color:var(--rd)}.nc{color:var(--t2)}\n/* TAPE */\n#tape{height:26px;background:var(--bg2);border-bottom:1px solid var(--b);overflow:hidden;display:flex;align-items:center;flex-shrink:0}\n#tape-inner{display:flex;white-space:nowrap;animation:tape 90s linear infinite;user-select:none;-webkit-user-select:none;cursor:default;}\n/* tape runs continuously */\n@keyframes tape{from{transform:translateX(0)}to{transform:translateX(-50%)}}\n.ti{display:inline-flex;align-items:center;gap:5px;padding:0 16px;height:26px;border-right:1px solid var(--b);font-size:11px;user-select:none;-webkit-user-select:none;}\n.ts{color:var(--t2);font-weight:600}.tc{font-size:10px}\n/* NAV */\n#nav{height:40px;background:var(--bg2);border-bottom:1px solid var(--b);display:flex;align-items:center;padding:0 10px;gap:2px;flex-shrink:0}\n.nl{display:flex;align-items:center;gap:5px;padding:5px 10px;border-radius:4px;font-size:11px;font-family:var(--fd);font-weight:700;letter-spacing:.04em;text-transform:uppercase;color:var(--t2);text-decoration:none;transition:all .15s;white-space:nowrap}\n.nl:hover{color:var(--t);background:var(--bg3)}\n.nl.on{color:var(--gr);background:rgba(0,229,160,.07);border:1px solid rgba(0,229,160,.12)}\n.nl svg{flex-shrink:0;opacity:.7}.nl.on svg{opacity:1}\n#nav-logo{font-family:var(--fd);font-size:14px;font-weight:800;color:var(--gr);letter-spacing:.04em;margin-right:12px;text-decoration:none}\n/* TICKER BAR */\n#tkbar{height:38px;background:var(--bg2);border-bottom:1px solid var(--b);display:flex;align-items:center;padding:0 14px;gap:10px;flex-shrink:0}\n#tk-sym{font-family:var(--fd);font-size:15px;font-weight:800;color:var(--gr);flex-shrink:0}\n#tk-name{font-size:11px;color:var(--t2);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}\n#tk-px{font-family:var(--fd);font-size:15px;font-weight:700;flex-shrink:0}\n#tk-chg{font-size:11px;padding:2px 8px;border-radius:3px;font-weight:600;flex-shrink:0}\n#tk-sw{position:relative}\n#tk-si{background:var(--bg3);border:1px solid var(--b2);color:var(--t);font-family:var(--fm);font-size:11px;padding:4px 9px;border-radius:3px;outline:none;width:160px}\n#tk-si:focus{border-color:var(--bl)}\n#tk-dr{position:absolute;top:calc(100% + 2px);right:0;background:var(--bg3);border:1px solid var(--b2);border-radius:3px;z-index:999;display:none;min-width:200px;max-height:200px;overflow-y:auto}\n.tdr{padding:6px 10px;cursor:pointer;display:flex;justify-content:space-between;gap:8px;font-size:11px}.tdr:hover{background:var(--bg4)}\n.tds{color:var(--gr);font-weight:700}.tdn{color:var(--t2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:120px}\n/* PAGE BODY */\n.pb{display:flex;flex-direction:column;height:100vh;overflow:hidden}\n.pc{flex:1;overflow-y:auto;overflow-x:hidden}\n/* SKELETON */\n.sk{background:linear-gradient(90deg,var(--bg3) 25%,var(--bg4) 50%,var(--bg3) 75%);background-size:200% 100%;animation:sk 1.2s ease infinite;border-radius:3px}\n@keyframes sk{0%{background-position:200% 0}100%{background-position:-200% 0}}\n.sk-line{height:14px;margin-bottom:8px}.sk-val{height:24px;margin-bottom:4px}\n/* SPIN */\n.spin-svg{display:inline-block;vertical-align:middle;margin-right:8px;width:44px;height:20px}\n/* EMPTY */\n.empty{text-align:center;padding:40px 20px;color:var(--t3);font-size:12px}\n\n.pc{padding:16px 20px}\n/* Analyst hero */\n.analyst-hero{background:var(--bg2);border:1px solid var(--b);border-radius:6px;padding:16px 20px;margin-bottom:16px;display:flex;align-items:center;gap:20px;flex-wrap:wrap}\n.rec-badge{font-family:var(--fd);font-size:28px;font-weight:800;padding:8px 20px;border-radius:6px;flex-shrink:0}\n.rec-meta{flex:1;min-width:0}\n.rec-name{font-size:11px;color:var(--t2);margin-bottom:4px}\n.targets{display:flex;gap:16px;flex-wrap:wrap}\n.tgt{text-align:center}\n.tgt-v{font-size:14px;font-weight:700;color:var(--t)}\n.tgt-l{font-size:9px;color:var(--t3);font-family:var(--fd);text-transform:uppercase;letter-spacing:.06em;margin-top:2px}\n/* Metric table IBKR style */\n.section{background:var(--bg2);border:1px solid var(--b);border-radius:6px;margin-bottom:10px;overflow:hidden}\n.section-hdr{padding:9px 14px;background:var(--bg3);border-bottom:1px solid var(--b);font-family:var(--fd);font-size:10px;font-weight:700;letter-spacing:.09em;text-transform:uppercase;color:var(--t2);display:flex;justify-content:space-between}\n.mtbl{width:100%;border-collapse:collapse}\n.mtbl tr:hover td{background:rgba(255,255,255,.02)}\n.mtbl td{padding:7px 14px;border-bottom:1px solid rgba(255,255,255,.025);font-size:12px}\n.mtbl td:last-child{text-align:right;font-weight:600}\n.mtbl tr:last-child td{border-bottom:none}\n.ml{color:var(--t2)}.mv{color:var(--t)}\n.mv.up{color:var(--gr)}.mv.dn{color:var(--rd)}.mv.yl{color:var(--yl)}\n/* mini bar inside cell */\n.bar-cell{display:flex;align-items:center;gap:8px;justify-content:flex-end}\n.bar-mini{height:4px;border-radius:2px;background:var(--gr);flex-shrink:0}\n/* Skeleton */\n.sk-section{background:var(--bg2);border:1px solid var(--b);border-radius:6px;padding:14px;margin-bottom:10px}\n</style>\n</head>\n<body>\n<div class=\"pb\">\n<div id=\"tape\"><div id=\"tape-inner\"></div></div>\n\n<div id=\"nav\"><a href=\"/\" id=\"nav-logo\" style=\"display:flex;align-items:center;gap:7px;text-decoration:none\">\n      <svg width=\"22\" height=\"22\" viewBox=\"0 0 64 64\" fill=\"none\">\n        <rect width=\"64\" height=\"64\" rx=\"10\" fill=\"rgba(0,229,160,0.15)\" stroke=\"rgba(0,229,160,0.4)\" stroke-width=\"2\"/>\n        <polyline points=\"8,44 20,28 30,36 42,18 56,22\" stroke=\"#00e5a0\" stroke-width=\"5\" fill=\"none\" stroke-linecap=\"round\" stroke-linejoin=\"round\"/>\n        <circle cx=\"42\" cy=\"18\" r=\"5\" fill=\"#00e5a0\"/>\n      </svg>\n      <span style=\"font-family:var(--fd);font-size:14px;font-weight:800;color:var(--gr);letter-spacing:.04em\">IST</span>\n    </a>\n    <a href=\"/chart\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"currentColor\"><rect x=\"1\" y=\"7\" width=\"2\" height=\"5\"/><rect x=\"5\" y=\"4\" width=\"2\" height=\"8\"/><rect x=\"9\" y=\"1\" width=\"2\" height=\"11\"/></svg>Chart</a><a href=\"/financials\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><polyline points=\"1,9 4,5 7,7 12,2\"/></svg>Financials</a><a href=\"/metrics\" class=\"nl on\"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"currentColor\"><circle cx=\"3\" cy=\"3\" r=\"1.5\"/><circle cx=\"10\" cy=\"3\" r=\"1.5\"/><circle cx=\"3\" cy=\"10\" r=\"1.5\"/><circle cx=\"10\" cy=\"10\" r=\"1.5\"/></svg>Metrics</a><a href=\"/insider\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><circle cx=\"6.5\" cy=\"4\" r=\"2.5\"/><path d=\"M1 12c0-3 2.5-5 5.5-5s5.5 2 5.5 5\"/></svg>Insider</a><a href=\"/congress\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><rect x=\"1\" y=\"5\" width=\"11\" height=\"7\" rx=\"1\"/><path d=\"M4 5V3a2.5 2.5 0 015 0v2\"/></svg>Congress</a><a href=\"/fairvalue\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><path d=\"M6.5 1L8 5h4l-3 2.5 1 4L6.5 9 3 11.5l1-4L1 5h4z\"/></svg>Fair Value</a><a href=\"/news\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><rect x=\"1\" y=\"1\" width=\"11\" height=\"11\" rx=\"1\"/><line x1=\"3\" y1=\"4\" x2=\"10\" y2=\"4\"/><line x1=\"3\" y1=\"7\" x2=\"10\" y2=\"7\"/><line x1=\"3\" y1=\"10\" x2=\"7\" y2=\"10\"/></svg>News</a><a href=\"/livefeed\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"currentColor\"><circle cx=\"6.5\" cy=\"6.5\" r=\"2\"/><circle cx=\"6.5\" cy=\"6.5\" r=\"4\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1\" opacity=\".5\"/></svg>Live Feed</a></div>\n<script id=\"asset-nav-fix\">\ndocument.addEventListener('DOMContentLoaded', function(){\n  const t=(new URLSearchParams(window.location.search).get('t')||'').toUpperCase();\n  const isCrypto=t.endsWith('-USD')&&!['GLD','SLV','USO','TLT','HYG','LQD','GDX'].includes(t);\n  const isFuture=t.endsWith('=F');\n  const isIndex=t.startsWith('^')||t==='DX-Y.NYB';\n  if(!isCrypto&&!isFuture&&!isIndex) return;\n  const STOCK_ONLY=['/financials','/insider','/congress','/fairvalue'];\n  document.querySelectorAll('#nav a.nl').forEach(a=>{\n    const href=a.getAttribute('href')||'';\n    if(STOCK_ONLY.some(p=>href.includes(p))) a.style.display='none';\n  });\n});\n</script>\n\n<div id=\"tkbar\">\n  <span id=\"tk-sym\">—</span><span id=\"tk-name\">—</span>\n  <span id=\"tk-px\" style=\"color:var(--t2)\">—</span><span id=\"tk-chg\"></span>\n  <div id=\"tk-sw\"><input id=\"tk-si\" placeholder=\"Change ticker…\" autocomplete=\"off\" spellcheck=\"false\"><div id=\"tk-dr\"></div></div>\n</div>\n<div class=\"pc\"><div class=\"empty\"><svg class=\"spin-svg\" viewBox=\"0 0 44 20\" fill=\"none\"><polyline points=\"2,16 8,10 14,13 22,5 30,9 42,3\" stroke=\"#00e5a0\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-dasharray=\"120\" stroke-dashoffset=\"120\"><animate attributeName=\"stroke-dashoffset\" from=\"120\" to=\"0\" dur=\"1.2s\" repeatCount=\"indefinite\"/></polyline></svg>Loading metrics…</div></div>\n</div>\n<script>\n\nconst TSYMS=['SPY','QQQ','^GSPC','^IXIC','^VIX','CL=F','BZ=F','GC=F','SI=F','DX-Y.NYB','^TNX','BTC-USD'];\nconst TLBL={SPY:'SPY',QQQ:'QQQ','^GSPC':'S&P500','^IXIC':'NASDAQ','^VIX':'VIX','CL=F':'WTI','BZ=F':'BRENT','GC=F':'GOLD','SI=F':'SILVER','DX-Y.NYB':'DXY','^TNX':'US10Y','BTC-USD':'BTC'};\nconst socket=io();\nfunction buildTape(){\n  document.getElementById('tape-inner').innerHTML=[...TSYMS,...TSYMS].map(t=>{\n    const id='ti_'+t.replace(/[^a-zA-Z0-9]/g,'_');\n    return`<div class=\"ti\" id=\"${id}\" onclick=\"navToTape('${t}')\" style=\"cursor:pointer;\"><span class=\"ts\">${TLBL[t]||t}</span>&nbsp;<span>—</span>&nbsp;<span class=\"tc nc\">—</span></div>`;\n  }).join('');\n  socket.emit('subscribe',{tickers:TSYMS});\n}\nsocket.on('price_update',({prices})=>{\n  prices.forEach(p=>{\n    const id='ti_'+p.ticker.replace(/[^a-zA-Z0-9]/g,'_');\n    document.querySelectorAll('#'+id).forEach(el=>{\n      const cs=el.children;\n      if(cs[1]&&p.price!=null)cs[1].textContent='$'+p.price.toFixed(2);\n      if(cs[2]&&p.change_pct!=null){const s=p.change_pct>=0?'+':'';cs[2].textContent=s+p.change_pct.toFixed(2)+'%';cs[2].className='tc '+(p.change_pct>0?'up':p.change_pct<0?'dn':'nc');}\n    });\n    if(p.ticker===window.TK)updateTkBar(p);\n    if(typeof onPriceUpdate==='function')onPriceUpdate(p);\n  });\n});\nbuildTape();\n\n/* ── FAST API CACHE ── */\nconst MEM={};  // in-memory cache: {data, ts}\nconst CACHE_TTL=300000; // 5 min\nasync function api(url, ttl=CACHE_TTL){\n  if(MEM[url]&&Date.now()-MEM[url].ts<ttl){return MEM[url].data;}\n  try{const r=await fetch(url);if(!r.ok)return null;const d=await r.json();MEM[url]={data:d,ts:Date.now()};return d;}\n  catch{return null;}\n}\n// Session storage for cross-page cache\nfunction ssGet(k){try{const v=sessionStorage.getItem(k);if(!v)return null;const p=JSON.parse(v);if(Date.now()-p.ts>CACHE_TTL){sessionStorage.removeItem(k);return null;}return p.data;}catch{return null;}}\nfunction ssSet(k,d){try{sessionStorage.setItem(k,JSON.stringify({data:d,ts:Date.now()}));}catch{}}\nasync function getStockData(t){\n  const k='full:'+t;\n  // 1. Session storage (instant, cross-page)\n  const ss=ssGet(k);if(ss)return ss;\n  // 2. Memory cache\n  const mc=MEM['/api/full/'+t];if(mc&&Date.now()-mc.ts<CACHE_TTL)return mc.data;\n  // 3. Fetch\n  const d=await api('/api/full/'+t,CACHE_TTL);\n  if(d)ssSet(k,d);\n  return d;\n}\n\n/* ── TICKER BAR ── */\nfunction updateTkBar(p){\n  const pxEl=document.getElementById('tk-px'),chEl=document.getElementById('tk-chg');\n  if(!pxEl)return;\n  if(p.price!=null){pxEl.textContent='$'+p.price.toFixed(2);pxEl.style.color=p.change_pct>0?'var(--gr)':p.change_pct<0?'var(--rd)':'var(--t)';}\n  if(p.change_pct!=null){const s=p.change_pct>=0?'+':'';chEl.textContent=s+p.change_pct.toFixed(2)+'%';chEl.style.cssText=`background:${p.change_pct>=0?'rgba(0,229,160,.12)':'rgba(255,77,109,.12)'};color:${p.change_pct>=0?'var(--gr)':'var(--rd)'};padding:2px 8px;border-radius:3px;`;}\n}\nasync function initTkBar(t){\n  document.getElementById('tk-sym').textContent=t;\n  const d=await api('/api/stock_fast/'+encodeURIComponent(t),6000);\n  if(d){document.getElementById('tk-name').textContent=d.name||t;updateTkBar(d);}\n  socket.emit('subscribe',{tickers:[t]});\n}\nlet sT;\nconst siEl=document.getElementById('tk-si'),drEl=document.getElementById('tk-dr');\nif(siEl){\n  siEl.addEventListener('input',function(){\n    clearTimeout(sT);const v=this.value.trim();\n    if(!v){drEl.style.display='none';return;}\n    sT=setTimeout(async()=>{\n      const d=await api('/api/universe?q='+encodeURIComponent(v)+'&limit=10',60000);\n      if(!d?.results?.length){drEl.style.display='none';return;}\n      drEl.innerHTML=d.results.map(x=>`<div class=\"tdr\" onclick=\"navTo('${x.ticker}')\"><span class=\"tds\">${x.ticker}</span><span class=\"tdn\">${x.name||''}</span></div>`).join('');\n      drEl.style.display='block';\n    },200);\n  });\n  document.addEventListener('click',e=>{if(!e.target.closest('#tk-sw'))drEl.style.display='none';});\n}\nfunction navTo(t){\n  if(!t)return;\n  if(siEl){siEl.value='';drEl.style.display='none';}\n  window.location.href='/chart?t='+encodeURIComponent(t);\n}\nwindow.TK=(new URLSearchParams(window.location.search).get('t')||'NVDA').toUpperCase();\nconst fmtB=v=>{if(v==null)return'—';const a=Math.abs(v),s=v<0?'-':'';if(a>=1e12)return s+'$'+(a/1e12).toFixed(2)+'T';if(a>=1e9)return s+'$'+(a/1e9).toFixed(2)+'B';if(a>=1e6)return s+'$'+(a/1e6).toFixed(1)+'M';return s+'$'+a.toLocaleString('en-US',{maximumFractionDigits:0});};\nconst fmtPx=v=>v==null?'—':'$'+Number(v).toFixed(2);\nconst p1=v=>v==null?'—':(v*100).toFixed(1)+'%';\nconst fn=(v,d=2)=>v==null?'—':Number(v).toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d});\n\n\nasync function loadSpecialMetrics(t, assetType){\n  const el=document.querySelector('.pc');\n  el.innerHTML='<div class=\"empty\"><svg class=\"spin-svg\" viewBox=\"0 0 44 20\" fill=\"none\"><polyline points=\"2,16 8,10 14,13 22,5 30,9 42,3\" stroke=\"#00e5a0\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-dasharray=\"120\" stroke-dashoffset=\"120\"><animate attributeName=\"stroke-dashoffset\" from=\"120\" to=\"0\" dur=\"1.2s\" repeatCount=\"indefinite\"/></polyline></svg>A carregar métricas…</div>';\n  \n  if(assetType==='crypto'){\n    const [ci,px]=await Promise.all([api('/api/crypto_info/'+encodeURIComponent(t),60000,15000),api('/api/stock_fast/'+encodeURIComponent(t),6000)]);\n    let html='';\n    const fg=ci?.fear_greed;const mkt=ci?.market;const coin=ci?.coin;const hv=ci?.halving;\n    if(fg){\n      const fgColor=v=>v>=75?'var(--gr)':v>=55?'#80e0a0':v>=45?'var(--yl)':v>=25?'var(--or)':'var(--rd)';\n      // Fear&Greed histogram (last 7 days)\n      const WDAYS=['Dom','Seg','Ter','Qua','Qui','Sex','Sáb'];\n      const fgHistHtml=(fg.history||[]).slice(0,7).reverse().map(h=>{\n        const dt=new Date(parseInt(h.date)*1000);\n        const wd=WDAYS[dt.getDay()];\n        const w=Math.round(h.value/100*140);\n        const bc=fgColor(h.value);\n        return `<div style=\"display:flex;align-items:center;gap:8px;margin-bottom:4px\">\n          <div style=\"width:28px;font-size:9px;color:var(--t3);text-align:right\">${wd}</div>\n          <div style=\"width:${w}px;height:14px;border-radius:2px;background:${bc};opacity:.85;min-width:8px\"></div>\n          <div style=\"font-size:10px;font-weight:700;color:${bc}\">${h.value}</div>\n          <div style=\"font-size:9px;color:var(--t3)\">${h.label}</div>\n        </div>`;\n      }).join('');\n      html+=`<div style=\"background:rgba(255,77,109,.06);border:1px solid rgba(255,77,109,.15);border-radius:8px;padding:16px;margin-bottom:12px\">\n        <div style=\"display:flex;gap:20px;align-items:flex-start;flex-wrap:wrap\">\n          <div style=\"text-align:center;min-width:80px\">\n            <div style=\"font-family:var(--fd);font-size:48px;font-weight:800;color:${fgColor(fg.value)};line-height:1\">${fg.value}</div>\n            <div style=\"font-size:10px;color:var(--t2);margin-top:4px\">/100</div>\n            <div style=\"font-size:13px;font-weight:700;color:${fgColor(fg.value)};margin-top:4px\">${fg.label}</div>\n          </div>\n          <div style=\"flex:1;min-width:160px\">${fgHistHtml}</div>\n        </div>\n        <div style=\"margin-top:10px;font-size:11px;color:var(--t2)\">Fear & Greed Index · Fonte: Alternative.me</div>\n      </div>`;\n      \n    }\n    // Metrics table\n    const metrics=[\n      ['Fear & Greed Index', fg?fg.value+'/100':'—', fg?(fg.value<25?'Comprar (pânico extremo)':fg.value<45?'Oportunidade':fg.value>75?'Vender (euforia)':fg.value>55?'Cuidado':'Neutro'):'—'],\n      ['BTC Dominância', mkt?mkt.btc_dominance+'%':'—', mkt?(mkt.btc_dominance>60?'BTC season':mkt.btc_dominance<40?'Alt season':'Transição'):'—'],\n      ['Market Cap Total', mkt?fmtB(mkt.total_market_cap_usd):'—', ''],\n      ['Var. 24h Market Cap', mkt?(mkt.market_cap_change_24h>0?'+':'')+mkt.market_cap_change_24h+'%':'—', ''],\n      ['ATH', coin?'$'+Number(coin.ath).toLocaleString('en-US',{maximumFractionDigits:2}):'—', ''],\n      ['vs ATH', coin&&coin.ath_change_pct!=null?coin.ath_change_pct.toFixed(1)+'%':'—', coin?(coin.ath_change_pct>-20?'Próximo ATH':coin.ath_change_pct>-50?'Meio ciclo':'Território acumulação'):'—'],\n      ['Performance 7D', coin&&coin.price_change_7d!=null?(coin.price_change_7d>0?'+':'')+coin.price_change_7d+'%':'—', ''],\n      ['Performance 30D', coin&&coin.price_change_30d!=null?(coin.price_change_30d>0?'+':'')+coin.price_change_30d+'%':'—', ''],\n      ['Performance 1 Ano', coin&&coin.price_change_1y!=null?(coin.price_change_1y>0?'+':'')+coin.price_change_1y+'%':'—', ''],\n      ['Market Cap Rank', coin?'#'+coin.market_cap_rank:'—', ''],\n      ['Market Cap', coin?fmtB(coin.market_cap):'—', ''],\n      ['Volume 24h', coin?fmtB(coin.volume_24h):'—', ''],\n      ['Supply Circulante', coin&&coin.circulating_supply?coin.circulating_supply.toLocaleString('en-US',{maximumFractionDigits:0}):'—', ''],\n      ['Supply Máximo', coin?.max_supply?coin.max_supply.toLocaleString('en-US',{maximumFractionDigits:0}):'∞', ''],\n      ['Next Bitcoin Halving', hv?hv.next_date_est+' (~'+hv.days_estimate+' dias)':'—', ''],\n    ];\n    html+=`<div class=\"section\"><div class=\"section-hdr\">Métricas ${t.replace('-USD','')}</div><table class=\"mtbl\"><thead><tr><th>Indicador</th><th>Valor</th><th>Interpretação</th></tr></thead><tbody>`;\n    metrics.forEach(([l,v,i])=>html+=`<tr><td class=\"ml\">${l}</td><td class=\"mv\">${v}</td><td class=\"mv\" style=\"font-size:10px;color:var(--t2)\">${i}</td></tr>`);\n    html+=`</tbody></table></div>`;\n    el.innerHTML=html;\n  } else {\n    // Commodity/Index metrics\n    const [ci,px]=await Promise.all([api('/api/commodity_info/'+encodeURIComponent(t),300000,10000),api('/api/stock_fast/'+encodeURIComponent(t),6000)]);\n    const COMM_NAMES={'GC=F':'Gold','SI=F':'Silver','CL=F':'WTI Crude','BZ=F':'Brent Crude','NG=F':'Natural Gas','HG=F':'Copper','ZC=F':'Corn','ZW=F':'Wheat','PL=F':'Platinum','PA=F':'Palladium'};\n    let html=`<div class=\"section\"><div class=\"section-hdr\">Métricas ${COMM_NAMES[t]||t}</div><table class=\"mtbl\"><tbody>`;\n    html+=`<tr><td class=\"ml\">Preço Actual</td><td class=\"mv\">${px?.price?'$'+px.price.toFixed(2):'—'}</td><td></td></tr>`;\n    html+=`<tr><td class=\"ml\">Variação Hoje</td><td class=\"mv ${px?.change_pct>0?'up':px?.change_pct<0?'dn':''}\">${px?.change_pct!=null?(px.change_pct>0?'+':'')+px.change_pct.toFixed(2)+'%':'—'}</td><td></td></tr>`;\n    if(ci?.seasonal)html+=`<tr><td class=\"ml\">Sazonalidade</td><td class=\"mv\" colspan=\"2\" style=\"font-size:10px\">${ci.seasonal}</td></tr>`;\n    html+=`</tbody></table></div>`;\n    if(ci?.drivers?.length){\n      html+=`<div class=\"section\"><div class=\"section-hdr\">Factores de Preço</div><div style=\"padding:10px 14px\">`;\n      ci.drivers.forEach(d=>html+=`<div style=\"padding:5px 0;border-bottom:1px solid var(--b);font-size:11px;color:var(--t)\">• ${d}</div>`);\n      html+=`</div></div>`;\n    }\n    el.innerHTML=html;\n  }\n}\n\nasync function loadMetrics(t){\n  const _isCr=t.endsWith('-USD')&&!['GLD','SLV','USO','TLT'].includes(t);\n  const _isFu=t.endsWith('=F')||t.startsWith('^')||t==='DX-Y.NYB';\n  if(_isCr||_isFu){\n    // Load appropriate metrics for non-stock assets\n    loadSpecialMetrics(t, _isCr?'crypto':_isFu?'commodity':'index');\n    return;\n  }\n  // Redirect non-stocks to their page immediately\n  const el=document.querySelector('.pc');\n  // Show skeleton immediately\n  el.innerHTML=`\n    <div class=\"sk-section\"><div class=\"sk sk-val\" style=\"width:40%;margin-bottom:12px\"></div>${'<div class=\"sk sk-line\"></div>'.repeat(4)}</div>\n    <div class=\"sk-section\">${'<div class=\"sk sk-line\"></div>'.repeat(6)}</div>\n    <div class=\"sk-section\">${'<div class=\"sk sk-line\"></div>'.repeat(5)}</div>`;\n  const d=await getStockData(t);\n  if(!d){el.innerHTML='<div class=\"empty\">No data</div>';return;}\n\n  const prc=d.price;\n  let html='';\n\n  // Analyst hero\n  if(d.recommendation_key){\n    // Normalize various formats from yfinance\n    const normalizeRec=r=>(r||'').toLowerCase().replace(/[_ ]/g,'');\n    const rm={'strongbuy':'Strong Buy','buy':'Buy','hold':'Hold','sell':'Sell','strongsell':'Strong Sell'};\n    const rc={strongbuy:'rgba(0,229,160,.15)',buy:'rgba(0,229,160,.08)',hold:'rgba(240,192,96,.1)',sell:'rgba(255,140,66,.1)',strongsell:'rgba(255,77,109,.15)'};\n    const tc={strongbuy:'var(--gr)',buy:'var(--gr)',hold:'var(--yl)',sell:'var(--or)',strongsell:'var(--rd)'};\n    const rk=normalizeRec(d.recommendation_key);\n    const up=d.upside;\n    html+=`<div class=\"analyst-hero\">\n      <div class=\"rec-badge\" style=\"background:${rc[rk]||'var(--bg3)'};color:${tc[rk]||'var(--t2)'}\">${rm[rk]||d.recommendation_key}</div>\n      <div class=\"rec-meta\">\n        <div class=\"rec-name\">${d.analyst_count||0} analistas · ${d.name||t}</div>\n        <div class=\"targets\">\n          <div class=\"tgt\"><div class=\"tgt-v\">${d.target_low?fmtPx(d.target_low):'—'}</div><div class=\"tgt-l\">Mínimo</div></div>\n          <div class=\"tgt\"><div class=\"tgt-v\" style=\"color:var(--bl)\">${d.target_mean?fmtPx(d.target_mean):'—'}</div><div class=\"tgt-l\">Consenso</div></div>\n          <div class=\"tgt\"><div class=\"tgt-v\">${d.target_high?fmtPx(d.target_high):'—'}</div><div class=\"tgt-l\">Máximo</div></div>\n          <div class=\"tgt\"><div class=\"tgt-v ${up>0?'up':up<0?'dn':''}\">${up!=null?(up>=0?'+':'')+up+'%':'—'}</div><div class=\"tgt-l\">Upside</div></div>\n        </div>\n      </div>\n    </div>`;\n  }\n\n  // Helper: row with optional mini bar\n  const row=(label,val,cls='',barPct=null)=>{\n    const valCell=barPct!=null\n      ?`<td><div class=\"bar-cell\"><div class=\"bar-mini\" style=\"width:${Math.min(barPct,100)}px\"></div><span class=\"mv ${cls}\">${val}</span></div></td>`\n      :`<td class=\"mv ${cls}\">${val}</td>`;\n    return`<tr><td class=\"ml\">${label}</td>${valCell}</tr>`;\n  };\n\n  // 52W range bar position\n  const rangePos=(prc&&d['52w_low']&&d['52w_high'])?Math.round((prc-d['52w_low'])/(d['52w_high']-d['52w_low'])*80):null;\n\n  // Sections — IBKR style\n  const sections=[\n    ['Preço & Mercado',[\n      ['Preço Actual', fmtPx(prc), ''],\n      ['Cap. Mercado', fmtB(d.market_cap), ''],\n      ['Volume Médio', d.avg_volume?Number(d.avg_volume).toLocaleString('en-US'):'—', ''],\n      ['Mínimo 52 Semanas', d['52w_low']?fmtPx(d['52w_low']):'—', 'dn'],\n      ['Máximo 52 Semanas', d['52w_high']?fmtPx(d['52w_high']):'—', 'up'],\n      ['Posição 52 Sem.', rangePos!=null?rangePos+'% do range':'—', '', rangePos],\n      ['Média 50 Dias', d['50d_avg']?fmtPx(d['50d_avg']):'—', prc&&d['50d_avg']?(prc>d['50d_avg']?'up':'dn'):''],\n      ['Média 200 Dias', d['200d_avg']?fmtPx(d['200d_avg']):'—', prc&&d['200d_avg']?(prc>d['200d_avg']?'up':'dn'):''],\n      ['Beta', fn(d.beta,2), d.beta>1.5?'dn':d.beta<0.8?'up':''],\n      ['Short Float', p1(d.short_pct_float), d.short_pct_float>0.2?'dn':''],\n      ['Dividend Yield', p1(d.dividend_yield), d.dividend_yield>0?'up':''],\n    ]],\n    ['Avaliação',[\n      ['P/E Trailing', fn(d.pe_trailing,1), d.pe_trailing>30?'dn':d.pe_trailing>0&&d.pe_trailing<15?'up':''],\n      ['P/E Forward', fn(d.pe_forward,1), d.pe_forward>30?'dn':d.pe_forward>0&&d.pe_forward<15?'up':''],\n      ['PEG Ratio', fn(d.peg_ratio,2), d.peg_ratio>2?'dn':d.peg_ratio>0&&d.peg_ratio<1?'up':''],\n      ['Price/Sales', fn(d.ps_ratio,2), ''],\n      ['Price/Book', fn(d.pb_ratio,2), ''],\n      ['EV/EBITDA', fn(d.ev_ebitda,1), d.ev_ebitda>25?'dn':d.ev_ebitda>0&&d.ev_ebitda<10?'up':''],\n      ['EV/Revenue', fn(d.ev_revenue,1), ''],\n    ]],\n    ['Rentabilidade',[\n      ['Receita TTM', fmtB(d.revenue_ttm), ''],\n      ['Resultado Líquido', fmtB(d.net_income), d.net_income>0?'up':'dn'],\n      ['Margem Bruta', p1(d.gross_margin), d.gross_margin>0.5?'up':d.gross_margin<0.2?'dn':''],\n      ['Margem Operacional', p1(d.operating_margin), d.operating_margin>0.2?'up':d.operating_margin<0?'dn':''],\n      ['Margem Líquida', p1(d.profit_margin), d.profit_margin>0.15?'up':d.profit_margin<0?'dn':''],\n      ['ROE', p1(d.roe), d.roe>0.15?'up':d.roe<0?'dn':''],\n      ['ROA', p1(d.roa), d.roa>0.1?'up':d.roa<0?'dn':''],\n      ['FCF', fmtB(d.fcf), d.fcf>0?'up':'dn'],\n    ]],\n    ['Crescimento',[\n      ['Crescimento Receita', p1(d.revenue_growth), d.revenue_growth>0.15?'up':d.revenue_growth<0?'dn':''],\n      ['Crescimento Resultados', p1(d.earnings_growth), d.earnings_growth>0.15?'up':d.earnings_growth<0?'dn':''],\n      ['EPS (TTM)', fn(d.eps_trailing,2), d.eps_trailing>0?'up':'dn'],\n      ['EPS (Forward)', fn(d.eps_forward,2), d.eps_forward>0?'up':'dn'],\n    ]],\n    ['Balanço',[\n      ['Caixa Total', fmtB(d.total_cash), 'up'],\n      ['Dívida Total', fmtB(d.total_debt), 'dn'],\n      ['Dívida/Capital', fn(d.debt_to_equity,2), d.debt_to_equity>2?'dn':d.debt_to_equity<1?'up':''],\n      ['Current Ratio', fn(d.current_ratio,2), d.current_ratio>2?'up':d.current_ratio<1?'dn':''],\n      ['Quick Ratio', fn(d.quick_ratio,2), d.quick_ratio>1?'up':d.quick_ratio<0.5?'dn':''],\n    ]],\n    ['Informação',[\n      ['Sector', d.sector||'—', ''],\n      ['Indústria', d.industry||'—', ''],\n      ['País', d.country||'—', ''],\n      ['Exchange', d.exchange||'—', ''],\n      ['Próx. Earnings', d.earnings_date||'—', ''],\n    ]],\n  ];\n\n  sections.forEach(([title,rows])=>{\n    html+=`<div class=\"section\"><div class=\"section-hdr\"><span>${title}</span></div><table class=\"mtbl\">`;\n    rows.forEach(([l,v,c,bar])=>html+=row(l,v,c,bar));\n    html+=`</table></div>`;\n  });\n\n  if(d.summary){html+=`<div class=\"section\"><div class=\"section-hdr\">Sobre a Empresa</div><div style=\"padding:12px 14px;font-size:11px;color:var(--t2);line-height:1.7\">${d.summary}</div></div>`;}\n  el.innerHTML=html;\n}\nfunction onTickerChange(t){loadMetrics(t);}\nloadMetrics(window.TK);\n\ninitTkBar(window.TK);\n\n// Fix nav links to carry current ticker\nfunction fixNavLinks(){\n  const t=window.TK;\n  if(!t)return;\n  document.querySelectorAll('#nav a.nl').forEach(a=>{\n    const href=a.getAttribute('href');\n    if(href&&href!=='/'&&!href.startsWith('javascript')){\n      const u=new URL(href,window.location.origin);\n      u.searchParams.set('t',t);\n      a.setAttribute('href',u.toString());\n    }\n  });\n}\nfixNavLinks();\n// Also update nav links whenever ticker changes\n// navTo already calls fixNavLinks()\n</script>\n</body>\n</html>",
    "news.html": "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"UTF-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\n<title>News · IST</title>\n<script src=\"https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.7.5/socket.io.min.js\"></script>\n<link href=\"https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600&family=Syne:wght@400;600;700;800&display=swap\" rel=\"stylesheet\">\n<style>\n*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}\n:root{\n  --bg:#080b0f;--bg2:#0d1117;--bg3:#111720;--bg4:#161d29;\n  --b:#1e2d3d;--b2:#243040;\n  --t:#c9d1d9;--t2:#8b949e;--t3:#484f58;\n  --gr:#00e5a0;--rd:#ff4d6d;--bl:#0095ff;\n  --yl:#f0c060;--pu:#a78bfa;--or:#ff8c42;\n  --fm:'JetBrains Mono',monospace;--fd:'Syne',sans-serif;\n}\nhtml,body{height:100%;background:var(--bg);color:var(--t);font-family:var(--fm);font-size:13px}\n::-webkit-scrollbar{width:5px;height:5px}::-webkit-scrollbar-track{background:transparent}::-webkit-scrollbar-thumb{background:var(--b2);border-radius:3px}\n.up{color:var(--gr)}.dn{color:var(--rd)}.nc{color:var(--t2)}\n/* TAPE */\n#tape{height:26px;background:var(--bg2);border-bottom:1px solid var(--b);overflow:hidden;display:flex;align-items:center;flex-shrink:0}\n#tape-inner{display:flex;white-space:nowrap;animation:tape 90s linear infinite;user-select:none;-webkit-user-select:none;cursor:default;}\n/* tape runs continuously */\n@keyframes tape{from{transform:translateX(0)}to{transform:translateX(-50%)}}\n.ti{display:inline-flex;align-items:center;gap:5px;padding:0 16px;height:26px;border-right:1px solid var(--b);font-size:11px;user-select:none;-webkit-user-select:none;}\n.ts{color:var(--t2);font-weight:600}.tc{font-size:10px}\n/* NAV */\n#nav{height:40px;background:var(--bg2);border-bottom:1px solid var(--b);display:flex;align-items:center;padding:0 10px;gap:2px;flex-shrink:0}\n.nl{display:flex;align-items:center;gap:5px;padding:5px 10px;border-radius:4px;font-size:11px;font-family:var(--fd);font-weight:700;letter-spacing:.04em;text-transform:uppercase;color:var(--t2);text-decoration:none;transition:all .15s;white-space:nowrap}\n.nl:hover{color:var(--t);background:var(--bg3)}\n.nl.on{color:var(--gr);background:rgba(0,229,160,.07);border:1px solid rgba(0,229,160,.12)}\n.nl svg{flex-shrink:0;opacity:.7}.nl.on svg{opacity:1}\n#nav-logo{font-family:var(--fd);font-size:14px;font-weight:800;color:var(--gr);letter-spacing:.04em;margin-right:12px;text-decoration:none}\n/* TICKER BAR */\n#tkbar{height:38px;background:var(--bg2);border-bottom:1px solid var(--b);display:flex;align-items:center;padding:0 14px;gap:10px;flex-shrink:0}\n#tk-sym{font-family:var(--fd);font-size:15px;font-weight:800;color:var(--gr);flex-shrink:0}\n#tk-name{font-size:11px;color:var(--t2);flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}\n#tk-px{font-family:var(--fd);font-size:15px;font-weight:700;flex-shrink:0}\n#tk-chg{font-size:11px;padding:2px 8px;border-radius:3px;font-weight:600;flex-shrink:0}\n#tk-sw{position:relative}\n#tk-si{background:var(--bg3);border:1px solid var(--b2);color:var(--t);font-family:var(--fm);font-size:11px;padding:4px 9px;border-radius:3px;outline:none;width:160px}\n#tk-si:focus{border-color:var(--bl)}\n#tk-dr{position:absolute;top:calc(100% + 2px);right:0;background:var(--bg3);border:1px solid var(--b2);border-radius:3px;z-index:999;display:none;min-width:200px;max-height:200px;overflow-y:auto}\n.tdr{padding:6px 10px;cursor:pointer;display:flex;justify-content:space-between;gap:8px;font-size:11px}.tdr:hover{background:var(--bg4)}\n.tds{color:var(--gr);font-weight:700}.tdn{color:var(--t2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:120px}\n/* PAGE BODY */\n.pb{display:flex;flex-direction:column;height:100vh;overflow:hidden}\n.pc{flex:1;overflow-y:auto;overflow-x:hidden}\n/* SKELETON */\n.sk{background:linear-gradient(90deg,var(--bg3) 25%,var(--bg4) 50%,var(--bg3) 75%);background-size:200% 100%;animation:sk 1.2s ease infinite;border-radius:3px}\n@keyframes sk{0%{background-position:200% 0}100%{background-position:-200% 0}}\n.sk-line{height:14px;margin-bottom:8px}.sk-val{height:24px;margin-bottom:4px}\n/* SPIN */\n.spin-svg{display:inline-block;vertical-align:middle;margin-right:8px;width:44px;height:20px}\n/* EMPTY */\n.empty{text-align:center;padding:40px 20px;color:var(--t3);font-size:12px}\n\n.pc{padding:16px 20px;max-width:1000px}\n.ntabs{display:flex;gap:6px;margin-bottom:14px}\n.ntab{padding:5px 14px;border-radius:4px;font-size:11px;font-family:var(--fd);font-weight:700;cursor:pointer;background:var(--bg2);border:1px solid var(--b);color:var(--t2);transition:all .15s}\n.ntab.on{background:var(--gr);color:var(--bg);border-color:var(--gr)}\n.ngrid{display:grid;grid-template-columns:1fr 1fr;gap:10px}\n.ncard{background:var(--bg2);border:1px solid var(--b);border-radius:5px;padding:12px;display:flex;flex-direction:column;gap:7px;transition:border .15s}\n.ncard:hover{border-color:var(--b2)}\n.ntitle{font-size:12px;color:var(--t);line-height:1.5}\n.ntitle a{color:inherit;text-decoration:none}.ntitle a:hover{color:var(--gr)}\n.nmeta{display:flex;gap:7px;align-items:center;flex-wrap:wrap;margin-top:auto}\n.sent{font-size:9px;padding:2px 7px;border-radius:2px;font-weight:700}\n.sent-positive{background:rgba(0,229,160,.15);color:var(--gr)}.sent-negative{background:rgba(255,77,109,.15);color:var(--rd)}.sent-neutral{background:rgba(255,255,255,.07);color:var(--t2)}\n.ndt{font-size:10px;color:var(--t3);margin-left:auto}\n</style>\n</head>\n<body>\n<div class=\"pb\">\n<div id=\"tape\"><div id=\"tape-inner\"></div></div>\n<div id=\"nav\"><a href=\"/\" id=\"nav-logo\" style=\"display:flex;align-items:center;gap:7px;text-decoration:none\">\n      <svg width=\"22\" height=\"22\" viewBox=\"0 0 64 64\" fill=\"none\">\n        <rect width=\"64\" height=\"64\" rx=\"10\" fill=\"rgba(0,229,160,0.15)\" stroke=\"rgba(0,229,160,0.4)\" stroke-width=\"2\"/>\n        <polyline points=\"8,44 20,28 30,36 42,18 56,22\" stroke=\"#00e5a0\" stroke-width=\"5\" fill=\"none\" stroke-linecap=\"round\" stroke-linejoin=\"round\"/>\n        <circle cx=\"42\" cy=\"18\" r=\"5\" fill=\"#00e5a0\"/>\n      </svg>\n      <span style=\"font-family:var(--fd);font-size:14px;font-weight:800;color:var(--gr);letter-spacing:.04em\">IST</span>\n    </a>\n    <a href=\"/chart\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"currentColor\"><rect x=\"1\" y=\"7\" width=\"2\" height=\"5\"/><rect x=\"5\" y=\"4\" width=\"2\" height=\"8\"/><rect x=\"9\" y=\"1\" width=\"2\" height=\"11\"/></svg>Chart</a><a href=\"/financials\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><polyline points=\"1,9 4,5 7,7 12,2\"/></svg>Financials</a><a href=\"/metrics\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"currentColor\"><circle cx=\"3\" cy=\"3\" r=\"1.5\"/><circle cx=\"10\" cy=\"3\" r=\"1.5\"/><circle cx=\"3\" cy=\"10\" r=\"1.5\"/><circle cx=\"10\" cy=\"10\" r=\"1.5\"/></svg>Metrics</a><a href=\"/insider\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><circle cx=\"6.5\" cy=\"4\" r=\"2.5\"/><path d=\"M1 12c0-3 2.5-5 5.5-5s5.5 2 5.5 5\"/></svg>Insider</a><a href=\"/congress\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><rect x=\"1\" y=\"5\" width=\"11\" height=\"7\" rx=\"1\"/><path d=\"M4 5V3a2.5 2.5 0 015 0v2\"/></svg>Congress</a><a href=\"/fairvalue\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><path d=\"M6.5 1L8 5h4l-3 2.5 1 4L6.5 9 3 11.5l1-4L1 5h4z\"/></svg>Fair Value</a><a href=\"/news\" class=\"nl on\"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.5\"><rect x=\"1\" y=\"1\" width=\"11\" height=\"11\" rx=\"1\"/><line x1=\"3\" y1=\"4\" x2=\"10\" y2=\"4\"/><line x1=\"3\" y1=\"7\" x2=\"10\" y2=\"7\"/></svg>News</a><a href=\"/livefeed\" class=\"nl \"><svg width=\"12\" height=\"12\" viewBox=\"0 0 13 13\" fill=\"currentColor\"><circle cx=\"6.5\" cy=\"6.5\" r=\"2\"/><circle cx=\"6.5\" cy=\"6.5\" r=\"4\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1\" opacity=\".5\"/></svg>Live Feed</a></div>\n<script id=\"asset-nav-fix\">\ndocument.addEventListener('DOMContentLoaded', function(){\n  const t=(new URLSearchParams(window.location.search).get('t')||'').toUpperCase();\n  const isCrypto=t.endsWith('-USD')&&!['GLD','SLV','USO','TLT','HYG','LQD','GDX'].includes(t);\n  const isFuture=t.endsWith('=F');\n  const isIndex=t.startsWith('^')||t==='DX-Y.NYB';\n  if(!isCrypto&&!isFuture&&!isIndex) return;\n  const STOCK_ONLY=['/financials','/insider','/congress','/fairvalue'];\n  document.querySelectorAll('#nav a.nl').forEach(a=>{\n    const href=a.getAttribute('href')||'';\n    if(STOCK_ONLY.some(p=>href.includes(p))) a.style.display='none';\n  });\n});\n</script>\n<div id=\"tkbar\">\n  <span id=\"tk-sym\">—</span><span id=\"tk-name\">—</span>\n  <span id=\"tk-px\" style=\"color:var(--t2)\">—</span><span id=\"tk-chg\"></span>\n  <div id=\"tk-sw\"><input id=\"tk-si\" placeholder=\"Change ticker…\" autocomplete=\"off\" spellcheck=\"false\"><div id=\"tk-dr\"></div></div>\n</div>\n<div class=\"pc\"><div class=\"empty\"><svg class=\"spin-svg\" viewBox=\"0 0 44 20\" fill=\"none\"><polyline points=\"2,16 8,10 14,13 22,5 30,9 42,3\" stroke=\"#00e5a0\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-dasharray=\"120\" stroke-dashoffset=\"120\"><animate attributeName=\"stroke-dashoffset\" from=\"120\" to=\"0\" dur=\"1.2s\" repeatCount=\"indefinite\"/></polyline></svg>A carregar notícias…</div></div>\n</div>\n<script>\nconst TSYMS=['SPY','QQQ','^GSPC','^IXIC','^VIX','CL=F','BZ=F','GC=F','SI=F','DX-Y.NYB','^TNX','BTC-USD'];\nconst TLBL={SPY:'SPY',QQQ:'QQQ','^GSPC':'S&P500','^IXIC':'NASDAQ','^VIX':'VIX','CL=F':'WTI','BZ=F':'BRENT','GC=F':'GOLD','SI=F':'SILVER','DX-Y.NYB':'DXY','^TNX':'US10Y','BTC-USD':'BTC'};\nconst socket=io();\nfunction buildTape(){\n  document.getElementById('tape-inner').innerHTML=[...TSYMS,...TSYMS].map(t=>{\n    const id='ti_'+t.replace(/[^a-zA-Z0-9]/g,'_');\n    return`<div class=\"ti\" id=\"${id}\" onclick=\"navToTape('${t}')\" style=\"cursor:pointer;\"><span class=\"ts\">${TLBL[t]||t}</span>&nbsp;<span>—</span>&nbsp;<span class=\"tc nc\">—</span></div>`;\n  }).join('');\n  socket.emit('subscribe',{tickers:TSYMS});\n}\nsocket.on('price_update',({prices})=>{\n  prices.forEach(p=>{\n    const id='ti_'+p.ticker.replace(/[^a-zA-Z0-9]/g,'_');\n    document.querySelectorAll('#'+id).forEach(el=>{\n      const cs=el.children;\n      if(cs[1]&&p.price!=null)cs[1].textContent='$'+p.price.toFixed(2);\n      if(cs[2]&&p.change_pct!=null){const s=p.change_pct>=0?'+':'';cs[2].textContent=s+p.change_pct.toFixed(2)+'%';cs[2].className='tc '+(p.change_pct>0?'up':p.change_pct<0?'dn':'nc');}\n    });\n    if(p.ticker===window.TK)updateTkBar(p);\n    if(typeof onPriceUpdate==='function')onPriceUpdate(p);\n  });\n});\nbuildTape();\nconst MEM={};const CACHE_TTL=300000;\nasync function api(url,ttl=CACHE_TTL,timeout=12000){\n  if(MEM[url]&&Date.now()-MEM[url].ts<ttl)return MEM[url].data;\n  try{\n    const ctrl=new AbortController();\n    const tid=setTimeout(()=>ctrl.abort(),timeout);\n    const r=await fetch(url,{signal:ctrl.signal});\n    clearTimeout(tid);\n    if(!r.ok)return null;\n    const d=await r.json();\n    MEM[url]={data:d,ts:Date.now()};\n    return d;\n  }catch{return null;}\n}\nfunction ssGet(k){try{const v=sessionStorage.getItem(k);if(!v)return null;const p=JSON.parse(v);if(Date.now()-p.ts>CACHE_TTL){sessionStorage.removeItem(k);return null;}return p.data;}catch{return null;}}\nfunction ssSet(k,d){try{sessionStorage.setItem(k,JSON.stringify({data:d,ts:Date.now()}));}catch{}}\nasync function getStockData(t){const k='full:'+t;const ss=ssGet(k);if(ss)return ss;const mc=MEM['/api/full/'+t];if(mc&&Date.now()-mc.ts<CACHE_TTL)return mc.data;const d=await api('/api/full/'+t,CACHE_TTL);if(d)ssSet(k,d);return d;}\nfunction updateTkBar(p){const pxEl=document.getElementById('tk-px'),chEl=document.getElementById('tk-chg');if(!pxEl)return;if(p.price!=null){pxEl.textContent='$'+p.price.toFixed(2);pxEl.style.color=p.change_pct>0?'var(--gr)':p.change_pct<0?'var(--rd)':'var(--t)';}if(p.change_pct!=null){const s=p.change_pct>=0?'+':'';chEl.textContent=s+p.change_pct.toFixed(2)+'%';chEl.style.cssText=`background:${p.change_pct>=0?'rgba(0,229,160,.12)':'rgba(255,77,109,.12)'};color:${p.change_pct>=0?'var(--gr)':'var(--rd)'};padding:2px 8px;border-radius:3px;`;}}\nasync function initTkBar(t){document.getElementById('tk-sym').textContent=t;const d=await api('/api/stock_fast/'+encodeURIComponent(t),6000);if(d){document.getElementById('tk-name').textContent=d.name||t;updateTkBar(d);}socket.emit('subscribe',{tickers:[t]});}\nlet sT;const siEl=document.getElementById('tk-si'),drEl=document.getElementById('tk-dr');\nif(siEl){siEl.addEventListener('input',function(){clearTimeout(sT);const v=this.value.trim();if(!v){drEl.style.display='none';return;}sT=setTimeout(async()=>{const d=await api('/api/universe?q='+encodeURIComponent(v)+'&limit=10',60000);if(!d?.results?.length){drEl.style.display='none';return;}drEl.innerHTML=d.results.map(x=>`<div class=\"tdr\" onclick=\"navTo('${x.ticker}')\"><span class=\"tds\">${x.ticker}</span><span class=\"tdn\">${x.name||''}</span></div>`).join('');drEl.style.display='block';},200);});document.addEventListener('click',e=>{if(!e.target.closest('#tk-sw'))drEl.style.display='none';});}\nfunction navTo(t){if(!t)return;if(siEl){siEl.value='';drEl.style.display='none';}window.location.href='/chart?t='+encodeURIComponent(t);}\n// navToTape: always go to chart page with the ticker\nfunction navToTape(t){\n  if(!t)return;\n  // Route to appropriate page based on asset type\n  window.location.href='/chart?t='+encodeURIComponent(t);\n}\nwindow.TK=(new URLSearchParams(window.location.search).get('t')||'NVDA').toUpperCase();\nconst fmtB=v=>{if(v==null)return'—';const a=Math.abs(v),s=v<0?'-':'';if(a>=1e12)return s+'$'+(a/1e12).toFixed(2)+'T';if(a>=1e9)return s+'$'+(a/1e9).toFixed(2)+'B';if(a>=1e6)return s+'$'+(a/1e6).toFixed(1)+'M';return s+'$'+a.toLocaleString('en-US',{maximumFractionDigits:0});};\nconst fmtPx=v=>v==null?'—':'$'+Number(v).toFixed(2);\nconst p1=v=>v==null?'—':(v*100).toFixed(1)+'%';\nconst fn=(v,d=2)=>v==null?'—':Number(v).toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d});\n\nlet NM='stock';\nasync function loadNews(t){\n  // Redirect non-stocks to their page immediately\n  const el=document.querySelector('.pc');\n  const url=NM==='stock'?'/api/news/'+t:'/api/macro_news';\n  el.innerHTML='<div class=\"empty\"><svg class=\"spin-svg\" viewBox=\"0 0 44 20\" fill=\"none\"><polyline points=\"2,16 8,10 14,13 22,5 30,9 42,3\" stroke=\"#00e5a0\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\" stroke-dasharray=\"120\" stroke-dashoffset=\"120\"><animate attributeName=\"stroke-dashoffset\" from=\"120\" to=\"0\" dur=\"1.2s\" repeatCount=\"indefinite\"/></polyline></svg>A carregar…</div>';\n  const d=await api(url,300000);\n  const items=d?.items||[];\n  let html=`<div class=\"ntabs\">\n    <button class=\"ntab ${NM==='stock'?'on':''}\" onclick=\"setNM('stock','${t}')\"> ${t}</button>\n    <button class=\"ntab ${NM==='macro'?'on':''}\" onclick=\"setNM('macro','${t}')\"> Macro / Economia</button>\n  </div>`;\n  if(!items.length){el.innerHTML=html+'<div class=\"empty\">Sem notícias encontradas</div>';return;}\n  html+='<div class=\"ngrid\">';\n  items.forEach(n=>{html+=`<div class=\"ncard\"><div class=\"ntitle\"><a href=\"${n.link||'#'}\" target=\"_blank\" rel=\"noopener\">${n.title||''}</a></div><div class=\"nmeta\"><span class=\"sent sent-${n.sentiment||'neutral'}\">${(n.sentiment||'neutral').toUpperCase()}</span><span style=\"font-size:10px;color:var(--t3)\">Trust: ${n.trust_score||'?'}%</span><span class=\"ndt\">${(n.published||'').slice(0,16)}</span></div></div>`;});\n  html+='</div>';el.innerHTML=html;\n}\nfunction setNM(m,t){NM=m;loadNews(t);}\nfunction onTickerChange(t){loadNews(t);}\nloadNews(window.TK);\n\ninitTkBar(window.TK);\n\n// Fix nav links to carry current ticker\nfunction fixNavLinks(){\n  const t=window.TK;\n  if(!t)return;\n  document.querySelectorAll('#nav a.nl').forEach(a=>{\n    const href=a.getAttribute('href');\n    if(href&&href!=='/'&&!href.startsWith('javascript')){\n      const u=new URL(href,window.location.origin);\n      u.searchParams.set('t',t);\n      a.setAttribute('href',u.toString());\n    }\n  });\n}\nfixNavLinks();\n// Also update nav links whenever ticker changes\n// navTo already calls fixNavLinks()\n</script>\n</body>\n</html>",
    "auth.html": "<!DOCTYPE html>\n<html lang=\"pt\">\n<head>\n<meta charset=\"UTF-8\">\n<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\n<title>IST · Entrar</title>\n<link href=\"https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;700&family=Syne:wght@400;700;800;900&display=swap\" rel=\"stylesheet\">\n<style>\n*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}\n:root{\n  --bg:#040608;--bg2:#070c10;--bg3:#0a1018;\n  --b:#152030;--b2:#1c2d40;\n  --t:#c8d8e8;--t2:#5a7a90;--t3:#243040;\n  --gr:#00e5a0;--rd:#ff3d5a;\n  --fm:'JetBrains Mono',monospace;--fd:'Syne',sans-serif;\n}\nhtml,body{height:100%;background:var(--bg);color:var(--t);font-family:var(--fm);overflow:hidden}\n.bg-grid{position:fixed;inset:0;background-image:linear-gradient(var(--b) 1px,transparent 1px),linear-gradient(90deg,var(--b) 1px,transparent 1px);background-size:72px 72px;opacity:.3;pointer-events:none}\n.bg-glow{position:fixed;top:50%;left:50%;transform:translate(-50%,-50%);width:700px;height:500px;background:radial-gradient(ellipse,rgba(0,229,160,.05) 0%,transparent 65%);pointer-events:none;animation:breathe 5s ease-in-out infinite}\n@keyframes breathe{0%,100%{transform:translate(-50%,-50%) scale(1)}50%{transform:translate(-50%,-50%) scale(1.07)}}\n.scanline{position:fixed;left:0;right:0;height:1px;background:linear-gradient(90deg,transparent,rgba(0,229,160,.25),transparent);animation:scan 8s linear infinite;pointer-events:none}\n@keyframes scan{from{top:-1px}to{top:100vh}}\n.wrap{height:100vh;display:flex;flex-direction:column;align-items:center;justify-content:center;position:relative;z-index:1;padding:24px}\n.logo{display:flex;align-items:center;gap:10px;text-decoration:none;margin-bottom:36px}\n.logo-txt{font-family:var(--fd);font-size:20px;font-weight:900;color:var(--t);letter-spacing:.12em}\n.card{width:100%;max-width:400px;background:var(--bg2);border:1px solid var(--b);border-radius:10px;overflow:hidden;box-shadow:0 32px 80px rgba(0,0,0,.6)}\n.tab-row{display:flex;border-bottom:1px solid var(--b)}\n.tab{flex:1;padding:13px;text-align:center;font-size:11px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:var(--t2);cursor:pointer;border-bottom:2px solid transparent;transition:all .2s;font-family:var(--fd);background:transparent}\n.tab.on{color:var(--gr);border-bottom-color:var(--gr)}\n.fp{padding:24px}\n.field{margin-bottom:14px}\n.field label{display:block;font-size:9px;font-weight:700;letter-spacing:.15em;text-transform:uppercase;color:var(--t2);margin-bottom:6px;font-family:var(--fd)}\n.field input{width:100%;background:var(--bg3);border:1px solid var(--b2);border-radius:5px;color:var(--t);font-family:var(--fm);font-size:13px;padding:10px 12px;outline:none;transition:border-color .2s}\n.field input:focus{border-color:var(--gr)}\n.field input::placeholder{color:var(--t3)}\n.field input:disabled{opacity:.4;cursor:not-allowed}\n.btn{width:100%;padding:12px;font-family:var(--fd);font-size:11px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;border:none;border-radius:5px;cursor:pointer;transition:all .2s;position:relative;overflow:hidden}\n.btn-primary{background:var(--gr);color:var(--bg)}\n.btn-primary:hover:not(:disabled){transform:translateY(-1px);box-shadow:0 8px 28px rgba(0,229,160,.3)}\n.btn-primary:disabled{opacity:.5;cursor:not-allowed;transform:none}\n.btn-ghost{background:transparent;border:1px solid var(--b2);color:var(--t2);margin-top:8px}\n.btn-ghost:hover{border-color:var(--t3);color:var(--t)}\n.divider{display:flex;align-items:center;gap:10px;margin:14px 0}\n.divider span{font-size:9px;color:var(--t3);letter-spacing:.1em;font-family:var(--fd);white-space:nowrap}\n.divider::before,.divider::after{content:\"\";flex:1;height:1px;background:var(--b2)}\n.btn-google{width:100%;padding:10px;background:transparent;border:1px solid var(--b2);border-radius:5px;color:var(--t2);font-family:var(--fd);font-size:11px;font-weight:700;letter-spacing:.05em;cursor:pointer;transition:all .2s;display:flex;align-items:center;justify-content:center;gap:9px}\n.btn-google:hover{border-color:var(--t3);color:var(--t);background:rgba(255,255,255,.02)}\n.msg{font-size:11px;padding:9px 12px;border-radius:4px;margin-bottom:12px;font-family:var(--fd);display:none;line-height:1.5}\n.msg.err{background:rgba(255,61,90,.08);border:1px solid rgba(255,61,90,.2);color:var(--rd);display:block}\n.msg.ok{background:rgba(0,229,160,.07);border:1px solid rgba(0,229,160,.2);color:var(--gr);display:block}\n.pw-strength{height:2px;background:var(--b2);border-radius:1px;margin-top:5px;overflow:hidden}\n.pw-bar{height:100%;border-radius:1px;width:0%;transition:width .3s,background .3s}\n.footer-txt{text-align:center;font-size:10px;color:var(--t3);padding:0 24px 18px;line-height:1.6}\n.footer-txt a{color:var(--t2);text-decoration:none}\n.footer-txt a:hover{color:var(--gr)}\n.back{position:fixed;top:22px;left:22px;display:flex;align-items:center;gap:6px;font-size:10px;font-family:var(--fd);color:var(--t2);text-decoration:none;letter-spacing:.08em;transition:color .15s;z-index:10}\n.back:hover{color:var(--t)}\n\n/* Code input */\n.code-wrap{display:flex;gap:8px;justify-content:center;margin:6px 0 16px}\n.code-digit{width:44px;height:52px;background:var(--bg3);border:1px solid var(--b2);border-radius:6px;color:var(--t);font-family:var(--fm);font-size:22px;font-weight:700;text-align:center;outline:none;transition:border-color .2s;caret-color:var(--gr)}\n.code-digit:focus{border-color:var(--gr);background:var(--bg2)}\n.code-digit.filled{border-color:var(--b2);color:var(--gr)}\n\n/* Step indicator */\n.steps{display:flex;align-items:center;gap:6px;margin-bottom:20px;justify-content:center}\n.step-dot{width:6px;height:6px;border-radius:50%;background:var(--b2);transition:all .3s}\n.step-dot.on{background:var(--gr);width:20px;border-radius:3px}\n\n/* Panel transitions */\n.panel{display:none;animation:fadeIn .25s ease}\n.panel.on{display:block}\n@keyframes fadeIn{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}\n</style>\n</head>\n<body>\n<div class=\"bg-grid\"></div>\n<div class=\"bg-glow\"></div>\n<div class=\"scanline\"></div>\n\n<a href=\"/\" class=\"back\">\n  <svg width=\"14\" height=\"14\" viewBox=\"0 0 14 14\" fill=\"none\"><polyline points=\"9,2 4,7 9,12\" stroke=\"currentColor\" stroke-width=\"1.5\" stroke-linecap=\"round\" stroke-linejoin=\"round\"/></svg>\n  VOLTAR\n</a>\n\n<div class=\"wrap\">\n  <a href=\"/\" class=\"logo\">\n    <svg width=\"26\" height=\"26\" viewBox=\"0 0 64 64\" fill=\"none\">\n      <rect width=\"64\" height=\"64\" rx=\"10\" fill=\"rgba(0,229,160,0.15)\" stroke=\"rgba(0,229,160,0.4)\" stroke-width=\"2\"/>\n      <polyline points=\"8,44 20,28 30,36 42,18 56,22\" stroke=\"#00e5a0\" stroke-width=\"5\" fill=\"none\" stroke-linecap=\"round\" stroke-linejoin=\"round\"/>\n      <circle cx=\"42\" cy=\"18\" r=\"5\" fill=\"#00e5a0\"/>\n    </svg>\n    <span class=\"logo-txt\">IST</span>\n  </a>\n\n  <div class=\"card\">\n    <div class=\"tab-row\">\n      <button class=\"tab on\" onclick=\"switchTab('login')\">Entrar</button>\n      <button class=\"tab\"    onclick=\"switchTab('register')\">Criar Conta</button>\n    </div>\n\n    <!-- ── LOGIN ── -->\n    <div class=\"panel on fp\" id=\"p-login\">\n      <div class=\"msg\" id=\"login-msg\"></div>\n      <div class=\"field\"><label>Email</label>\n        <input type=\"email\" id=\"l-email\" placeholder=\"email@exemplo.com\" autocomplete=\"email\"/>\n      </div>\n      <div class=\"field\"><label>Password</label>\n        <input type=\"password\" id=\"l-pw\" placeholder=\"••••••••\" autocomplete=\"current-password\"\n          onkeydown=\"if(event.key==='Enter')doLogin()\"/>\n      </div>\n      <button class=\"btn btn-primary\" id=\"btn-login\" onclick=\"doLogin()\">ENTRAR NO TERMINAL &rarr;</button>\n      <div class=\"divider\"><span>OU</span></div>\n      <button class=\"btn-google\" onclick=\"doGoogle()\">\n        <svg width=\"15\" height=\"15\" viewBox=\"0 0 24 24\"><path d=\"M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z\" fill=\"#4285F4\"/><path d=\"M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z\" fill=\"#34A853\"/><path d=\"M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l3.66-2.84z\" fill=\"#FBBC05\"/><path d=\"M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z\" fill=\"#EA4335\"/></svg>\n        Continuar com Google\n      </button>\n    </div>\n\n    <!-- ── REGISTER STEP 1: Dados ── -->\n    <div class=\"panel fp\" id=\"p-reg1\">\n      <div class=\"steps\">\n        <div class=\"step-dot on\" id=\"sd1\"></div>\n        <div class=\"step-dot\" id=\"sd2\"></div>\n      </div>\n      <div class=\"msg\" id=\"reg-msg\"></div>\n      <div class=\"field\"><label>Nome</label>\n        <input type=\"text\" id=\"r-name\" placeholder=\"O teu nome\" autocomplete=\"name\"/>\n      </div>\n      <div class=\"field\"><label>Email</label>\n        <input type=\"email\" id=\"r-email\" placeholder=\"email@exemplo.com\" autocomplete=\"email\"/>\n      </div>\n      <div class=\"field\"><label>Password</label>\n        <input type=\"password\" id=\"r-pw\" placeholder=\"mín. 8 caracteres\" autocomplete=\"new-password\"\n          oninput=\"checkPw(this.value)\" onkeydown=\"if(event.key==='Enter')sendCode()\"/>\n        <div class=\"pw-strength\"><div class=\"pw-bar\" id=\"pw-bar\"></div></div>\n      </div>\n      <button class=\"btn btn-primary\" id=\"btn-sendcode\" onclick=\"sendCode()\">ENVIAR CÓDIGO DE VERIFICAÇÃO &rarr;</button>\n      <div class=\"divider\"><span>OU</span></div>\n      <button class=\"btn-google\" onclick=\"doGoogle()\">\n        <svg width=\"15\" height=\"15\" viewBox=\"0 0 24 24\"><path d=\"M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z\" fill=\"#4285F4\"/><path d=\"M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z\" fill=\"#34A853\"/><path d=\"M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l3.66-2.84z\" fill=\"#FBBC05\"/><path d=\"M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z\" fill=\"#EA4335\"/></svg>\n        Continuar com Google\n      </button>\n    </div>\n\n    <!-- ── REGISTER STEP 2: Código ── -->\n    <div class=\"panel fp\" id=\"p-reg2\">\n      <div class=\"steps\">\n        <div class=\"step-dot\" id=\"sd1b\"></div>\n        <div class=\"step-dot on\" id=\"sd2b\"></div>\n      </div>\n      <div class=\"msg\" id=\"code-msg\"></div>\n      <p style=\"font-size:11px;color:var(--t2);margin-bottom:18px;line-height:1.7;text-align:center\">\n        Enviámos um código de 6 dígitos para<br>\n        <strong id=\"code-email-display\" style=\"color:var(--t)\"></strong>\n      </p>\n      <label style=\"display:block;font-size:9px;font-weight:700;letter-spacing:.15em;text-transform:uppercase;color:var(--t2);margin-bottom:10px;text-align:center;font-family:var(--fd)\">Código de verificação</label>\n      <div class=\"code-wrap\">\n        <input class=\"code-digit\" id=\"cd0\" maxlength=\"1\" type=\"text\" inputmode=\"numeric\" pattern=\"[0-9]\" onkeydown=\"codeKey(event,0)\" oninput=\"codeInput(event,0)\"/>\n        <input class=\"code-digit\" id=\"cd1\" maxlength=\"1\" type=\"text\" inputmode=\"numeric\" pattern=\"[0-9]\" onkeydown=\"codeKey(event,1)\" oninput=\"codeInput(event,1)\"/>\n        <input class=\"code-digit\" id=\"cd2\" maxlength=\"1\" type=\"text\" inputmode=\"numeric\" pattern=\"[0-9]\" onkeydown=\"codeKey(event,2)\" oninput=\"codeInput(event,2)\"/>\n        <input class=\"code-digit\" id=\"cd3\" maxlength=\"1\" type=\"text\" inputmode=\"numeric\" pattern=\"[0-9]\" onkeydown=\"codeKey(event,3)\" oninput=\"codeInput(event,3)\"/>\n        <input class=\"code-digit\" id=\"cd4\" maxlength=\"1\" type=\"text\" inputmode=\"numeric\" pattern=\"[0-9]\" onkeydown=\"codeKey(event,4)\" oninput=\"codeInput(event,4)\"/>\n        <input class=\"code-digit\" id=\"cd5\" maxlength=\"1\" type=\"text\" inputmode=\"numeric\" pattern=\"[0-9]\" onkeydown=\"codeKey(event,5)\" oninput=\"codeInput(event,5)\"/>\n      </div>\n      <button class=\"btn btn-primary\" id=\"btn-verify\" onclick=\"verifyCode()\" disabled>VERIFICAR CÓDIGO &rarr;</button>\n      <button class=\"btn btn-ghost\" onclick=\"goBack()\">Alterar email / reenviar</button>\n      <p style=\"font-size:10px;color:var(--t3);text-align:center;margin-top:14px\">Expira em <span id=\"countdown\" style=\"color:var(--t2)\">10:00</span></p>\n    </div>\n\n    <div class=\"footer-txt\">Ao criar conta aceitas os <a href=\"#\">Termos</a> e a <a href=\"#\">Privacidade</a>.</div>\n  </div>\n</div>\n\n<script>\nlet _tab='login', _regEmail='', _countdownTimer=null;\n\nfunction switchTab(t){\n  _tab=t;\n  document.querySelectorAll('.tab').forEach((el,i)=>el.classList.toggle('on',['login','register'][i]===t));\n  showPanel(t==='login'?'p-login':'p-reg1');\n}\n\nfunction showPanel(id){\n  document.querySelectorAll('.panel').forEach(p=>p.classList.remove('on'));\n  document.getElementById(id).classList.add('on');\n}\n\nfunction msg(id,type,text){\n  const el=document.getElementById(id);\n  el.className='msg '+(type||'');\n  el.textContent=text||'';\n}\n\nfunction setBusy(btnId,busy,label){\n  const btn=document.getElementById(btnId);\n  btn.disabled=busy;btn.style.opacity=busy?'.6':'1';\n  if(label)btn.textContent=label;\n}\n\nfunction checkPw(v){\n  const bar=document.getElementById('pw-bar');\n  if(!bar)return;\n  const s=v.length<6?1:v.length<8?2:(v.match(/[A-Z]/)&&v.match(/[0-9]/))?4:3;\n  bar.style.width=['0%','30%','55%','80%','100%'][s];\n  bar.style.background=['','#ff3d5a','#f5b942','#0088ee','#00e5a0'][s];\n}\n\n// ── CODE INPUT BEHAVIOUR ──\nfunction codeInput(e,i){\n  const v=e.target.value.replace(/\\D/g,'');\n  e.target.value=v.slice(0,1);\n  e.target.classList.toggle('filled',v.length>0);\n  if(v.length===1&&i<5)document.getElementById('cd'+(i+1)).focus();\n  checkCodeComplete();\n}\n\nfunction codeKey(e,i){\n  if(e.key==='Backspace'&&!e.target.value&&i>0){\n    document.getElementById('cd'+(i-1)).focus();\n  }\n  if(e.key==='Enter')verifyCode();\n  // Handle paste\n  if(e.key==='v'&&(e.ctrlKey||e.metaKey)){\n    setTimeout(()=>{\n      const pasted=document.getElementById('cd0').value+document.getElementById('cd1').value+\n        document.getElementById('cd2').value+document.getElementById('cd3').value+\n        document.getElementById('cd4').value+document.getElementById('cd5').value;\n      if(pasted.length===6)checkCodeComplete();\n    },50);\n  }\n}\n\n// Handle paste on any digit\ndocument.addEventListener('paste',e=>{\n  const active=document.activeElement;\n  if(!active||!active.classList.contains('code-digit'))return;\n  const text=(e.clipboardData||window.clipboardData).getData('text').replace(/\\D/g,'');\n  if(text.length===6){\n    e.preventDefault();\n    for(let i=0;i<6;i++){const d=document.getElementById('cd'+i);d.value=text[i];d.classList.add('filled');}\n    document.getElementById('cd5').focus();\n    checkCodeComplete();\n  }\n});\n\nfunction checkCodeComplete(){\n  const code=getCode();\n  document.getElementById('btn-verify').disabled=code.length!==6;\n}\n\nfunction getCode(){\n  return[0,1,2,3,4,5].map(i=>document.getElementById('cd'+i).value).join('');\n}\n\nfunction startCountdown(secs){\n  clearInterval(_countdownTimer);\n  let s=secs;\n  const el=document.getElementById('countdown');\n  _countdownTimer=setInterval(()=>{\n    s--;if(s<=0){clearInterval(_countdownTimer);if(el)el.textContent='Expirado';}\n    else if(el)el.textContent=Math.floor(s/60)+':'+(s%60).toString().padStart(2,'0');\n  },1000);\n}\n\n// ── ACTIONS ──\nasync function sendCode(){\n  const name=document.getElementById('r-name').value.trim();\n  const email=document.getElementById('r-email').value.trim();\n  const pw=document.getElementById('r-pw').value;\n  if(!name||!email||!pw){msg('reg-msg','err','Preenche todos os campos.');return;}\n  if(!email.includes('@')){msg('reg-msg','err','Email inválido.');return;}\n  if(pw.length<8){msg('reg-msg','err','Password mínimo 8 caracteres.');return;}\n  setBusy('btn-sendcode',true,'A enviar...');\n  msg('reg-msg','','');\n  try{\n    const r=await fetch('/api/auth/send-code',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name,email,password:pw})});\n    const d=await r.json();\n    if(d.ok){\n      _regEmail=email;\n      document.getElementById('code-email-display').textContent=email;\n      for(let i=0;i<6;i++){const cd=document.getElementById('cd'+i);cd.value='';cd.classList.remove('filled');}\n      document.getElementById('btn-verify').disabled=true;\n      msg('code-msg','','');\n      showPanel('p-reg2');\n      setTimeout(()=>document.getElementById('cd0').focus(),100);\n      startCountdown(600);\n      if(!d.email_sent)msg('code-msg','ok','Código gerado (sem chave Resend: ver consola do servidor).');\n    }else{\n      msg('reg-msg','err',d.error||'Erro ao enviar código.');\n    }\n  }catch(e){msg('reg-msg','err','Erro de ligação.');}\n  setBusy('btn-sendcode',false,'ENVIAR CÓDIGO DE VERIFICAÇÃO →');\n}\n\nasync function verifyCode(){\n  const code=getCode();\n  if(code.length!==6){msg('code-msg','err','Código incompleto.');return;}\n  setBusy('btn-verify',true,'A verificar...');\n  msg('code-msg','','');\n  try{\n    const r=await fetch('/api/auth/verify-code',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({email:_regEmail,code})});\n    const d=await r.json();\n    if(d.ok){\n      clearInterval(_countdownTimer);\n      msg('code-msg','ok','Conta criada! A entrar...');\n      setTimeout(()=>window.location.href='/terminal',900);\n    }else{\n      msg('code-msg','err',d.error||'Código incorrecto.');\n      setBusy('btn-verify',false,'VERIFICAR CÓDIGO →');\n    }\n  }catch(e){msg('code-msg','err','Erro de ligação.');setBusy('btn-verify',false,'VERIFICAR CÓDIGO →');}\n}\n\nasync function doLogin(){\n  const email=document.getElementById('l-email').value.trim();\n  const pw=document.getElementById('l-pw').value;\n  if(!email||!pw){msg('login-msg','err','Preenche todos os campos.');return;}\n  setBusy('btn-login',true,'A verificar...');\n  msg('login-msg','','');\n  try{\n    const r=await fetch('/api/auth/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({email,password:pw})});\n    const d=await r.json();\n    if(d.ok&&d.step==='2fa'){\n      // Show 2FA panel for login\n      _regEmail=email;\n      document.getElementById('code-email-display').textContent=email;\n      for(let i=0;i<6;i++){const cd=document.getElementById('cd'+i);cd.value='';cd.classList.remove('filled');}\n      document.getElementById('btn-verify').disabled=true;\n      document.getElementById('btn-verify').onclick=doLogin2fa;\n      msg('code-msg','ok','Código enviado para o teu email.');\n      showPanel('p-reg2');\n      setTimeout(()=>document.getElementById('cd0').focus(),100);\n      startCountdown(600);\n    }else if(d.ok){\n      window.location.href='/terminal';\n    }else{\n      msg('login-msg','err',d.error||'Credenciais incorrectas.');\n      setBusy('btn-login',false,'ENTRAR NO TERMINAL →');\n    }\n  }catch(e){msg('login-msg','err','Erro de ligação.');setBusy('btn-login',false,'ENTRAR NO TERMINAL →');}\n}\n\nasync function doLogin2fa(){\n  const code=getCode();\n  if(code.length!==6){msg('code-msg','err','Código incompleto.');return;}\n  setBusy('btn-verify',true,'A verificar...');\n  msg('code-msg','');\n  try{\n    const r=await fetch('/api/auth/login-2fa',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({email:_regEmail,code})});\n    const d=await r.json();\n    if(d.ok){clearInterval(_countdownTimer);window.location.href='/terminal';}\n    else{msg('code-msg','err',d.error||'Código incorrecto.');setBusy('btn-verify',false,'VERIFICAR CÓDIGO →');}\n  }catch(e){msg('code-msg','err','Erro de ligação.');setBusy('btn-verify',false,'VERIFICAR CÓDIGO →');}\n}\n\nfunction goBack(){\n  clearInterval(_countdownTimer);\n  showPanel('p-reg1');\n  msg('reg-msg','','');\n}\n\nfunction doGoogle(){window.location.href='/api/auth/google';}\n</script>\n</body>\n</html>\n"
}

def login_required(f):
    """Redirect to /auth if not logged in."""
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('user'):
            return _flask_redirect('/auth')
        return f(*args, **kwargs)
    return decorated


app = Flask(__name__)

# render_template uses embedded _TEMPLATES dict (defined above)
import json as _json
def render_template(name, **ctx):
    html = _TEMPLATES.get(name)
    if html is None:
        return Response(_json.dumps({"error": name}), status=404, mimetype="application/json")
    # Encode safely - strip any surrogate chars that would break utf-8
    safe = html.encode('utf-8', 'replace').decode('utf-8')
    return Response(safe.encode('utf-8','replace'), mimetype="text/html; charset=utf-8")

app.secret_key = os.getenv("SECRET_KEY", "ist-railway-secret-2024-xk29")
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

_cache      = {}
_cache_lock = threading.RLock()

TTL_PRICE      = 5
TTL_INFO       = 3600
TTL_STATEMENTS = 3600
TTL_SEC        = 1800
TTL_UNIVERSE   = 86400
TTL_HISTORY    = 300
TTL_NEWS       = 900
TTL_MACRO      = 21600

SEC_HEADERS = {"User-Agent": SEC_USER_AGENT, "Accept-Encoding": "gzip, deflate"}

CORE_WATCHLIST = list(dict.fromkeys([
    "AAPL","MSFT","NVDA","GOOGL","META","AMZN","TSLA","AVGO","AMD","INTC",
    "CRM","ORCL","ADBE","QCOM","TXN","MU","AMAT","LRCX","KLAC","MRVL",
    "LLY","UNH","JNJ","ABBV","MRK","PFE","TMO","ABT","DHR","BMY",
    "JPM","BAC","GS","MS","WFC","BLK","SCHW","AXP","V","MA",
    "CAT","DE","HON","RTX","LMT","GE","BA","UPS","FDX","EMR",
    "XOM","CVX","COP","EOG","SLB","PSX","VLO","MPC","OXY","HAL",
    "WMT","COST","TGT","HD","LOW","MCD","SBUX","NKE","TJX","BURL",
    "F","GM","RIVN","UBER","ABNB","BKNG","MAR","NFLX","DIS","SPOT",
    "PLTR","SNOW","DDOG","ZS","CRWD","NET","OKTA","MDB","SMCI","DELL",
    "APP","HIMS","RDDT","HOOD","COIN","MSTR","RIOT","MARA","ARM","ASML",
    "TSM","BABA","NIO","PDD","JD","QQQ","SPY","IWM","DIA","VTI",
]))

MARKET_TAPE = {
    "SPY":"SPY","QQQ":"QQQ","^GSPC":"S&P 500","^IXIC":"NASDAQ",
    "^VIX":"VIX","CL=F":"WTI","BZ=F":"BRENT","GC=F":"GOLD",
    "SI=F":"SILVER","DX-Y.NYB":"DXY","^TNX":"US10Y","BTC-USD":"BTC",
}


# Hardcoded names for 200+ tickers — always works, no API needed
TICKER_NAMES_STATIC = {
    "AAPL":"Apple","MSFT":"Microsoft","NVDA":"NVIDIA","GOOGL":"Alphabet","GOOG":"Alphabet",
    "META":"Meta Platforms","AMZN":"Amazon","TSLA":"Tesla","AVGO":"Broadcom","AMD":"AMD",
    "INTC":"Intel","QCOM":"Qualcomm","TXN":"Texas Instruments","MU":"Micron",
    "ORCL":"Oracle","CRM":"Salesforce","ADBE":"Adobe","NOW":"ServiceNow",
    "PLTR":"Palantir","SNOW":"Snowflake","DDOG":"Datadog","CRWD":"CrowdStrike",
    "NET":"Cloudflare","ZS":"Zscaler","OKTA":"Okta","MDB":"MongoDB",
    "NFLX":"Netflix","DIS":"Disney","SPOT":"Spotify","ROKU":"Roku",
    "JPM":"JPMorgan Chase","BAC":"Bank of America","GS":"Goldman Sachs",
    "MS":"Morgan Stanley","WFC":"Wells Fargo","C":"Citigroup",
    "V":"Visa","MA":"Mastercard","AXP":"American Express","PYPL":"PayPal",
    "LLY":"Eli Lilly","JNJ":"Johnson & Johnson","UNH":"UnitedHealth",
    "ABBV":"AbbVie","MRK":"Merck","PFE":"Pfizer","TMO":"Thermo Fisher",
    "XOM":"ExxonMobil","CVX":"Chevron","COP":"ConocoPhillips",
    "HD":"Home Depot","WMT":"Walmart","COST":"Costco","TGT":"Target",
    "MCD":"McDonald\'s","SBUX":"Starbucks","NKE":"Nike","AMGN":"Amgen",
    "CAT":"Caterpillar","DE":"Deere & Co","HON":"Honeywell","BA":"Boeing",
    "RTX":"Raytheon","LMT":"Lockheed Martin","GE":"GE Aerospace",
    "BRK-B":"Berkshire Hathaway B","BRK-A":"Berkshire Hathaway A",
    "SPY":"S&P 500 ETF","QQQ":"NASDAQ 100 ETF","IWM":"Russell 2000 ETF",
    "GLD":"Gold ETF","TLT":"20Y Treasury ETF","TQQQ":"3x NASDAQ Bull",
    "COIN":"Coinbase","HOOD":"Robinhood","MSTR":"MicroStrategy",
    "SMCI":"Super Micro","DELL":"Dell","HPQ":"HP","IBM":"IBM",
    "UBER":"Uber","LYFT":"Lyft","ABNB":"Airbnb","BKNG":"Booking Holdings",
    "APP":"AppLovin","HIMS":"Hims & Hers","RDDT":"Reddit","ARM":"ARM Holdings",
    "TSM":"Taiwan Semiconductor","ASML":"ASML Holding","BABA":"Alibaba",
    "^GSPC":"S&P 500","^IXIC":"NASDAQ","^DJI":"Dow Jones","^VIX":"VIX",
    "^RUT":"Russell 2000","^TNX":"US 10Y Treasury","^TYX":"US 30Y Treasury",
    "DX-Y.NYB":"US Dollar Index",
    "GC=F":"Gold Futures","SI=F":"Silver Futures","CL=F":"WTI Crude Oil",
    "BZ=F":"Brent Crude Oil","NG=F":"Natural Gas","HG=F":"Copper",
    "BTC-USD":"Bitcoin","ETH-USD":"Ethereum","SOL-USD":"Solana",
    "BNB-USD":"BNB","XRP-USD":"XRP","ADA-USD":"Cardano",
    "DOGE-USD":"Dogecoin","AVAX-USD":"Avalanche",
}

# Display names shown in tkbar (overrides yfinance name)
TICKER_DISPLAY = {
    "GC=F":"Gold","SI=F":"Silver","CL=F":"WTI Crude Oil","BZ=F":"Brent Crude Oil",
    "NG=F":"Natural Gas","HG=F":"Copper","ZC=F":"Corn","ZW=F":"Wheat",
    "ZS=F":"Soybeans","PL=F":"Platinum","PA=F":"Palladium","HO=F":"Heating Oil",
    "^GSPC":"S&P 500","^IXIC":"NASDAQ Composite","^DJI":"Dow Jones","^VIX":"VIX",
    "^RUT":"Russell 2000","^TNX":"US 10Y Yield","^TYX":"US 30Y Yield",
    "DX-Y.NYB":"US Dollar Index","BTC-USD":"Bitcoin","ETH-USD":"Ethereum",
    "SOL-USD":"Solana","BNB-USD":"BNB","XRP-USD":"XRP","ADA-USD":"Cardano",
    "DOGE-USD":"Dogecoin","AVAX-USD":"Avalanche","DOT-USD":"Polkadot",
    "LINK-USD":"Chainlink","MATIC-USD":"Polygon","LTC-USD":"Litecoin",
}

TICKER_NAMES = {
    "BTC-USD":"Bitcoin","ETH-USD":"Ethereum","SOL-USD":"Solana","BNB-USD":"BNB",
    "XRP-USD":"XRP","ADA-USD":"Cardano","DOGE-USD":"Dogecoin","AVAX-USD":"Avalanche",
    "DOT-USD":"Polkadot","LINK-USD":"Chainlink","MATIC-USD":"Polygon","LTC-USD":"Litecoin",
    "BCH-USD":"Bitcoin Cash","UNI-USD":"Uniswap","ATOM-USD":"Cosmos","NEAR-USD":"NEAR Protocol",
    "GC=F":"Gold Futures","SI=F":"Silver Futures","CL=F":"WTI Crude Oil",
    "BZ=F":"Brent Crude Oil","NG=F":"Natural Gas","HG=F":"Copper Futures",
    "ZC=F":"Corn Futures","ZW=F":"Wheat Futures","ZS=F":"Soybean Futures","PL=F":"Platinum",
    "^GSPC":"S&P 500 Index","^IXIC":"NASDAQ Composite","^GSPC":"S&P 500 Index","^DJI":"Dow Jones",
    "^VIX":"CBOE VIX","^RUT":"Russell 2000","^TNX":"US 10Y Treasury",
    "^TYX":"US 30Y Treasury","DX-Y.NYB":"US Dollar Index",
    "BRK-B":"Berkshire Hathaway B","BRK-A":"Berkshire Hathaway A",
    "TSM":"Taiwan Semiconductor","ASML":"ASML Holding",
}
CORE_LOOKUP = {}
for _t in CORE_WATCHLIST:
    CORE_LOOKUP[_t] = {"ticker":_t, "name":TICKER_NAMES.get(_t,_t), "exchange":"CORE"}

# ── helpers ──────────────────────────────────────────────────
def cache_get(key, ttl):
    with _cache_lock:
        e = _cache.get(key)
        if e and time.time() - e["ts"] < ttl:
            return e["data"]
    return None

def cache_set(key, data):
    with _cache_lock:
        _cache[key] = {"ts": time.time(), "data": data}

def sf(v, d=None):
    try:
        if v is None: return None
        v = float(v)
        return round(v, d) if d is not None else v
    except: return None

def si(v):
    try: return int(v) if v is not None else None
    except: return None

def fast_value(fi, key):
    try:
        if hasattr(fi, key): return getattr(fi, key)
    except: pass
    try: return fi.get(key)
    except: return None

def is_special(t):
    return any(x in t for x in ["^","=","-USD",".","/"])

def period_start(period):
    days = {"5d":10,"1mo":35,"3mo":100,"6mo":190,"1y":370,"2y":740,"5y":1850,"10y":3700}.get(period,370)
    return (datetime.utcnow().date() - timedelta(days=days)).isoformat()

def filter_period(pts, period):
    s = period_start(period)
    return [p for p in pts if str(p.get("date",""))[:10] >= s]

def lookup_name(ticker):
    if ticker in CORE_LOOKUP: return CORE_LOOKUP[ticker]
    for r in (cache_get("universe", TTL_UNIVERSE) or []):
        if r.get("ticker") == ticker: return r
    return {"ticker":ticker,"name":ticker,"exchange":""}

# ── Universe ──────────────────────────────────────────────────
def parse_sym_file(text, exchange):
    out = []
    for row in csv.DictReader(io.StringIO(text), delimiter="|"):
        t = (row.get("Symbol") or row.get("ACT Symbol") or "").strip().upper()
        n = (row.get("Security Name") or row.get("Company Name") or "").strip()
        if not t or t=="FILE CREATION TIME" or (row.get("Test Issue") or "N").strip()=="Y" or "$" in t: continue
        out.append({"ticker":t.replace(".","-"),"name":n,"exchange":exchange})
    return out

def load_universe():
    c = cache_get("universe", TTL_UNIVERSE)
    if c: return c
    # Load from pre-built universe.json first (instant)
    seed = []
    try:
        upath = os.path.join(os.path.dirname(__file__), "data", "universe.json")
        with open(upath, encoding="utf-8") as uf:
            seed = json.load(uf)
    except: pass
    # Merge with CORE_LOOKUP
    existing = {x["ticker"] for x in seed}
    for t, info in CORE_LOOKUP.items():
        if t not in existing:
            seed.append(info)
    cache_set("universe", seed)
    # Try to download full list in background
    def _download():
        rows = []
        # Strategy 1: FMP full stock list (30,000+ tickers with names)
        try:
            if FMP_API_KEY:
                r = requests.get(
                    f"https://financialmodelingprep.com/api/v3/stock/list?apikey={FMP_API_KEY}",
                    headers={"User-Agent":"IST/1.0"}, timeout=20)
                if r.ok:
                    for s in r.json():
                        sym = str(s.get("symbol","")).strip().upper()
                        name = str(s.get("name","")).strip()
                        exch = str(s.get("exchangeShortName","")).strip()
                        # Filter: only common stocks on major exchanges
                        if sym and name and exch in ("NASDAQ","NYSE","AMEX","NYSE ARCA","BATS"):
                            # Skip ETFs/funds in this pass (can add separately)
                            typ = str(s.get("type","")).lower()
                            if typ in ("etf","fund"): exch = "ETF"
                            rows.append({"ticker":sym,"name":name,"exchange":exch})
        except: pass
        # Strategy 2: NASDAQ listed files as fallback
        if not rows:
            try:
                for url,ex in [("https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt","NASDAQ"),
                               ("https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt","NYSE/AMEX")]:
                    r = requests.get(url, timeout=15, headers={"User-Agent":"IST/1.0 contact@ist.local"})
                    r.raise_for_status()
                    rows.extend(parse_sym_file(r.text, ex))
            except: pass
        if rows:
            base = cache_get("universe", TTL_UNIVERSE) or seed
            etks = {x["ticker"] for x in base}
            for x in rows:
                if x["ticker"] not in etks:
                    base.append(x); etks.add(x["ticker"])
            base.sort(key=lambda x: x.get("ticker",""))
            cache_set("universe", base)
            try:
                upath = os.path.join(os.path.dirname(__file__), "data", "universe.json")
                with open(upath,"w",encoding="utf-8") as uf: json.dump(base, uf, separators=(",",":"))
            except: pass
    threading.Thread(target=_download, daemon=True).start()
    return seed

# ── Price ─────────────────────────────────────────────────────
def fetch_price_finnhub(ticker):
    return None  # Finnhub removed — using FMP only

def fetch_price(ticker):
    ticker = ticker.upper().strip()
    c = cache_get(f"price:{ticker}", TTL_PRICE)
    if c: return c
    # Try FMP first for all tickers
    if FMP_API_KEY and not any(x in ticker for x in ['=F','^','-USD','DX-','TNX']):
        fmp = _fmp_single_quote(ticker)
        if fmp: fmp=_clean(fmp); cache_set(f"price:{ticker}", fmp); return fmp
    result = {"ticker":ticker,"price":None,"change_pct":None,"error":None}
    # Try v7 quote API + FMP in parallel for robustness
    try:
        def _yf_v7():
            for host in ["query1.finance.yahoo.com","query2.finance.yahoo.com"]:
                try:
                    url = f"https://{host}/v7/finance/quote?symbols={ticker}&fields=regularMarketPrice,regularMarketPreviousClose,regularMarketVolume,regularMarketChangePercent,regularMarketChange"
                    r = requests.get(url, headers={"User-Agent":"Mozilla/5.0","Accept":"application/json"}, timeout=3)
                    if r.ok: return r
                except: continue
            return None
        r = _yf_v7()
        if r and r.ok:
            quotes = r.json().get("quoteResponse",{}).get("result",[])
            if quotes:
                q = quotes[0]
                price = sf(q.get("regularMarketPrice"),2)
                prev  = sf(q.get("regularMarketPreviousClose"),2)
                chg   = sf(q.get("regularMarketChange"),2)
                chgp  = sf(q.get("regularMarketChangePercent"),2)
                vol   = si(q.get("regularMarketVolume"))
                if price:
                    if chg is None and price and prev:
                        chg = round(price-prev,2); chgp = round(chg/prev*100,2) if prev else None
                    result = {"ticker":ticker,"label":TICKER_DISPLAY.get(ticker,MARKET_TAPE.get(ticker,ticker)),
                              "price":price,"prev_close":prev,"change":chg,"change_pct":chgp,"volume":vol,
                              "provider":"yf_quote","ts":datetime.now().strftime("%H:%M:%S"),"error":None}
                    cache_set(f"price:{ticker}", result)
                    return result
    except: pass
    # Fallback: yfinance fast_info
    try:
        t = yf.Ticker(ticker); fi = t.fast_info
        price = sf(fast_value(fi,"last_price"),2)
        prev  = sf(fast_value(fi,"previous_close"),2)
        vol   = si(fast_value(fi,"last_volume"))
        if price is None:
            h = t.history(period="2d",interval="1d",auto_adjust=False)
            if h is not None and not h.empty:
                price = sf(h["Close"].iloc[-1],2)
                prev  = sf(h["Close"].iloc[-2],2) if len(h)>1 else prev
                vol   = vol or si(h["Volume"].iloc[-1])
        chg  = round(price-prev,2) if price and prev else None
        chgp = round(chg/prev*100,2) if chg and prev else None
        result = {"ticker":ticker,"label":TICKER_DISPLAY.get(ticker,MARKET_TAPE.get(ticker,ticker)),"price":price,
                  "prev_close":prev,"change":chg,"change_pct":chgp,"volume":vol,
                  "provider":"yfinance","ts":datetime.now().strftime("%H:%M:%S"),"error":None}
        cache_set(f"price:{ticker}", result)
    except Exception as e: result["error"] = str(e)
    return result


def _fmp_batch_quote(tickers):
    """FMP batch quotes — fallback se Yahoo v7 falhar"""
    if not FMP_API_KEY or not tickers: return {}
    out = {}
    try:
        syms = ",".join(tickers[:50])
        r = requests.get(
            f"https://financialmodelingprep.com/api/v3/quote/{syms}?apikey={FMP_API_KEY}",
            headers={"User-Agent":"IST/1.0"}, timeout=5)
        if not r.ok: return {}
        for q in r.json():
            t = q.get("symbol","")
            if not t: continue
            price = sf(q.get("price"),2)
            prev  = sf(q.get("previousClose"),2)
            chg   = sf(q.get("change"),2)
            chgp  = sf(q.get("changesPercentage"),2)
            if price is None: continue
            d = {"ticker":t,"label":MARKET_TAPE.get(t,TICKER_DISPLAY.get(t,t)),
                 "price":price,"prev_close":prev,"change":chg,"change_pct":chgp,
                 "volume":si(q.get("volume")),"provider":"fmp_quote",
                 "ts":datetime.now().strftime("%H:%M:%S"),"error":None}
            out[t] = d
            cache_set(f"price:{t}", d)
    except Exception: pass
    return out

def _fmp_profile(ticker):
    """FMP company profile"""
    if not FMP_API_KEY: return {}
    try:
        r = requests.get(
            f"https://financialmodelingprep.com/api/v3/profile/{ticker}?apikey={FMP_API_KEY}",
            headers={"User-Agent":"IST/1.0"}, timeout=5)
        if not r.ok: return {}
        data = r.json(); p = data[0] if data else {}
        rng = str(p.get("range","")).split("-")
        return {
            "name":p.get("companyName"),"sector":p.get("sector"),
            "industry":p.get("industry"),"exchange":p.get("exchangeShortName"),
            "country":p.get("country"),"website":p.get("website"),
            "summary":p.get("description"),"market_cap":sf(p.get("mktCap")),
            "beta":sf(p.get("beta"),2),"pe_trailing":sf(p.get("pe"),2),
            "eps_trailing":sf(p.get("eps"),2),"avg_volume":si(p.get("volAvg")),
            "52w_high":sf(rng[-1]) if len(rng)>1 else None,
            "52w_low":sf(rng[0]) if rng else None,
        }
    except Exception: return {}

def _fmp_historical(ticker, period="1y"):
    """FMP historical OHLCV — fallback para charts"""
    if not FMP_API_KEY: return []
    from datetime import datetime as _dt, timedelta as _td
    days = {"5d":10,"1mo":35,"3mo":100,"1y":370,"2y":740,"5y":1850}.get(period,370)
    from_date = (_dt.now()-_td(days=days)).strftime("%Y-%m-%d")
    try:
        r = requests.get(
            f"https://financialmodelingprep.com/api/v3/historical-price-full/{ticker}"
            f"?from={from_date}&apikey={FMP_API_KEY}",
            headers={"User-Agent":"IST/1.0"}, timeout=8)
        if not r.ok: return []
        pts = []
        for h in r.json().get("historical",[]):
            close = sf(h.get("close"),2)
            if close is None: continue
            pts.append({"date":str(h.get("date",""))[:10],
                        "open":sf(h.get("open"),2),"high":sf(h.get("high"),2),
                        "low":sf(h.get("low"),2),"close":close,"value":close,
                        "volume":si(h.get("volume"))})
        return list(reversed(pts))
    except Exception: return []


def _batch_download(tickers):
    """FMP batch quote — primary source, falls back to Yahoo for indices/crypto"""
    out = {}
    if not tickers: return out
    # Use FMP for regular stocks
    stocks = [t for t in tickers if not any(x in t for x in ['=F','^','-USD','DX-','TNX'])]
    specials = [t for t in tickers if t not in stocks]
    if stocks and FMP_API_KEY:
        fmp_result = _fmp_batch_quotes(stocks)
        out.update(fmp_result)
    # Yahoo only for indices, futures, crypto (FMP has different format)
    still_missing = [t for t in tickers if out.get(t) is None]
    if not still_missing: return out
    tickers = still_missing
    """Yahoo Finance v7/finance/quote — uma request para todos os tickers, ~200ms"""
    out = {}
    if not tickers: return out
    # Chunk into batches of 80 (Yahoo limit per request)
    def _chunk(lst, n):
        for i in range(0, len(lst), n): yield lst[i:i+n]
    for chunk in _chunk(tickers, 80):
        try:
            symbols = ",".join(chunk)
            # Try query1 first, then query2 if it fails (Yahoo has 2 servers)
            for yf_host in ["query1.finance.yahoo.com", "query2.finance.yahoo.com"]:
                try:
                    url = f"https://{yf_host}/v7/finance/quote?symbols={symbols}&fields=regularMarketPrice,regularMarketPreviousClose,regularMarketVolume,regularMarketChangePercent,regularMarketChange"
                    headers = {
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                        "Accept": "application/json",
                    }
                    r = requests.get(url, headers=headers, timeout=4)
                    if r.ok: break
                except Exception: continue
            if not r.ok: raise Exception(f"HTTP {r.status_code}")
            data = r.json()
            quotes = data.get("quoteResponse", {}).get("result", [])
            for q in quotes:
                t = q.get("symbol", "")
                if not t: continue
                price = sf(q.get("regularMarketPrice"), 2)
                prev  = sf(q.get("regularMarketPreviousClose"), 2)
                chg   = sf(q.get("regularMarketChange"), 2)
                chgp  = sf(q.get("regularMarketChangePercent"), 2)
                vol   = si(q.get("regularMarketVolume"))
                if price is None: continue
                if chg is None and price and prev:
                    chg  = round(price - prev, 2)
                    chgp = round(chg / prev * 100, 2) if prev else None
                d = {"ticker": t,
                     "label": MARKET_TAPE.get(t, TICKER_DISPLAY.get(t, t)),
                     "price": price, "prev_close": prev,
                     "change": chg, "change_pct": chgp, "volume": vol,
                     "provider": "yf_quote", "ts": datetime.now().strftime("%H:%M:%S"),
                     "error": None}
                out[t] = d
                cache_set(f"price:{t}", d)
            # FMP gap-fill: fetch any tickers Yahoo missed
            missed = [t for t in chunk if t not in out and not is_special(t)]
            if missed and FMP_API_KEY:
                fmp_q = _fmp_batch_quote(missed)
                for t, d in fmp_q.items():
                    out[t] = d
        except Exception:
            # Full fallback: FMP batch quotes
            fmp_q = _fmp_batch_quote(chunk)
            for t, d in fmp_q.items(): out[t] = d
            # Fallback 2: yf.download
            try:
                raw = yf.download(" ".join(chunk), period="5d", interval="1d",
                                  group_by="ticker", threads=True, progress=False, auto_adjust=False)
                for t in chunk:
                    try:
                        df = raw[t] if len(chunk)>1 else raw
                        if hasattr(df.columns,"nlevels") and df.columns.nlevels>1:
                            df.columns = df.columns.get_level_values(-1)
                        df = df.dropna(how="all")
                        if df.empty: continue
                        last = df.iloc[-1]; prev_r = df.iloc[-2] if len(df)>1 else last
                        price = sf(last.get("Close"),2); prev = sf(prev_r.get("Close"),2)
                        chg   = round(price-prev,2) if price and prev else None
                        chgp  = round(chg/prev*100,2) if chg and prev else None
                        d = {"ticker":t,"label":MARKET_TAPE.get(t,t),"price":price,
                             "prev_close":prev,"change":chg,"change_pct":chgp,
                             "volume":si(last.get("Volume")),"provider":"yf_batch_fallback",
                             "ts":datetime.now().strftime("%H:%M:%S"),"error":None}
                        out[t] = d; cache_set(f"price:{t}", d)
                    except: pass
            except: pass
    return out

def fetch_prices_batch(tickers):
    tickers = [t.upper().strip() for t in tickers if t.strip()]
    result  = {t: cache_get(f"price:{t}", TTL_PRICE) for t in tickers}
    missing = [t for t in tickers if result[t] is None]
    if not missing:
        return [result.get(t) or {"ticker":t,"price":None,"change_pct":None,"error":"unavailable"} for t in tickers]
    # Try v7 API for all missing at once (handles stocks + crypto + futures + indices)
    batch = _batch_download(missing)
    result.update(batch)
    # Anything still missing: parallel individual fetches
    still_missing = [t for t in missing if result.get(t) is None]
    if still_missing:
        with ThreadPoolExecutor(max_workers=min(16, len(still_missing))) as ex:
            for fut in as_completed({ex.submit(fetch_price,t):t for t in still_missing}, timeout=8):
                try:
                    d = fut.result()
                    if d and d.get("ticker"): result[d["ticker"]] = d
                except: pass
    return [result.get(t) or {"ticker":t,"price":None,"change_pct":None,"error":"unavailable"} for t in tickers]

# ── Info / Fundamentals ───────────────────────────────────────
def fetch_info(ticker):
    ticker = ticker.upper().strip()
    c = cache_get(f"info:{ticker}", TTL_INFO)
    if c: return c
    # Try FMP profile first (fast ~300ms)
    if FMP_API_KEY:
        fmp_data = _fmp_profile(ticker)
        if fmp_data.get("name"):
            cache_set(f"info:{ticker}", fmp_data)
            return fmp_data
    try:
        def _get_info():
            return yf.Ticker(ticker).info or {}
        with ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(_get_info)
            info = fut.result(timeout=5)
        cache_set(f"info:{ticker}", info); return info
    except: return {}

def fundamentals(ticker):
    i = fetch_info(ticker)
    return {
        "name":TICKER_NAMES_STATIC.get(ticker) or i.get("longName") or i.get("shortName") or ticker,
        "sector":i.get("sector"),"industry":i.get("industry"),"exchange":i.get("exchange"),
        "currency":i.get("currency"),"country":i.get("country"),"website":i.get("website"),
        "summary":i.get("longBusinessSummary"),
        "market_cap":i.get("marketCap"),"enterprise_value":i.get("enterpriseValue"),
        "shares_outstanding":i.get("sharesOutstanding"),"float_shares":i.get("floatShares"),
        "avg_volume":i.get("averageVolume"),"beta":i.get("beta"),
        "52w_high":i.get("fiftyTwoWeekHigh"),"52w_low":i.get("fiftyTwoWeekLow"),
        "50d_avg":i.get("fiftyDayAverage"),"200d_avg":i.get("twoHundredDayAverage"),
        "pe_trailing":i.get("trailingPE"),"pe_forward":i.get("forwardPE"),
        "peg_ratio":i.get("pegRatio"),"ps_ratio":i.get("priceToSalesTrailing12Months"),
        "pb_ratio":i.get("priceToBook"),"ev_ebitda":i.get("enterpriseToEbitda"),
        "ev_revenue":i.get("enterpriseToRevenue"),"revenue_ttm":i.get("totalRevenue"),
        "ebitda":i.get("ebitda"),"net_income":i.get("netIncomeToCommon"),
        "eps_trailing":i.get("trailingEps"),"eps_forward":i.get("forwardEps"),
        "profit_margin":i.get("profitMargins"),"gross_margin":i.get("grossMargins"),
        "operating_margin":i.get("operatingMargins"),"revenue_growth":i.get("revenueGrowth"),
        "earnings_growth":i.get("earningsGrowth"),"total_cash":i.get("totalCash"),
        "total_debt":i.get("totalDebt"),"debt_to_equity":i.get("debtToEquity"),
        "current_ratio":i.get("currentRatio"),"quick_ratio":i.get("quickRatio"),
        "roe":i.get("returnOnEquity"),"roa":i.get("returnOnAssets"),
        "fcf":i.get("freeCashflow"),"operating_cf":i.get("operatingCashflow"),
        "dividend_yield":i.get("dividendYield"),"short_pct_float":i.get("shortPercentOfFloat"),
        "target_mean":i.get("targetMeanPrice"),"target_high":i.get("targetHighPrice"),
        "target_low":i.get("targetLowPrice"),"recommendation":i.get("recommendationMean"),
        "recommendation_key":i.get("recommendationKey"),
        "analyst_count":i.get("numberOfAnalystOpinions"),
        "earnings_timestamp":i.get("earningsTimestamp") or i.get("earningsTimestampStart"),
        "book_value":i.get("bookValue"),
    }

# ── Asset type detection ─────────────────────────────────────
CRYPTO_SUFFIXES = ["-USD","-USDT","-BTC","BTC","ETH","SOL","XRP","BNB","ADA","DOGE","AVAX","DOT","LINK","MATIC","SHIB","LTC","BCH","XLM","ALGO","ATOM","NEAR"]
COMMODITY_TICKERS = {"CL=F":"WTI Crude Oil","BZ=F":"Brent Crude","GC=F":"Gold","SI=F":"Silver","HG=F":"Copper","NG=F":"Natural Gas","ZC=F":"Corn","ZW=F":"Wheat","ZS=F":"Soybeans","PL=F":"Platinum","PA=F":"Palladium"}
INDEX_TICKERS = {"^GSPC":"S&P 500","^IXIC":"NASDAQ","^DJI":"Dow Jones","^VIX":"VIX","^RUT":"Russell 2000","^FTSE":"FTSE 100","^N225":"Nikkei","^HSI":"Hang Seng","DX-Y.NYB":"DXY","^TNX":"US 10Y","^TYX":"US 30Y"}

def detect_asset_type(ticker, info=None):
    t = ticker.upper()
    if t in INDEX_TICKERS: return "index"
    if t in COMMODITY_TICKERS: return "commodity"
    if any(t.endswith(s) or t==s for s in CRYPTO_SUFFIXES): return "crypto"
    if info:
        qt = (info.get("quoteType") or "").upper()
        if qt == "CRYPTOCURRENCY": return "crypto"
        if qt in ("FUTURE","COMMODITY"): return "commodity"
        if qt == "INDEX": return "index"
        if qt == "ETF": return "etf"
        if qt == "MUTUALFUND": return "fund"
    return "stock"

# ── Fair Value ────────────────────────────────────────────────
SECTOR_EV = {
    "Technology":20,"Healthcare":16,"Financials":12,"Financial Services":12,
    "Energy":8,"Consumer Cyclical":12,"Consumer Defensive":14,"Industrials":13,
    "Communication Services":15,"Real Estate":18,"Utilities":11,"Materials":10,
}

def compute_fair_value(fund, asset_type="stock"):
    eps_fwd   = fund.get("eps_forward")
    eps_trail = fund.get("eps_trailing")
    book      = fund.get("book_value")
    eg        = fund.get("earnings_growth") or fund.get("revenue_growth") or 0
    sector    = fund.get("sector","")
    ebitda    = fund.get("ebitda")
    shares    = fund.get("shares_outstanding")
    debt      = fund.get("total_debt") or 0
    cash      = fund.get("total_cash") or 0
    t_mean    = fund.get("target_mean")
    t_low     = fund.get("target_low")
    t_high    = fund.get("target_high")
    models    = {}
    note      = None

    if asset_type == "crypto":
        note = "Crypto: fair value baseado em momentum e sentiment — modelos DCF/Graham não aplicáveis."
        if t_mean:
            models["Analyst Target"] = {"value":round(float(t_mean),2),"label":"Analyst Consensus","low":round(float(t_low),2) if t_low else None,"high":round(float(t_high),2) if t_high else None}
        composite = round(float(t_mean),2) if t_mean else None
        return {"composite":composite,"models":models,"note":note,"asset_type":asset_type}

    if asset_type in ("commodity","index"):
        note = f"{'Commodity' if asset_type=='commodity' else 'Index'}: modelos de fair value baseados em fundamentos não se aplicam. Mostrando dados técnicos."
        return {"composite":None,"models":{},"note":note,"asset_type":asset_type}

    if asset_type == "etf":
        note = "ETF: fair value baseado no NAV e analyst targets."
        if t_mean:
            models["Analyst Target"] = {"value":round(float(t_mean),2),"label":"Analyst Consensus"}
        composite = round(float(t_mean),2) if t_mean else None
        return {"composite":composite,"models":models,"note":note,"asset_type":asset_type}

    # STOCK — full models
    # 1. Analyst Consensus (highest weight — Wall St professionals)
    if t_mean:
        models["Analyst"] = {"value":round(float(t_mean),2),
                             "low":round(float(t_low),2) if t_low else None,
                             "high":round(float(t_high),2) if t_high else None,
                             "label":"Wall Street Consensus (target médio)"}

    # 2. DCF — 10Y discounted cash flow, WACC 10%, terminal growth 3%
    try:
        eps = eps_fwd or eps_trail
        if eps and eps > 0:
            # Conservative growth: capped at 20%, minimum 3%
            # Use sector-average if no growth data
            sector_growth = {"Technology":0.12,"Healthcare":0.08,"Financials":0.07,
                           "Consumer Cyclical":0.09,"Communication Services":0.10}.get(sector,0.08)
            raw_g = float(eg) if eg else sector_growth
            g = max(min(raw_g, 0.20), 0.03)  # strict cap: 3%-20%
            wacc = 0.10
            tg   = 0.025
            pv   = sum(eps*(1+g)**yr / (1+wacc)**yr for yr in range(1,11))
            tv   = (eps*(1+g)**10 * (1+tg)) / (wacc-tg) / (1+wacc)**10
            val  = round(pv+tv, 2)
            # Hard sanity: DCF must be < 15x analyst target and < 50x EPS
            ref = t_mean or (eps * 30)
            if val > 0 and val < ref * 12 and val < eps * 500:
                models["DCF"] = {"value":val,"label":f"DCF 10Y (crescimento {g*100:.0f}%, WACC 10%)"}
    except: pass

    # 3. EV/EBITDA — sector multiples
    mult = SECTOR_EV.get(sector, 14)
    try:
        if ebitda and shares and ebitda > 0 and shares > 0:
            ev_val = round(((ebitda*mult) - debt + cash) / shares, 2)
            if ev_val > 0:
                models["EV/EBITDA"] = {"value":ev_val,"label":f"EV/EBITDA {mult}x — sector: {sector or 'média'}"}
    except: pass

    # 4. Graham Number
    try:
        if eps_trail and book and eps_trail > 0 and book > 0:
            val = round(math.sqrt(22.5 * float(eps_trail) * float(book)), 2)
            if val > 0:
                models["Graham"] = {"value":val,"label":"Graham Number √(22.5 × EPS × Book Value)"}
    except: pass

    # 5. Peter Lynch — Fair PEG=1
    try:
        eps = eps_fwd or eps_trail
        if eps and eps > 0 and eg and eg > 0:
            val = round(float(eps) * min(eg*100, 40), 2)
            if val > 0:
                models["Lynch"] = {"value":val,"label":f"Peter Lynch — PEG=1 (crescimento {eg*100:.0f}%)"}
    except: pass

    # Weighted composite — analyst has highest weight
    WEIGHTS = {"Analyst":0.40,"DCF":0.30,"EV/EBITDA":0.18,"Lynch":0.07,"Graham":0.05}
    ws = tw = 0
    for m,w in WEIGHTS.items():
        v = models.get(m,{}).get("value")
        if v and v > 0:
            ws += v*w; tw += w
    composite = round(ws/tw, 2) if tw > 0 else None
    return {"composite":composite,"models":models,"note":note,"asset_type":asset_type}

# ── SEC ───────────────────────────────────────────────────────
def sec_ticker_map():
    c = cache_get("sec_map", TTL_UNIVERSE)
    if c: return c
    try:
        r = requests.get("https://www.sec.gov/files/company_tickers.json", headers=SEC_HEADERS, timeout=5)
        r.raise_for_status()
        m = {str(v.get("ticker","")).upper(): str(v.get("cik_str","")).zfill(10) for v in r.json().values()}
        cache_set("sec_map", m); return m
    except: return {}

def cik_for(ticker):
    return sec_ticker_map().get(ticker.upper())

def sec_form4(ticker):
    """Rápido: apenas lê o índice de submissões (1 request). Sem parse de XML."""
    ticker = ticker.upper().strip()
    c = cache_get(f"sec:{ticker}", TTL_SEC)
    if c: return c
    out = {"cik":None,"sec_url":None,"filings":[],"error":None}
    try:
        cik = cik_for(ticker)
        if not cik:
            out["error"] = "CIK não encontrado. ADRs/estrangeiras podem não ter Form 4 no SEC."
            cache_set(f"sec:{ticker}", out); return out
        cik_clean = cik.lstrip("0")
        out["cik"]     = cik
        out["sec_url"] = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={cik}&type=4&owner=include&count=40"
        r = requests.get(f"https://data.sec.gov/submissions/CIK{cik}.json", headers=SEC_HEADERS, timeout=10)
        r.raise_for_status()
        recent = r.json().get("filings",{}).get("recent",{})
        forms  = recent.get("form",[]); dates  = recent.get("filingDate",[])
        accs   = recent.get("accessionNumber",[]); docs = recent.get("primaryDocument",[])
        names  = recent.get("reportingOwnerName") or []
        cutoff = (datetime.now()-timedelta(days=180)).strftime("%Y-%m-%d")
        for idx, form in enumerate(forms):
            if form not in ("4","4/A"): continue
            date = dates[idx] if idx<len(dates) else ""
            if date and date<cutoff: continue
            acc       = accs[idx] if idx<len(accs) else ""
            acc_clean = acc.replace("-","")
            doc       = docs[idx] if idx<len(docs) else ""
            owner     = str(names[idx]) if idx<len(names) else "Insider"
            filing_url = (f"https://www.sec.gov/Archives/edgar/data/{cik_clean}/{acc_clean}/{doc}"
                          if doc else out["sec_url"])
            out["filings"].append({"date":date,"form":form,"owner":owner,
                                   "accession":acc,"filing_url":filing_url})
            if len(out["filings"]) >= 15: break
    except Exception as e:
        out["error"] = str(e)
    cache_set(f"sec:{ticker}", out); return out

def sec_form4_deep(ticker):
    """Parse Form 4 XML for real trade data: shares, price, value, position after trade."""
    ticker = ticker.upper().strip()
    c = cache_get(f"sec_deep:{ticker}", TTL_SEC)
    if c: return c
    base = sec_form4(ticker)
    out  = {**base, "trades": [], "insider_profiles": {}}

    def parse_form4_xml(xml_text, filing_meta):
        results = []
        try:
            root = ET.fromstring(xml_text)
            def xt(node, path):
                x = node.find(path)
                return x.text.strip() if x is not None and x.text else None

            issuer_name   = xt(root, ".//issuer/issuerName") or ticker
            owner_name    = xt(root, ".//reportingOwner/reportingOwnerId/rptOwnerName") or "Unknown"
            owner_cik     = xt(root, ".//reportingOwner/reportingOwnerId/rptOwnerCik") or ""
            title         = xt(root, ".//reportingOwner/reportingOwnerRelationship/officerTitle") or ""
            is_dir        = xt(root, ".//reportingOwner/reportingOwnerRelationship/isDirector") == "1"
            is_off        = xt(root, ".//reportingOwner/reportingOwnerRelationship/isOfficer") == "1"
            is_10p        = xt(root, ".//reportingOwner/reportingOwnerRelationship/isTenPercentOwner") == "1"
            relation      = title or ("Director" if is_dir else "Officer" if is_off else "10% Owner" if is_10p else "Insider/Executive")

            # Non-derivative transactions (actual stock purchases/sales)
            for tx in root.findall(".//nonDerivativeTransaction"):
                code         = xt(tx, ".//transactionCoding/transactionCode")
                if code not in ("P", "S", "A", "D", "F", "G", "M", "X"): continue
                shares       = sf(xt(tx, ".//transactionAmounts/transactionShares/value"))
                price        = sf(xt(tx, ".//transactionAmounts/transactionPricePerShare/value"))
                date_tx      = xt(tx, ".//transactionDate/value") or filing_meta.get("date", "")
                shares_after = sf(xt(tx, ".//postTransactionAmounts/sharesOwnedFollowingTransaction/value"))
                security     = xt(tx, ".//securityTitle/value") or "Common Stock"
                acq_disp     = xt(tx, ".//transactionAmounts/transactionAcquiredDisposedCode/value") or ""

                # Map code to readable action
                action_map = {"P":"BUY","S":"SELL","A":"BUY","D":"SELL","F":"SELL","G":"BUY","M":"BUY","X":"BUY"}
                action = action_map.get(code, "FILING")

                value = None
                if shares and price and price > 0:
                    value = round(shares * price, 2)
                elif shares_after and price and price > 0:
                    value = None  # can't compute without shares transacted

                # Only include if above minimum or has meaningful data
                if value and value < MIN_TRADE_VALUE and action not in ("BUY","SELL"):
                    continue
                if not shares and not shares_after:
                    continue

                results.append({
                    "source_type":  "SEC Form 4",
                    "date":         date_tx,
                    "owner":        owner_name,
                    "owner_cik":    owner_cik,
                    "relation":     relation,
                    "action":       action,
                    "tx_code":      code,
                    "security":     security,
                    "shares":       round(shares, 0) if shares else None,
                    "price":        round(price, 4) if price else None,
                    "value":        value,
                    "shares_after": round(shares_after, 0) if shares_after else None,
                    "filing_url":   filing_meta.get("filing_url"),
                })

            # Derivative transactions (options, warrants)
            for tx in root.findall(".//derivativeTransaction"):
                code         = xt(tx, ".//transactionCoding/transactionCode")
                shares       = sf(xt(tx, ".//transactionAmounts/transactionShares/value"))
                price        = sf(xt(tx, ".//transactionAmounts/transactionPricePerShare/value"))
                date_tx      = xt(tx, ".//transactionDate/value") or filing_meta.get("date", "")
                shares_after = sf(xt(tx, ".//postTransactionAmounts/sharesOwnedFollowingTransaction/value"))
                security     = xt(tx, ".//securityTitle/value") or "Derivative"
                conv_price   = sf(xt(tx, ".//conversionOrExercisePrice/value"))
                exp_date     = xt(tx, ".//expirationDate/value") or ""
                if not shares and not shares_after: continue
                action_map = {"M":"EXERCISE","X":"EXERCISE","C":"CONVERT","S":"SELL","P":"BUY","A":"GRANT","D":"DISPOSE"}
                action = action_map.get(code, "DERIVATIVE")
                results.append({
                    "source_type":  "SEC Form 4 (Derivative)",
                    "date":         date_tx,
                    "owner":        owner_name,
                    "owner_cik":    owner_cik,
                    "relation":     relation,
                    "action":       action,
                    "tx_code":      code,
                    "security":     security + (f" (exp {exp_date[:5]})" if exp_date else ""),
                    "shares":       round(shares, 0) if shares else None,
                    "price":        round(conv_price or price or 0, 4) if (conv_price or price) else None,
                    "value":        None,
                    "shares_after": round(shares_after, 0) if shares_after else None,
                    "filing_url":   filing_meta.get("filing_url"),
                    "derivative":   True,
                })
        except Exception as e:
            pass
        return results

    def fetch_and_parse(filing):
        """Fetch the XML for a filing and parse it."""
        try:
            # filing_url points to a document — we need the XML
            url = filing.get("filing_url", "")
            if not url: return []

            # If it's already an XML file, fetch directly
            if url.endswith(".xml"):
                r = requests.get(url, headers=SEC_HEADERS, timeout=5)
                if r.ok and "ownershipDocument" in r.text:
                    return parse_form4_xml(r.text, filing)

            # Otherwise fetch the filing index and find the XML
            # URL format: .../edgar/data/CIK/ACCESSION/DOCNAME
            # We need: .../edgar/data/CIK/ACCESSION/
            parts = url.split("/")
            if "Archives" in parts:
                arc_idx = parts.index("Archives")
                # Rebuild index URL
                if len(parts) >= arc_idx + 6:
                    idx_url = "/".join(parts[:arc_idx+6]) + "/"
                    ri = requests.get(idx_url, headers=SEC_HEADERS, timeout=4)
                    if ri.ok:
                        xml_links = re.findall(r'href="(/Archives/edgar/data/[^"]+[.]xml)"', ri.text)
                        for xl in xml_links:
                            rx = requests.get("https://www.sec.gov" + xl, headers=SEC_HEADERS, timeout=3)
                            if rx.ok and "ownershipDocument" in rx.text:
                                return parse_form4_xml(rx.text, filing)

            # Last resort: try the EDGAR submissions API
            cik = base.get("cik", "")
            acc = filing.get("accession", "").replace("-", "")
            if cik and acc:
                cik_clean = str(cik).lstrip("0")
                idx_url = f"https://www.sec.gov/Archives/edgar/data/{cik_clean}/{acc}/"
                ri = requests.get(idx_url, headers=SEC_HEADERS, timeout=4)
                if ri.ok:
                    xml_links = re.findall(r'href="(/Archives/edgar/data/[^"]+[.]xml)"', ri.text)
                    for xl in xml_links:
                        rx = requests.get("https://www.sec.gov" + xl, headers=SEC_HEADERS, timeout=3)
                        if rx.ok and "ownershipDocument" in rx.text:
                            return parse_form4_xml(rx.text, filing)
        except Exception as e:
            pass
        return []

    # Process up to 10 filings in parallel
    filings_to_parse = base.get("filings", [])[:5]
    all_trades = []

    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(fetch_and_parse, f): f for f in filings_to_parse}
        for fut in as_completed(futures, timeout=12):
            try:
                trades = fut.result(timeout=8)
                all_trades.extend(trades)
            except Exception: pass

    # Filter and sort
    significant = [t for t in all_trades if t.get("value") and t["value"] >= MIN_TRADE_VALUE and t.get("action") in ("BUY","SELL")]
    other       = [t for t in all_trades if t not in significant]

    # Sort: significant first by value desc, then others by date desc
    significant.sort(key=lambda x: x.get("value") or 0, reverse=True)
    other.sort(key=lambda x: x.get("date") or "", reverse=True)
    out["trades"] = significant + other[:max(0, 15 - len(significant))]

    # Build per-person profile
    profiles = {}
    for t in out["trades"]:
        n = t.get("owner", "?")
        if n not in profiles:
            profiles[n] = {
                "name":       n,
                "owner_cik":  t.get("owner_cik", ""),
                "relation":   t.get("relation", "Insider"),
                "trades":     [],
                "total_bought": 0,
                "total_sold":   0,
                "shares_held":  None,
            }
        profiles[n]["trades"].append(t)
        if t.get("action") == "BUY"  and t.get("value"): profiles[n]["total_bought"] += t["value"]
        if t.get("action") == "SELL" and t.get("value"): profiles[n]["total_sold"]   += t["value"]
        # Most recent shares_after
        if t.get("shares_after") and (profiles[n]["shares_held"] is None or t.get("date","") >= max(x.get("date","") for x in profiles[n]["trades"])):
            profiles[n]["shares_held"] = t["shares_after"]

    out["insider_profiles"] = profiles

    # If we got no meaningful trades, keep the raw filings as fallback
    if not out["trades"]:
        out["trades"] = [{
            "source_type": "SEC Form 4",
            "date":        f.get("date"),
            "owner":       f.get("owner", "Insider"),
            "owner_cik":   "",
            "relation":    "Insider/Executive",
            "action":      "FILING",
            "shares":      None, "price": None, "value": None, "shares_after": None,
            "filing_url":  f.get("filing_url"),
            "note":        "Clica 'Form 4 →' para ver detalhes no SEC"
        } for f in filings_to_parse[:5]]

    cache_set(f"sec_deep:{ticker}", out)
    return out

# ── Congress ──────────────────────────────────────────────────
_congress      = {}
_cong_lock     = threading.Lock()
_cong_loaded   = False

def _load_congress():
    global _congress, _cong_loaded
    trades = []
    path = os.path.join(os.path.dirname(__file__),"data","political_trades.json")
    try:
        with open(path,encoding="utf-8") as f: raw = json.load(f)
        for x in raw:
            t = str(x.get("ticker","")).upper().strip()
            if t: trades.append({"ticker":t,"name":x.get("politician",""),"chamber":x.get("chamber",""),
                                  "party":x.get("party",""),"type":str(x.get("transaction","")).lower(),
                                  "amount":x.get("amount",""),"date":str(x.get("date",""))[:10],"asset":""})
    except: pass
    cutoff = (datetime.now()-timedelta(days=1095)).strftime("%Y-%m-%d")  # 3 years
    for url, name_key, chamber in [
        ("https://house-stock-watcher-data.s3-us-west-2.amazonaws.com/data/all_transactions.json","representative","House"),
        ("https://senate-stock-watcher-data.s3-us-west-2.amazonaws.com/aggregate/all_transactions.json","senator","Senate"),
    ]:
        try:
            r = requests.get(url, timeout=15)
            if not r.ok: continue
            for x in r.json():
                tk = (x.get("ticker") or "").strip().upper().replace("$","").replace(" ","")
                if not tk or tk in ("N/A","NA","--","CASH") or len(tk)>6: continue
                if not re.sub(r'[-.]','',tk).replace('BRK','').isalpha() and not tk.endswith('-B'): continue
                d  = (x.get("transaction_date") or x.get("disclosure_date") or "")[:10]
                if d < cutoff: continue
                trades.append({"ticker":tk,"name":x.get(name_key,""),"chamber":chamber,
                               "party":x.get("party",""),"type":str(x.get("type","")).lower(),
                               "amount":x.get("amount",""),"date":d,"asset":x.get("asset_description","")})
        except: pass
    by_tk = {}
    for t in trades: by_tk.setdefault(t["ticker"],[]).append(t)
    with _cong_lock: _congress = by_tk; _cong_loaded = True

def get_congress(ticker):
    if not _cong_loaded: return {"trades":[],"buy_count":0,"sell_count":0,"members":[]}
    with _cong_lock:
        trades = sorted(_congress.get(ticker.upper(),[]),key=lambda x:x.get("date",""),reverse=True)
    # Enrich with avatar URLs (UI-Avatars, no API key needed, always works)
    for t in trades:
        name = t.get("name","")
        if name:
            t["avatar"] = f"https://ui-avatars.com/api/?name={quote_plus(name)}&size=80&background=161d29&color=00e5a0&bold=true&format=svg"
    buys  = [t for t in trades if "purchase" in t.get("type","") or "buy" in t.get("type","")]
    sells = [t for t in trades if "sale"     in t.get("type","") or "sell" in t.get("type","")]
    # Member summary with avatar
    members_seen = {}
    for t in trades:
        n = t.get("name","")
        if n and n not in members_seen:
            members_seen[n] = {
                "name": n, "party": t.get("party",""),
                "chamber": t.get("chamber",""),
                "avatar": t.get("avatar",""),
                "buy_count":  sum(1 for x in trades if x.get("name")==n and ("purchase" in x.get("type","") or "buy" in x.get("type",""))),
                "sell_count": sum(1 for x in trades if x.get("name")==n and ("sale" in x.get("type","") or "sell" in x.get("type",""))),
                "total_min":  sum(parse_amount_min(x.get("amount","")) for x in trades if x.get("name")==n),
            }
    return {"trades":trades[:25],"buy_count":len(buys),"sell_count":len(sells),
            "members_detail": list(members_seen.values())[:10],
            "members":list(members_seen.keys())[:8]}

def parse_amount_min(amt):
    """Parse '$1,001 - $15,000' → 1001 (minimum of range)"""
    try:
        s = str(amt).replace("$","").replace(",","").split("-")[0].strip().split(" ")[0]
        return int(float(s))
    except: return 0

def congress_top():
    with _cong_lock:
        counts = {}
        for tk, trades in _congress.items():
            buys = [t for t in trades if "purchase" in t.get("type","") or "buy" in t.get("type","")]
            if buys: counts[tk] = {"count":len(buys),"members":list({t["name"] for t in buys})[:4],
                                   "latest":max(t.get("date","") for t in buys)}
    return sorted(counts.items(),key=lambda x:x[1]["count"],reverse=True)[:20]

# ── Statements (parallelized) ─────────────────────────────────
def df_rows(df, max_cols=6):
    if df is None or getattr(df,"empty",True): return []
    cols = list(df.columns)[:max_cols]; rows = []
    for m in list(df.index)[:120]:
        item = {"metric":str(m)}
        for c in cols:
            k   = str(c.date()) if hasattr(c,"date") else str(c)[:10]
            val = df.loc[m,c]
            item[k] = None if pd.isna(val) else sf(val)
        rows.append(item)
    return rows

def extract_metric(rows, names):
    if not rows: return []
    first = rows[0]
    if "metric" in first:
        lower = {r.get("metric","").lower(): r for r in rows}
        for n in names:
            row = lower.get(n.lower())
            if row:
                pts = [{"date": k, "value": v} for k, v in row.items()
                       if k != "metric" and v is not None]
                return sorted(pts, key=lambda x: x["date"])
    else:
        for n in names:
            n_lower = n.lower()
            pts = []
            for row in rows:
                for k, v in row.items():
                    if k.lower() == n_lower and k != "date" and v is not None:
                        pts.append({"date": row.get("date", ""), "value": v})
                        break
            if pts:
                return sorted(pts, key=lambda x: x["date"])
    return []

def _yf_v10_statements(ticker):
    """Yahoo Finance v10 quoteSummary API — returns financials in one request ~500ms"""
    modules = "incomeStatementHistory,balanceSheetHistory,cashflowStatementHistory"
    url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules={modules}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
               "Accept": "application/json"}
    r = requests.get(url, headers=headers, timeout=6)
    if not r.ok: raise Exception(f"HTTP {r.status_code}")
    data = r.json().get("quoteSummary", {})
    if data.get("error"): raise Exception(str(data["error"]))
    result = data.get("result", [{}])[0]

    def _parse_stmts(stmts_key):
        rows = []
        for stmt in result.get(stmts_key, {}).get("statements", []):
            row = {}
            for k, v in stmt.items():
                if isinstance(v, dict):
                    row[k] = sf(v.get("raw"), 2)
                elif k == "endDate":
                    row["date"] = v.get("fmt", str(v)) if isinstance(v, dict) else str(v)
            rows.append(row)
        return rows

    income   = _parse_stmts("incomeStatementHistory")
    balance  = _parse_stmts("balanceSheetHistory")
    cashflow = _parse_stmts("cashflowStatementHistory")

    # Normalise field names to match what the frontend expects
    def _norm(rows, key_map):
        out = []
        for row in rows:
            nr = {key_map.get(k, k): v for k, v in row.items()}
            out.append(nr)
        return out

    income_map = {
        "totalRevenue": "Total Revenue",
        "grossProfit": "Gross Profit",
        "netIncome": "Net Income",
        "operatingIncome": "Operating Income",
        "ebit": "EBIT",
        "ebitda": "EBITDA",
        "totalOperatingExpenses": "Total Operating Expenses",
        "costOfRevenue": "Cost Of Revenue",
    }
    balance_map = {
        "totalAssets": "Total Assets",
        "totalLiab": "Total Liabilities",
        "totalStockholderEquity": "Total Stockholder Equity",
        "totalCurrentAssets": "Total Current Assets",
        "totalCurrentLiabilities": "Total Current Liabilities",
        "cash": "Cash And Cash Equivalents",
        "longTermDebt": "Long Term Debt",
        "shortLongTermDebt": "Short Long Term Debt",
    }
    cf_map = {
        "totalCashFromOperatingActivities": "Operating Cash Flow",
        "capitalExpenditures": "Capital Expenditure",
        "freeCashFlow": "Free Cash Flow",
        "dividendsPaid": "Dividends Paid",
        "repurchaseOfStock": "Repurchase Of Capital Stock",
        "totalCashFromFinancingActivities": "Total Cash From Financing Activities",
    }
    return _norm(income, income_map), _norm(balance, balance_map), _norm(cashflow, cf_map)


FMP_API_KEY = os.getenv("FMP_API_KEY", "8o9ercOTHhnTas3sasFajrY3sa324qju")

def _fmp_statements(ticker):
    """FMP financials — 3 requests em paralelo (~1-2s total)"""
    if not FMP_API_KEY: raise Exception("No FMP_API_KEY")
    base = "https://financialmodelingprep.com/api/v3"
    def _get(ep):
        r = requests.get(f"{base}{ep}?limit=5&apikey={FMP_API_KEY}",
                         headers={"User-Agent":"IST/1.0"}, timeout=6)
        if not r.ok: raise Exception(f"FMP {r.status_code}")
        return r.json()
    with ThreadPoolExecutor(max_workers=3) as ex:
        f_inc = ex.submit(_get, f"/income-statement/{ticker}")
        f_bal = ex.submit(_get, f"/balance-sheet-statement/{ticker}")
        f_cf  = ex.submit(_get, f"/cash-flow-statement/{ticker}")
    inc_r = f_inc.result(timeout=8)
    bal_r = f_bal.result(timeout=8)
    cf_r  = f_cf.result(timeout=8)
    def _n(rows, km):
        out = []
        for row in rows:
            nr = {"date": str(row.get("date",""))[:10]}
            for k,v in km.items(): nr[v] = sf(row.get(k))
            out.append(nr)
        return out
    return (_n(inc_r, {"revenue":"Total Revenue","grossProfit":"Gross Profit",
                        "netIncome":"Net Income","operatingIncome":"Operating Income",
                        "costOfRevenue":"Cost Of Revenue","ebitda":"EBITDA"}),
            _n(bal_r, {"totalAssets":"Total Assets","totalLiabilities":"Total Liabilities",
                        "totalStockholdersEquity":"Total Stockholder Equity",
                        "cashAndCashEquivalents":"Cash And Cash Equivalents",
                        "longTermDebt":"Long Term Debt"}),
            _n(cf_r,  {"operatingCashFlow":"Operating Cash Flow",
                        "capitalExpenditure":"Capital Expenditure",
                        "freeCashFlow":"Free Cash Flow",
                        "dividendsPaid":"Dividends Paid",
                        "commonStockRepurchased":"Repurchase Of Capital Stock"}))


def statements(ticker):
    ticker = ticker.upper().strip()
    c = cache_get(f"stmt:{ticker}", TTL_STATEMENTS)
    if c: return c
    out = {"income":[],"balance":[],"cashflow":[],"charts":{},"error":None}
    income, balance, cashflow = [], [], []

    # Run ALL strategies in parallel, use first one that succeeds
    def _try_v10():
        return _yf_v10_statements(ticker)

    def _try_fmp():
        return _fmp_statements(ticker)

    def _try_yfinance():
        """yfinance with parallel attribute access for speed."""
        t = yf.Ticker(ticker)
        def _get_income():
            for attr in ["income_stmt", "financials"]:
                try:
                    df = getattr(t, attr, None)
                    if df is not None and not getattr(df, "empty", True):
                        return df_rows(df)
                except: pass
            return []
        def _get_balance():
            for attr in ["balance_sheet", "quarterly_balance_sheet"]:
                try:
                    df = getattr(t, attr, None)
                    if df is not None and not getattr(df, "empty", True):
                        return df_rows(df)
                except: pass
            return []
        def _get_cashflow():
            for attr in ["cashflow", "cash_flow"]:
                try:
                    df = getattr(t, attr, None)
                    if df is not None and not getattr(df, "empty", True):
                        return df_rows(df)
                except: pass
            return []
        with ThreadPoolExecutor(max_workers=3) as _ex:
            fi = _ex.submit(_get_income)
            fb = _ex.submit(_get_balance)
            fc = _ex.submit(_get_cashflow)
            income   = fi.result(timeout=12)
            balance  = fb.result(timeout=12)
            cashflow = fc.result(timeout=12)
        return income, balance, cashflow

    # Strategy: try fast sources (v10, FMP) in parallel first (~2s)
    # then yfinance alone (avoids thread contention with yf internals)
    with ThreadPoolExecutor(max_workers=2) as ex:
        f_v10 = ex.submit(_try_v10)
        f_fmp = ex.submit(_try_fmp)
        for fut in as_completed([f_v10, f_fmp], timeout=8):
            try:
                i, b, c = fut.result(timeout=1)
                if i:
                    income, balance, cashflow = i, b, c
                    break
            except Exception: pass
    # If fast sources failed, use yfinance (works but needs ~2-5s alone)
    if not income:
        try:
            income, balance, cashflow = _try_yfinance()
        except Exception as e:
            out["error"] = str(e)

    out.update({"income": income, "balance": balance, "cashflow": cashflow})
    out["charts"] = {
        "revenue":       extract_metric(income,  ["Total Revenue","Operating Revenue"]),
        "net_income":    extract_metric(income,  ["Net Income","Net Income Common Stockholders"]),
        "gross_profit":  extract_metric(income,  ["Gross Profit"]),
        "cash":          extract_metric(balance, ["Cash And Cash Equivalents","Cash Cash Equivalents And Short Term Investments"]),
        "debt":          extract_metric(balance, ["Total Debt","Long Term Debt","Short Long Term Debt"]),
        "fcf":           extract_metric(cashflow,["Free Cash Flow"]),
        "operating_cf":  extract_metric(cashflow,["Operating Cash Flow","Total Cash From Operating Activities"]),
        "capex":         extract_metric(cashflow,["Capital Expenditure","Purchase Of Property Plant And Equipment"]),
        "dividends_paid":extract_metric(cashflow,["Dividends Paid","Common Stock Dividends Paid"]),
        "buybacks":      extract_metric(cashflow,["Repurchase Of Capital Stock","Repurchase Of Common Stock"]),
        "debt_repayment":extract_metric(cashflow,["Repayment Of Debt","Long Term Debt Payments"]),
    }
    cache_set(f"stmt:{ticker}", out)
    return out


# ── History ───────────────────────────────────────────────────

def _stooq_symbol(ticker):
    t = ticker.upper()
    index_map = {"^GSPC":"^spx","^IXIC":"^ndq","^DJI":"^dji","^VIX":"^vix",
                 "^RUT":"^rut","^TNX":"^tnx","DX-Y.NYB":"dx.f"}
    if t in index_map: return index_map[t]
    if t.endswith("-USD"):
        return t.replace("-USD","").lower() + "usd"
    if t.endswith("=F"):
        fut_map = {"GC=F":"gc.f","SI=F":"si.f","CL=F":"cl.f","BZ=F":"cb.f",
                   "NG=F":"ng.f","HG=F":"hg.f","ZC=F":"zc.f","ZW=F":"zw.f"}
        return fut_map.get(t, t.replace("=F",".f").lower())
    return f"{t.lower()}.us"

def _history_stooq(ticker, period="1y"):
    """Stooq.com CSV — sem API key, dados históricos ilimitados"""
    from datetime import datetime as _dt, timedelta as _td
    sym = _stooq_symbol(ticker)
    days = {"5d":10,"1mo":35,"3mo":100,"1y":370,"2y":740,"5y":1850}.get(period,370)
    d1 = (_dt.now()-_td(days=days)).strftime("%Y%m%d")
    d2 = _dt.now().strftime("%Y%m%d")
    url = f"https://stooq.com/q/d/l/?s={sym}&d1={d1}&d2={d2}&i=d"
    r = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=8)
    if not r.ok or "No data" in r.text[:50]: return []
    lines = r.text.strip().split("\n")
    if len(lines) < 2: return []
    pts = []
    for line in lines[1:]:
        parts = line.strip().split(",")
        if len(parts) < 5: continue
        try:
            close_f = sf(parts[4])
            if close_f is None: continue
            pts.append({"date":parts[0][:10],"open":sf(parts[1]),"high":sf(parts[2]),
                        "low":sf(parts[3]),"close":close_f,"value":close_f,
                        "volume":si(parts[5]) if len(parts)>5 else None})
        except: continue
    return pts


def history(ticker, period="1y", interval="1d"):
    ticker = ticker.upper().strip()
    key = f"hist:{ticker}:{period}:{interval}"
    c = cache_get(key, TTL_HISTORY)
    if c: return c
    pts = []

    # ── Strategy 1: Yahoo Finance v8/finance/chart API (fast ~300ms) ──
    _RANGE_MAP = {
        "5d":"5d","1mo":"1mo","3mo":"3mo","1y":"1y","2y":"2y","5y":"5y","max":"max",
    }
    _INTERVAL_MAP = {
        "5d":"5m","1mo":"1h","3mo":"1d","1y":"1d","2y":"1d","5y":"1wk","max":"1mo",
    }
    use_range    = _RANGE_MAP.get(period, period)
    use_interval = interval if interval != "1d" or period not in _INTERVAL_MAP else _INTERVAL_MAP.get(period, "1d")
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        params = {"interval": use_interval, "range": use_range, "includeAdjustedClose": "true"}
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
        }
        r = requests.get(url, params=params, headers=headers, timeout=8)
        if r.ok:
            data = r.json().get("chart", {})
            if data.get("error"): raise Exception(str(data["error"]))
            result = data.get("result", [])
            if result:
                res  = result[0]
                ts   = res.get("timestamp", [])
                q    = res.get("indicators", {}).get("quote", [{}])[0]
                adj  = res.get("indicators", {}).get("adjclose", [{}])
                adj_close = adj[0].get("adjclose", []) if adj else []
                opens  = q.get("open",   [])
                highs  = q.get("high",   [])
                lows   = q.get("low",    [])
                closes = q.get("close",  [])
                vols   = q.get("volume", [])
                for i, t_stamp in enumerate(ts):
                    try:
                        close_val = sf(closes[i] if i < len(closes) else None)
                        if close_val is None: continue
                        # Use adjusted close when available
                        adj_val = sf(adj_close[i] if i < len(adj_close) else None) or close_val
                        dt = datetime.utcfromtimestamp(t_stamp)
                        date_str = dt.strftime("%Y-%m-%d")
                        pts.append({
                            "date":   date_str,
                            "open":   sf(opens[i] if i < len(opens) else None),
                            "high":   sf(highs[i] if i < len(highs) else None),
                            "low":    sf(lows[i]  if i < len(lows)  else None),
                            "close":  close_val,
                            "value":  adj_val,
                            "volume": si(vols[i]  if i < len(vols)  else None),
                        })
                    except Exception: continue
                if pts:
                    cache_set(key, pts)
                    return pts
    except Exception: pass

    # ── Strategy 2: FMP Historical (se FMP_API_KEY definida) ──
    try:
        fmp_pts = _fmp_historical(ticker, period)
        if fmp_pts:
            cache_set(key, fmp_pts)
            return fmp_pts
    except Exception: pass

    # ── Strategy 3: Stooq.com CSV (sem API key) ──
    try:
        stooq_pts = _history_stooq(ticker, period)
        if stooq_pts:
            cache_set(key, stooq_pts)
            return stooq_pts
    except Exception: pass

    # ── Strategy 4: yfinance fallback ──
    def _fetch_yf():
        h = yf.download(ticker, period=period, interval=interval,
                        progress=False, auto_adjust=False, threads=True)
        if h is None or h.empty: return []
        if hasattr(h.columns,"nlevels") and h.columns.nlevels>1:
            h.columns = h.columns.get_level_values(0)
        h = h.reset_index()
        dc = "Date" if "Date" in h.columns else "Datetime"
        rows = []
        for _,row in h.iterrows():
            dt    = row[dc]
            close = sf(row.get("Close"))
            if close is None: continue
            rows.append({"date":dt.strftime("%Y-%m-%d") if hasattr(dt,"strftime") else str(dt)[:10],
                        "open":sf(row.get("Open")),"high":sf(row.get("High")),
                        "low":sf(row.get("Low")),"close":close,"value":close,
                        "volume":si(row.get("Volume"))})
        return rows
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(_fetch_yf)
            pts = fut.result(timeout=15)
    except Exception: pts = []
    cache_set(key, pts)
    return pts

def normalize(pts):
    pts = [p for p in pts if (p.get("value") or p.get("close")) is not None]
    if not pts: return []
    base = pts[0].get("value") or pts[0].get("close")
    if not base: return []
    return [{"date":p["date"],"value":round(((p.get("value") or p.get("close"))/base)*100,4)} for p in pts]

def chart_series(ticker, period, overlays, normalized=True):
    # Build list of all symbols to fetch
    tk = ticker.upper()
    to_fetch = {tk: (tk, period)}  # key → (actual_ticker, period)
    for ov in overlays:
        o = ov.upper().strip()
        if o in ("SP500","S&P500","S&P 500","^GSPC"): to_fetch["S&P 500"] = ("^GSPC", period)
        elif o in ("QQQ","NASDAQ"): to_fetch["QQQ"] = ("QQQ", period)
        elif o in ("M2","M2SL","MONEY"): to_fetch["M2 Money"] = ("M2SL", period)
        elif o: to_fetch[o] = (o, period)
    # Fetch all in parallel (max 12s per fetch)
    out = {}
    def _fetch_one(label, sym, per):
        if label == "M2 Money":
            return label, filter_period(fred(sym), per)
        return label, filter_period(history(sym, per), per)
    with ThreadPoolExecutor(max_workers=len(to_fetch)) as ex:
        futures = {ex.submit(_fetch_one, lbl, sym, per): lbl
                   for lbl, (sym, per) in to_fetch.items()}
        for fut in as_completed(futures, timeout=25):
            try:
                lbl, data = fut.result(timeout=1)
                out[lbl] = data
            except Exception: pass
    if normalized: out = {k:normalize(v) for k,v in out.items()}
    return out

def fred(series="M2SL"):
    key = f"fred:{series}"
    c = cache_get(key, TTL_MACRO)
    if c: return c
    pts = []
    try:
        r = requests.get(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series}",timeout=10)
        r.raise_for_status()
        for line in r.text.strip().splitlines()[1:]:
            d,v = line.split(",")[:2]
            if v!=".": pts.append({"date":d,"value":sf(v)})
    except: pass
    cache_set(key,pts); return pts

# ── News ──────────────────────────────────────────────────────
TRUSTED = ["reuters","apnews","bloomberg","wsj","ft.com","cnbc","marketwatch","yahoo","nasdaq","sec.gov","investing.com"]
RISKY   = ["rumor","unconfirmed","anonymous","reddit","x.com","twitter","stocktwits"]
POS     = ["beats","raises","upgrade","growth","profit","approval","record","buyback","contract","surge"]
NEG     = ["misses","cuts","downgrade","lawsuit","probe","fraud","decline","warning","recall","plunge"]

def news_assess(title,link,summary):
    text  = f"{title} {link} {summary}".lower()
    trust = 50+(25 if any(d in text for d in TRUSTED) else 0)-(20 if any(r in text for r in RISKY) else 0)
    pos,neg = sum(x in text for x in POS), sum(x in text for x in NEG)
    sent  = "positive" if pos>neg else "negative" if neg>pos else "neutral"
    label = "higher-confidence" if trust>=70 else "needs-check" if trust>=45 else "low-confidence"
    return {"trust_score":max(0,min(100,trust)),"label":label,"sentiment":sent}

def fetch_google_news(queries, limit_per_query=8, total=16):
    items = []
    for q in queries:
        try:
            feed = feedparser.parse(f"https://news.google.com/rss/search?q={quote_plus(q)}&hl=en-US&gl=US&ceid=US:en")
            for e in feed.entries[:limit_per_query]:
                title   = getattr(e,"title",""); link = getattr(e,"link","")
                summary = re.sub("<.*?>","",getattr(e,"summary",""))[:260]
                items.append({"title":title.encode('utf-8','replace').decode('utf-8'),"link":link,"summary":summary.encode('utf-8','replace').decode('utf-8'),"published":getattr(e,"published",""),**news_assess(title,link,summary)})
        except: pass
    seen,final = set(),[]
    for x in items:
        k = x.get("title","")[:90]
        if k and k not in seen: seen.add(k); final.append(x)
    return final[:total]

def news(ticker):
    ticker = ticker.upper().strip()
    c = cache_get(f"news:{ticker}", TTL_NEWS)
    if c: return c
    # For crypto: use crypto-specific queries
    is_crypto = ticker.endswith('-USD')
    is_future = ticker.endswith('=F')
    if is_crypto:
        coin = ticker.replace('-USD','')
        queries = [f"{coin} cryptocurrency price", f"{coin} crypto market analysis"]
    elif is_future:
        queries = [f"{ticker.replace('=F','')} commodity price outlook", f"oil gold commodity market"]
    else:
        queries = [f"{ticker} stock news", f"{ticker} earnings revenue"]
    out = {"items": fetch_google_news(queries), "note": "Google News RSS · filtro heurístico de credibilidade."}
    cache_set(f"news:{ticker}", out)
    return out

def macro_news():
    c = cache_get("news:macro", TTL_NEWS)
    if c: return c
    out = {"items":fetch_google_news(["stock market economy inflation Federal Reserve","Brent crude oil dollar treasury","US CPI jobs GDP recession","semiconductors AI earnings"],7,20),"note":"Macro/economy news."}
    cache_set("news:macro",out); return out

# ── Earnings ──────────────────────────────────────────────────
def earnings_detail(ticker):
    ticker = ticker.upper().strip()
    c = cache_get(f"ed:{ticker}", TTL_INFO)
    if c: return c
    out = {"next_earnings_date":None,"eps_estimate":None,"last_eps_actual":None,
           "last_eps_estimate":None,"last_eps_surprise":None,"history":[]}
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        t = yf.Ticker(ticker)
        # Calendar (most reliable for next date)
        try:
            cal = t.calendar
            if isinstance(cal,dict):
                ed = cal.get("Earnings Date")
                if isinstance(ed,(list,tuple)):
                    for d in ed:
                        ds = str(d)[:10]
                        if ds >= today:
                            out["next_earnings_date"] = ds
                            break
                out["eps_estimate"] = sf(cal.get("EPS Estimate"),4)
        except: pass
        # Full earnings history
        try:
            edf = t.get_earnings_dates(limit=16)
            if edf is not None and not edf.empty:
                for _,row in edf.reset_index().iterrows():
                    actual   = sf(row.get("Reported EPS"))
                    estimate = sf(row.get("EPS Estimate"))
                    dt       = str(row.iloc[0])[:10]
                    surprise = sf(row.get("Surprise(%)"),2)
                    # Future: no actual reported yet
                    if actual is None:
                        if out["next_earnings_date"] is None and dt >= today:
                            out["next_earnings_date"] = dt
                            out["eps_estimate"] = estimate
                    else:
                        # Past earnings
                        if out["last_eps_actual"] is None:
                            out["last_eps_actual"]    = actual
                            out["last_eps_estimate"]  = estimate
                            out["last_eps_surprise"]  = surprise
                        out["history"].append({
                            "date": dt,
                            "actual": actual,
                            "estimate": estimate,
                            "surprise": surprise,
                            "beat": actual >= estimate if (actual is not None and estimate is not None) else None
                        })
                    if len(out["history"]) >= 8: break
        except: pass
    except: pass
    cache_set(f"ed:{ticker}",out); return out

# ── Score ─────────────────────────────────────────────────────
def score_summary(px, f, sec, cong):
    score,b = 0,{}
    n = len(sec.get("filings",[])); ins = min(n*5,25)
    score+=ins; b["insider"]=ins
    pe,peg,eve = f.get("pe_trailing"),f.get("peg_ratio"),f.get("ev_ebitda")
    val = 0
    if pe and 0<pe<15: val+=8
    elif pe and 0<pe<25: val+=5
    elif pe and 0<pe<35: val+=2
    if peg and 0<peg<1: val+=7
    elif peg and 0<peg<1.5: val+=4
    if eve and 0<eve<10: val+=5
    elif eve and 0<eve<20: val+=2
    val=min(20,val); score+=val; b["valuation"]=val
    rg,eg = f.get("revenue_growth") or 0, f.get("earnings_growth") or 0
    gp=0
    if rg>.3: gp+=8
    elif rg>.15: gp+=5
    elif rg>.05: gp+=2
    if eg>.3: gp+=7
    elif eg>.15: gp+=4
    elif eg>.05: gp+=2
    gp=min(15,gp); score+=gp; b["growth"]=gp
    rec=f.get("recommendation"); ap=0
    if rec:
        if rec<=1.5: ap=10
        elif rec<=2: ap=8
        elif rec<=2.5: ap=5
        elif rec<=3: ap=2
    score+=ap; b["analyst"]=ap
    tp=0
    if px.get("price") and f.get("50d_avg") and px["price"]>f["50d_avg"]: tp+=5
    if px.get("price") and f.get("200d_avg") and px["price"]>f["200d_avg"]: tp+=5
    score+=tp; b["technical"]=tp
    cp = min(cong.get("buy_count",0)*2,5); score+=cp; b["congress"]=cp
    return min(100,round(score)), b

# ── Build stock ───────────────────────────────────────────────
def quick_stock(ticker):
    ticker = ticker.upper().strip()
    px = fetch_price(ticker); meta = lookup_name(ticker)
    ci = cache_get(f"info:{ticker}",TTL_INFO) or {}
    f  = {"name":TICKER_NAMES_STATIC.get(ticker) or TICKER_DISPLAY.get(ticker) or ci.get("longName") or ci.get("shortName") or meta.get("name") or ticker,
          "sector":ci.get("sector"),"industry":ci.get("industry"),
          "exchange":ci.get("exchange") or meta.get("exchange"),"country":ci.get("country"),
          "market_cap":ci.get("marketCap"),"52w_high":ci.get("fiftyTwoWeekHigh"),
          "52w_low":ci.get("fiftyTwoWeekLow"),"50d_avg":ci.get("fiftyDayAverage"),
          "200d_avg":ci.get("twoHundredDayAverage"),"target_mean":ci.get("targetMeanPrice"),
          "analyst_count":ci.get("numberOfAnalystOpinions"),"summary":ci.get("longBusinessSummary")}
    price,target = px.get("price"),f.get("target_mean")
    upside = round((target-price)/price*100,1) if price and target else None
    return {"ticker":ticker,**f,**px,"upside":upside,"signal_score":None,"insider_trade_count":None,
            "insights":[{"level":"info","text":"Dados rápidos. Fundamentais/SEC/notícias nas abas."}]}

def build_stock(ticker, include_sec=False):
    ticker = ticker.upper().strip()
    px = fetch_price(ticker); f = fundamentals(ticker); cong = get_congress(ticker)
    sec = sec_form4(ticker) if include_sec else {"cik":None,"sec_url":None,"error":None,"filings":[]}
    atype = detect_asset_type(ticker, fetch_info(ticker))
    score,breakdown = score_summary(px,f,sec,cong)
    price,target = px.get("price"),f.get("target_mean")
    upside = round((target-price)/price*100,1) if price and target else None
    ed = None
    if f.get("earnings_timestamp"):
        try:
            ts = f["earnings_timestamp"]
            ed_dt = datetime.fromtimestamp(ts)
            today = datetime.now()
            # Only show future dates as "next earnings"
            ed = ed_dt.strftime("%Y-%m-%d") if ed_dt > today else None
        except: pass
    fv = compute_fair_value(f, asset_type=atype)
    insights = [{"level":"good" if score>=55 else "warn","text":f"Signal score {score}/100."}]
    if fv.get("composite") and price:
        mos    = round((fv["composite"]-price)/price*100,1)
        rating = ("Muito Barata" if mos>25 else "Barata" if mos>10 else
                  "Justa" if mos>-5 else "Cara" if mos>-20 else "Muito Cara")
        lv = "good" if mos>10 else "warn" if mos<-10 else "info"
        insights.append({"level":lv,"text":f"Preço Justo: ${fv['composite']:,.2f} → {rating} ({'+' if mos>=0 else ''}{mos}% vs preço)."})
    if cong.get("buy_count",0):
        insights.append({"level":"good","text":f"[Congress] {cong['buy_count']} compra(s) por congressistas: {', '.join(cong['members'][:3])}."})
    if ed: insights.append({"level":"info","text":f"Próximo earnings: {ed}."})
    return {"ticker":ticker,**f,**px,"upside":upside,"earnings_date":ed,
            "cik":sec.get("cik"),"sec_url":sec.get("sec_url"),"sec_error":sec.get("error"),
            "insider_trade_count":len(sec.get("filings",[])),
            "fair_value":fv.get("composite"),"fair_value_models":fv.get("models",{}),"fair_value_note":fv.get("note"),"asset_type":atype,
            "congress_buys":cong.get("buy_count",0),"congress_members":cong.get("members",[]),
            "members_detail":cong.get("members_detail",[]),
            "signal_score":score,"score_breakdown":breakdown,"insights":insights}

# ── WebSocket ─────────────────────────────────────────────────
active_tickers   = set(CORE_WATCHLIST[:50]) | set(MARKET_TAPE.keys()) | set(TICKER_DISPLAY.keys())
active_lock      = threading.RLock()
broadcast_thread = None
stop_event       = threading.Event()


# ── Binance WebSocket for real-time crypto prices ─────────────
_BINANCE_MAP = {
    'BTC-USD':'btcusdt','ETH-USD':'ethusdt','SOL-USD':'solusdt',
    'BNB-USD':'bnbusdt','XRP-USD':'xrpusdt','ADA-USD':'adausdt',
    'DOGE-USD':'dogeusdt','AVAX-USD':'avaxusdt','DOT-USD':'dotusdt',
    'LINK-USD':'linkusdt','MATIC-USD':'maticusdt','LTC-USD':'ltcusdt',
    'BCH-USD':'bchusdt','UNI-USD':'uniusdt','ATOM-USD':'atomusdt',
    'NEAR-USD':'nearusdt',
}
_BINANCE_REV = {v: k for k, v in _BINANCE_MAP.items()}  # btcusdt → BTC-USD

def _binance_ws_thread():
    """Connects to Binance WebSocket and updates price cache in real-time."""
    import websocket, json as _json2
    streams = "/".join(f"{s}@miniTicker" for s in _BINANCE_MAP.values())
    ws_url  = f"wss://stream.binance.com:9443/stream?streams={streams}"
    
    def on_message(ws, message):
        try:
            outer = _json2.loads(message)
            d = outer.get("data", outer)
            sym = d.get("s","").lower()  # e.g. "btcusdt"
            our_ticker = _BINANCE_REV.get(sym)
            if not our_ticker: return
            price = sf(float(d["c"]), 2)   # current price
            prev  = sf(float(d["o"]), 2)   # 24h open (use as prev close)
            chg   = round(price - prev, 2) if price and prev else None
            chgp  = round(chg / prev * 100, 2) if chg and prev else None
            entry = {
                "ticker": our_ticker,
                "label":  our_ticker.replace("-USD",""),
                "price":  price, "prev_close": prev,
                "change": chg, "change_pct": chgp,
                "volume": si(float(d.get("v",0))),
                "provider": "binance_ws",
                "ts": datetime.now().strftime("%H:%M:%S"),
                "error": None,
            }
            cache_set(f"price:{our_ticker}", entry)
            # Emit immediately to all connected clients
            try:
                socketio.emit("price_update", {
                    "prices": [_clean(entry)],
                    "ts": entry["ts"]
                })
            except Exception: pass
        except Exception: pass

    def on_error(ws, error):
        time.sleep(5)  # wait before reconnecting

    def on_close(ws, *args):
        time.sleep(5)  # reconnect after close

    while not stop_event.is_set():
        try:
            ws = websocket.WebSocketApp(
                ws_url,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception:
            pass
        if stop_event.is_set(): break
        time.sleep(5)  # reconnect delay


def broadcast_loop():
    while not stop_event.is_set():
        try:
            # Always include tape tickers regardless of active_tickers
            tape_ticks = list(MARKET_TAPE.keys())
            with active_lock: user_ticks = list(active_tickers)[:50]
            ticks = list(dict.fromkeys(tape_ticks + user_ticks))[:70]
            normals  = [t for t in ticks if not is_special(t)]
            specials = [t for t in ticks if is_special(t)]
            prices   = []
            # Check cache first (fresh = within TTL, stale = up to 60s)
            cached = [cache_get(f"price:{t}", TTL_PRICE) for t in ticks]
            stale  = [cache_get(f"price:{t}", 60) for t in ticks]  # up to 60s stale
            prices += [_clean(v) for v in cached if v is not None]
            need = [ticks[i] for i,v in enumerate(cached) if v is None]
            # Emit stale prices immediately so UI never shows "—" for long
            stale_fill = [_clean(s) for i,s in enumerate(stale) if s is not None and cached[i] is None]
            if stale_fill:
                socketio.emit("price_update", {"prices": stale_fill, "ts": datetime.now().strftime("%H:%M:%S")})
            if need:
                # v7 quote API — one request for all tickers (stocks + crypto + futures)
                try:
                    batch = _batch_download(need)
                    prices += [_clean(v) for v in batch.values() if v]
                    need = [t for t in need if t not in batch]
                except Exception: pass
            # Anything still missing: individual fetch with short timeout
            if need:
                with ThreadPoolExecutor(max_workers=min(8,len(need))) as ex:
                    fmap = {ex.submit(fetch_price, t): t for t in need}
                    done, _ = __import__('concurrent.futures').wait(
                        fmap.keys(), timeout=5,
                        return_when=__import__('concurrent.futures').ALL_COMPLETED
                    )
                    for fut in done:
                        try:
                            d = fut.result(timeout=1)
                            if d and d.get("price"):
                                prices.append(_clean(d))
                        except Exception: pass
            if prices:
                socketio.emit("price_update", {
                    "prices": prices,
                    "ts": datetime.now().strftime("%H:%M:%S")
                })
        except Exception:
            pass
        stop_event.wait(1)

def _prefetch_prices():
    """Imediatamente ao arranque: faz fetch de todos os tape tickers"""
    try:
        tape_ticks = list(MARKET_TAPE.keys())
        batch = _batch_download(tape_ticks)
        if batch:
            socketio.emit("price_update", {
                "prices": [_clean(v) for v in batch.values() if v],
                "ts": datetime.now().strftime("%H:%M:%S")
            })
    except Exception: pass

def ensure_broadcast():
    global broadcast_thread
    if broadcast_thread is None or not broadcast_thread.is_alive():
        stop_event.clear()
        broadcast_thread = threading.Thread(target=broadcast_loop,daemon=True)
        broadcast_thread.start()
        # Pre-fetch tape prices immediately on first connect
        threading.Thread(target=_prefetch_prices, daemon=True).start()
        # Start Binance WebSocket for real-time crypto prices
        try:
            import websocket as _ws_test
            _binance_thread = threading.Thread(target=_binance_ws_thread,daemon=True)
            _binance_thread.start()
        except ImportError:
            pass  # websocket-client not installed, crypto via polling

@socketio.on("connect")
def on_connect(): ensure_broadcast(); emit("connected",{"ok":True})

@socketio.on("subscribe")
def on_subscribe(data):
    ticks = data.get("tickers",[]) if isinstance(data,dict) else []
    with active_lock:
        for t in ticks:
            if str(t).strip(): active_tickers.add(str(t).upper().strip())
    emit("subscribed",{"tickers":ticks})
    # Immediately send back cached prices (fresh or stale up to 5min)
    instant = []
    missing = []
    for t in ticks:
        cached = cache_get(f"price:{t.upper()}", 300)  # up to 5min stale
        if cached and cached.get("price"):
            instant.append(cached)
        else:
            missing.append(t.upper())
    if instant:
        emit("price_update", {"prices": instant, "ts": datetime.now().strftime("%H:%M:%S")})
    # Fetch truly missing tickers immediately in background
    if missing:
        def _fetch_missing():
            batch = _batch_download(missing)
            if batch:
                socketio.emit("price_update", {
                    "prices": [_clean(v) for v in batch.values() if v],
                    "ts": datetime.now().strftime("%H:%M:%S")
                })
        threading.Thread(target=_fetch_missing, daemon=True).start()

# ── Routes ────────────────────────────────────────────────────
@app.route("/")
def home():
    return render_template("landing.html")

@app.route("/pricing")
def page_pricing():
    html = _TEMPLATES.get("pricing.html","")
    return Response(html, mimetype="text/html")

@app.route("/terminal")
@login_required
def page_terminal():
    return render_template("terminal.html")

@app.route("/auth")
def page_auth():
    return render_template("auth.html")

@app.route("/chart")
@login_required
def page_chart():
    return render_template("index.html")

@app.route("/financials")
@login_required
def page_financials():
    return render_template("financials.html")

@app.route("/metrics")
@login_required
def page_metrics():
    return render_template("metrics.html")

@app.route("/insider")
@login_required
def page_insider():
    return render_template("insider.html")

@app.route("/congress")
@login_required
def page_congress():
    return render_template("congress.html")

@app.route("/fairvalue")
@login_required
def page_fairvalue():
    return render_template("fairvalue.html")

@app.route("/news")
@login_required
def page_news():
    return render_template("news.html")



@app.route("/livefeed")
@login_required
def page_livefeed():
    return render_template("livefeed.html")

@app.route("/crypto")
@login_required
def page_crypto():
    return render_template("crypto.html")

@app.route("/commodity")
@login_required
def page_commodity():
    return render_template("commodity.html")

@app.route("/api/crypto_info/<ticker>")
def api_crypto_info(ticker):
    ticker = ticker.upper().strip()
    result = {"ticker": ticker, "asset_type": "crypto"}
    try:
        fg = requests.get("https://api.alternative.me/fng/?limit=7&format=json", timeout=6)
        if fg.ok:
            data = fg.json().get("data", [])
            if data:
                latest = data[0]
                result["fear_greed"] = {
                    "value": int(latest.get("value", 0)),
                    "label": latest.get("value_classification", ""),
                    "history": [{"value":int(d["value"]),"label":d["value_classification"],"date":d["timestamp"]} for d in data],
                }
    except: pass
    try:
        cg = requests.get("https://api.coingecko.com/api/v3/global", timeout=6)
        if cg.ok:
            d = cg.json().get("data", {})
            result["market"] = {
                "btc_dominance": round(d.get("market_cap_percentage",{}).get("btc",0),1),
                "eth_dominance": round(d.get("market_cap_percentage",{}).get("eth",0),1),
                "total_market_cap_usd": d.get("total_market_cap",{}).get("usd"),
                "market_cap_change_24h": round(d.get("market_cap_change_percentage_24h_usd",0),2),
            }
    except: pass
    try:
        bh = requests.get("https://mempool.space/api/blocks/tip/height", timeout=5)
        if bh.ok:
            current_block = int(bh.text.strip())
            next_halving  = 1050000
            blocks_left   = max(0, next_halving - current_block)
            days_left     = round(blocks_left * 10 / 60 / 24)
            from datetime import date, timedelta
            result["halving"] = {
                "current_block": current_block,
                "next_halving_block": next_halving,
                "blocks_remaining": blocks_left,
                "days_estimate": days_left,
                "next_date_est": (date.today() + timedelta(days=days_left)).isoformat(),
                "last_halving": "2024-04-19",
                "reward_after": "3.125 BTC per block",
                "note": "Estimativa baseada em ~10 min/bloco.",
            }
    except: pass
    COIN_IDS = {
        "BTC-USD":"bitcoin","ETH-USD":"ethereum","SOL-USD":"solana","BNB-USD":"binancecoin",
        "XRP-USD":"ripple","ADA-USD":"cardano","DOGE-USD":"dogecoin","AVAX-USD":"avalanche-2",
        "DOT-USD":"polkadot","LINK-USD":"chainlink","MATIC-USD":"matic-network",
    }
    coin_id = COIN_IDS.get(ticker)
    if coin_id:
        try:
            cr = requests.get(f"https://api.coingecko.com/api/v3/coins/{coin_id}?localization=false&tickers=false&community_data=true&developer_data=false", timeout=8)
            if cr.ok:
                cd = cr.json(); md = cd.get("market_data",{})
                result["coin"] = {
                    "name": cd.get("name"), "symbol": cd.get("symbol","").upper(),
                    "description": (cd.get("description",{}).get("en","") or "")[:400],
                    "ath": md.get("ath",{}).get("usd"),
                    "ath_change_pct": md.get("ath_change_percentage",{}).get("usd"),
                    "ath_date": (md.get("ath_date",{}).get("usd","") or "")[:10],
                    "market_cap_rank": cd.get("market_cap_rank"),
                    "market_cap": md.get("market_cap",{}).get("usd"),
                    "volume_24h": md.get("total_volume",{}).get("usd"),
                    "price_change_7d":  round(md.get("price_change_percentage_7d",0),2),
                    "price_change_30d": round(md.get("price_change_percentage_30d",0),2),
                    "price_change_1y":  round(md.get("price_change_percentage_1y",0),2),
                    "circulating_supply": md.get("circulating_supply"),
                    "max_supply": md.get("max_supply"),
                    "supply_pct": round(md.get("circulating_supply",0)/md.get("max_supply",1)*100,1) if md.get("max_supply") else None,
                }
        except: pass
    return jsonify(result)

@app.route("/api/commodity_info/<path:ticker>")
def api_commodity_info(ticker):
    ticker = ticker.upper().strip()
    INFO = {
        "GC=F":{"name":"Gold","unit":"USD/oz","drivers":["USD strength (inverso)","Inflação e juros reais","Compras de bancos centrais","Risco geopolítico"],"seasonal":"Mais forte Set-Nov, mais fraco Mar-Abr"},
        "SI=F":{"name":"Silver","unit":"USD/oz","drivers":["Demanda industrial (solar, electrónica)","Correlação com ouro","Oferta mineira"],"seasonal":"Similar ao ouro"},
        "CL=F":{"name":"WTI Crude Oil","unit":"USD/bbl","drivers":["Decisões OPEC+","Produção shale EUA","EIA Inventory (Qua 15:30 ET)","Risco geopolítico"],"seasonal":"Mais forte Primavera/Verão, mais fraco Nov-Jan"},
        "BZ=F":{"name":"Brent Crude Oil","unit":"USD/bbl","drivers":["Igual WTI + prémio europeu/asiático","Suez Canal","Tensões Médio Oriente"],"seasonal":"Igual ao WTI"},
        "NG=F":{"name":"Natural Gas","unit":"USD/MMBtu","drivers":["Temperatura (aquecimento/arrefecimento)","EIA Storage (Qui 14:30 ET)","Exportações LNG"],"seasonal":"Muito sazonal: forte Nov-Fev, fraco Abr-Mai"},
        "HG=F":{"name":"Copper","unit":"USD/lb","drivers":["PMI Manufactureiro China","Transição energética (EVs, solar)","Disruções mineiras"],"seasonal":"Mais forte T1-T2 (reabastecimento China)"},
        "ZC=F":{"name":"Corn","unit":"USD/bushel","drivers":["Relatórios USDA","Colheitas Brasil/Argentina","Demanda etanol","Importações China"],"seasonal":"Volátil em plantio (Abr-Mai) e colheita (Set-Out)"},
        "ZW=F":{"name":"Wheat","unit":"USD/bushel","drivers":["Oferta Mar Negro","Condições trigo inverno EUA","Demanda global alimentar"],"seasonal":"Volátil em época de colheita"},
        "PL=F":{"name":"Platinum","unit":"USD/oz","drivers":["Emissões automóvel (catalisadores)","Oferta África do Sul","Substituição por paládio"],"seasonal":"Varia com ciclo auto"},
    }
    info = INFO.get(ticker, {"name":ticker,"unit":"USD","drivers":[],"seasonal":""})
    result = {"ticker":ticker,"asset_type":"commodity",**info}
    try:
        px = fetch_price(ticker)
        result.update({"price":px.get("price"),"change_pct":px.get("change_pct")})
    except: pass
    return jsonify(result)

@app.route("/api/health")
def api_health():
    return jsonify({"ok": True, "ts": datetime.now().isoformat(timespec="seconds")})

@app.route("/api/universe")
def api_universe():
    q = request.args.get("q","").upper().strip()
    limit = min(int(request.args.get("limit",40)),200)
    rows = load_universe()
    if q:
        # Priority 1: exact ticker match
        exact = [x for x in rows if x["ticker"] == q]
        # Priority 2: ticker starts with q
        starts = [x for x in rows if x["ticker"].startswith(q) and x not in exact]
        # Priority 3: company name contains full q
        name_full = [x for x in rows if q in x.get("name","").upper() and x not in exact and x not in starts]
        # Priority 4: any word in company name starts with q (for partial searches)
        name_partial = []
        if len(q) >= 3:
            for x in rows:
                if x in exact or x in starts or x in name_full: continue
                words = x.get("name","").upper().split()
                if any(w.startswith(q) for w in words):
                    name_partial.append(x)
        rows = exact + starts + name_full + name_partial
        # Inject TICKER_NAMES/TICKER_DISPLAY only for ticker matches (not name)
        # This prevents "co" from returning ^IXIC (NASDAQ Composite) etc.
        for t, name in {**TICKER_NAMES, **TICKER_DISPLAY}.items():
            if (q == t or t.startswith(q) or q == t.replace("^","").replace("=F","")) and not any(r["ticker"]==t for r in rows):
                rows.insert(0, {"ticker":t,"name":name,"exchange":"SPECIAL"})
    return jsonify({"count":len(rows),"results":rows[:limit]})

@app.route("/api/watchlist")
def api_watchlist():
    raw   = request.args.get("tickers",",".join(CORE_WATCHLIST[:70]))
    ticks = [x.strip().upper() for x in raw.split(",") if x.strip()][:150]
    rows  = fetch_prices_batch(ticks)
    rows.sort(key=lambda x:(x.get("change_pct") is not None, x.get("change_pct") or -999),reverse=True)
    return jsonify({"count":len(rows),"stocks":rows,"provider":"batch"})

@app.route("/api/stock_fast/<path:ticker>")
def api_stock_fast(ticker):
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(quick_stock, ticker.upper())
            result = fut.result(timeout=7)
    except Exception:
        t = ticker.upper()
        result = {"ticker":t,"name":TICKER_NAMES_STATIC.get(t,t),"price":None,"change_pct":None,"error":"timeout"}
    return jsonify(result)

@app.route("/api/stock/<path:ticker>")
def api_stock(ticker):
    with active_lock: active_tickers.add(ticker.upper())
    try:
        return jsonify(build_stock(ticker,include_sec=False))
    except Exception as e:
        return jsonify({"ticker":ticker.upper(),"error":str(e),"price":None}), 200

@app.route("/api/full/<path:ticker>")
def api_full(ticker):
    """Single endpoint: price + fundamentals + fair value + insider count + congress."""
    ticker = ticker.upper().strip()
    with active_lock: active_tickers.add(ticker)
    cached = cache_get(f"build:{ticker}", TTL_INFO)
    if cached: return jsonify({**cached, "_cached": True})
    # Parallel fetch with individual timeouts
    with ThreadPoolExecutor(max_workers=3) as ex:
        f_px   = ex.submit(fetch_price, ticker)
        f_fund = ex.submit(fundamentals, ticker)
        f_cong = ex.submit(get_congress, ticker)
    try: px   = f_px.result(timeout=4)
    except: px = {}
    try: fund = f_fund.result(timeout=6)
    except: fund = {}
    try: cong = f_cong.result(timeout=3)
    except: cong = {}
    sec  = {"cik":None,"sec_url":None,"error":None,"filings":[]}
    # Quick SEC filing count from cache only (no network)
    sec_cached = cache_get(f"sec:{ticker}", TTL_SEC)
    if sec_cached: sec = sec_cached
    score, breakdown = score_summary(px, fund, sec, cong)
    price  = px.get("price")
    target = fund.get("target_mean")
    upside = round((target-price)/price*100,1) if price and target else None
    ed = None
    if fund.get("earnings_timestamp"):
        try: ed = datetime.fromtimestamp(fund["earnings_timestamp"]).strftime("%Y-%m-%d")
        except: pass
    atype = detect_asset_type(ticker, fetch_info(ticker))
    fv = compute_fair_value(fund, asset_type=atype)
    result = {"ticker":ticker,**fund,**px,"upside":upside,"earnings_date":ed,
              "cik":sec.get("cik"),"sec_url":sec.get("sec_url"),
              "insider_trade_count":len(sec.get("filings",[])),
              "fair_value":fv.get("composite"),"fair_value_models":fv.get("models",{}),"fair_value_note":fv.get("note"),"asset_type":atype,
              "congress_buys":cong.get("buy_count",0),"congress_members":cong.get("members",[]),
              "signal_score":score,"score_breakdown":breakdown,"_cached":False}
    cache_set(f"build:{ticker}", result)
    return jsonify(result)

@app.route("/api/insider/<path:ticker>")
def api_insider(ticker):
    # Hard timeout: never block the server
    import concurrent.futures as _cf
    try:
        with _cf.ThreadPoolExecutor(max_workers=1) as _ex:
            _fut = _ex.submit(sec_form4_deep, ticker)
            sec = _fut.result(timeout=22)
    except Exception:
        sec = sec_form4(ticker)  # fallback to basic (no XML parse)
        sec["trades"] = []
    cong = get_congress(ticker)
    trades = sec.get("trades",[])
    seen,deduped = set(),[]
    for t in trades:
        k = (t.get("date"),t.get("owner"),t.get("action"),round(float(t.get("value") or 0),0))
        if k not in seen: seen.add(k); deduped.append(t)
    deduped.sort(key=lambda x:x.get("value") or 0,reverse=True)
    return jsonify({"cik":sec.get("cik"),"sec_url":sec.get("sec_url"),"sec_error":sec.get("error"),
                    "insider_trades":deduped,"insider_trade_count":len(deduped),"congress":cong})

@app.route("/api/fairvalue/<path:ticker>")
def api_fairvalue(ticker):
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(fair_value, ticker.upper())
            result = fut.result(timeout=12)
    except Exception:
        result = {"error": "timeout"}
    return jsonify(result)

@app.route("/api/congress/<path:ticker>")
def api_congress(ticker):
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(get_congress, ticker.upper())
            result = fut.result(timeout=8)
    except Exception:
        result = {"trades": [], "error": "timeout"}
    return jsonify(result)

@app.route("/api/congress/top")
def api_congress_top(): return jsonify({"top":[{"ticker":t,**d} for t,d in congress_top()]})

@app.route("/api/debug/financials/<path:ticker>")
def api_debug_financials(ticker):
    """Quick debug: test each statements source independently."""
    import time
    results = {}
    ticker = ticker.upper().strip()
    # Test yfinance
    t0 = time.time()
    try:
        import yfinance as yf
        tk = yf.Ticker(ticker)
        df = getattr(tk, 'income_stmt', None)
        if df is not None and not getattr(df, 'empty', True):
            results['yfinance'] = {'ok': True, 'rows': len(df), 'time': round(time.time()-t0,2)}
        else:
            results['yfinance'] = {'ok': False, 'error': 'empty df', 'time': round(time.time()-t0,2)}
    except Exception as e:
        results['yfinance'] = {'ok': False, 'error': str(e), 'time': round(time.time()-t0,2)}
    # Test FMP
    t0 = time.time()
    try:
        r = requests.get(f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?limit=1&apikey={FMP_API_KEY}", timeout=5)
        results['fmp'] = {'ok': r.ok, 'status': r.status_code, 'time': round(time.time()-t0,2), 'rows': len(r.json()) if r.ok else 0}
    except Exception as e:
        results['fmp'] = {'ok': False, 'error': str(e), 'time': round(time.time()-t0,2)}
    return jsonify(results)

@app.route("/api/statements/<path:ticker>")
def api_statements(ticker):
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(statements, ticker)
            result = fut.result(timeout=25)
    except Exception:
        result = None
    if result is None:
        return jsonify({"error":"timeout"}), 503
    # Return 503 if we got empty data (all sources failed)
    if not result.get("income") and not result.get("balance"):
        result["error"] = "Dados indisponível — todas as fontes falharam"
        return jsonify(result), 503
    return jsonify(result)

@app.route("/api/chart/<path:ticker>")
def api_chart(ticker):
    period     = request.args.get("period","1y")
    overlays   = [x.strip() for x in request.args.get("overlays","SP500").split(",") if x.strip()]
    normalized = request.args.get("normalized","1") != "0"
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(chart_series, ticker, period, overlays, normalized)
            series = fut.result(timeout=18)
    except Exception:
        series = {}
    return jsonify({"series":series,"period":period,"normalized":normalized})

@app.route("/api/earnings/<path:ticker>")
def api_earnings(ticker):
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(earnings_detail, ticker.upper())
            result = fut.result(timeout=10)
    except Exception:
        result = {"history": [], "upcoming": None, "error": "timeout"}
    return jsonify(result)

@app.route("/api/news/<path:ticker>")
def api_news(ticker): return jsonify(news(ticker))

@app.route("/api/macro_news")
def api_macro_news():
    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(macro_news)
            result = fut.result(timeout=10)
    except Exception:
        result = {"articles": [], "error": "timeout"}
    return jsonify(result)


@app.route("/api/insider_realtime")
def api_insider_realtime():
    """Latest Form 4 filings from SEC EDGAR — last 48h."""
    try:
        action = request.args.get("action","").upper().strip()
        limit  = min(int(request.args.get("limit", 80)), 200)
        feed_url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&dateb=&owner=include&count=40&search_text=&output=atom"
        r = requests.get(feed_url, headers=SEC_HEADERS, timeout=10)
        if not r.ok:
            return jsonify({"count":0,"trades":[],"note":"SEC feed unavailable","last_updated":None})
        feed = feedparser.parse(r.text)
        cutoff = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
        trades = []
        def parse_entry(entry):
            result = []
            try:
                link    = getattr(entry,"link","")
                updated = getattr(entry,"updated","")[:10]
                if updated < cutoff: return result
                ri = requests.get(link, headers=SEC_HEADERS, timeout=5)
                if not ri.ok: return result
                xml_links = re.findall(r'href="(/Archives/edgar/data/[^"]+[.]xml)"', ri.text)
                if not xml_links: return result
                rx = requests.get("https://www.sec.gov"+xml_links[0], headers=SEC_HEADERS, timeout=5)
                if not rx.ok or "ownershipDocument" not in rx.text: return result
                root = ET.fromstring(rx.text)
                def xt(node,p):
                    x=node.find(p); return x.text.strip() if x is not None and x.text else None
                issuer_ticker = (xt(root,".//issuer/issuerTradingSymbol") or "").upper()
                issuer_name   = xt(root,".//issuer/issuerName") or ""
                owner_name    = xt(root,".//reportingOwner/reportingOwnerId/rptOwnerName") or "Unknown"
                owner_cik     = xt(root,".//reportingOwner/reportingOwnerId/rptOwnerCik") or ""
                title         = xt(root,".//reportingOwner/reportingOwnerRelationship/officerTitle") or ""
                is_dir        = xt(root,".//reportingOwner/reportingOwnerRelationship/isDirector")=="1"
                is_off        = xt(root,".//reportingOwner/reportingOwnerRelationship/isOfficer")=="1"
                is_10p        = xt(root,".//reportingOwner/reportingOwnerRelationship/isTenPercentOwner")=="1"
                relation      = title or ("Director" if is_dir else "Officer" if is_off else "10% Owner" if is_10p else "Insider")
                for tx in root.findall(".//nonDerivativeTransaction"):
                    code   = xt(tx,".//transactionCoding/transactionCode")
                    if code not in ("P","S"): continue
                    shares = sf(xt(tx,".//transactionAmounts/transactionShares/value"))
                    price  = sf(xt(tx,".//transactionAmounts/transactionPricePerShare/value"))
                    date   = xt(tx,".//transactionDate/value") or updated
                    shares_after = sf(xt(tx,".//postTransactionAmounts/sharesOwnedFollowingTransaction/value"))
                    if not shares or not price: continue
                    value = shares * price
                    if value < MIN_TRADE_VALUE: continue
                    result.append({"filing_date":updated,"trade_date":date,"ticker":issuer_ticker,
                        "company":issuer_name,"owner":owner_name,"owner_cik":owner_cik,
                        "relation":relation,"action":"BUY" if code=="P" else "SELL",
                        "shares":round(shares,0),"price":round(price,4),"value":round(value,2),
                        "shares_after":round(shares_after,0) if shares_after else None,
                        "filing_url":link})
            except: pass
            return result
        with ThreadPoolExecutor(max_workers=6) as ex:
            for fut in as_completed({ex.submit(parse_entry,e):e for e in feed.entries[:40]},timeout=40):
                try: trades.extend(fut.result())
                except: pass
        trades.sort(key=lambda x:(x.get("filing_date",""),x.get("value") or 0),reverse=True)
        if action: trades=[t for t in trades if t.get("action")==action]
        return jsonify({"count":len(trades),"trades":trades[:limit],
                        "last_updated":datetime.now().isoformat(),
                        "note":"SEC Form 4: insiders têm até 2 dias úteis para reportar."})
    except Exception as e:
        return jsonify({"count":0,"trades":[],"error":str(e),"last_updated":None})

@app.route("/api/insider_photo/<path:name>")
def api_insider_photo(name):
    """Photo search: Wikipedia → DuckDuckGo → LinkedIn Google → UI-Avatars fallback."""
    key = f"photo:{name[:80]}"
    cached = cache_get(key, 86400*7)
    if cached: return jsonify(cached)
    
    name = name.strip()
    parts = name.split()
    
    # ── 1. Wikipedia: try name + reverse ──────────────────────────────────
    try:
        searches = [name]
        if len(parts) >= 2:
            searches.append(f"{parts[-1]} {parts[0]}")
        for sq in searches[:2]:
            r = requests.get("https://en.wikipedia.org/w/api.php", params={
                "action":"query","list":"search","srsearch":sq,
                "srlimit":5,"format":"json","srnamespace":0
            }, headers=SEC_HEADERS, timeout=5)
            if not r.ok: continue
            for hit in r.json().get("query",{}).get("search",[])[:4]:
                title = hit.get("title","")
                if any(x in title.lower() for x in ["disambiguation","list ","company","inc.","corp","llc"]): continue
                nw = {w.lower() for w in name.split() if len(w)>2}
                tw = {w.lower() for w in title.split()}
                if not nw & tw: continue
                r2 = requests.get("https://en.wikipedia.org/w/api.php", params={
                    "action":"query","titles":title,"prop":"pageimages",
                    "pithumbsize":300,"format":"json","pilicense":"any"
                }, headers=SEC_HEADERS, timeout=4)
                if not r2.ok: continue
                for pg in r2.json().get("query",{}).get("pages",{}).values():
                    thumb = (pg.get("thumbnail") or {}).get("source","")
                    if thumb and len(thumb)>20 and "logo" not in thumb.lower():
                        result = {"url":thumb,"source":"wikipedia","name":name}
                        cache_set(key, result); return jsonify(result)
    except: pass
    
    # ── 2. DuckDuckGo instant answer ──────────────────────────────────────
    try:
        r = requests.get("https://api.duckduckgo.com/", params={
            "q":name,"format":"json","no_html":1,"skip_disambig":1
        }, headers=SEC_HEADERS, timeout=4)
        if r.ok:
            d = r.json()
            img = d.get("Image","") or ""
            if img.startswith("http") and len(img)>20:
                result = {"url":img,"source":"duckduckgo","name":name}
                cache_set(key, result); return jsonify(result)
    except: pass
    
    # ── 3. Google image search via scraping (LinkedIn/company photos) ──────
    try:
        company_hint = ""  # Could add company name here if available
        query = f"{name} executive linkedin photo"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml",
        }
        r = requests.get(
            f"https://www.google.com/search?q={quote_plus(query)}&tbm=isch&tbs=itp:face",
            headers=headers, timeout=6
        )
        if r.ok:
            # Extract first image URL from Google Images response
            imgs = re.findall(r'"(https://[^"]+\.(?:jpg|jpeg|png))"', r.text)
            # Filter for likely person photos (not logos, not generic)
            for img in imgs[:5]:
                if any(x in img.lower() for x in ['logo','icon','brand','stock']): continue
                if 'media.licdn' in img or 'pbs.twimg' in img or 'photos' in img:
                    result = {"url":img,"source":"google","name":name}
                    cache_set(key, result); return jsonify(result)
            # Take first valid image
            for img in imgs[:3]:
                if len(img)>20 and 'logo' not in img.lower():
                    result = {"url":img,"source":"google","name":name}
                    cache_set(key, result); return jsonify(result)
    except: pass
    
    # ── 4. UI-Avatars fallback (always works) ─────────────────────────────
    result = {
        "url": f"https://ui-avatars.com/api/?name={quote_plus(name)}&size=200&background=1a2332&color=00d68f&bold=true&format=svg",
        "source": "generated", "name": name
    }
    cache_set(key, result)
    return jsonify(result)


@app.route("/favicon.ico")
@app.route("/favicon.svg")
def favicon():
    svg = ('<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32">'
           '<rect width="32" height="32" rx="6" fill="#080b0f" stroke="#00e5a0" stroke-width="1.5"/>'
           '<polyline points="4,22 10,14 16,18 22,9 28,11" stroke="#00e5a0" stroke-width="2.5"'
           ' fill="none" stroke-linecap="round" stroke-linejoin="round"/>'
           '<circle cx="22" cy="9" r="2.5" fill="#00e5a0"/>'
           '</svg>')
    return Response(svg, mimetype="image/svg+xml",
                    headers={"Cache-Control": "public, max-age=86400"})


# ── AUTH ──────────────────────────────────────────────────────
import hashlib, secrets, urllib.request as _ureq, json as _json_auth
from datetime import datetime as _dt, timedelta as _td
import threading as _threading

# ── PostgreSQL ──
def _get_db():
    import psycopg2
    url = os.getenv("DATABASE_URL", "")
    if not url: raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(url, sslmode="require")

def _init_db():
    try:
        conn = _get_db(); cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS ist_users (email TEXT PRIMARY KEY, name TEXT NOT NULL, pw_hash TEXT NOT NULL, created_at TIMESTAMPTZ DEFAULT NOW(), verified BOOLEAN DEFAULT TRUE)")
        conn.commit(); cur.close(); conn.close()
        print("[DB] ist_users ready")
    except Exception as e:
        print(f"[DB] Init error: {e}")

def _db_user_exists(email):
    try:
        conn=_get_db(); cur=conn.cursor()
        cur.execute("SELECT 1 FROM ist_users WHERE email=%s",(email,))
        r=cur.fetchone() is not None; cur.close(); conn.close(); return r
    except: return False

def _db_get_user(email):
    try:
        conn=_get_db(); cur=conn.cursor()
        cur.execute("SELECT email,name,pw_hash FROM ist_users WHERE email=%s",(email,))
        row=cur.fetchone(); cur.close(); conn.close()
        return {"email":row[0],"name":row[1],"pw_hash":row[2]} if row else None
    except: return None

def _db_create_user(email, name, pw_hash):
    conn=_get_db(); cur=conn.cursor()
    cur.execute("INSERT INTO ist_users (email,name,pw_hash) VALUES (%s,%s,%s)",(email,name,pw_hash))
    conn.commit(); cur.close(); conn.close()

def _db_list_users():
    try:
        conn=_get_db(); cur=conn.cursor()
        cur.execute("SELECT email,name,created_at FROM ist_users ORDER BY created_at DESC")
        rows=cur.fetchall(); cur.close(); conn.close()
        return [{"email":r[0],"name":r[1],"created":str(r[2])} for r in rows]
    except Exception as e: return []

_threading.Thread(target=_init_db, daemon=True).start()

# ── Password hashing ──
def _hash_pw(password, salt=None):
    if salt is None: salt = secrets.token_hex(32)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 600_000)
    return salt + ":" + dk.hex()

def _verify_pw(password, stored):
    try:
        salt,_=stored.split(":",1)
        return secrets.compare_digest(stored, _hash_pw(password, salt))
    except: return False

# ── Pending codes ──
_PENDING = {}; _PENDING_LOCK = _threading.Lock()

def _send_code_email(to_email, name, code, is_login=False):
    """Send 6-digit code via Brevo (Sendinblue) SMTP API — no domain needed."""
    api_key = os.getenv("BREVO_API_KEY", "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")
    if not api_key:
        print(f"[AUTH] BREVO_API_KEY not set. Code for {to_email}: {code}")
        return False
    subject = f"{code} — Código de login IST" if is_login else f"{code} — Verificação IST"
    action  = "fazer login" if is_login else "criar conta"
    html = (
        "<!DOCTYPE html><html><body style=\"background:#040608;color:#c8d8e8;font-family:monospace;padding:40px\">"
        "<table width=\"100%\"><tr><td align=\"center\"><table width=\"440\" style=\"background:#070c10;border:1px solid #152030;border-radius:10px;overflow:hidden\">"
        "<tr><td style=\"padding:24px 32px;border-bottom:1px solid #152030\"><span style=\"font-size:20px;font-weight:900;color:#c8d8e8\">IST</span>"
        "<span style=\"font-size:11px;color:#5a7a90;margin-left:10px\">Insider Signal Terminal</span></td></tr>"
        "<tr><td style=\"padding:32px\">"
        f"<p style=\"color:#5a7a90;margin:0 0 8px\">Olá {name},</p>"
        f"<p style=\"color:#c8d8e8;margin:0 0 24px\">O teu código para {action}:</p>"
        "<div style=\"background:#0a1018;border:1px solid #1c2d40;border-radius:8px;padding:24px;text-align:center;margin-bottom:24px\">"
        f"<span style=\"font-size:42px;font-weight:700;letter-spacing:14px;color:#00e5a0\">{code}</span></div>"
        "<p style=\"font-size:11px;color:#364860\">Expira em <b style=\"color:#5a7a90\">10 minutos</b>.</p>"
        "</td></tr><tr><td style=\"padding:14px 32px;border-top:1px solid #152030\">"
        "<p style=\"font-size:10px;color:#243040\">© IST · Insider Signal Terminal</p>"
        "</td></tr></table></td></tr></table></body></html>"
    )
    try:
        import urllib.request as _ur, json as _jb
        body = _jb.dumps({
            "sender": {"name": "IST Terminal", "email": "noreply@ist-terminal.com"},
            "to": [{"email": to_email, "name": name}],
            "subject": subject,
            "htmlContent": html
        }).encode()
        req = _ur.Request(
            "https://api.brevo.com/v3/smtp/email",
            data=body,
            headers={"api-key": api_key, "Content-Type": "application/json", "Accept": "application/json"},
            method="POST"
        )
        with _ur.urlopen(req, timeout=8) as resp:
            r = _jb.loads(resp.read())
            ok = bool(r.get("messageId"))
            if ok: print(f"[AUTH] Email sent to {to_email} id={r['messageId']}")
            return ok
    except Exception as e:
        print(f"[AUTH] Brevo failed: {e}")
        return False


def _issue_code(email, name, pw_hash, code_type):
    with _PENDING_LOCK:
        ex = _PENDING.get(email)
        if ex and ex["expires"] > _dt.utcnow() + _td(minutes=9):
            return None, "Código já enviado. Aguarda 1 minuto."
    code = str(secrets.randbelow(900000) + 100000)
    with _PENDING_LOCK:
        _PENDING[email] = {"code":code,"name":name,"pw_hash":pw_hash,
            "expires":_dt.utcnow()+_td(minutes=10),"attempts":0,"type":code_type}
    return code, None

def _pop_pending(email, code):
    with _PENDING_LOCK:
        p = _PENDING.get(email)
        if not p: return None, "Nenhum código pendente. Começa de novo."
        if _dt.utcnow() > p["expires"]:
            del _PENDING[email]; return None, "Código expirado. Começa de novo."
        p["attempts"] += 1
        if p["attempts"] > 5:
            del _PENDING[email]; return None, "Demasiadas tentativas. Começa de novo."
        if not secrets.compare_digest(p["code"], code):
            return None, f'Código incorrecto. {5-p["attempts"]} tentativa(s).'
        r = dict(p); del _PENDING[email]; return r, None

# ── REGISTER step 1 ──
@app.route("/api/auth/send-code", methods=["POST"])
def api_auth_send_code():
    d=request.get_json(silent=True) or {}
    name=(d.get("name") or "").strip(); email=(d.get("email") or "").strip().lower(); pw=(d.get("password") or "")
    if not name or not email or not pw: return jsonify({"ok":False,"error":"Preenche todos os campos."}),400
    if "@" not in email: return jsonify({"ok":False,"error":"Email inválido."}),400
    if len(pw)<8: return jsonify({"ok":False,"error":"Password mínimo 8 caracteres."}),400
    if _db_user_exists(email): return jsonify({"ok":False,"error":"Email já registado."}),409
    code,err=_issue_code(email,name,_hash_pw(pw),"register")
    if err: return jsonify({"ok":False,"error":err}),429
    if not _send_code_email(email,name,code,False):
        with _PENDING_LOCK: _PENDING.pop(email,None)
        return jsonify({"ok":False,"error":"Erro ao enviar email. Verifica o endereço."}),500
    return jsonify({"ok":True})

# ── REGISTER step 2 ──
@app.route("/api/auth/verify-code", methods=["POST"])
def api_auth_verify_code():
    d=request.get_json(silent=True) or {}
    email=(d.get("email") or "").strip().lower(); code=(d.get("code") or "").strip()
    if not email or not code: return jsonify({"ok":False,"error":"Campos em falta."}),400
    p,err=_pop_pending(email,code)
    if err: return jsonify({"ok":False,"error":err}),401
    if p.get("type")!="register": return jsonify({"ok":False,"error":"Código inválido."}),400
    try: _db_create_user(email,p["name"],p["pw_hash"])
    except Exception as e:
        if "unique" in str(e).lower() or "duplicate" in str(e).lower():
            return jsonify({"ok":False,"error":"Email já registado."}),409
        return jsonify({"ok":False,"error":"Erro ao criar conta."}),500
    session["user"]={"email":email,"name":p["name"]}
    return jsonify({"ok":True,"name":p["name"]})

# ── LOGIN step 1: password check + send 2FA ──
@app.route("/api/auth/login", methods=["POST"])
def api_auth_login():
    d=request.get_json(silent=True) or {}
    email=(d.get("email") or "").strip().lower(); pw=(d.get("password") or "")
    if not email or not pw: return jsonify({"ok":False,"error":"Preenche todos os campos."}),400
    user=_db_get_user(email)
    if not user or not _verify_pw(pw,user["pw_hash"]):
        return jsonify({"ok":False,"error":"Email ou password incorrectos."}),401
    code,err=_issue_code(email,user["name"],user["pw_hash"],"login")
    if err: return jsonify({"ok":False,"error":err}),429
    if not _send_code_email(email,user["name"],code,True):
        with _PENDING_LOCK: _PENDING.pop(email,None)
        return jsonify({"ok":False,"error":"Erro ao enviar código 2FA."}),500
    return jsonify({"ok":True,"step":"2fa"})

# ── LOGIN step 2: 2FA verify ──
@app.route("/api/auth/login-2fa", methods=["POST"])
def api_auth_login_2fa():
    d=request.get_json(silent=True) or {}
    email=(d.get("email") or "").strip().lower(); code=(d.get("code") or "").strip()
    if not email or not code: return jsonify({"ok":False,"error":"Campos em falta."}),400
    p,err=_pop_pending(email,code)
    if err: return jsonify({"ok":False,"error":err}),401
    if p.get("type")!="login": return jsonify({"ok":False,"error":"Código inválido."}),400
    user=_db_get_user(email)
    if not user: return jsonify({"ok":False,"error":"Utilizador não encontrado."}),404
    session["user"]={"email":email,"name":user["name"]}
    return jsonify({"ok":True,"name":user["name"]})

@app.route("/api/auth/logout", methods=["POST"])
def api_auth_logout():
    session.pop("user",None); return jsonify({"ok":True})

@app.route("/api/auth/me")
def api_auth_me():
    u=session.get("user")
    return jsonify({"ok":True,"user":u}) if u else (jsonify({"ok":False}),401)

@app.route("/api/auth/users")
def api_auth_users():
    if request.headers.get("X-Admin-Key","")!=os.getenv("ADMIN_KEY","ist-admin-2024"):
        return jsonify({"ok":False,"error":"Unauthorized"}),401
    return jsonify({"ok":True,"users":_db_list_users()})


if __name__ == "__main__":
    # Seed universe immediately (sync, fast — just CORE list)
    load_universe()
    # Heavy background tasks — don't block startup
    threading.Thread(target=sec_ticker_map,  daemon=True).start()
    threading.Thread(target=_load_congress,  daemon=True).start()
    print("\n"+"="*60)
    print("  INSIDER SIGNAL TERMINAL v4.5")
    print("  Arranque rápido · Waterfall · Hover charts")
    print(f"  → http://localhost:{APP_PORT}")
    print("="*60+"\n")
    print(f"[STARTUP] Binding to 0.0.0.0:{APP_PORT} (PORT env={os.getenv('PORT','unset')})", flush=True)
    socketio.run(app,host="0.0.0.0",port=APP_PORT,debug=False,allow_unsafe_werkzeug=True)

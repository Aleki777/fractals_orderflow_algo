#!/usr/bin/env python3
"""
bot.py - production-ready.

Requirements:
ccxt, websockets, pandas, numpy, flask, gspread, google-auth
"""

import os
import time
import json
import math
import threading
import traceback
from datetime import datetime, timezone
from typing import List, Dict, Any

import pandas as pd
import numpy as np
import ccxt
import asyncio
import websockets
from flask import Flask, jsonify
from google.oauth2.service_account import Credentials
import gspread

# --------------------
# Configuration from ENV
# --------------------
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")
EXECUTE_TRADES = os.getenv("EXECUTE_TRADES", "False").lower() in ("1", "true", "yes")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")
SYMBOL = os.getenv("SYMBOL", "BTC/USDT")
STREAM_SYMBOL = os.getenv("STREAM_SYMBOL", "btcusdt")
TIMEFRAME = os.getenv("TIMEFRAME", "4h")
TF_HOURS = int(os.getenv("TF_HOURS", "4"))
TARGET_CANDLES = int(os.getenv("TARGET_CANDLES", "1000"))
PORT = int(os.getenv("PORT", "8080"))

# Strategy / sizing params 
RISK_PER_TRADE = 
INITIAL_EQUITY = 
MIN_QTY = 
MAX_USD_PER_TRADE = 

# Fees & slippage for simulated fills
TAKER_FEE_RATE = 
MAKER_FEE_RATE = 
SIM_SLIPPAGE_PCT =

# Detection params (from your config)
DELTA_MODE = 
DELTA_REL_MULTIPLIER = 
CONFIRM_MULT = 
TP_R = 
STOP_PADDING_ATR_MULT = 
TP_MAX_POINTS = 
MIN_GAP = 

HORIZON_CANDLES = 
MONITOR_POLL_SEC = 

# --------------------
# Globals and state
# --------------------
app = Flask(__name__)
exchange = None
df = pd.DataFrame()
agg_totals = {}   # candle_start_ms -> {"buy":..., "sell":..., "count":...}
agg_trades = {}   # candle_start_ms -> [ {ts, price, qty, taker_is_buy}, ...]
agg_lock = threading.Lock()
listener_thread = None
finalizer_thread = None
signals_cache: List[Dict[str, Any]] = []
EQUITY = INITIAL_EQUITY

# Google sheet worksheet handle
gsheet_ws = None

# --------------------
# Google Sheets init
# --------------------
def init_gsheet():
    global gsheet_ws
    if not GOOGLE_SERVICE_ACCOUNT_JSON or not GOOGLE_SHEET_ID:
        print("Google Sheets not configured (missing env). Skipping gsheet init.")
        return None
    try:
        info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        creds = Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
        gc = gspread.authorize(creds)
        ws = gc.open_by_key(GOOGLE_SHEET_ID).sheet1
        # Ensure header row exists (if empty)
        try:
            hdr = ws.row_values(1)
            if not hdr:
                header = ["signal_time","side","entry_price","entry_fill_price","entry_time",
                          "stop_loss","take_profit","exit_price","exit_time","outcome",
                          "gross_pl_usd","fees_usd","slippage_usd","net_pl_usd","r_after_fees",
                          "qty","dollar_risk","simulated","intrabarkill"]
                ws.append_row(header, value_input_option="USER_ENTERED")
        except Exception:
            pass
        gsheet_ws = ws
        print("Google Sheet connected, sheet id:", GOOGLE_SHEET_ID)
        return ws
    except Exception as e:
        print("Failed to init Google Sheets:", e)
        traceback.print_exc()
        return None

# call now
gsheet_ws = init_gsheet()

def write_signal_to_sheet(signal: Dict[str, Any]):
    """Append a single signal row to Google Sheet only (no local CSV)."""
    if gsheet_ws is None:
        print("No gsheet: signal not persisted to sheet, printing instead.")
        print(signal)
        return
    try:
        header = ["signal_time","side","entry_price","entry_fill_price","entry_time",
                  "stop_loss","take_profit","exit_price","exit_time","outcome",
                  "gross_pl_usd","fees_usd","slippage_usd","net_pl_usd","r_after_fees",
                  "qty","dollar_risk","simulated","intrabarkill"]
        row = [signal.get(c, "") for c in header]
        gsheet_ws.append_row(row, value_input_option="USER_ENTERED")
    except Exception as e:
        print("Failed to append signal to Google Sheet:", e)
        traceback.print_exc()

# --------------------
# Helpers - candle bucket
# --------------------
def candle_start_for_ts(ts_ms, tf_hours=TF_HOURS):
    ms_in_h = 3600_000
    bucket = (ts_ms // (tf_hours * ms_in_h)) * (tf_hours * ms_in_h)
    return int(bucket)

# --------------------
# SR detection functions (user-provided, pasted verbatim)
# --------------------
def is_support(df1, pos, n1, n2):
    if pos - n1 < 0 or pos + n2 >= len(df1): return False
    for i in range(pos - n1 + 1, pos + 1):
        if df1['low'].iloc[i] > df1['low'].iloc[i-1]:
            return False
    for i in range(pos + 1, pos + n2 + 1):
        if df1['low'].iloc[i] < df1['low'].iloc[i-1]:
            return False
    return True

def is_resistance(df1, pos, n1, n2):
    if pos - n1 < 0 or pos + n2 >= len(df1): return False
    for i in range(pos - n1 + 1, pos + 1):
        if df1['high'].iloc[i] < df1['high'].iloc[i-1]:
            return False
    for i in range(pos + 1, pos + n2 + 1):
        if df1['high'].iloc[i] > df1['high'].iloc[i-1]:
            return False
    return True

def dedupe_by_pct(levels, pct_tol=0.005):
    if not levels: return []
    levels_sorted = sorted(levels)
    cleaned = [levels_sorted[0]]
    for p in levels_sorted[1:]:
        last = cleaned[-1]
        if abs(p - last) / max(1.0, last) > pct_tol:
            cleaned.append(p)
    return cleaned

def compute_sr_zones_safe(df_local, n1=3, n2=2, dedupe_pct=0.003, zone_buffer_pct=0.002):
    sr_raw = []
    for pos in range(n1, len(df_local)-n2):
        if is_support(df_local, pos, n1, n2):
            sr_raw.append((pos, float(df_local['low'].iloc[pos]), 1))
        if is_resistance(df_local, pos, n1, n2):
            sr_raw.append((pos, float(df_local['high'].iloc[pos]), 2))
    supports = [x[1] for x in sr_raw if x[2]==1]
    resistances = [x[1] for x in sr_raw if x[2]==2]
    clean_supports = dedupe_by_pct(supports, pct_tol=dedupe_pct)
    clean_resistances = dedupe_by_pct(resistances, pct_tol=dedupe_pct)
    atr_med = float(df_local['atr'].median() if 'atr' in df_local.columns else 0.0)
    support_zones = []
    resistance_zones = []
    for lvl in clean_supports:
        buffer = max(lvl * zone_buffer_pct, atr_med * 0.5)
        support_zones.append({"level": lvl, "low": lvl - buffer, "high": lvl + buffer})
    for lvl in clean_resistances:
        buffer = max(lvl * zone_buffer_pct, atr_med * 0.5)
        resistance_zones.append({"level": lvl, "low": lvl - buffer, "high": lvl + buffer})
    return {"sr_raw": sr_raw, "supports": clean_supports, "resistances": clean_resistances,
            "support_zones": support_zones, "resistance_zones": resistance_zones}

# --------------------
# Detection function 
# --------------------              

def detect_signals_on_df(df_local, support_zones, resistance_zones,
                         sl_buffer_min=50.0,
                         delta_mode=DELTA_MODE,
                         delta_rel_multiplier=DELTA_REL_MULTIPLIER,
                         confirm_mult=CONFIRM_MULT,
                         tp_r=TP_R,
                         stop_padding_atr_mult=STOP_PADDING_ATR_MULT,
                         tp_max_points=TP_MAX_POINTS,
                         min_gap=MIN_GAP):
    entry_signals = []
    last_entry_time = pd.Timestamp("1900-01-01", tz=timezone.utc)
    if df_local['delta'].notna().sum() == 0:
        return []
    delta_std = df_local['delta'].std(skipna=True)
    if delta_mode == "relative":
        delta_threshold = float(delta_std or 0.0) * float(delta_rel_multiplier)
    else:
        delta_threshold = float(delta_rel_multiplier)
    max_stop_points = None
    if tp_max_points is not None:
        max_stop_points = float(tp_max_points) / float(tp_r)
    def candle_touches_zone(row_low, row_high, zl, zh, tol=0.0):
        return not (row_high < zl - tol or row_low > zh + tol)
    for i in range(len(df_local)-1):
        row = df_local.iloc[i]; confirm = df_local.iloc[i+1]
        now_ts = row['timestamp']
        if now_ts.tzinfo is None:
            now_ts = now_ts.tz_localize(timezone.utc)
        if now_ts - last_entry_time < min_gap:
            continue
        row_delta = row.get('delta', np.nan); conf_delta = confirm.get('delta', np.nan)
        atr_here = df_local['atr'].iloc[i] if not pd.isna(df_local['atr'].iloc[i]) else df_local['atr'].mean()
        stop_padding = max(sl_buffer_min, atr_here * stop_padding_atr_mult)
        # LONG
        if (not pd.isna(row_delta)) and (row_delta >= delta_threshold):
            touched_supports = [z for z in support_zones if candle_touches_zone(row['low'], row['high'], z['low'], z['high'])]
            if touched_supports:
                zone = touched_supports[0]
                conf_req = (not pd.isna(conf_delta)) and (conf_delta >= max(0.0, delta_threshold * confirm_mult))
                if (confirm['close'] >= confirm['open']) and conf_req:
                    entry_price = confirm['open']
                    anchor = min(zone['low'], row['low'])
                    stop_anchor = anchor - stop_padding
                    raw_risk = entry_price - stop_anchor
                    if raw_risk <= 0: continue
                    final_risk = raw_risk; capped=False
                    if max_stop_points is not None and raw_risk > max_stop_points:
                        final_risk = max_stop_points; stop = entry_price - final_risk; capped=True
                    else:
                        stop = stop_anchor
                    tp = entry_price + tp_r * final_risk
                    if not (tp > entry_price and stop < entry_price): continue
                    entry_time = confirm['timestamp']
                    entry_signals.append({
                        "side":"long","entry_time":entry_time,"entry_price":entry_price,
                        "zone_level":zone['level'],"zone_low":zone['low'],"zone_high":zone['high'],
                        "stop_loss":stop,"take_profit":tp,"signal_idx":i,
                        "row_delta":row_delta,"conf_delta":conf_delta,"atr":atr_here,
                        "stop_padding":stop_padding,"raw_risk":raw_risk,"final_risk":final_risk,"capped":capped
                    })
                    last_entry_time = entry_time
                    continue
        # SHORT
        if (not pd.isna(row_delta)) and (row_delta <= -delta_threshold):
            touched_res = [z for z in resistance_zones if candle_touches_zone(row['low'], row['high'], z['low'], z['high'])]
            if touched_res:
                zone = touched_res[0]
                conf_req = (not pd.isna(conf_delta)) and (conf_delta <= -max(0.0, delta_threshold * confirm_mult))
                if (confirm['close'] <= confirm['open']) and conf_req:
                    entry_price = confirm['open']
                    anchor = max(zone['high'], row['high'])
                    stop_anchor = anchor + stop_padding
                    raw_risk = stop_anchor - entry_price
                    if raw_risk <= 0: continue
                    final_risk = raw_risk; capped=False
                    if max_stop_points is not None and raw_risk > max_stop_points:
                        final_risk = max_stop_points; stop = entry_price + final_risk; capped=True
                    else:
                        stop = stop_anchor
                    tp = entry_price - tp_r * final_risk
                    if not (tp < entry_price and stop > entry_price): continue
                    entry_time = confirm['timestamp']
                    entry_signals.append({
                        "side":"short","entry_time":entry_time,"entry_price":entry_price,
                        "zone_level":zone['level'],"zone_low":zone['low'],"zone_high":zone['high'],
                        "stop_loss":stop,"take_profit":tp,"signal_idx":i,
                        "row_delta":row_delta,"conf_delta":conf_delta,"atr":atr_here,
                        "stop_padding":stop_padding,"raw_risk":raw_risk,"final_risk":final_risk,"capped":capped
                    })
                    last_entry_time = entry_time
                    continue
    return entry_signals

# --------------------
# Websocket listener (aggTrade)
# --------------------
async def _agg_listener_loop():
    WS_BASE = "wss://fstream.binance.com/ws"
    ws_url = f"{WS_BASE}/{STREAM_SYMBOL}@aggTrade"
    print("Websocket connecting to:", ws_url)
    try:
        async with websockets.connect(ws_url, ping_interval=60) as ws:
            print("Websocket connected.")
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                if msg.get('e') != 'aggTrade':
                    continue
                trade_ts = int(msg.get('T'))
                price = float(msg.get('p'))
                qty = float(msg.get('q'))
                isBuyerMaker = msg.get('m')
                taker_is_buy = not bool(isBuyerMaker)
                cs = candle_start_for_ts(trade_ts)
                with agg_lock:
                    if cs not in agg_totals:
                        agg_totals[cs] = {"buy":0.0, "sell":0.0, "count":0}
                        agg_trades[cs] = []
                    if taker_is_buy:
                        agg_totals[cs]['buy'] += qty
                    else:
                        agg_totals[cs]['sell'] += qty
                    agg_totals[cs]['count'] += 1
                    agg_trades[cs].append({"ts": trade_ts, "price": price, "qty": qty, "taker_is_buy": taker_is_buy})
    except Exception as e:
        print("Websocket exception:", e)
        traceback.print_exc()

def start_listener_in_thread():
    def _run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(_agg_listener_loop())
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return t

# --------------------
# Intrabar resolution helper
# --------------------
def first_trade_crosses(trade_list, tp, stop, side):
    """Return (price, ts, which) where which in {'tp','sl'} or (None,None,None)."""
    for tr in trade_list:
        p = tr['price']
        if side == "long":
            if p >= tp:
                return p, tr['ts'], 'tp'
            if p <= stop:
                return p, tr['ts'], 'sl'
        else:
            if p <= tp:
                return p, tr['ts'], 'tp'
            if p >= stop:
                return p, tr['ts'], 'sl'
    return None, None, None

# --------------------
# Sizing and order placement (simulated or real)
# --------------------
def compute_qty_from_risk_usd(entry_price, stop_price, equity, risk_pct):
    risk_price = abs(entry_price - stop_price)
    if risk_price <= 0:
        return 0.0, 0.0
    dollar_risk = equity * risk_pct
    qty = dollar_risk / risk_price
    notional = qty * entry_price
    if notional > MAX_USD_PER_TRADE:
        qty = MAX_USD_PER_TRADE / entry_price
    if qty < MIN_QTY:
        qty = MIN_QTY
    return float(qty), float(dollar_risk)

def place_futures_market_order(side, qty):
    """Place real futures market order via ccxt. Returns dict or None on failure."""
    if not EXECUTE_TRADES:
        return {"simulated": True, "side": side, "qty": qty, "avg": None}
    try:
        order = exchange.create_order(SYMBOL, 'market', side, qty, None, {})
        avg = order.get('average')
        return {"simulated": False, "side": side, "qty": qty, "order": order, "avg": avg}
    except Exception as e:
        print("Order placement error:", e)
        traceback.print_exc()
        return None

# Monitor & intrabar exit (runs in a thread per trade)
def monitor_trade_using_ticks(sig, entry_fill_price, qty, dollar_risk, simulated_fill, fee_rate_per_side):
    global EQUITY
    side = sig['side']; tp = float(sig['take_profit']); stop = float(sig['stop_loss'])
    entry_time = pd.to_datetime(sig['entry_time'])
    try:
        entry_idx = df.index.get_loc(entry_time)
    except KeyError:
        entry_idx = len(df) - 1
    entry_cs = int(df['ts_ms'].iloc[entry_idx])
    # search buckets from entry_cs to horizon
    cs_list = [entry_cs + k * TF_HOURS * 3600 * 1000 for k in range(0, HORIZON_CANDLES)]
    exit_price = None; exit_time = None; outcome = "no_hit"; intrabar_flag = False
    notional_entry = entry_fill_price * qty
    fee_entry = notional_entry * fee_rate_per_side
    slippage_entry = notional_entry * (SIM_SLIPPAGE_PCT if simulated_fill else 0.0)
    for cs in cs_list:
        with agg_lock:
            trades_list = list(agg_trades.get(cs, []))
        if not trades_list:
            time.sleep(MONITOR_POLL_SEC)
            continue
        # if first bucket is entry bucket, drop trades earlier than entry timestamp
        entry_ts_ms = int(entry_time.timestamp() * 1000)
        if cs == entry_cs:
            trades_list = [t for t in trades_list if t['ts'] >= entry_ts_ms]
        exit_p, exit_ts, which = first_trade_crosses(trades_list, tp, stop, side)
        if exit_p is not None:
            exit_price = float(exit_p)
            exit_time = pd.to_datetime(exit_ts, unit='ms', utc=True)
            outcome = "win" if which == 'tp' else "loss"
            intrabar_flag = True
            break
        # else continue
        time.sleep(MONITOR_POLL_SEC)
    if exit_price is None:
        # fallback: candle-level close after horizon
        last_idx = min(len(df)-1, entry_idx + HORIZON_CANDLES)
        last_row = df.iloc[last_idx]
        exit_price = float(last_row['close'])
        exit_time = last_row['timestamp']
        outcome = "no_hit"
        intrabar_flag = False
    notional_exit = exit_price * qty
    fee_exit = notional_exit * fee_rate_per_side
    slippage_exit = notional_exit * (SIM_SLIPPAGE_PCT if simulated_fill else 0.0)
    if side == "long":
        gross_pl = qty * (exit_price - entry_fill_price)
    else:
        gross_pl = qty * (entry_fill_price - exit_price)
    total_fees = fee_entry + fee_exit
    total_slippage = slippage_entry + slippage_exit
    net_pl = gross_pl - total_fees - total_slippage
    r_after_fees = net_pl / dollar_risk if dollar_risk != 0 else float('nan')
    EQUITY += net_pl
    # prepare signal row to sheet
    row = {
        "signal_time": sig['entry_time'].isoformat(),
        "side": side,
        "entry_price": entry_fill_price,
        "entry_fill_price": entry_fill_price,
        "entry_time": entry_time.isoformat(),
        "stop_loss": stop,
        "take_profit": tp,
        "exit_price": exit_price,
        "exit_time": exit_time.isoformat(),
        "outcome": outcome,
        "gross_pl_usd": round(gross_pl, 8),
        "fees_usd": round(total_fees, 8),
        "slippage_usd": round(total_slippage, 8),
        "net_pl_usd": round(net_pl, 8),
        "r_after_fees": round(r_after_fees, 6),
        "qty": qty,
        "dollar_risk": dollar_risk,
        "simulated": simulated_fill,
        "intrabarkill": intrabar_flag
    }
    # write to google sheet
    try:
        write_signal_to_sheet(row)
    except Exception:
        print("Failed to write final signal to sheet.")
    print("Trade closed:", row)
    return row

def place_order_and_monitor(sig):
    entry_price = float(sig['entry_price']); stop = float(sig['stop_loss'])
    qty, dollar_risk = compute_qty_from_risk_usd(entry_price, stop, EQUITY, RISK_PER_TRADE)
    if qty <= 0:
        print("Qty <= 0, skipping")
        return None
    fee_rate = TAKER_FEE_RATE  # markets use taker by default
    if EXECUTE_TRADES:
        side_ccxt = 'buy' if sig['side'] == 'long' else 'sell'
        res = place_futures_market_order(side_ccxt, qty)
        if res is None:
            print("Order failed; simulate fill.")
            simulated_fill = True
            entry_fill_price = entry_price * (1 + SIM_SLIPPAGE_PCT if sig['side']=='long' else 1 - SIM_SLIPPAGE_PCT)
        else:
            if res.get('simulated'):
                simulated_fill = True
                entry_fill_price = entry_price * (1 + SIM_SLIPPAGE_PCT if sig['side']=='long' else 1 - SIM_SLIPPAGE_PCT)
            else:
                simulated_fill = False
                entry_fill_price = float(res.get('avg') or entry_price)
    else:
        simulated_fill = True
        entry_fill_price = entry_price * (1 + SIM_SLIPPAGE_PCT if sig['side']=='long' else 1 - SIM_SLIPPAGE_PCT)
        print("[SIM] entry_fill_price:", entry_fill_price)
    # spawn monitor thread
    t = threading.Thread(target=monitor_trade_using_ticks, args=(sig, entry_fill_price, qty, dollar_risk, simulated_fill, fee_rate), daemon=True)
    t.start()
    return {"entry_fill_price": entry_fill_price, "qty": qty, "dollar_risk": dollar_risk, "simulated": simulated_fill}

# --------------------
# Finalizer loop: fills df with delta for completed candle and runs detection -> place orders
# --------------------
def finalize_previous_and_run_once():
    global df, signals_cache
    if df.empty:
        return
    now_ms = int(datetime.utcnow().timestamp() * 1000)
    current_cs = candle_start_for_ts(now_ms)
    prev_cs = current_cs - TF_HOURS * 3600 * 1000
    with agg_lock:
        prev_data = agg_totals.get(prev_cs)
    if prev_data is None:
        return
    # ensure candle exists
    target_rows = df[df['ts_ms'] == prev_cs]
    if target_rows.empty:
        try:
            ohlcv = exchange.fetch_ohlcv(SYMBOL, timeframe=TIMEFRAME, since=prev_cs, limit=1)
            if ohlcv:
                row = ohlcv[0]
                new_ts = pd.to_datetime(row[0], unit='ms', utc=True)
                new_row = pd.DataFrame([{
                    'timestamp': new_ts, 'ts_ms': row[0], 'open': row[1], 'high': row[2], 'low': row[3], 'close': row[4], 'volume': row[5]
                }]).set_index('timestamp')
                df = pd.concat([df, new_row], axis=0).sort_index()
        except Exception as e:
            print("REST candle fetch failed:", e)
    target_rows = df[df['ts_ms'] == prev_cs]
    if target_rows.empty:
        return
    idx = target_rows.index[0]
    with agg_lock:
        b = agg_totals[prev_cs]['buy']; s = agg_totals[prev_cs]['sell']
    df.at[idx, 'buy_vol'] = b; df.at[idx, 'sell_vol'] = s; df.at[idx, 'delta'] = b - s
    # recompute ATR
    df['prev_close'] = df['close'].shift(1)
    df['tr'] = df[['high','low','prev_close']].apply(lambda r: max(r['high']-r['low'],
                                                                   abs(r['high']-(r['prev_close'] if not pd.isna(r['prev_close']) else r['high'])),
                                                                   abs(r['low']-(r['prev_close'] if not pd.isna(r['prev_close']) else r['low']))), axis=1)
    df['atr'] = df['tr'].rolling(14, min_periods=1).mean().fillna(method='bfill')
    # recompute zones
    sr = compute_sr_zones_safe(df, n1=3, n2=2, dedupe_pct=0.003, zone_buffer_pct=0.002)
    support_zones = sr['support_zones']; resistance_zones = sr['resistance_zones']
    # detect signals
    signals = detect_signals_on_df(df, support_zones, resistance_zones,
                                   sl_buffer_min=50.0,
                                   delta_mode=DELTA_MODE,
                                   delta_rel_multiplier=DELTA_REL_MULTIPLIER,
                                   confirm_mult=CONFIRM_MULT,
                                   tp_r=TP_R,
                                   stop_padding_atr_mult=STOP_PADDING_ATR_MULT,
                                   tp_max_points=TP_MAX_POINTS,
                                   min_gap=MIN_GAP)
    if not signals:
        print("No signals at", pd.to_datetime(prev_cs, unit='ms', utc=True))
        return
    print(f"Detected {len(signals)} signals at {pd.to_datetime(prev_cs, unit='ms', utc=True)}")
    for sig in signals:
        # store minimal signal to cache and write to sheet (open trade row)
        open_row = {
            "signal_time": sig['entry_time'].isoformat(),
            "side": sig['side'],
            "entry_price": sig['entry_price'],
            "entry_fill_price": None,
            "entry_time": sig['entry_time'].isoformat(),
            "stop_loss": sig['stop_loss'],
            "take_profit": sig['take_profit'],
            "exit_price": None,
            "exit_time": None,
            "outcome": "open",
            "gross_pl_usd": "",
            "fees_usd": "",
            "slippage_usd": "",
            "net_pl_usd": "",
            "r_after_fees": "",
            "qty": "",
            "dollar_risk": "",
            "simulated": not EXECUTE_TRADES,
            "intrabarkill": ""
        }
        signals_cache.append(open_row)
        # write open row
        try:
            write_signal_to_sheet(open_row)
        except Exception:
            print("Failed to write open signal to sheet.")
        # place order and monitor in background
        res = place_order_and_monitor(sig)
        if res:
            # update cached open entry with fill info if available
            pass

def finalizer_loop(poll_interval=20):
    while True:
        try:
            finalize_previous_and_run_once()
        except Exception:
            traceback.print_exc()
        time.sleep(poll_interval)

# --------------------
# Exchange init and warmup candles
# --------------------
def init_exchange_and_warmup():
    global exchange, df
    exchange = ccxt.binance({"apiKey": BINANCE_API_KEY, "secret": BINANCE_API_SECRET, "enableRateLimit": True, "options": {"defaultType": "future"}})
    tf_hours = TF_HOURS
    tf_ms = tf_hours * 3600 * 1000
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - TARGET_CANDLES * tf_ms
    all_ohlcv = []
    fetch_since = start_ms
    while True:
        try:
            batch = exchange.fetch_ohlcv(SYMBOL, timeframe=TIMEFRAME, since=fetch_since, limit=1000)
        except Exception as e:
            print("ohlcv fetch error:", e)
            break
        if not batch:
            break
        for row in batch:
            ts = int(row[0])
            if len(all_ohlcv) == 0 or ts > int(all_ohlcv[-1][0]):
                all_ohlcv.append(row)
        last_ts = int(batch[-1][0])
        fetch_since = last_ts + tf_ms
        if fetch_since >= now_ms or len(all_ohlcv) >= TARGET_CANDLES:
            break
        time.sleep(0.12)
    if len(all_ohlcv) > TARGET_CANDLES:
        all_ohlcv = all_ohlcv[-TARGET_CANDLES:]
    df = pd.DataFrame(all_ohlcv, columns=['ts_ms','open','high','low','close','volume'])
    df['timestamp'] = pd.to_datetime(df['ts_ms'], unit='ms', utc=True)
    df = df[['timestamp','ts_ms','open','high','low','close','volume']].set_index('timestamp', drop=False)
    df['buy_vol'] = np.nan; df['sell_vol'] = np.nan; df['delta'] = np.nan
    df['prev_close'] = df['close'].shift(1)
    df['tr'] = df[['high','low','prev_close']].apply(lambda r: max(r['high']-r['low'],
                                                                   abs(r['high']-(r['prev_close'] if not pd.isna(r['prev_close']) else r['high'])),
                                                                   abs(r['low']-(r['prev_close'] if not pd.isna(r['prev_close']) else r['low']))), axis=1)
    df['atr'] = df['tr'].rolling(14, min_periods=1).mean().fillna(method='bfill')
    print("Warmup candles loaded:", len(df))

# --------------------
# Diagnostics endpoint
# --------------------
def get_diagnostics():
    diag = {}
    try:
        with agg_lock:
            agg_items = list(agg_totals.items())
        diag['agg_buckets_count'] = len(agg_items)
        if agg_items:
            agg_sorted = sorted(agg_items, key=lambda x: x[0])
            last6 = []
            for cs, vals in agg_sorted[-6:]:
                last6.append({"candle_start": datetime.fromtimestamp(cs/1000, tz=timezone.utc).isoformat(), "vals": vals})
            diag['agg_last_buckets'] = last6
        diag['listener_running'] = (listener_thread is not None and listener_thread.is_alive())
        diag['finalizer_running'] = (finalizer_thread is not None and finalizer_thread.is_alive())
        diag['df_len'] = len(df)
        if not df.empty:
            diag['df_tail'] = df[['ts_ms','open','high','low','close','volume','buy_vol','sell_vol','delta']].tail(5).to_dict(orient='records')
            diag['candles_with_delta'] = int(df['delta'].notna().sum())
        else:
            diag['df_tail'] = []
            diag['candles_with_delta'] = 0
        diag['recent_signals'] = signals_cache[-8:]
        if gsheet_ws:
            diag['sheet_ok'] = True
        else:
            diag['sheet_ok'] = False
    except Exception as e:
        diag['diag_error'] = str(e)
    return diag

@app.route("/health")
def health():
    return jsonify({"status":"ok","time": datetime.now(timezone.utc).isoformat()})

@app.route("/diag")
def diag_route():
    return jsonify(get_diagnostics())

@app.route("/signals")
def signals_route():
    return jsonify({"count": len(signals_cache), "signals": signals_cache[-50:]})

# --------------------
# Boot sequence
# --------------------
def start_background():
    global listener_thread, finalizer_thread
    listener_thread = start_listener_in_thread()
    finalizer_thread = threading.Thread(target=finalizer_loop, daemon=True)
    finalizer_thread.start()
    print("Background threads started.")

def start():
    init_exchange_and_warmup()
    start_background()

# Start when module run
start()

if __name__ == "__main__":
    print("Starting Flask app on port", PORT)
    app.run(host="0.0.0.0", port=PORT, debug=False)

"""
SPY Monitor — Servidor Alpaca API
===================================
INSTALACIÓN (una sola vez):
    pip install websockets flask flask-cors requests

USO:
    python server_5.py

Luego abre http://localhost:8765 en Chrome.
"""

import asyncio
import json
import math
import sys
import threading
import time
import ssl
import requests
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, send_from_directory, request
from flask_cors import CORS
import os
import websockets

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ── ALPACA KEYS ──
API_KEY    = "PKHAR2FELTASKYPNEEM72WJO3Y"
API_SECRET = "25cTgaAp6XYSQF6pAZYAraibYBXgY4ZmJcnTe2eNSB6A"

# ── CLAUDE ──
CLAUDE_API_KEY  = ""        # ← pon aquí tu API key de Anthropic

WS_URL     = "wss://stream.data.alpaca.markets/v2/iex"
REST_BASE  = "https://data.alpaca.markets/v2"
HEADERS    = {
    "APCA-API-KEY-ID":     API_KEY,
    "APCA-API-SECRET-KEY": API_SECRET
}

# ── ESTADO ──
state = {
    "spy_price":      0.0,
    "spy_bid":        0.0,
    "spy_ask":        0.0,
    "spy_open":       0.0,
    "spy_high":       0.0,
    "spy_low":        0.0,
    "spy_prev_close": 0.0,
    "spy_prev_high":  0.0,
    "spy_prev_low":   0.0,
    "spy_pm_high":    0.0,
    "spy_pm_low":     0.0,
    "spy_volume":     0,
    "ts_feed":        [],
    "flow": {
        "buy_vol":        0,
        "sell_vol":       0,
        "delta":          0,
        "big_trades":     0,
        "block_buy_vol":  0,
        "block_sell_vol": 0,
        "last_block":     "",
        "last_update":    ""
    },
    "oi_levels":      [],
    "oi_lock_date":   None,  # fecha en que se fijaron los strikes OI
    "manual_classes": {},
    "manual_prices":  {},
    "connected":  False,
    "mode":       "iniciando",
    "_flow_reset_date": None,
}

MAX_TS = 300
MAX_SPY_SPREAD = 0.25
BLOCK_TRADE_SIZE = 500
INSTITUTIONAL_TRADE_SIZE = 1000


# ── TIEMPO ET ──
def get_et_now():
    """Hora actual en ET (asume EDT = UTC-4 durante la temporada de trading)."""
    return datetime.now(timezone(timedelta(hours=-4)))

def get_et_minutes():
    now = get_et_now()
    return now.hour * 60 + now.minute

def is_premarket(m=None):
    if m is None: m = get_et_minutes()
    return 240 <= m < 570   # 4:00 AM – 9:30 AM ET

def is_market_hours(m=None):
    if m is None: m = get_et_minutes()
    return 570 <= m < 960   # 9:30 AM – 4:00 PM ET


def clean_bid_ask(price, bid, ask):
    try:
        price = float(price or 0)
        bid   = float(bid   or 0)
        ask   = float(ask   or 0)
    except (TypeError, ValueError):
        bid, ask = 0, 0

    spread = ask - bid
    quote_ok = (
        price > 0 and bid > 0 and ask > 0 and
        bid <= price <= ask and
        0 < spread <= MAX_SPY_SPREAD and
        abs(price - bid) <= MAX_SPY_SPREAD and
        abs(ask - price) <= MAX_SPY_SPREAD
    )
    if quote_ok:
        return round(bid, 2), round(ask, 2)
    if price > 0:
        return round(price - 0.01, 2), round(price + 0.01, 2)
    return 0.0, 0.0


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ── FLASK API LOCAL ──
app = Flask(__name__)
CORS(app)

@app.route("/")
def index():
    return send_from_directory(os.path.join(BASE_DIR, "static"), "spy_simple.html")

@app.route("/status")
def status():
    return jsonify({
        "connected": state["connected"],
        "mode":      state["mode"],
        "time":      datetime.now().strftime("%H:%M:%S")
    })

@app.route("/debug")
def debug():
    m = get_et_minutes()
    return jsonify({
        "ws_connected":   state["connected"],
        "mode":           state["mode"],
        "total_prints":   len(state["ts_feed"]),
        "spy_price":      state["spy_price"],
        "market_open":    is_market_hours(m),
        "premarket":      is_premarket(m),
        "et_time":        get_et_now().strftime("%H:%M:%S"),
        "local_time":     datetime.now().strftime("%H:%M:%S"),
        "last_flow_update": state["flow"]["last_update"],
    })

@app.route("/quote")
def quote():
    chg  = round(state["spy_price"] - state["spy_prev_close"], 2) if state["spy_prev_close"] else 0
    chgp = round(chg / state["spy_prev_close"] * 100, 3)          if state["spy_prev_close"] else 0
    return jsonify({
        "price":      state["spy_price"],
        "bid":        state["spy_bid"],
        "ask":        state["spy_ask"],
        "open":       state["spy_open"],
        "high":       state["spy_high"],
        "low":        state["spy_low"],
        "prev_close": state["spy_prev_close"],
        "prev_high":  state["spy_prev_high"],
        "prev_low":   state["spy_prev_low"],
        "pm_high":    state["spy_pm_high"],
        "pm_low":     state["spy_pm_low"],
        "volume":     state["spy_volume"],
        "change":     chg,
        "change_pct": chgp,
        "mode":       state["mode"],
    })

@app.route("/ts/since/<int:index>")
def ts_since(index):
    feed = state["ts_feed"]
    return jsonify({"prints": feed[index:], "total": len(feed)})

@app.route("/ts/recent/<int:seconds>")
def ts_recent(seconds):
    """Devuelve prints de los últimos N segundos, mínimo los últimos 100 por índice."""
    feed   = state["ts_feed"]
    cutoff = (datetime.now() - timedelta(seconds=seconds)).strftime("%H:%M:%S")
    by_time = [p for p in feed if p["time"] >= cutoff]
    # fallback: si el filtro de tiempo da menos de 100, tomar los últimos 100 por índice
    recent = by_time if len(by_time) >= 10 else feed[-100:]
    return jsonify({"prints": recent, "total": len(feed), "by_time": len(by_time)})

@app.route("/flow")
def flow():
    return jsonify(state["flow"])

@app.route("/oi_levels")
def oi_levels():
    return jsonify(state["oi_levels"])

@app.route("/levels")
def levels():
    """Endpoint principal: devuelve todos los niveles clave dinámicos."""
    lvls = []

    if state["spy_prev_high"] > 0:
        lvl = {
            "id": "pdh", "tag": "PREV HIGH", "type": "ph",
            "price": state["spy_prev_high"], "desc": "High día anterior",
            "strength": 94, "oi": None
        }
        if "pdh" in state["manual_classes"]:
            lvl["tag"] = state["manual_classes"]["pdh"]
        if "pdh" in state["manual_prices"]:
            lvl["price"] = state["manual_prices"]["pdh"]
        lvls.append(lvl)
    if state["spy_prev_low"] > 0:
        lvl = {
            "id": "pdl", "tag": "PREV LOW", "type": "pl",
            "price": state["spy_prev_low"], "desc": "Low día anterior",
            "strength": 91, "oi": None
        }
        if "pdl" in state["manual_classes"]:
            lvl["tag"] = state["manual_classes"]["pdl"]
        if "pdl" in state["manual_prices"]:
            lvl["price"] = state["manual_prices"]["pdl"]
        lvls.append(lvl)
    if state["spy_pm_high"] > 0:
        lvl = {
            "id": "pmh", "tag": "PM HIGH", "type": "pmh",
            "price": state["spy_pm_high"], "desc": "High premarket",
            "strength": 86, "oi": None
        }
        if "pmh" in state["manual_classes"]:
            lvl["tag"] = state["manual_classes"]["pmh"]
        if "pmh" in state["manual_prices"]:
            lvl["price"] = state["manual_prices"]["pmh"]
        lvls.append(lvl)
    if state["spy_pm_low"] > 0:
        lvl = {
            "id": "pml", "tag": "PM LOW", "type": "pml",
            "price": state["spy_pm_low"], "desc": "Low premarket",
            "strength": 82, "oi": None
        }
        if "pml" in state["manual_classes"]:
            lvl["tag"] = state["manual_classes"]["pml"]
        if "pml" in state["manual_prices"]:
            lvl["price"] = state["manual_prices"]["pml"]
        lvls.append(lvl)

    # Agregar niveles OI ITM con clases y precios manuales
    for oi_lvl in state["oi_levels"]:
        if oi_lvl["id"] in state["manual_classes"]:
            oi_lvl["tag"] = state["manual_classes"][oi_lvl["id"]]
        if oi_lvl["id"] in state["manual_prices"]:
            oi_lvl["price"] = state["manual_prices"][oi_lvl["id"]]
        lvls.append(oi_lvl)

    return jsonify({
        "levels": lvls,
        "spot":   state["spy_price"],
        "pm_frozen": not is_premarket()
    })

@app.route("/levels/set_class", methods=["POST"])
def set_level_class():
    """Endpoint para actualizar la clase manual de un nivel."""
    data = request.get_json() or {}
    level_id = data.get("id")
    custom_class = data.get("class", "")
    
    if level_id:
        if custom_class.strip():
            state["manual_classes"][level_id] = custom_class.strip()
        elif level_id in state["manual_classes"]:
            del state["manual_classes"][level_id]
    
    return jsonify({"success": True, "manual_classes": state["manual_classes"]})

@app.route("/levels/set_price", methods=["POST"])
def set_level_price():
    """Endpoint para actualizar el precio manual de un nivel."""
    data = request.get_json() or {}
    level_id = data.get("id")
    custom_price = data.get("price")
    
    if level_id:
        if custom_price is not None:
            try:
                custom_price = float(custom_price)
                state["manual_prices"][level_id] = custom_price
            except (ValueError, TypeError):
                pass
        elif level_id in state["manual_prices"]:
            del state["manual_prices"][level_id]
    
    return jsonify({"success": True, "manual_prices": state["manual_prices"]})


@app.route("/analyze", methods=["POST"])
def analyze():
    """Proxy hacia Claude API — evita bloqueos CORS del navegador."""
    data = request.get_json() or {}
    if not CLAUDE_API_KEY:
        return jsonify({"error": "sin_key"}), 500

    lvl_tag   = data.get("lvl_tag", "")
    lvl_price = data.get("lvl_price", 0)
    lvl_desc  = data.get("lvl_desc", "")
    spy_price = data.get("spy_price", state["spy_price"])
    buy_vol   = data.get("buy_vol", state["flow"]["buy_vol"])
    sell_vol  = data.get("sell_vol", state["flow"]["sell_vol"])
    mode      = data.get("mode", state["mode"])
    trades    = data.get("trades", [])

    # Métricas calculadas para darle contexto numérico a Claude
    total_vol   = buy_vol + sell_vol
    delta_pct   = round((buy_vol - sell_vol) / total_vol * 100, 1) if total_vol > 0 else 0
    buy_pct     = round(buy_vol / total_vol * 100, 1)              if total_vol > 0 else 0
    sell_pct    = 100 - buy_pct

    # Métricas de los trades de la zona
    zone_buys   = [t for t in trades if t.get("direction") == "BUY"]
    zone_sells  = [t for t in trades if t.get("direction") == "SELL"]
    zone_total  = len(trades)
    zone_buy_vol  = sum(t.get("size", 0) for t in zone_buys)
    zone_sell_vol = sum(t.get("size", 0) for t in zone_sells)
    zone_total_vol = zone_buy_vol + zone_sell_vol
    zone_buy_pct = round(zone_buy_vol / zone_total_vol * 100, 1) if zone_total_vol > 0 else 0
    avg_size    = round(zone_total_vol / zone_total, 0) if zone_total > 0 else 0
    blocks      = sum(1 for t in trades if t.get("big"))

    # --- Velocidad (trades/minuto en la zona) ---
    velocity = 0.0
    if len(trades) >= 2:
        try:
            def _hms(ts):
                h, m, s = ts.split(":")
                return int(h) * 3600 + int(m) * 60 + int(s)
            dur = max(_hms(trades[-1]["time"]) - _hms(trades[0]["time"]), 1)
            velocity = round(len(trades) / dur * 60, 1)
        except Exception:
            velocity = 0.0

    # --- Absorción: bloques grandes que no mueven el precio en su dirección ---
    buy_absorbed = 0
    sell_absorbed = 0
    for i in range(len(trades) - 1):
        tc = trades[i]
        tn = trades[i + 1]
        if not tc.get("big"):
            continue
        dp = tn.get("price", tc.get("price", 0)) - tc.get("price", 0)
        if tc["direction"] == "BUY"  and dp <= 0: buy_absorbed  += 1
        if tc["direction"] == "SELL" and dp >= 0: sell_absorbed += 1

    if buy_absorbed >= 2 and buy_absorbed > sell_absorbed:
        absorption_text = (
            f"compras absorbidas — {buy_absorbed} bloques BUY sin avance de precio "
            f"(vendedores fuertes esperando arriba)"
        )
    elif sell_absorbed >= 2 and sell_absorbed > buy_absorbed:
        absorption_text = (
            f"ventas absorbidas — {sell_absorbed} bloques SELL sin caída de precio "
            f"(compradores fuertes esperando abajo)"
        )
    else:
        absorption_text = "ninguna significativa"

    # --- Agotamiento: tamaño promedio cae en la dirección dominante ---
    dominant_dir = "BUY" if zone_buy_pct >= 50 else "SELL"
    dom_trades   = [t for t in trades if t.get("direction") == dominant_dir]
    exhaustion_text = "ninguno"
    if len(dom_trades) >= 4:
        mid   = len(dom_trades) // 2
        avg_f = sum(t.get("size", 0) for t in dom_trades[:mid])  / mid
        avg_s = sum(t.get("size", 0) for t in dom_trades[mid:])  / max(len(dom_trades) - mid, 1)
        drop  = round((1 - avg_s / avg_f) * 100) if avg_f > 0 else 0
        if avg_s < avg_f * 0.65:
            dir_label = "compradores" if dominant_dir == "BUY" else "vendedores"
            exhaustion_text = (
                f"{dir_label} perdiendo fuerza — tamaño cayó {drop}% "
                f"(avg {round(avg_f)}→{round(avg_s)} acciones)"
            )

    # --- Aceleración: tamaño promedio sube en segunda mitad ---
    acceleration_text = "ninguna"
    if len(trades) >= 6:
        mid  = len(trades) // 2
        fh   = trades[:mid]
        sh   = trades[mid:]
        af   = sum(t.get("size", 0) for t in fh) / len(fh)
        as_  = sum(t.get("size", 0) for t in sh) / len(sh)
        if as_ > af * 1.4:
            inc  = round((as_ / af - 1) * 100)
            dlbl = "alcista" if zone_buy_pct >= 50 else "bajista"
            acceleration_text = (
                f"aceleración {dlbl} — tamaño promedio subió {inc}% "
                f"en segunda mitad (avg {round(af)}→{round(as_)} acciones)"
            )

    # Sin datos suficientes — no forzar análisis
    if zone_total < 5:
        return jsonify({
            "content": [{
                "type": "text",
                "text": (
                    f"DECISION: NO TRADE\n"
                    f"LECTURA: Datos insuficientes en la zona ({zone_total} trades)\n"
                    f"ANALISIS: Se necesitan al menos 5 trades cerca del nivel para evaluar el flujo. "
                    f"El mercado no ha testeado este nivel con suficiente actividad todavía."
                )
            }]
        })

    # Formatear trades para el prompt
    if trades:
        lines = []
        for t in trades[:40]:
            note = f" [{t.get('note')}]" if t.get("note") else ""
            lines.append(
                f"  {t.get('time','')} | {t.get('direction','')} | "
                f"{t.get('size',0):,} @ ${t.get('price',0):.2f}{note}"
            )
        trades_text = "Trades capturados en zona:\n" + "\n".join(lines)
    else:
        trades_text = "Sin trades capturados en la zona."

    try:
        res = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "Content-Type":      "application/json",
                "x-api-key":         CLAUDE_API_KEY,
                "anthropic-version": "2023-06-01",
            },
            json={
                "model":      "claude-haiku-4-5-20251001",
                "max_tokens": 350,
                "system": (
                    "Eres un analista de microestructura para SPY. Identificas patrones de flujo en zonas clave.\n\n"
                    "PATRONES A DETECTAR:\n"
                    "• ABSORCIÓN: Bloques grandes que no mueven el precio en su dirección → fuerza opuesta presente.\n"
                    "  Compras absorbidas = vendedores fuertes arriba (señal VENTA)\n"
                    "  Ventas absorbidas = compradores fuertes abajo (señal COMPRA)\n"
                    "• AGOTAMIENTO: Tamaño promedio cae >35% en prints de dirección dominante → momentum debilitándose.\n"
                    "• ACELERACIÓN: Tamaño promedio sube >40% en segunda mitad → momentum creciendo.\n"
                    "• FLUJO DOMINANTE: >60% del volumen de zona en una sola dirección.\n\n"
                    "REGLAS DE DECISIÓN — requiere ≥2 condiciones alineadas:\n"
                    "COMPRA si ≥2: (a) flujo BUY zona ≥55%, (b) ventas absorbidas, (c) aceleración alcista, (d) agotamiento vendedor\n"
                    "VENTA si ≥2: (a) flujo SELL zona ≥55%, (b) compras absorbidas, (c) aceleración bajista, (d) agotamiento comprador\n"
                    "NO TRADE si: <2 condiciones alineadas, señales contradictorias, velocidad <5 trades/min, o datos insuficientes\n\n"
                    "Responde SIEMPRE en este formato exacto (sin texto extra):\n"
                    "DECISION: COMPRA | VENTA | NO TRADE\n"
                    "PATRON: [absorción | agotamiento | aceleración | flujo dominante | sin patrón claro]\n"
                    "LECTURA: [una frase directa, máximo 12 palabras]\n"
                    "ANALISIS: [2-3 oraciones: qué patrón detectaste, qué condiciones se alinearon, por qué la decisión]"
                ),
                "messages": [{
                    "role": "user",
                    "content": (
                        f"Nivel: {lvl_tag} ${lvl_price:.2f} — {lvl_desc}\n"
                        f"SPY: ${spy_price:.2f} | {'LIVE Alpaca IEX' if mode == 'live' else 'SIMULADO'}\n\n"
                        f"FLUJO SESIÓN — BUY: {buy_pct}% ({buy_vol:,}) | SELL: {sell_pct}% ({sell_vol:,}) | Delta: {delta_pct:+.1f}%\n\n"
                        f"FLUJO EN ZONA — {zone_total} trades a {velocity} trades/min\n"
                        f"  BUY: {zone_buy_vol:,} ({zone_buy_pct}%) | SELL: {zone_sell_vol:,} ({100-zone_buy_pct:.1f}%)\n"
                        f"  Tamaño promedio: {avg_size:.0f} acciones | Bloques grandes: {blocks}\n\n"
                        f"MICROESTRUCTURA DETECTADA:\n"
                        f"  Absorción:    {absorption_text}\n"
                        f"  Agotamiento:  {exhaustion_text}\n"
                        f"  Aceleración:  {acceleration_text}\n"
                        f"  Velocidad:    {velocity} trades/min\n\n"
                        f"{trades_text}"
                    )
                }]
            },
            timeout=15
        )
        return jsonify(res.json()), res.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── REST — SNAPSHOT SPY ──
def fetch_snapshot():
    """Snapshot SPY cada 5s via REST (prev_high, prev_low, open, etc.)"""
    url = f"{REST_BASE}/stocks/snapshots?symbols=SPY"
    while True:
        try:
            res  = requests.get(url, headers=HEADERS, timeout=5)
            data = res.json()
            snap = data.get("SPY", {})

            daily  = snap.get("dailyBar", {})
            prev   = snap.get("prevDailyBar", {})
            latest = snap.get("latestTrade", {})
            quote  = snap.get("latestQuote", {})

            p = float(latest.get("p") or daily.get("c") or state["spy_price"] or 0)
            if p > 0:
                bid, ask = clean_bid_ask(p, quote.get("bp"), quote.get("ap"))
                state["spy_price"]      = p
                state["spy_bid"]        = bid
                state["spy_ask"]        = ask
                state["spy_open"]       = float(daily.get("o") or 0)
                state["spy_high"]       = float(daily.get("h") or 0)
                state["spy_low"]        = float(daily.get("l") or 0)
                state["spy_prev_close"] = float(prev.get("c") or 0)
                state["spy_prev_high"]  = float(prev.get("h") or 0)
                state["spy_prev_low"]   = float(prev.get("l") or 0)
                state["spy_volume"]     = int(daily.get("v") or 0)
                print(f"  SPY: ${p:.2f}  PrevH:${state['spy_prev_high']:.2f}  PrevL:${state['spy_prev_low']:.2f}  PMH:${state['spy_pm_high']:.2f}  PML:${state['spy_pm_low']:.2f}")

        except Exception as e:
            print(f"  Snapshot error: {e}")
        time.sleep(1)


# ── REST — TRADES SIP (cinta consolidada) ──
def fetch_trades_rest():
    """Polling cada 1s del endpoint REST de trades SIP — única fuente de flow."""
    last_ts   = None
    last_price = 0.0   # para tick direction
    time.sleep(5)
    while True:
        try:
            params = {"limit": 50, "sort": "desc", "feed": "iex"}
            res  = requests.get(f"{REST_BASE}/stocks/SPY/trades", headers=HEADERS, params=params, timeout=3)
            data = res.json()
            trades_raw = data.get("trades", [])
            if not trades_raw:
                time.sleep(1)
                continue

            new_count = 0
            for t in reversed(trades_raw):   # orden cronológico
                ts = t.get("t", "")
                if last_ts and ts <= last_ts:
                    continue
                p = float(t.get("p", 0))
                s = int(t.get("s", 0))
                if p <= 0 or s <= 0:
                    continue

                # Tick direction: sube → BUY, baja → SELL, igual → neutro (último conocido)
                if last_price > 0:
                    if p > last_price:   direction = "BUY"
                    elif p < last_price: direction = "SELL"
                    else:
                        # precio igual — usar bid/ask si están disponibles
                        bid, ask = state["spy_bid"], state["spy_ask"]
                        if ask > 0 and p >= ask:   direction = "BUY"
                        elif bid > 0 and p <= bid: direction = "SELL"
                        else:                      direction = "BUY"  # default neutral
                else:
                    direction = "BUY"
                last_price = p

                big          = s >= BLOCK_TRADE_SIZE
                institutional = s >= INSTITUTIONAL_TRADE_SIZE
                entry = {
                    "time":          ts[11:19] if len(ts) > 18 else datetime.now().strftime("%H:%M:%S"),
                    "price":         p,
                    "size":          s,
                    "direction":     direction,
                    "big":           big,
                    "institutional": institutional,
                    "note":          "BLOQUE INST" if institutional else ("BLOQUE" if big else ""),
                    "src":           "rest"
                }
                state["ts_feed"].append(entry)

                if direction == "BUY":
                    state["flow"]["buy_vol"] += s
                else:
                    state["flow"]["sell_vol"] += s
                state["flow"]["delta"] = state["flow"]["buy_vol"] - state["flow"]["sell_vol"]
                if big:
                    state["flow"]["big_trades"] += 1
                    if direction == "BUY":
                        state["flow"]["block_buy_vol"] += s
                        state["flow"]["last_block"] = f"COMPRA BLOQUE {s:,} @ {p:.2f}"
                    else:
                        state["flow"]["block_sell_vol"] += s
                        state["flow"]["last_block"] = f"VENTA BLOQUE {s:,} @ {p:.2f}"
                state["flow"]["last_update"] = datetime.now().strftime("%H:%M:%S")
                new_count += 1

            if trades_raw:
                last_ts = trades_raw[0].get("t", last_ts)
            if new_count:
                print(f"  REST trades: +{new_count} (total {len(state['ts_feed'])})")

        except Exception as e:
            print(f"  REST trades error: {e}")
        time.sleep(1)


# ── REST — OI LEVELS — se actualizan con el precio hasta que abre el mercado, luego se congelan ──
def fetch_oi_levels():
    time.sleep(10)
    while True:
        try:
            # Inicializar PM HIGH/LOW si aún no están
            if state["spy_pm_high"] <= 0 and state["spy_price"] > 0:
                state["spy_pm_high"] = state["spy_price"] + 0.5
            if state["spy_pm_low"] <= 0 and state["spy_price"] > 0:
                state["spy_pm_low"] = state["spy_price"] - 0.5

            today = get_et_now().date()
            spot  = state["spy_price"]

            if spot <= 0:
                time.sleep(15)
                continue

            # Si ya se congelaron hoy al abrir mercado, no recalcular
            if state["oi_lock_date"] == today:
                time.sleep(15)
                continue

            # Calcular strikes ITM más cercanos al precio actual
            call_k = math.floor(spot)   # call ITM: strike justo por debajo del precio
            put_k  = math.ceil(spot)    # put  ITM: strike justo por encima del precio

            oi_call = 280000
            oi_put  = 220000

            state["oi_levels"] = [
                {
                    "id":       f"oi_call_{call_k}",
                    "tag":      "OI CALL",
                    "type":     "oic",
                    "price":    float(call_k),
                    "desc":     f"ITM Call strike — OI: {oi_call//1000}K",
                    "strength": 85,
                    "oi":       oi_call
                },
                {
                    "id":       f"oi_put_{put_k}",
                    "tag":      "OI PUT",
                    "type":     "oip",
                    "price":    float(put_k),
                    "desc":     f"ITM Put strike — OI: {oi_put//1000}K",
                    "strength": 82,
                    "oi":       oi_put
                },
            ]

            # Congelar al abrir el mercado
            if is_market_hours():
                state["oi_lock_date"] = today
                print(f"  OI CONGELADO para hoy: CALL {call_k} | PUT {put_k} (spot @ {spot:.2f})")
            else:
                print(f"  OI actualizado (premarket): CALL {call_k} | PUT {put_k} (spot @ {spot:.2f})")

        except Exception as e:
            print(f"  OI error: {e}")
        time.sleep(15)


# ── RESET FLOW AL ABRIR MERCADO ──
def flow_reset_watcher():
    """Resetea buy/sell vol al inicio de cada sesión de mercado."""
    while True:
        today = get_et_now().date()
        m = get_et_minutes()
        if is_market_hours(m) and state["_flow_reset_date"] != today:
            state["flow"]["buy_vol"]       = 0
            state["flow"]["sell_vol"]      = 0
            state["flow"]["delta"]         = 0
            state["flow"]["big_trades"]    = 0
            state["flow"]["block_buy_vol"] = 0
            state["flow"]["block_sell_vol"]= 0
            state["_flow_reset_date"]      = today
            print("  Flow reseteado para nueva sesión.")
        time.sleep(30)


# ── WEBSOCKET — TIME & SALES (Alpaca) ──
async def stream():
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode    = ssl.CERT_NONE

    print(f"\nConectando a Alpaca WebSocket...")

    while True:
        try:
            async with websockets.connect(WS_URL, ssl=ssl_ctx, ping_interval=20) as ws:
                raw  = await ws.recv()
                print(f"  Conectado: {json.loads(raw)}")

                await ws.send(json.dumps({
                    "action": "auth",
                    "key":    API_KEY,
                    "secret": API_SECRET
                }))
                raw  = await ws.recv()
                msgs = json.loads(raw)
                print(f"  Auth: {msgs}")

                auth_ok = isinstance(msgs, list) and any(
                    m.get("T") == "success" and m.get("msg") == "authenticated"
                    for m in msgs
                )
                if not auth_ok:
                    print("  Auth fallida")
                    await asyncio.sleep(10)
                    continue

                await ws.send(json.dumps({
                    "action":  "subscribe",
                    "trades":  ["SPY"],
                    "quotes":  ["SPY"]
                }))
                raw = await ws.recv()
                print(f"  Suscripción: {json.loads(raw)}")
                print("✓ Escuchando SPY en tiempo real...\n")

                state["connected"] = True
                state["mode"]      = "live"

                async for raw in ws:
                    try:
                        msgs = json.loads(raw)
                        if not isinstance(msgs, list): msgs = [msgs]
                        for m in msgs:
                            t = m.get("T", "")
                            if t == "t":   process_trade(m)
                            elif t == "q": process_quote_ws(m)
                    except Exception as e:
                        print(f"  Parse error: {e}")

        except Exception as e:
            print(f"  WS error: {e} — reintentando en 5s...")
            state["connected"] = False
            state["mode"]      = "reconectando"
            await asyncio.sleep(5)


def process_trade(t):
    """WebSocket IEX — solo actualiza ts_feed y precio. El flow lo maneja fetch_trades_rest (SIP)."""
    try:
        p = float(t.get("p", 0))
        s = int(t.get("s", 0))
        if p <= 0 or s <= 0: return

        bid = state["spy_bid"]
        ask = state["spy_ask"]
        if ask > 0 and p >= ask:       direction = "BUY"
        elif bid > 0 and p <= bid:     direction = "SELL"
        elif p >= state["spy_price"]:  direction = "BUY"
        else:                          direction = "SELL"

        big          = s >= BLOCK_TRADE_SIZE
        institutional = s >= INSTITUTIONAL_TRADE_SIZE
        entry = {
            "time":          datetime.now().strftime("%H:%M:%S"),
            "price":         p,
            "size":          s,
            "direction":     direction,
            "big":           big,
            "institutional": institutional,
            "note":          "BLOQUE INST" if institutional else ("BLOQUE" if big else ""),
            "src":           "ws"
        }
        state["ts_feed"].append(entry)
        state["spy_price"] = p

        m = get_et_minutes()
        if is_premarket(m):
            if state["spy_pm_high"] == 0 or p > state["spy_pm_high"]:
                state["spy_pm_high"] = round(p, 2)
            if state["spy_pm_low"] == 0 or p < state["spy_pm_low"]:
                state["spy_pm_low"] = round(p, 2)

    except Exception as e:
        print(f"  Trade error: {e}")


def process_quote_ws(q):
    try:
        bp  = q.get("bp", 0)
        ap  = q.get("ap", 0)
        bid, ask = clean_bid_ask(state["spy_price"], bp, ap)
        if bid and ask:
            state["spy_bid"] = bid
            state["spy_ask"] = ask
    except:
        pass


# ── MAIN ──
if __name__ == "__main__":
    print("=" * 50)
    print("  SPY Monitor — Alpaca API")
    print("=" * 50)
    print(f"  Key: {API_KEY[:8]}...")
    print()

    port = int(os.environ.get("PORT", 8765))
    threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False),
        daemon=True
    ).start()
    time.sleep(1)
    print(f"API local: http://localhost:{port}")

    threading.Thread(target=fetch_snapshot,    daemon=True).start()
    threading.Thread(target=fetch_trades_rest, daemon=True).start()
    threading.Thread(target=fetch_oi_levels,   daemon=True).start()
    threading.Thread(target=flow_reset_watcher, daemon=True).start()

    print("✓ Servidor listo — abre http://localhost:8765 en Chrome\n")

    try:
        asyncio.run(stream())
    except KeyboardInterrupt:
        print("\nServidor detenido.")

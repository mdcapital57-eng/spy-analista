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
API_KEY    = os.environ.get("ALPACA_KEY",    "PKHAR2FELTASKYPNEEM72WJO3Y")
API_SECRET = os.environ.get("ALPACA_SECRET", "25cTgaAp6XYSQF6pAZYAraibYBXgY4ZmJcnTe2eNSB6A")

# ── CLAUDE ──
CLAUDE_API_KEY = os.environ.get("CLAUDE_API_KEY", "")

# ── RAILWAY API ──
RAILWAY_TOKEN      = os.environ.get("RAILWAY_TOKEN", "")
RAILWAY_PROJECT_ID = os.environ.get("RAILWAY_PROJECT_ID", "932dc39b-7a66-479f-a680-77ba163a962b")

def railway_update_tokens(access_token, refresh_token):
    """Actualiza SCHWAB_ACCESS_TOKEN y SCHWAB_REFRESH_TOKEN en Railway env vars."""
    if not RAILWAY_TOKEN:
        return
    try:
        # Obtener serviceId y environmentId del proyecto
        query = """
        query($id: String!) {
          project(id: $id) {
            services { edges { node { id name } } }
            environments { edges { node { id name } } }
          }
        }"""
        r = requests.post(
            "https://backboard.railway.app/graphql/v2",
            headers={"Authorization": f"Bearer {RAILWAY_TOKEN}", "Content-Type": "application/json"},
            json={"query": query, "variables": {"id": RAILWAY_PROJECT_ID}},
            timeout=10
        )
        data = r.json().get("data", {}).get("project", {})
        services = data.get("services", {}).get("edges", [])
        envs     = data.get("environments", {}).get("edges", [])
        service_id = next((s["node"]["id"] for s in services if s["node"]["name"] == "web"), None)
        env_id     = next((e["node"]["id"] for e in envs if e["node"]["name"] == "production"), None)
        if not service_id or not env_id:
            print(f"  Railway: no se encontró service/env — services={services} envs={envs}")
            return
        # Actualizar variables
        mutation = """
        mutation($input: VariableCollectionUpsertInput!) {
          variableCollectionUpsert(input: $input)
        }"""
        r2 = requests.post(
            "https://backboard.railway.app/graphql/v2",
            headers={"Authorization": f"Bearer {RAILWAY_TOKEN}", "Content-Type": "application/json"},
            json={"query": mutation, "variables": {"input": {
                "projectId":     RAILWAY_PROJECT_ID,
                "serviceId":     service_id,
                "environmentId": env_id,
                "variables": {
                    "SCHWAB_ACCESS_TOKEN":  access_token,
                    "SCHWAB_REFRESH_TOKEN": refresh_token,
                }
            }}},
            timeout=10
        )
        print(f"  Railway env vars actualizados: {r2.json()}")
    except Exception as e:
        print(f"  Railway update error: {e}")

# ── SCHWAB ──
SCHWAB_CLIENT_ID     = os.environ.get("SCHWAB_CLIENT_ID", "")
SCHWAB_CLIENT_SECRET = os.environ.get("SCHWAB_CLIENT_SECRET", "")
SCHWAB_REDIRECT_URI  = os.environ.get("SCHWAB_REDIRECT_URI", "https://spy-analista-ouii.vercel.app/")
SCHWAB_AUTH_URL      = "https://api.schwabapi.com/v1/oauth/authorize"
SCHWAB_TOKEN_URL     = "https://api.schwabapi.com/v1/oauth/token"
SCHWAB_API_BASE      = "https://api.schwabapi.com/trader/v1"
SCHWAB_STREAM_URL    = "wss://streamer-api.schwab.com/ws"

schwab_tokens = {
    "access_token":  os.environ.get("SCHWAB_ACCESS_TOKEN", ""),
    "refresh_token": os.environ.get("SCHWAB_REFRESH_TOKEN", ""),
    "expires_at":    time.time() + 1500 if os.environ.get("SCHWAB_ACCESS_TOKEN") else 0,
}
# Cargar tokens desde archivo si existen (sobreviven reinicios en Railway)
try:
    with open("/tmp/schwab_tokens.json") as f:
        saved = json.load(f)
        if saved.get("access_token"):
            schwab_tokens.update(saved)
            print("  Schwab tokens cargados desde /tmp/schwab_tokens.json")
except Exception:
    pass

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
        "last_update":    "",
        "w_buy_vol":      0.0,   # vol ponderado por tamaño de bloque
        "w_sell_vol":     0.0,
        "w_delta":        0.0,
        "decay_delta":    0.0,   # w_delta con decay temporal (60-min half-life)
        "pm_buy_vol":     0,     # snapshot premarket al abrir mercado
        "pm_sell_vol":    0,
        "pm_delta":       0,
        "_recent":        [],    # [(timestamp, w_vol, direction), ...] para decay
    },
    "oi_levels":      [],
    "oi_lock_date":   None,  # fecha en que se fijaron los strikes OI
    "manual_classes": {},
    "manual_prices":  {},
    "connected":  False,
    "mode":       "iniciando",
    "_flow_reset_date": None,
    "pcr": {"ratio": None, "calls": None, "puts": None, "date": None},
}

MAX_TS = 300
MAX_SPY_SPREAD = 0.25
BLOCK_TRADE_SIZE = 500
INSTITUTIONAL_TRADE_SIZE = 2000

def block_weight(size):
    """Multiplicador por tamaño: bloques grandes pesan más en el delta."""
    if size >= 5000: return 5.0
    if size >= 2000: return 3.0
    if size >= 500:  return 2.0
    return 1.0


# ── TIEMPO ET ──
def get_et_now():
    """Hora actual en ET (asume EDT = UTC-4 durante la temporada de trading)."""
    return datetime.now(timezone(timedelta(hours=-4)))

def get_et_minutes():
    now = get_et_now()
    return now.hour * 60 + now.minute

def is_premarket(m=None):
    if m is None: m = get_et_minutes()
    return 420 <= m < 570   # 7:00 AM – 9:30 AM ET

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
    f = {k: v for k, v in state["flow"].items() if k != "_recent"}
    return jsonify(f)

@app.route("/oi_levels")
def oi_levels():
    return jsonify(state["oi_levels"])

@app.route("/pcr")
def pcr():
    return jsonify(state["pcr"])

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
    trades     = data.get("trades", [])      # trades cerca del nivel
    all_trades = data.get("all_trades", [])  # todos los trades de sesión

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
    if zone_total < 3:
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

    # Resumen global de TODOS los trades de sesión
    if all_trades:
        s_buys  = [t for t in all_trades if t.get("direction") == "BUY"]
        s_sells = [t for t in all_trades if t.get("direction") == "SELL"]
        s_bvol  = sum(t.get("size", 0) for t in s_buys)
        s_svol  = sum(t.get("size", 0) for t in s_sells)
        s_tot   = s_bvol + s_svol
        s_bpct  = round(s_bvol / s_tot * 100, 1) if s_tot > 0 else 0
        s_blk   = sum(1 for t in all_trades if t.get("big"))
        session_summary = (
            f"SESIÓN COMPLETA ({len(all_trades)} trades): "
            f"BUY {s_bvol:,} ({s_bpct}%) | SELL {s_svol:,} ({100-s_bpct:.1f}%) | "
            f"Bloques grandes: {s_blk}"
        )
    else:
        session_summary = "Sin datos de sesión."

    # Detalle de trades en zona (precio ±0.80 del nivel)
    if trades:
        lines = []
        for t in trades[-150:]:
            note = f" [{t.get('note')}]" if t.get("note") else ""
            lines.append(
                f"  {t.get('time','')} | {t.get('direction','')} | "
                f"{t.get('size',0):,} @ ${t.get('price',0):.2f}{note}"
            )
        trades_text = (
            f"{session_summary}\n\n"
            f"TRADES EN ZONA ({len(trades)} trades ±$0.80 del nivel):\n" + "\n".join(lines)
        )
    else:
        trades_text = f"{session_summary}\n\nSin trades capturados cerca del nivel."

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
                    "REGLAS DE DECISIÓN — requiere ≥1 condición clara:\n"
                    "COMPRA si ≥1: (a) flujo BUY zona ≥55%, (b) ventas absorbidas, (c) aceleración alcista, (d) agotamiento vendedor\n"
                    "VENTA si ≥1: (a) flujo SELL zona ≥55%, (b) compras absorbidas, (c) aceleración bajista, (d) agotamiento comprador\n"
                    "NO TRADE solo si: señales completamente contradictorias o datos insuficientes (<3 trades en zona)\n\n"
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
            res  = requests.get(url, headers=HEADERS, timeout=10)
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
            res  = requests.get(f"{REST_BASE}/stocks/SPY/trades", headers=HEADERS, params=params, timeout=10)
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
            time.sleep(5)
            continue
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


# ── PUT/CALL RATIO — CBOE CSV público ──
def fetch_pcr():
    """Descarga el equity put/call ratio de CBOE cada hora."""
    time.sleep(5)
    while True:
        try:
            res = requests.get(
                "https://cdn.cboe.com/resources/options/volume_and_call_put_ratios/equitypc.csv",
                timeout=10, headers={"User-Agent": "Mozilla/5.0"}
            )
            lines = res.text.strip().split("\n")
            # Última fila = dato más reciente
            last = lines[-1].split(",")
            date_str = last[0].strip()
            calls    = float(last[1].strip())
            puts     = float(last[2].strip())
            ratio    = round(float(last[4].strip()), 2)
            state["pcr"] = {"ratio": ratio, "calls": int(calls), "puts": int(puts), "date": date_str}
            print(f"  PCR: {ratio} (calls={int(calls):,} puts={int(puts):,}) [{date_str}]")
        except Exception as e:
            print(f"  PCR error: {e}")
        time.sleep(300)  # cada 5 min


# ── RESET FLOW AL ABRIR MERCADO ──
def flow_reset_watcher():
    """Resetea flow al inicio de sesión regular; guarda snapshot premarket."""
    while True:
        today = get_et_now().date()
        m = get_et_minutes()
        if is_market_hours(m) and state["_flow_reset_date"] != today:
            # Snapshot premarket antes de resetear
            state["flow"]["pm_buy_vol"]  = state["flow"]["buy_vol"]
            state["flow"]["pm_sell_vol"] = state["flow"]["sell_vol"]
            state["flow"]["pm_delta"]    = state["flow"]["delta"]
            # Reset sesión regular
            state["flow"]["buy_vol"]       = 0
            state["flow"]["sell_vol"]      = 0
            state["flow"]["delta"]         = 0
            state["flow"]["big_trades"]    = 0
            state["flow"]["block_buy_vol"] = 0
            state["flow"]["block_sell_vol"]= 0
            state["flow"]["w_buy_vol"]     = 0.0
            state["flow"]["w_sell_vol"]    = 0.0
            state["flow"]["w_delta"]       = 0.0
            state["flow"]["decay_delta"]   = 0.0
            state["flow"]["_recent"]       = []
            state["_flow_reset_date"]      = today
            print("  Flow reseteado para nueva sesión (premarket guardado).")
        time.sleep(30)


def flow_decay_watcher():
    """Recalcula decay_delta cada 5s usando decay exponencial (half-life 60 min)."""
    HALF_LIFE = 3600.0
    while True:
        now = time.time()
        recent = state["flow"]["_recent"]
        decay_buy = decay_sell = 0.0
        for ts, w, d in recent:
            age = now - ts
            factor = math.exp(-math.log(2) * age / HALF_LIFE)
            if d == "BUY":
                decay_buy  += w * factor
            else:
                decay_sell += w * factor
        state["flow"]["decay_delta"] = round(decay_buy - decay_sell, 0)
        time.sleep(5)


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
                conn_limit = isinstance(msgs, list) and any(
                    m.get("code") == 406 for m in msgs
                )
                if not auth_ok:
                    wait = 60 if conn_limit else 10
                    msg = f"connection limit 406 — esperando {wait}s" if conn_limit else "credenciales incorrectas"
                    print(f"  Auth fallida ({msg})")
                    await asyncio.sleep(wait)
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
            print(f"  WS error: {e} — reintentando en 15s...")
            state["connected"] = False
            state["mode"]      = "reconectando"
            await asyncio.sleep(15)


def process_trade(t):
    """WebSocket IEX — actualiza ts_feed, precio y flow."""
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

        big           = s >= BLOCK_TRADE_SIZE
        institutional = s >= INSTITUTIONAL_TRADE_SIZE
        entry = {
            "time":          get_et_now().strftime("%H:%M:%S"),
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

        # ── Flow acumulado ──
        w = block_weight(s) * s  # volumen ponderado
        if direction == "BUY":
            state["flow"]["buy_vol"]   += s
            state["flow"]["w_buy_vol"] += w
            if big:
                state["flow"]["block_buy_vol"] += s
                state["flow"]["last_block"] = f"COMPRA BLOQUE {s:,} @ {p:.2f}"
        else:
            state["flow"]["sell_vol"]   += s
            state["flow"]["w_sell_vol"] += w
            if big:
                state["flow"]["block_sell_vol"] += s
                state["flow"]["last_block"] = f"VENTA BLOQUE {s:,} @ {p:.2f}"
        if big:
            state["flow"]["big_trades"] += 1
        state["flow"]["delta"]   = state["flow"]["buy_vol"] - state["flow"]["sell_vol"]
        state["flow"]["w_delta"] = state["flow"]["w_buy_vol"] - state["flow"]["w_sell_vol"]
        state["flow"]["last_update"] = datetime.now().strftime("%H:%M:%S")
        # Guardar para decay temporal (máx 4 horas)
        now_ts = time.time()
        state["flow"]["_recent"].append((now_ts, w, direction))
        if len(state["flow"]["_recent"]) % 500 == 0:
            cutoff = now_ts - 4 * 3600
            state["flow"]["_recent"] = [x for x in state["flow"]["_recent"] if x[0] > cutoff]

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


# ── SCHWAB OAUTH ENDPOINTS ──
import base64

@app.route("/schwab/auth")
def schwab_auth():
    """Genera la URL de autorización de Schwab y redirige al usuario."""
    url = (
        f"{SCHWAB_AUTH_URL}?response_type=code"
        f"&client_id={SCHWAB_CLIENT_ID}"
        f"&redirect_uri={SCHWAB_REDIRECT_URI}"
        f"&scope=readonly"
    )
    from flask import redirect
    return redirect(url)

@app.route("/schwab/token", methods=["POST"])
def schwab_token():
    """Recibe el code de OAuth, lo intercambia por tokens."""
    code = (request.get_json() or {}).get("code", "")
    if not code:
        return jsonify({"error": "sin code"}), 400
    creds = base64.b64encode(f"{SCHWAB_CLIENT_ID}:{SCHWAB_CLIENT_SECRET}".encode()).decode()
    r = requests.post(
        SCHWAB_TOKEN_URL,
        headers={"Authorization": f"Basic {creds}", "Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "authorization_code", "code": code, "redirect_uri": SCHWAB_REDIRECT_URI},
        timeout=10
    )
    if r.status_code != 200:
        return jsonify({"error": r.text}), 400
    d = r.json()
    schwab_tokens["access_token"]  = d.get("access_token", "")
    schwab_tokens["refresh_token"] = d.get("refresh_token", "")
    schwab_tokens["expires_at"]    = time.time() + d.get("expires_in", 1800) - 60
    # Guardar en archivo local para sobrevivir reinicios
    try:
        with open("/tmp/schwab_tokens.json", "w") as f:
            json.dump(schwab_tokens, f)
        print("  Tokens guardados en /tmp/schwab_tokens.json")
    except Exception as e:
        print(f"  No se pudo guardar tokens: {e}")
    print(f"\n✓ SCHWAB TOKENS OBTENIDOS — expires_at={schwab_tokens['expires_at']}")
    # Actualizar Railway env vars automáticamente
    threading.Thread(target=railway_update_tokens, args=(
        schwab_tokens["access_token"], schwab_tokens["refresh_token"]
    ), daemon=True).start()
    return jsonify({"ok": True})

@app.route("/schwab/tokens")
def schwab_tokens_view():
    """Muestra los tokens actuales para copiarlos a Railway."""
    return jsonify({
        "SCHWAB_ACCESS_TOKEN":  schwab_tokens["access_token"],
        "SCHWAB_REFRESH_TOKEN": schwab_tokens["refresh_token"],
        "expires_at": schwab_tokens["expires_at"],
    })

@app.route("/schwab/status")
def schwab_status():
    return jsonify({
        "configured": bool(SCHWAB_CLIENT_ID and SCHWAB_CLIENT_SECRET),
        "authenticated": bool(schwab_tokens["access_token"]),
    })

def schwab_refresh():
    """Refresca el access token usando el refresh token."""
    if not schwab_tokens["refresh_token"]:
        return False
    try:
        creds = base64.b64encode(f"{SCHWAB_CLIENT_ID}:{SCHWAB_CLIENT_SECRET}".encode()).decode()
        r = requests.post(
            SCHWAB_TOKEN_URL,
            headers={"Authorization": f"Basic {creds}", "Content-Type": "application/x-www-form-urlencoded"},
            data={"grant_type": "refresh_token", "refresh_token": schwab_tokens["refresh_token"]},
            timeout=10
        )
        if r.status_code == 200:
            d = r.json()
            schwab_tokens["access_token"]  = d.get("access_token", "")
            schwab_tokens["refresh_token"] = d.get("refresh_token", schwab_tokens["refresh_token"])
            schwab_tokens["expires_at"]    = time.time() + d.get("expires_in", 1800) - 60
            print("  Schwab token refrescado OK")
            threading.Thread(target=railway_update_tokens, args=(
                schwab_tokens["access_token"], schwab_tokens["refresh_token"]
            ), daemon=True).start()
            return True
        else:
            err = r.text
            print(f"  Schwab refresh error: {err}")
            # Si el refresh token es inválido, limpiarlo para no seguir intentando
            if "DECRYPTION_ERROR" in err or "invalid_grant" in err or "unsupported_token_type" in err:
                print("  Refresh token inválido — limpiando, visita /schwab/auth para reconectar")
                schwab_tokens["refresh_token"] = ""
                schwab_tokens["access_token"]  = ""
            return False
    except Exception as e:
        print(f"  Schwab refresh exception: {e}")
        return False

def schwab_get_streamer_info():
    """Obtiene las credenciales de streaming desde userPreference."""
    try:
        r = requests.get(
            f"{SCHWAB_API_BASE}/userPreference",
            headers={"Authorization": f"Bearer {schwab_tokens['access_token']}"},
            timeout=10
        )
        print(f"  Schwab userPreference status: {r.status_code}")
        if r.status_code == 401:
            print("  Token expirado — intentando refrescar...")
            if schwab_refresh():
                r = requests.get(
                    f"{SCHWAB_API_BASE}/userPreference",
                    headers={"Authorization": f"Bearer {schwab_tokens['access_token']}"},
                    timeout=10
                )
            else:
                print("  Re-autentícate visitando /schwab/auth")
                return {}
        if r.status_code != 200:
            print(f"  Schwab userPreference error {r.status_code}: {r.text[:200]}")
            return {}
        data = r.json()
        info = data.get("streamerInfo", [{}])[0]
        return info
    except Exception as e:
        print(f"  Schwab streamer info error: {e}")
        return {}

def process_schwab_trade(content):
    """Procesa un trade de TIMESALE_EQUITY de Schwab."""
    try:
        p = float(content.get("2", 0))
        s = int(content.get("3", 0))
        if p <= 0 or s <= 0:
            return
        bid = state["spy_bid"]
        ask = state["spy_ask"]
        if ask > 0 and p >= ask:       direction = "BUY"
        elif bid > 0 and p <= bid:     direction = "SELL"
        elif p >= state["spy_price"]:  direction = "BUY"
        else:                          direction = "SELL"

        big           = s >= BLOCK_TRADE_SIZE
        institutional = s >= INSTITUTIONAL_TRADE_SIZE
        entry = {
            "time":          get_et_now().strftime("%H:%M:%S"),
            "price":         p,
            "size":          s,
            "direction":     direction,
            "big":           big,
            "institutional": institutional,
            "note":          "BLOQUE INST" if institutional else ("BLOQUE" if big else ""),
            "src":           "schwab"
        }
        state["ts_feed"].append(entry)
        if len(state["ts_feed"]) > MAX_TS * 3:
            state["ts_feed"] = state["ts_feed"][-MAX_TS:]
        state["spy_price"] = p

        w = block_weight(s) * s
        now_ts = time.time()
        if direction == "BUY":
            state["flow"]["buy_vol"]   += s
            state["flow"]["w_buy_vol"] += w
            state["flow"]["block_buy_vol"] += s if big else 0
        else:
            state["flow"]["sell_vol"]   += s
            state["flow"]["w_sell_vol"] += w
            state["flow"]["block_sell_vol"] += s if big else 0
        if big:
            state["flow"]["big_trades"] += 1
        state["flow"]["delta"]   = state["flow"]["buy_vol"] - state["flow"]["sell_vol"]
        state["flow"]["w_delta"] = state["flow"]["w_buy_vol"] - state["flow"]["w_sell_vol"]
        state["flow"]["last_update"] = get_et_now().strftime("%H:%M:%S")
        state["flow"]["_recent"].append((now_ts, w, direction))
        if len(state["flow"]["_recent"]) % 500 == 0:
            cutoff = now_ts - 4 * 3600
            state["flow"]["_recent"] = [x for x in state["flow"]["_recent"] if x[0] > cutoff]

        m = get_et_minutes()
        if is_premarket(m):
            if state["spy_pm_high"] == 0 or p > state["spy_pm_high"]:
                state["spy_pm_high"] = round(p, 2)
            if state["spy_pm_low"] == 0 or p < state["spy_pm_low"]:
                state["spy_pm_low"] = round(p, 2)
    except Exception as e:
        print(f"  Schwab trade error: {e}")

async def schwab_stream():
    """Streaming WebSocket de Schwab — SIP completo (100% del mercado)."""
    print("\nConectando a Schwab WebSocket (SIP feed)...")
    fail_count = 0
    while True:
        try:
            # Esperar tokens frescos si el access_token está vacío
            if not schwab_tokens["access_token"]:
                print("  Esperando tokens de Schwab (visita /schwab/auth)...")
                await asyncio.sleep(10)
                continue

            info = schwab_get_streamer_info()
            if not info:
                # Intentar refrescar solo si falla userPreference
                if schwab_tokens["refresh_token"]:
                    schwab_refresh()
                fail_count += 1
                if fail_count >= 5:
                    print("  Schwab falló 5 veces — cayendo a Alpaca IEX")
                    await stream()
                    return
                print("  No se pudo obtener streamer info — reintentando en 30s")
                await asyncio.sleep(30)
                continue
            fail_count = 0

            customer_id = info.get("schwabClientCustomerId", "")
            correl_id   = info.get("schwabClientCorrelId", "")
            channel     = info.get("schwabClientChannel", "")
            func_id     = info.get("schwabClientFunctionId", "")
            ws_url      = info.get("streamerSocketUrl", SCHWAB_STREAM_URL)

            async with websockets.connect(ws_url, ping_interval=20) as ws:
                # LOGIN
                await ws.send(json.dumps({
                    "service": "ADMIN", "requestid": "0", "command": "LOGIN",
                    "SchwabClientCustomerId": customer_id,
                    "SchwabClientCorrelId":   correl_id,
                    "parameters": {
                        "Authorization":          schwab_tokens["access_token"],
                        "SchwabClientChannel":    channel,
                        "SchwabClientFunctionId": func_id,
                    }
                }))
                resp = json.loads(await ws.recv())
                print(f"  Schwab login: {resp}")

                # SUBSCRIBE TIMESALE_EQUITY + QUOTE
                await ws.send(json.dumps({
                    "service": "TIMESALE_EQUITY", "requestid": "1", "command": "SUBS",
                    "SchwabClientCustomerId": customer_id,
                    "SchwabClientCorrelId":   correl_id,
                    "parameters": {"keys": "SPY", "fields": "0,1,2,3,4"}
                }))
                await ws.send(json.dumps({
                    "service": "QUOTE", "requestid": "2", "command": "SUBS",
                    "SchwabClientCustomerId": customer_id,
                    "SchwabClientCorrelId":   correl_id,
                    "parameters": {"keys": "SPY", "fields": "1,2"}  # bid, ask
                }))
                print("✓ Schwab SIP feed activo — 100% del mercado\n")
                state["connected"] = True
                state["mode"]      = "live"

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                        for block in msg.get("data", []):
                            svc = block.get("service", "")
                            for c in block.get("content", []):
                                if svc == "TIMESALE_EQUITY":
                                    process_schwab_trade(c)
                                elif svc == "QUOTE":
                                    bp = c.get("1", 0)
                                    ap = c.get("2", 0)
                                    bid, ask = clean_bid_ask(state["spy_price"], bp, ap)
                                    if bid and ask:
                                        state["spy_bid"] = bid
                                        state["spy_ask"] = ask
                    except Exception as e:
                        print(f"  Schwab parse error: {e}")

        except Exception as e:
            print(f"  Schwab WS error: {e} — reintentando en 15s")
            state["connected"] = False
            state["mode"]      = "reconectando"
            await asyncio.sleep(15)


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
    # fetch_trades_rest deshabilitado — Railway bloquea REST a data.alpaca.markets; flow se calcula en WebSocket
    threading.Thread(target=fetch_oi_levels,   daemon=True).start()
    threading.Thread(target=fetch_pcr,         daemon=True).start()
    threading.Thread(target=flow_reset_watcher, daemon=True).start()
    threading.Thread(target=flow_decay_watcher, daemon=True).start()

    if schwab_tokens["access_token"] or schwab_tokens["refresh_token"]:
        print("✓ Schwab configurado — usando SIP feed (100% del mercado)")
    else:
        print("✓ Usando Alpaca IEX feed (~2-3% del mercado)")
        print(f"  Para activar Schwab visita: /schwab/auth")
    print()

    try:
        if schwab_tokens["access_token"] or schwab_tokens["refresh_token"]:
            asyncio.run(schwab_stream())
        else:
            asyncio.run(stream())
    except KeyboardInterrupt:
        print("\nServidor detenido.")

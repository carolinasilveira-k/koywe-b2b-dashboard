#!/usr/bin/env python3
"""
Koywe B2B Dashboard — Auto-updater (Metabase API only, no direct MongoDB)
- Rampa: MongoDB "Rampa" (db_id=14) via Metabase REST API
- OTC:   MongoDB "Rampa" (db_id=14) via Metabase REST API
- K3:    K3 PostgreSQL (db_id=7)   via Metabase REST API
No pymongo / MONGODB_URI required.
"""
import os, re, json, calendar, requests
from datetime import datetime, timedelta

# ── Config ────────────────────────────────────────────────────────────────────
METABASE_API_KEY = os.environ["METABASE_API_KEY"]
METABASE_URL     = "https://metabase.koywe.com"
RAMPA_DB         = 14   # MongoDB "Rampa"
K3_DB            = 7    # K3 PostgreSQL
HDRS = {"x-api-key": METABASE_API_KEY, "Content-Type": "application/json"}

TODAY        = datetime.utcnow()
MONTH_START  = datetime(TODAY.year, TODAY.month, 1)
_nm          = (TODAY.month % 12) + 1
_ny          = TODAY.year + (1 if TODAY.month == 12 else 0)
MONTH_END    = datetime(_ny, _nm, 1)
PREV_START   = datetime(TODAY.year - 1 if TODAY.month == 1 else TODAY.year,
                        12 if TODAY.month == 1 else TODAY.month - 1, 1)
PREV_END     = MONTH_START
WEEK_AGO     = TODAY - timedelta(days=7)

DAYS_ELAPSED  = TODAY.day
DAYS_IN_MONTH = calendar.monthrange(TODAY.year, TODAY.month)[1]
PACE          = DAYS_ELAPSED / DAYS_IN_MONTH

MESES = {1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",5:"Mayo",6:"Junio",
         7:"Julio",8:"Agosto",9:"Septiembre",10:"Octubre",11:"Noviembre",12:"Diciembre"}
CUR_MES  = MESES[TODAY.month]
PREV_MES = MESES[PREV_START.month]

OTC_1 = "63eae12e45c8376a48c70bac"
OTC_2 = "6908f615c2c27d0132c7609c"
ORDER_TYPES = ["crypto-currency", "currency-crypto", "settlement", "topUp"]

INTERNAL_SLUGS = (
    "'koywe-3-sys','koywe-spa','koywe-arg','koywe-s-de-rl-de-cv','koywe-inc',"
    "'chile-demo-merchant','koywe-peru-sac','koywe-tecnologa-y-software-ltda',"
    "'koywe-sas','koywe-otc','otc-koywe-con-deuda'"
)

# ── Formatters ────────────────────────────────────────────────────────────────
def fmt(n):
    if n >= 1_000_000: return f"${n/1_000_000:.1f}M"
    if n >= 1_000:     return f"${n/1_000:.0f}K"
    return f"${n:.0f}"

def fmt_full(n):
    return f"${n:,.0f}"

def calc_pct(curr, prev):
    if prev == 0: return None
    return round((curr - prev) / prev * 100, 1)

def pct_str(p):
    if p is None: return "+∞%"
    return f"{'+' if p >= 0 else ''}{p:.1f}%"

def oid(s):     return {"$oid": s}
def dt(d):      return {"$date": d.strftime("%Y-%m-%dT%H:%M:%S.000Z")}

def norm_id(v):
    """Normalise a MongoDB _id to a plain hex string."""
    if isinstance(v, dict) and "$oid" in v:
        return v["$oid"]
    return str(v) if v is not None else None

# ── Metabase helpers ──────────────────────────────────────────────────────────
def mb_mongo(coll, pipeline):
    """Run a MongoDB aggregation via Metabase /api/dataset."""
    r = requests.post(
        f"{METABASE_URL}/api/dataset", headers=HDRS, timeout=90,
        json={"database": RAMPA_DB, "type": "native",
              "native": {"collection": coll, "query": json.dumps(pipeline)}}
    )
    d = r.json()
    if "error" in d:
        print(f"  WARNING mb_mongo({coll}): {d['error']}")
        return []
    cols = [c["name"] for c in d.get("data", {}).get("cols", [])]
    return [dict(zip(cols, row)) for row in d.get("data", {}).get("rows", [])]

def mb_sql(sql):
    """Run a SQL query via Metabase /api/dataset (K3 PostgreSQL)."""
    r = requests.post(
        f"{METABASE_URL}/api/dataset", headers=HDRS, timeout=60,
        json={"database": K3_DB, "type": "native", "native": {"query": sql}}
    )
    return r.json().get("data", {}).get("rows", [])

def first_num(rows, key):
    if not rows: return 0.0
    v = rows[0].get(key)
    return float(v) if v is not None else 0.0

# ── Rampa queries (MongoDB) ───────────────────────────────────────────────────
def _rampa_base(start, end):
    return {
        "createdAt": {"$gte": dt(start), "$lt": dt(end)},
        "status": "PAYED",
        "orderType": {"$in": ORDER_TYPES},
        "lastPriceUsd": {"$gt": 0},
        "metaAccount": {"$nin": [oid(OTC_1), oid(OTC_2)]}
    }

def query_rampa_vol(start, end):
    rows = mb_mongo("paymentorders", [
        {"$match": _rampa_base(start, end)},
        {"$group": {"_id": None,
                    "v": {"$sum": {"$divide": ["$currencyAmount", "$lastPriceUsd"]}}}}
    ])
    return first_num(rows, "v")

def query_rampa():
    """(vol_mtd, vol_prev, mau, active_clients, new_clients)"""
    vol_mtd  = query_rampa_vol(MONTH_START, MONTH_END)
    vol_prev = query_rampa_vol(PREV_START, PREV_END)

    # MAU = active merchants (distinct metaAccount), same as active clients
    # accountId = individual end-user, not merchant-level
    active_rows = mb_mongo("paymentorders", [
        {"$match": _rampa_base(MONTH_START, MONTH_END)},
        {"$group": {"_id": "$metaAccount"}}, {"$count": "n"}
    ])
    active = int(first_num(active_rows, "n"))
    mau = active  # same metric, both merchant-level

    new_rows = mb_mongo("metaaccounts", [
        {"$match": {"createdAt": {"$gte": dt(MONTH_START), "$lt": dt(MONTH_END)},
                    "_id": {"$nin": [oid(OTC_1), oid(OTC_2)]}}},
        {"$count": "n"}
    ])
    new_clients = int(first_num(new_rows, "n"))

    return vol_mtd, vol_prev, mau, active, new_clients

def query_rampa_performers():
    """List of {n, m, f, p, w} sorted by m desc. Returns None on failure."""
    try:
        # Get metaaccount names
        name_rows = mb_mongo("metaaccounts", [
            {"$project": {"_id": 1, "name": 1}}, {"$limit": 500}
        ])
        names = {norm_id(r.get("_id")): r.get("name", "Unknown") for r in name_rows}

        # Current month vol by metaAccount
        cur_rows = mb_mongo("paymentorders", [
            {"$match": _rampa_base(MONTH_START, MONTH_END)},
            {"$group": {"_id": "$metaAccount",
                        "m": {"$sum": {"$divide": ["$currencyAmount", "$lastPriceUsd"]}}}}
        ])
        cur = {norm_id(r["_id"]): float(r["m"] or 0) for r in cur_rows}

        # Prev month vol by metaAccount
        prv_rows = mb_mongo("paymentorders", [
            {"$match": _rampa_base(PREV_START, PREV_END)},
            {"$group": {"_id": "$metaAccount",
                        "f": {"$sum": {"$divide": ["$currencyAmount", "$lastPriceUsd"]}}}}
        ])
        prv = {norm_id(r["_id"]): float(r["f"] or 0) for r in prv_rows}

        # Active this week
        week_rows = mb_mongo("paymentorders", [
            {"$match": {**_rampa_base(WEEK_AGO, MONTH_END),
                        "createdAt": {"$gte": dt(WEEK_AGO), "$lt": dt(MONTH_END)}}},
            {"$group": {"_id": "$metaAccount"}}
        ])
        week_ids = {norm_id(r["_id"]) for r in week_rows}

        result = []
        for mid in set(cur) | set(prv):
            m = cur.get(mid, 0)
            f = prv.get(mid, 0)
            result.append({
                "n": names.get(mid, "Unknown"),
                "m": round(m), "f": round(f),
                "p": calc_pct(m, f),
                "w": mid in week_ids
            })
        result.sort(key=lambda x: -x["m"])
        return result
    except Exception as e:
        print(f"  WARNING rampa_performers: {e}")
        return None

# ── OTC queries (MongoDB) ─────────────────────────────────────────────────────
def _otc_match(start, end):
    return {"createdAt": {"$gte": dt(start), "$lt": dt(end)},
            "metaAccount": oid(OTC_1), "status": "PAYED",
            "orderType": {"$in": ORDER_TYPES}, "lastPriceUsd": {"$gt": 0}}

def query_otc():
    """(vol_mtd, vol_prev, mau, new_clients)"""
    def vol(start, end):
        rows = mb_mongo("paymentorders", [
            {"$match": _otc_match(start, end)},
            {"$group": {"_id": None,
                        "v": {"$sum": {"$divide": ["$currencyAmount", "$lastPriceUsd"]}}}}
        ])
        return first_num(rows, "v")

    vol_mtd  = vol(MONTH_START, MONTH_END)
    vol_prev = vol(PREV_START, PREV_END)

    mau_rows = mb_mongo("paymentorders", [
        {"$match": _otc_match(MONTH_START, MONTH_END)},
        {"$group": {"_id": "$accountId"}}, {"$count": "n"}
    ])
    mau = int(first_num(mau_rows, "n"))

    new_rows = mb_mongo("accounts", [
        {"$match": {"metaAccount": oid(OTC_1),
                    "createdAt": {"$gte": dt(MONTH_START), "$lt": dt(MONTH_END)}}},
        {"$count": "n"}
    ])
    new_clients = int(first_num(new_rows, "n"))

    return vol_mtd, vol_prev, mau, new_clients

# ── K3 queries (PostgreSQL) ───────────────────────────────────────────────────
def _get_fx():
    try:
        rates = requests.get("https://open.er-api.com/v6/latest/USD", timeout=10).json().get("rates", {})
    except Exception as e:
        print(f"  WARNING FX: {e}")
        rates = {}
    fallback = {"CLP":920,"ARS":1450,"PEN":3.45,"MXN":17.9,"COP":3700,
                "BRL":5.3,"EUR":0.87,"HKD":7.83,"BOB":6.9,"USD":1,"USDT":1,"USDC":1}
    for k, v in fallback.items():
        rates.setdefault(k, v)
    rates["USDT"] = rates["USDC"] = 1.0
    return rates

def query_k3():
    """(vol_mtd, vol_prev, merch_active, merch_new, performers_list)"""
    ms = MONTH_START.strftime("%Y-%m-%d")
    ps = PREV_START.strftime("%Y-%m-%d")
    me = MONTH_END.strftime("%Y-%m-%d")
    fx = _get_fx()

    def rows_to_usd(rows):
        totals = {}
        for row in rows:
            slug = str(row[0] or "")
            ccy  = str(row[1] or "CLP")
            amt  = float(row[2] or 0)
            rate = fx.get(ccy, 1.0)
            totals[slug] = totals.get(slug, 0) + (amt / rate if rate else 0)
        return totals

    sql_vol = """
SELECT m.slug, COALESCE(q."originCurrencySymbol",'CLP'), SUM(o."amountIn")
FROM "Orders" o
LEFT JOIN "Quote" q ON o."quoteId"=q.id
JOIN "Merchants" m ON o."merchantId"=m.id
WHERE o."createdAt" >= '{s}' AND o."createdAt" < '{e}'
  AND o.status='COMPLETED' AND m.slug NOT IN ({sl})
GROUP BY m.slug, 2
"""
    cur_rows  = mb_sql(sql_vol.format(s=ms, e=me, sl=INTERNAL_SLUGS))
    prev_rows = mb_sql(sql_vol.format(s=ps, e=ms, sl=INTERNAL_SLUGS))
    cur  = rows_to_usd(cur_rows)
    prev = rows_to_usd(prev_rows)

    vol_mtd  = sum(cur.values())
    vol_prev = sum(prev.values())

    merch_active = len(cur)

    # New merchants = active this month but never before
    sql_new = f"""
SELECT COUNT(DISTINCT o."merchantId")
FROM "Orders" o
JOIN "Merchants" m ON o."merchantId"=m.id
WHERE o."createdAt" >= '{ms}' AND o.status='COMPLETED'
  AND m.slug NOT IN ({INTERNAL_SLUGS})
  AND o."merchantId" NOT IN (
    SELECT DISTINCT "merchantId" FROM "Orders"
    WHERE "createdAt" < '{ms}' AND status='COMPLETED'
  )
"""
    nr = mb_sql(sql_new)
    merch_new = int(nr[0][0] or 0) if nr else 0

    # Active this week
    sql_week = f"""
SELECT DISTINCT m.slug FROM "Orders" o
JOIN "Merchants" m ON o."merchantId"=m.id
WHERE o."createdAt" >= (NOW() - INTERVAL '7 days') AND o.status='COMPLETED'
  AND m.slug NOT IN ({INTERNAL_SLUGS})
"""
    week_rows  = mb_sql(sql_week)
    week_slugs = {r[0] for r in week_rows}

    # Build performers
    all_slugs = set(cur) | set(prev)
    performers = []
    for slug in all_slugs:
        m = cur.get(slug, 0)
        f = prev.get(slug, 0)
        performers.append({
            "n": slug, "m": round(m), "f": round(f),
            "p": calc_pct(m, f), "w": slug in week_slugs
        })
    performers.sort(key=lambda x: -x["m"])

    return vol_mtd, vol_prev, merch_active, merch_new, performers

# ── JS array formatter ────────────────────────────────────────────────────────
def js_arr(items):
    parts = []
    for c in items:
        p_str = "null" if c["p"] is None else str(c["p"])
        w_str = "true" if c["w"] else "false"
        n_esc = c["n"].replace("\\","\\\\").replace('"','\\"')
        parts.append(f'  {{n:"{n_esc}",m:{c["m"]},f:{c["f"]},p:{p_str},w:{w_str}}}')
    return "[\n" + ",\n".join(parts) + "\n]"

# ── Run all queries ───────────────────────────────────────────────────────────
print(f"Date: {TODAY.strftime('%Y-%m-%d %H:%M')} UTC | "
      f"Pace: {DAYS_ELAPSED}/{DAYS_IN_MONTH} ({PACE*100:.1f}%)")

print("\nQuerying Rampa (MongoDB via Metabase)...")
rampa_vol, rampa_vol_prev, rampa_mau, rampa_active, rampa_new = query_rampa()
rampa_proj = rampa_vol / PACE if PACE > 0 else 0
print(f"  MTD={fmt(rampa_vol)}  prev={fmt(rampa_vol_prev)}  proj={fmt(rampa_proj)}")
print(f"  MAU={rampa_mau}  active={rampa_active}  new={rampa_new}")

print("\nQuerying Rampa performers...")
rampa_perf = query_rampa_performers()
if rampa_perf:
    print(f"  {len(rampa_perf)} entries, top: {rampa_perf[0]['n']} {fmt(rampa_perf[0]['m'])}")

print("\nQuerying OTC (MongoDB via Metabase)...")
otc_vol, otc_vol_prev, otc_mau, otc_new = query_otc()
otc_proj  = otc_vol / PACE if PACE > 0 else 0
otc_daily = otc_vol / DAYS_ELAPSED if DAYS_ELAPSED > 0 else 0
otc_pct   = calc_pct(otc_vol, otc_vol_prev)
print(f"  MTD={fmt(otc_vol)}  prev={fmt(otc_vol_prev)}  proj={fmt(otc_proj)}")
print(f"  MAU={otc_mau}  new={otc_new}  change={pct_str(otc_pct)}")

print("\nQuerying K3 (PostgreSQL via Metabase)...")
k3_vol, k3_vol_prev, k3_merch, k3_merch_new, k3_perf = query_k3()
k3_proj = k3_vol / PACE if PACE > 0 else 0
print(f"  MTD={fmt(k3_vol)}  prev={fmt(k3_vol_prev)}  proj={fmt(k3_proj)}")
print(f"  Merchants={k3_merch}  new={k3_merch_new}  performers={len(k3_perf)}")

total_vol  = rampa_vol + otc_vol + k3_vol
total_proj = total_vol / PACE if PACE > 0 else 0
print(f"\nTotal: {fmt(total_vol)}  proj: {fmt(total_proj)}")

# ── Load HTML ─────────────────────────────────────────────────────────────────
with open("index.html", "r", encoding="utf-8") as f:
    html = f.read()

# ── Helper: safe sub ─────────────────────────────────────────────────────────
def sub(pattern, repl, s, count=0, flags=0):
    new_s = re.sub(pattern, repl, s, count=count, flags=flags)
    if new_s == s:
        print(f"  WARN: pattern not found: {pattern[:60]}")
    return new_s

# ── Date & Pace ───────────────────────────────────────────────────────────────
new_date = TODAY.strftime("%-d %b %Y")
html = re.sub(r'Actualizado \d+ \w+ \d+', f'Actualizado {new_date}', html)
html = re.sub(r'Pace: \d+/\d+ días \([\d.]+% del mes\)',
              f'Pace: {DAYS_ELAPSED}/{DAYS_IN_MONTH} días ({PACE*100:.1f}% del mes)', html)
html = re.sub(r'pace \d+/\d+ días',
              f'pace {DAYS_ELAPSED}/{DAYS_IN_MONTH} días', html)

# ── Month name labels ─────────────────────────────────────────────────────────
# Chart titles with month names
html = re.sub(r'Volumen por Segmento — \w+ MTD',
              f'Volumen por Segmento — {CUR_MES} MTD', html)
html = re.sub(r'\w+ vs \w+ por Segmento',
              f'{CUR_MES} vs {PREV_MES} por Segmento', html)
html = re.sub(r'Top Clientes OTC — Volumen \w+ \d{4}',
              f'Top Clientes OTC — Volumen {CUR_MES} {TODAY.year}', html)
html = re.sub(r'\w+ MTD \(\d+d\)',
              f'{CUR_MES} MTD ({DAYS_ELAPSED}d)', html)
# KPI labels with month names
html = re.sub(r'(Clientes Activos )\w+(</div><div class="value" style="color:var\(--lima\)">)',
              rf'\g<1>{CUR_MES}\g<2>', html)
html = re.sub(r'(Nuevos Onboarded )\w+(</div><div class="value" style="color:#4ade80">)',
              rf'\g<1>{CUR_MES}\g<2>', html)
html = re.sub(r'(vs )\w+( \d{4}</div><div class="value">)',
              rf'\g<1>{PREV_MES}\g<2>', html)
html = re.sub(r'(Nuevos en )\w+(</div><div class="value" style="color:#fbbf24">)',
              rf'\g<1>{CUR_MES}\g<2>', html)
html = re.sub(r'(pace-label">Volumen )\w+( MTD</span>)',
              rf'\g<1>{CUR_MES}\g<2>', html)
html = re.sub(r'(pace-label">Volumen )\w+(</span><span class="pace-value">)\$[\d,]+',
              rf'\g<1>{PREV_MES}\g<2>{fmt_full(otc_vol_prev)}', html)
html = re.sub(r'(Marzo MTD</div><div class="sub">)',
              rf'{CUR_MES} MTD\g<1>', html)   # OTC/Rampa KPI sub

# ── Segment cards ─────────────────────────────────────────────────────────────
# Rampa
html = re.sub(
    r'(seg-badge" style="background:rgba\(56,189,248,0\.15\);color:#38bdf8">)\d+( clientes</span>)',
    rf'\g<1>{rampa_active}\g<2>', html)
html = re.sub(r'(<div class="seg-vol" style="color:#38bdf8">)\$[\d.]+[MK]?(</div>)',
              rf'\g<1>{fmt(rampa_vol)}\g<2>', html)
html = re.sub(r'(Volumen MTD USD · )\d+( MAU · )\d+( nuevos</div>)',
              rf'\g<1>{rampa_mau}\g<2>{rampa_new}\g<3>', html)
rampa_bar = round(rampa_vol / total_vol * 100, 1) if total_vol > 0 else 0
html = re.sub(r'(seg-bar-fill" style="width:)[\d.]+(%";background:#38bdf8)',
              rf'\g<1>{rampa_bar}\g<2>', html)

# OTC
html = re.sub(
    r'(seg-badge" style="background:rgba\(167,139,250,0\.15\);color:#a78bfa">)\d+( clientes</span>)',
    rf'\g<1>{otc_mau}\g<2>', html)
html = re.sub(r'(<div class="seg-vol" style="color:#a78bfa">)\$[\d.]+[MK]?(</div>)',
              rf'\g<1>{fmt(otc_vol)}\g<2>', html)
html = re.sub(r'(Volumen MTD USD · Proyección )\$[\d.]+[MK]?(</div>)',
              rf'\g<1>{fmt(otc_proj)}\g<2>', html)
otc_bar = round(otc_vol / total_vol * 100, 1) if total_vol > 0 else 0
html = re.sub(r'(seg-bar-fill" style="width:)[\d.]+(%";background:#a78bfa)',
              rf'\g<1>{otc_bar}\g<2>', html)

# K3
html = re.sub(
    r'(seg-badge" style="background:rgba\(74,222,128,0\.15\);color:#4ade80">)\d+( merchants</span>)',
    rf'\g<1>{k3_merch}\g<2>', html)
html = re.sub(r'(<div class="seg-vol" style="color:#4ade80">)\$[\d.]+[MK]?(</div>)',
              rf'\g<1>{fmt(k3_vol)}\g<2>', html)
html = re.sub(r'(Volumen MTD USD · )\d+( nuevos este mes</div>)',
              rf'\g<1>{k3_merch_new}\g<2>', html)
k3_bar = round(k3_vol / total_vol * 100, 1) if total_vol > 0 else 0
html = re.sub(r'(seg-bar-fill" style="width:)[\d.]+(%";background:#4ade80)',
              rf'\g<1>{k3_bar}\g<2>', html)

# ── Summary card (26px) ───────────────────────────────────────────────────────
html = re.sub(
    r'(font-size:26px;font-weight:700;color:var\(--white\);font-family:[^>]+>)\$[\d.]+[MK]?(</div>)',
    rf'\g<1>{fmt(total_vol)}\g<2>', html)
html = re.sub(
    r'(font-size:26px;font-weight:700;color:var\(--lima\);font-family:[^>]+>)~\$[\d.]+[MK]?(</div>)',
    rf'\g<1>~{fmt(total_proj)}\g<2>', html)

# ── Summary grid (16px per-segment MTD + projection) ─────────────────────────
html = re.sub(
    r'(font-size:16px;font-weight:700;color:#a78bfa;font-family:[^>]+>)\$[\d.]+[MK]?(</div>)',
    rf'\g<1>{fmt(otc_vol)}\g<2>', html)
html = re.sub(
    r'(font-size:16px;font-weight:700;color:#4ade80;font-family:[^>]+>)\$[\d.]+[MK]?(</div>)',
    rf'\g<1>{fmt(k3_vol)}\g<2>', html)
html = re.sub(
    r'(font-size:16px;font-weight:700;color:#38bdf8;font-family:[^>]+>)\$[\d.]+[MK]?(</div>)',
    rf'\g<1>{fmt(rampa_vol)}\g<2>', html)
# Projections (--lima): first = OTC, second = Rampa
html = re.sub(
    r'(font-size:16px;font-weight:700;color:var\(--lima\);font-family:[^>]+>)~\$[\d.]+[MK]?(</div>)',
    rf'\g<1>~{fmt(otc_proj)}\g<2>', html, count=1)
html = re.sub(
    r'(font-size:16px;font-weight:700;color:var\(--lima\);font-family:[^>]+>)~\$[\d.]+[MK]?(</div>)',
    rf'\g<1>~{fmt(rampa_proj)}\g<2>', html, count=1)

# ── Rampa tab KPIs ────────────────────────────────────────────────────────────
html = re.sub(
    r'(Volumen MTD</div><div class="value" style="color:#38bdf8">)\$[\d.]+[MK]?(</div>)',
    rf'\g<1>{fmt(rampa_vol)}\g<2>', html)
html = re.sub(
    r'(Clientes Activos</div><div class="value">)\d+(</div><div class="sub">MAU)',
    rf'\g<1>{rampa_active}\g<2>', html)
html = re.sub(r'(MAU · )\d+( nuevos onboarded)', rf'\g<1>{rampa_new}\g<2>', html)

# ── OTC tab KPIs ──────────────────────────────────────────────────────────────
html = re.sub(
    r'(Volumen MTD OTC</div><div class="value" style="color:#a78bfa">)\$[\d.]+[MK]?(</div>)',
    rf'\g<1>{fmt(otc_vol)}\g<2>', html)
html = re.sub(
    r'(vs \w+ \d{4}</div><div class="value">)\$[\d.]+[MK]?(</div>)',
    rf'\g<1>{fmt(otc_vol_prev)}\g<2>', html)
html = re.sub(
    r'(<div class="sub" style="color:#f87171">)[^<]+(</div></div>)',
    rf'\g<1>{pct_str(otc_pct)}\g<2>', html)
html = re.sub(
    r'(Clientes Activos \w+</div><div class="value" style="color:var\(--lima\)">)\d+(</div>)',
    rf'\g<1>{otc_mau}\g<2>', html)
html = re.sub(
    r'(Nuevos Onboarded \w+</div><div class="value" style="color:#4ade80">)\d+(</div>)',
    rf'\g<1>{otc_new}\g<2>', html)

# OTC Resumen table
html = re.sub(
    r'(pace-label">Volumen \w+ MTD</span><span class="pace-value">)\$[\d,]+(</span>)',
    rf'\g<1>{fmt_full(otc_vol)}\g<2>', html)
html = re.sub(
    r'(pace-label">Variación</span><span class="change (?:up|down|flat)">)[^<]+(</span>)',
    rf'\g<1>{pct_str(otc_pct)}\g<2>', html)
html = re.sub(
    r'(pace-label">Promedio diario</span><span class="pace-value">)\$[\d,]+(</span>)',
    rf'\g<1>{fmt_full(otc_daily)}\g<2>', html)
html = re.sub(
    r'(pace-label">Proyección</span><span class="pace-value"[^>]*>)~\$[\d.]+[MK]?(</span>)',
    rf'\g<1>~{fmt(otc_proj)}\g<2>', html)
html = re.sub(
    r'(pace-bar-fill" style="width:)[\d.]+(%">)',
    rf'\g<1>{PACE*100:.1f}\g<2>', html)

# ── K3 tab KPIs ───────────────────────────────────────────────────────────────
html = re.sub(
    r'(Volumen MTD K3</div><div class="value" style="color:#4ade80">)\$[\d.]+[MK]?(</div>)',
    rf'\g<1>{fmt(k3_vol)}\g<2>', html)
html = re.sub(
    r'(Merchants Activos</div><div class="value">)\d+(</div>)',
    rf'\g<1>{k3_merch}\g<2>', html)
html = re.sub(
    r'(Nuevos en \w+</div><div class="value" style="color:#fbbf24">)\d+(</div>)',
    rf'\g<1>{k3_merch_new}\g<2>', html)

# ── segChart ──────────────────────────────────────────────────────────────────
html = re.sub(
    r"(getElementById\('segChart'\)[^\n]+data:\[)[\d,]+(\])",
    rf'\g<1>{int(rampa_vol)},{int(otc_vol)},{int(k3_vol)}\g<2>', html)

# ── mvfChart — update both label names and data ───────────────────────────────
def update_mvfchart(h):
    # Pattern: find the two datasets inside mvfChart (prev, then current)
    pat = (r"(getElementById\('mvfChart'\)[\s\S]{0,100}?"
           r"datasets:\[\{label:')([\w]+)(',data:\[)[\d,]+"
           r"([\s\S]{0,200}?\},\{label:')([\w]+)(',data:\[)[\d,]+(\])")
    def rep(m):
        return (m.group(1) + PREV_MES + m.group(3) +
                f"{int(rampa_vol_prev)},{int(otc_vol_prev)},{int(k3_vol_prev)}" +
                m.group(4) + CUR_MES + m.group(6) +
                f"{int(rampa_vol)},{int(otc_vol)},{int(k3_vol)}" +
                m.group(7))
    new_h = re.sub(pat, rep, h, flags=re.DOTALL)
    if new_h == h:
        print("  WARN: mvfChart pattern not matched, skipping")
    return new_h

html = update_mvfchart(html)

# ── oTrendChart ───────────────────────────────────────────────────────────────
html = re.sub(
    r"(getElementById\('oTrendChart'\)[^\n]+data:\[)[\d,]+(\])",
    rf'\g<1>{int(otc_vol_prev)},{int(otc_vol)},{int(otc_proj)}\g<2>', html)

# ── const otc JS array ────────────────────────────────────────────────────────
otc_pct_val = round(otc_pct, 1) if otc_pct is not None else 0
html = re.sub(
    r'const otc=\[.*?\];',
    f'const otc=[{{n:"OTC Koywe",m:{int(otc_vol)},f:{int(otc_vol_prev)},p:{otc_pct_val},w:true,s:"OTC"}}];',
    html)

# ── const rampa JS array ──────────────────────────────────────────────────────
if rampa_perf:
    html = re.sub(r'const rampa=\[[\s\S]*?\];',
                  "const rampa=" + js_arr(rampa_perf) + ";", html)
    print(f"  Rampa performers updated: {len(rampa_perf)} entries")

# ── const k3 JS array ─────────────────────────────────────────────────────────
if k3_perf:
    html = re.sub(r'const k3=\[[\s\S]*?\];',
                  "const k3=" + js_arr(k3_perf) + ";", html)
    print(f"  K3 performers updated: {len(k3_perf)} entries")

# ── Write HTML ────────────────────────────────────────────────────────────────
with open("index.html", "w", encoding="utf-8") as f:
    f.write(html)

print(f"\n✓ index.html actualizado — {new_date}")
print(f"  Rampa {fmt(rampa_vol)} (prev {fmt(rampa_vol_prev)}) | "
      f"OTC {fmt(otc_vol)} (prev {fmt(otc_vol_prev)}) | "
      f"K3 {fmt(k3_vol)} | Total {fmt(total_vol)}")

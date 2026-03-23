#!/usr/bin/env python3
"""
Koywe B2B Dashboard — Auto-updater
- Rampa: K3 PostgreSQL via Metabase API (metabase.koywe.com)
- K3:    K3 PostgreSQL via Metabase API (ALL types, SUM(amountIn) × live FX rate)
- OTC:   MongoDB Atlas (scheduleddeals collection)
Runs daily via GitHub Actions.
"""
import os, re, requests
from datetime import datetime
from pymongo import MongoClient
from bson import ObjectId

# ── Config ────────────────────────────────────────────────────────────────────
MONGO_URI        = os.environ["MONGODB_URI"]
METABASE_API_KEY = os.environ["METABASE_API_KEY"]
METABASE_URL     = "https://metabase.koywe.com"
K3_DB_ID         = 7

TODAY        = datetime.utcnow()
MONTH_START  = datetime(TODAY.year, TODAY.month, 1)
MONTH_END    = datetime(TODAY.year + (1 if TODAY.month == 12 else 0),
                        (TODAY.month % 12) + 1, 1)
DAYS_ELAPSED = TODAY.day
DAYS_IN_MONTH = 31 if TODAY.month in [1,3,5,7,8,10,12] else (28 if TODAY.month == 2 else 30)
PACE = DAYS_ELAPSED / DAYS_IN_MONTH

OTC_KOYWE = ObjectId("63eae12e45c8376a48c70bac")

# Internal merchant slugs to exclude from K3 and MAU
INTERNAL_SLUGS = (
    "'koywe-3-sys','koywe-spa','koywe-arg','koywe-s-de-rl-de-cv','koywe-inc',"
    "'chile-demo-merchant','koywe-peru-sac','koywe-tecnologa-y-software-ltda',"
    "'koywe-sas','koywe-otc','otc-koywe-con-deuda'"
)

def fmt(n):
    if n >= 1_000_000: return f"${n/1_000_000:.1f}M"
    if n >= 1_000:     return f"${n/1_000:.0f}K"
    return f"${n:.0f}"

def fmt_full(n):
    return f"${n:,.0f}"

def metabase_query(sql):
    resp = requests.post(
        f"{METABASE_URL}/api/dataset",
        headers={"x-api-key": METABASE_API_KEY, "Content-Type": "application/json"},
        json={"database": K3_DB_ID, "type": "native", "native": {"query": sql}},
        timeout=30
    )
    return resp.json().get("data", {}).get("rows", [])

# ── Rampa via K3 Metabase API ─────────────────────────────────────────────────
def query_rampa_k3():
    month_str = MONTH_START.strftime("%Y-%m-%d")

    # Volume: ONRAMP + OFFRAMP + PAYMENT_LINK, USD conversion via Quote
    sql_vol = f"""
SELECT 
  SUM(CASE 
    WHEN q."destinationCurrencySymbol" IN ('USDT','USDC','USD') THEN o."amountOut"
    WHEN q."originCurrencySymbol" IN ('USDT','USDC','USD') THEN o."amountIn"
    ELSE o."amountOut" / NULLIF(q."exchangeRate", 0)
  END) as rampa_vol_usd
FROM "Orders" o
LEFT JOIN "Quote" q ON o."quoteId" = q.id
WHERE o."createdAt" >= '{month_str}'
  AND o.status = 'COMPLETED'
  AND o.type IN ('ONRAMP','OFFRAMP','PAYMENT_LINK')
"""

    # MAU: ALL types, exclude internal merchants
    sql_mau = f"""
SELECT COUNT(DISTINCT o."merchantId") as mau
FROM "Orders" o
JOIN "Merchants" m ON o."merchantId" = m.id
WHERE o."createdAt" >= '{month_str}'
  AND o.status = 'COMPLETED'
  AND m.slug NOT IN ({INTERNAL_SLUGS})
"""

    vol_rows = metabase_query(sql_vol)
    mau_rows = metabase_query(sql_mau)

    vol = float(vol_rows[0][0] or 0) if vol_rows else 0
    mau = int(mau_rows[0][0] or 0) if mau_rows else 0
    return vol, mau

# ── K3 Cloud via K3 Metabase API ─────────────────────────────────────────────
def query_k3():
    """ALL order types, SUM(amountIn) x live FX rate per currency."""
    month_str = MONTH_START.strftime("%Y-%m-%d")

    # Get live USD FX rates (1 USD = X units of currency)
    try:
        fx_resp = requests.get("https://open.er-api.com/v6/latest/USD", timeout=10)
        fx_rates = fx_resp.json().get("rates", {})
    except Exception as e:
        print(f"  WARNING: FX fetch failed ({e}), using fallback rates")
        fx_rates = {}

    # Fallback rates if API fails
    fallback = {
        "CLP": 920.0, "ARS": 1450.0, "PEN": 3.45, "MXN": 17.9,
        "COP": 3700.0, "BRL": 5.3, "EUR": 0.87, "HKD": 7.83,
        "BOB": 6.9, "USD": 1.0, "USDT": 1.0, "USDC": 1.0,
    }
    for ccy, rate in fallback.items():
        if ccy not in fx_rates or not fx_rates[ccy]:
            fx_rates[ccy] = rate
    # Stablecoins = 1:1
    fx_rates["USDT"] = 1.0
    fx_rates["USDC"] = 1.0

    # Query ALL types, group by origin currency
    # Null originCurrencySymbol = ONRAMP orders without a Quote -> treat as CLP
    sql = f"""
SELECT
  COALESCE(q."originCurrencySymbol", 'CLP') as currency,
  SUM(o."amountIn") as total_amountIn
FROM "Orders" o
LEFT JOIN "Quote" q ON o."quoteId" = q.id
JOIN "Merchants" m ON o."merchantId" = m.id
WHERE o."createdAt" >= '{month_str}'
  AND o.status = 'COMPLETED'
  AND m.slug NOT IN ({INTERNAL_SLUGS})
GROUP BY COALESCE(q."originCurrencySymbol", 'CLP')
"""

    rows = metabase_query(sql)
    total_usd = 0.0
    for (currency, amount_in) in rows:
        amount_in = float(amount_in or 0)
        rate = fx_rates.get(currency, 1.0)
        if rate and rate > 0:
            usd = amount_in / rate
        else:
            usd = 0
        print(f"    K3 {currency}: {amount_in:,.0f} -> ${usd:,.0f}")
        total_usd += usd

    return total_usd

# ── OTC via MongoDB ───────────────────────────────────────────────────────────
def query_otc(col):
    col_deals = col.database["scheduleddeals"]
    pipeline = [
        {"$match": {
            "createdAt": {"$gte": MONTH_START, "$lt": MONTH_END},
            "metaAccountId": str(OTC_KOYWE),
            "orderType": "scheduled_deal_to_buy"
        }},
        {"$addFields": {"amountUsd": {"$divide": ["$amount", "$exchangeRate"]}}},
        {"$group": {"_id": None, "totalUsd": {"$sum": "$amountUsd"},
                    "accounts": {"$addToSet": "$accountId"}}}
    ]
    r = list(col_deals.aggregate(pipeline))
    total = r[0]["totalUsd"] if r else 0
    n_accounts = len(r[0]["accounts"]) if r else 0
    return total, n_accounts

# ── Run queries ───────────────────────────────────────────────────────────────
print("Querying Rampa (K3)...")
rampa_vol, rampa_mau = query_rampa_k3()

print("Querying K3 Cloud...")
k3_vol = query_k3()

print("Querying OTC (MongoDB)...")
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15000)
db = client["rampa-koywe"]
otc_vol, otc_mau = query_otc(db["transactions"])
client.close()

rampa_proj = rampa_vol / PACE if PACE > 0 else 0
k3_proj    = k3_vol    / PACE if PACE > 0 else 0
otc_proj   = otc_vol   / PACE if PACE > 0 else 0
total_vol  = rampa_vol + otc_vol + k3_vol
total_proj = total_vol / PACE if PACE > 0 else 0
otc_daily  = otc_vol / DAYS_ELAPSED if DAYS_ELAPSED > 0 else 0

print(f"Rampa: {fmt(rampa_vol)} proj {fmt(rampa_proj)} MAU {rampa_mau}")
print(f"K3:    {fmt(k3_vol)} proj {fmt(k3_proj)}")
print(f"OTC:   {fmt(otc_vol)} proj {fmt(otc_proj)} MAU {otc_mau}")
print(f"Total: {fmt(total_vol)} proj {fmt(total_proj)}")
print(f"Pace:  {DAYS_ELAPSED}/{DAYS_IN_MONTH} = {PACE*100:.1f}%")

# ── Update HTML ───────────────────────────────────────────────────────────────
with open("index.html", "r", encoding="utf-8") as f:
    html = f.read()

new_date = TODAY.strftime("%-d %b %Y")
html = re.sub(r'Actualizado \d+ \w+ \d+', f'Actualizado {new_date}', html)
html = re.sub(r'pace \d+/\d+ d\u00edas', f'pace {DAYS_ELAPSED}/{DAYS_IN_MONTH} d\u00edas', html)
html = re.sub(r'Pace: \d+/\d+ d\u00edas', f'Pace: {DAYS_ELAPSED}/{DAYS_IN_MONTH} d\u00edas', html)
html = re.sub(r'\d+/\d+ d\u00edas \([\d.]+% del mes\)',
              f'{DAYS_ELAPSED}/{DAYS_IN_MONTH} d\u00edas ({PACE*100:.1f}% del mes)', html)

# Rampa seg-vol (blue)
html = re.sub(r'(<div class="seg-vol" style="color:#38bdf8">)\$[\d.]+[MK]?(</div>)',
              rf'\g<1>{fmt(rampa_vol)}\g<2>', html)
# OTC seg-vol (purple)
html = re.sub(r'(<div class="seg-vol" style="color:#a78bfa">)\$[\d.]+[MK]?(</div>)',
              rf'\g<1>{fmt(otc_vol)}\g<2>', html)
# K3 seg-vol (green)
html = re.sub(r'(<div class="seg-vol" style="color:#4ade80">)\$[\d.]+[MK]?(</div>)',
              rf'\g<1>{fmt(k3_vol)}\g<2>', html)

# Rampa seg-sub MAU
html = re.sub(r'(Volumen MTD USD \u00b7 )\d+( MAU)',
              rf'\g<1>{rampa_mau}\g<2>', html)
# OTC seg-sub proyeccion
html = re.sub(r'(Volumen MTD USD \u00b7 Proyecci\u00f3n )\$[\d.]+[MK]?',
              rf'\g<1>{fmt(otc_proj)}', html)

# Total actual
html = re.sub(r'(font-size:26px;font-weight:700;color:var\(--white\)[^>]+>)\$[\d.]+[MK]?(</div>)',
              rf'\g<1>{fmt(total_vol)}\g<2>', html)
# Total proyeccion
html = re.sub(r'(font-size:26px;font-weight:700;color:var\(--lima\)[^>]+>)~\$[\d.]+[MK]?(</div>)',
              rf'\g<1>~{fmt(total_proj)}\g<2>', html)

# OTC MTD tabla (purple)
html = re.sub(r'(font-size:16px;font-weight:700;color:#a78bfa[^>]+>)\$[\d.]+[MK]?(</div>)',
              rf'\g<1>{fmt(otc_vol)}\g<2>', html)
# OTC proyeccion tabla
html = re.sub(r'(font-size:16px;font-weight:700;color:var\(--lima\)[^>]+>)\$[\d.]+[MK]?(</div>)',
              rf'\g<1>~{fmt(otc_proj)}\g<2>', html)
# Rampa MTD tabla (blue)
html = re.sub(r'(font-size:16px;font-weight:700;color:#38bdf8[^>]+>)\$[\d.]+[MK]?(</div>)',
              rf'\g<1>{fmt(rampa_vol)}\g<2>', html)
# Rampa proyeccion tabla
html = re.sub(r'(font-size:16px;font-weight:700;color:var\(--lima3\)[^>]+>)\$[\d.]+[MK]?(</div>)',
              rf'\g<1>~{fmt(rampa_proj)}\g<2>', html)
# K3 MTD tabla (green)
html = re.sub(r'(font-size:16px;font-weight:700;color:#4ade80[^>]+>)\$[\d.]+[MK]?(</div>)',
              rf'\g<1>{fmt(k3_vol)}\g<2>', html)

# Rampa KPI
html = re.sub(r'(Volumen MTD</div><div class="value" style="color:#38bdf8">)\$[\d.]+[MK]?(</div>)',
              rf'\g<1>{fmt(rampa_vol)}\g<2>', html)
html = re.sub(r'(Clientes Activos</div><div class="value">)\d+(</div>)',
              rf'\g<1>{rampa_mau}\g<2>', html)
# Rampa paises total
html = re.sub(r'(<span class="ctry-amounts">\$)[\d.]+[MK]?( <span class="ctry-proj-amt">/ ~\$)[\d.]+[MK]?(</span></span>)',
              rf'\g<1>{fmt(rampa_vol)[1:]}\g<2>{fmt(rampa_proj)[1:]}\g<3>', html)

# K3 KPI
html = re.sub(r'(Volumen MTD K3</div><div class="value" style="color:#4ade80">)\$[\d.]+[MK]?(</div>)',
              rf'\g<1>{fmt(k3_vol)}\g<2>', html)

# OTC KPI
html = re.sub(r'(Volumen MTD OTC</div><div class="value" style="color:#a78bfa">)\$[\d.]+[MK]?(</div>)',
              rf'\g<1>{fmt(otc_vol)}\g<2>', html)
html = re.sub(r'(Clientes Activos Marzo</div><div class="value">)\d+(</div>)',
              rf'\g<1>{otc_mau}\g<2>', html)
# OTC resumen table
html = re.sub(r'(pace-label">Volumen Marzo MTD</span><span class="pace-value">)\$[\d,]+(</span>)',
              rf'\g<1>{fmt_full(otc_vol)}\g<2>', html)
html = re.sub(r'(pace-label">Promedio diario</span><span class="pace-value">)\$[\d,]+(</span>)',
              rf'\g<1>{fmt_full(otc_daily)}\g<2>', html)
html = re.sub(r'(pace-label">Proyecci\u00f3n</span><span class="pace-value"[^>]*>)~\$[\d.]+[MK]?(</span>)',
              rf'\g<1>~{fmt(otc_proj)}\g<2>', html)

# segChart JS data: [rampa, otc, k3]
html = re.sub(r"(segChart\),\{type:'bar',data:\{labels:\['Rampa','OTC Koywe','K3'\],datasets:\[\{data:\[)[\d,]+(\])",
              rf'\g<1>{int(rampa_vol)},{int(otc_vol)},{int(k3_vol)}\g<2>', html)

with open("index.html", "w", encoding="utf-8") as f:
    f.write(html)

print("\u2705 index.html actualizado correctamente")

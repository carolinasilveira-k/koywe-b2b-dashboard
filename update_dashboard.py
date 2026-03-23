#!/usr/bin/env python3
"""
Koywe B2B Dashboard — Auto-updater
- Rampa: K3 PostgreSQL via Metabase API (metabase.koywe.com)
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

def fmt(n):
    if n >= 1_000_000: return f"${n/1_000_000:.1f}M"
    if n >= 1_000:     return f"${n/1_000:.0f}K"
    return f"${n:.0f}"

def fmt_full(n):
    return f"${n:,.0f}"

# ── Rampa via K3 Metabase API ─────────────────────────────────────────────────
def query_rampa_k3():
    month_str = MONTH_START.strftime("%Y-%m-%d")
    sql = f"""
SELECT 
  SUM(CASE 
    WHEN q."destinationCurrencySymbol" IN ('USDT','USDC','USD') THEN o."amountOut"
    WHEN q."originCurrencySymbol" IN ('USDT','USDC','USD') THEN o."amountIn"
    ELSE o."amountOut" / NULLIF(q."exchangeRate", 0)
  END) as rampa_vol_usd,
  COUNT(DISTINCT o."merchantId") as mau
FROM "Orders" o
LEFT JOIN "Quote" q ON o."quoteId" = q.id
WHERE o."createdAt" >= '{month_str}'
  AND o.status = 'COMPLETED'
  AND o.type IN ('ONRAMP','OFFRAMP','PAYMENT_LINK')
"""
    resp = requests.post(
        f"{METABASE_URL}/api/dataset",
        headers={"x-api-key": METABASE_API_KEY, "Content-Type": "application/json"},
        json={"database": K3_DB_ID, "type": "native", "native": {"query": sql}},
        timeout=30
    )
    rows = resp.json().get("data", {}).get("rows", [[0, 0]])
    vol = float(rows[0][0] or 0)
    mau = int(rows[0][1] or 0)
    return vol, mau

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

print("Querying OTC (MongoDB)...")
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15000)
db = client["rampa-koywe"]
otc_vol, otc_mau = query_otc(db["transactions"])
client.close()

k3_vol     = 1359757  # TODO: add K3 query
rampa_proj = rampa_vol / PACE if PACE > 0 else 0
otc_proj   = otc_vol   / PACE if PACE > 0 else 0
total_vol  = rampa_vol + otc_vol + k3_vol
total_proj = total_vol / PACE if PACE > 0 else 0
otc_daily  = otc_vol / DAYS_ELAPSED if DAYS_ELAPSED > 0 else 0

print(f"Rampa: {fmt(rampa_vol)} proj {fmt(rampa_proj)} MAU {rampa_mau}")
print(f"OTC:   {fmt(otc_vol)} proj {fmt(otc_proj)} MAU {otc_mau}")
print(f"Total: {fmt(total_vol)} proj {fmt(total_proj)}")
print(f"Pace:  {DAYS_ELAPSED}/{DAYS_IN_MONTH} = {PACE*100:.1f}%")

# ── Update HTML ───────────────────────────────────────────────────────────────
with open("index.html", "r", encoding="utf-8") as f:
    html = f.read()

new_date = TODAY.strftime("%-d %b %Y")
html = re.sub(r'Actualizado \d+ \w+ \d+', f'Actualizado {new_date}', html)
html = re.sub(r'pace \d+/\d+ días', f'pace {DAYS_ELAPSED}/{DAYS_IN_MONTH} días', html)
html = re.sub(r'Pace: \d+/\d+ días', f'Pace: {DAYS_ELAPSED}/{DAYS_IN_MONTH} días', html)
html = re.sub(r'\d+/\d+ días \([\d.]+% del mes\)',
              f'{DAYS_ELAPSED}/{DAYS_IN_MONTH} días ({PACE*100:.1f}% del mes)', html)

# Rampa seg-vol (blue)
html = re.sub(r'(<div class="seg-vol" style="color:#38bdf8">)\$[\d.]+[MK]?(</div>)',
              rf'\g<1>{fmt(rampa_vol)}\g<2>', html)
# OTC seg-vol (purple)
html = re.sub(r'(<div class="seg-vol" style="color:#a78bfa">)\$[\d.]+[MK]?(</div>)',
              rf'\g<1>{fmt(otc_vol)}\g<2>', html)
# Rampa seg-sub MAU
html = re.sub(r'(Volumen MTD USD · )\d+( MAU)',
              rf'\g<1>{rampa_mau}\g<2>', html)
# OTC seg-sub proyección
html = re.sub(r'(Volumen MTD USD · Proyección )\$[\d.]+[MK]?',
              rf'\g<1>{fmt(otc_proj)}', html)
# Total actual
html = re.sub(r'(font-size:26px;font-weight:700;color:var\(--white\)[^>]+>)\$[\d.]+[MK]?(</div>)',
              rf'\g<1>{fmt(total_vol)}\g<2>', html)
# Total proyección
html = re.sub(r'(font-size:26px;font-weight:700;color:var\(--lima\)[^>]+>)~\$[\d.]+[MK]?(</div>)',
              rf'\g<1>~{fmt(total_proj)}\g<2>', html)
# OTC MTD tabla (purple)
html = re.sub(r'(font-size:16px;font-weight:700;color:#a78bfa[^>]+>)\$[\d.]+[MK]?(</div>)',
              rf'\g<1>{fmt(otc_vol)}\g<2>', html)
# OTC proyección tabla
html = re.sub(r'(font-size:16px;font-weight:700;color:var\(--lima\)[^>]+>)\$[\d.]+[MK]?(</div>)',
              rf'\g<1>~{fmt(otc_proj)}\g<2>', html)
# Rampa MTD tabla (blue)
html = re.sub(r'(font-size:16px;font-weight:700;color:#38bdf8[^>]+>)\$[\d.]+[MK]?(</div>)',
              rf'\g<1>{fmt(rampa_vol)}\g<2>', html)
# Rampa proyección tabla
html = re.sub(r'(font-size:16px;font-weight:700;color:var\(--lima3\)[^>]+>)\$[\d.]+[MK]?(</div>)',
              rf'\g<1>~{fmt(rampa_proj)}\g<2>', html)
# Rampa KPI
html = re.sub(r'(Volumen MTD</div><div class="value" style="color:#38bdf8">)\$[\d.]+[MK]?(</div>)',
              rf'\g<1>{fmt(rampa_vol)}\g<2>', html)
html = re.sub(r'(Clientes Activos</div><div class="value">)\d+(</div>)',
              rf'\g<1>{rampa_mau}\g<2>', html)
# Rampa países total
html = re.sub(r'(<span class="ctry-amounts">\$)[\d.]+[MK]?( <span class="ctry-proj-amt">/ ~\$)[\d.]+[MK]?(</span></span>)',
              rf'\g<1>{fmt(rampa_vol)[1:]}\g<2>{fmt(rampa_proj)[1:]}\g<3>', html)
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
html = re.sub(r'(pace-label">Proyección</span><span class="pace-value"[^>]*>)~\$[\d.]+[MK]?(</span>)',
              rf'\g<1>~{fmt(otc_proj)}\g<2>', html)

with open("index.html", "w", encoding="utf-8") as f:
    f.write(html)

print("✅ index.html actualizado correctamente")

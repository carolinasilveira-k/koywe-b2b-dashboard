#!/usr/bin/env python3
"""
Koywe B2B Dashboard — Auto-updater
Queries MongoDB Atlas and updates index.html with fresh numbers.
Runs daily via GitHub Actions.
"""
import os, base64, json, re
from datetime import datetime
from pymongo import MongoClient
from bson import ObjectId
import urllib.request

MONGO_URI = os.environ["MONGODB_URI"]
TODAY = datetime.utcnow()
MONTH_START = datetime(TODAY.year, TODAY.month, 1)
MONTH_END   = datetime(TODAY.year + (1 if TODAY.month == 12 else 0),
                        (TODAY.month % 12) + 1, 1)
DAYS_ELAPSED = TODAY.day
DAYS_IN_MONTH = 31 if TODAY.month in [1,3,5,7,8,10,12] else (28 if TODAY.month == 2 else 30)
PACE = DAYS_ELAPSED / DAYS_IN_MONTH

OTC_KOYWE = ObjectId("63eae12e45c8376a48c70bac")
OTC_K3    = ObjectId("6908f615c2c27d0132c7609c")

def fmt(n):
    if n >= 1_000_000: return f"${n/1_000_000:.1f}M"
    if n >= 1_000:     return f"${n/1_000:.0f}K"
    return f"${n:.0f}"

def query_rampa(col):
    pipeline = [
        {"$match": {
            "createdAt": {"$gte": MONTH_START, "$lt": MONTH_END},
            "status": "DELIVERED",
            "type": {"$in": ["currency-crypto","crypto-currency","settlement","topUp"]},
            "metaAccount": {"$nin": [OTC_KOYWE, OTC_K3]}
        }},
        {"$group": {
            "_id": None,
            "totalUsd": {"$sum": {"$divide": ["$currencyAmount", "$lastPriceUsd"]}}
        }}
    ]
    r = list(col.aggregate(pipeline))
    return r[0]["totalUsd"] if r else 0

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

def query_rampa_mau(col):
    pipeline = [
        {"$match": {
            "createdAt": {"$gte": MONTH_START, "$lt": MONTH_END},
            "status": "DELIVERED",
            "type": {"$in": ["currency-crypto","crypto-currency","settlement","topUp"]},
            "metaAccount": {"$nin": [OTC_KOYWE, OTC_K3]}
        }},
        {"$group": {"_id": "$metaAccount", "n": {"$sum": 1}}}
    ]
    return len(list(col.aggregate(pipeline)))

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15000)
db     = client["rampa-koywe"]
txns   = db["transactions"]

rampa_vol   = query_rampa(txns)
otc_vol, otc_mau = query_otc(txns)
rampa_mau   = query_rampa_mau(txns)
k3_vol      = 1359757  # TODO: add K3 query

rampa_proj  = rampa_vol / PACE
otc_proj    = otc_vol   / PACE
total_vol   = rampa_vol + otc_vol + k3_vol
total_proj  = total_vol / PACE

print(f"Rampa: {fmt(rampa_vol)} → proj {fmt(rampa_proj)} | MAU {rampa_mau}")
print(f"OTC:   {fmt(otc_vol)} → proj {fmt(otc_proj)} | MAU {otc_mau}")
print(f"Total: {fmt(total_vol)} → proj {fmt(total_proj)}")
print(f"Pace:  {DAYS_ELAPSED}/{DAYS_IN_MONTH} = {PACE*100:.1f}%")

client.close()

# Read current index.html
with open("index.html", "r", encoding="utf-8") as f:
    html = f.read()

# Update the "Actualizado" date in topbar
new_date = TODAY.strftime("%-d %b %Y")
html = re.sub(r'Actualizado \d+ \w+ \d+', f'Actualizado {new_date}', html)

# Update pace display
html = re.sub(r'pace \d+/\d+ días', f'pace {DAYS_ELAPSED}/{DAYS_IN_MONTH} días', html)
html = re.sub(r'Pace: \d+/\d+ días', f'Pace: {DAYS_ELAPSED}/{DAYS_IN_MONTH} días', html)
html = re.sub(r'\d+/\d+ días \([\d.]+% del mes\)', 
              f'{DAYS_ELAPSED}/{DAYS_IN_MONTH} días ({PACE*100:.1f}% del mes)', html)

with open("index.html", "w", encoding="utf-8") as f:
    f.write(html)

print("index.html updated successfully")

#!/usr/bin/env python3
"""
Synthetic data generator for the AI-Ready Lakehouse & Copilot PoC.

Fictional company: NorthPeak Industrial Supply (a Canadian B2B distributor).
Produces THREE deliberately messy, siloed sources:

  1. erp_orders.csv        - ERP order lines (mixed date/price/discount formats, duplicates, casing)
  2. inventory_log.csv     - weekly warehouse stock snapshots (inconsistent SKU/warehouse names, nulls)
  3. crm_customers.json    - CRM export (different customer key format, duplicates, label variants)

It also writes ground_truth.json: answers computed from the CLEAN data BEFORE the mess is
injected. Use it to (a) reconcile your Silver/Gold layers and (b) grade Copilot's answers.

Standard library only (runs locally or in a Fabric notebook).

Usage:
    python generate_synthetic_data.py                       # defaults
    python generate_synthetic_data.py --orders 6000 --customers 800 --seed 7 --out ./data
"""
import argparse
import csv
import json
import os
import random
from collections import defaultdict
from datetime import date, datetime, timedelta

START = date(2025, 1, 6)     # a Monday
END = date(2026, 10, 31)

# ---------------------------------------------------------------- reference data
CATEGORIES = {
    "Power Tools":  [("Cordless Drill 18V", 129.0, 78.0), ("Circular Saw 7.25in", 149.0, 92.0),
                     ("Angle Grinder 4.5in", 89.0, 52.0), ("Orbital Sander", 79.0, 46.0),
                     ("Impact Driver 18V", 139.0, 84.0), ("Jigsaw Variable Speed", 99.0, 58.0),
                     ("Rotary Hammer SDS", 249.0, 155.0), ("Heat Gun 1500W", 49.0, 27.0)],
    "Fasteners":    [("Hex Bolt Kit M8", 24.0, 12.0), ("Wood Screw Box 500pc", 18.0, 8.5),
                     ("Anchor Bolt Set", 32.0, 17.0), ("Lock Washer Pack", 9.0, 3.8),
                     ("Self-Tapping Screws 1000pc", 28.0, 14.0), ("Rivet Assortment", 21.0, 10.0),
                     ("Threaded Rod 3ft", 15.0, 7.5), ("Nut & Bolt Bin Set", 45.0, 24.0)],
    "Safety Gear":  [("Hard Hat Class E", 34.0, 17.0), ("Safety Glasses 12pk", 42.0, 22.0),
                     ("Work Gloves Cut-5", 19.0, 9.0), ("Hi-Vis Vest Class 2", 22.0, 10.5),
                     ("Steel Toe Boots", 139.0, 82.0), ("Ear Muffs NRR 30", 27.0, 13.0),
                     ("Respirator Half-Mask", 59.0, 33.0), ("Fall Arrest Harness", 189.0, 118.0)],
    "Electrical":   [("Extension Cord 50ft", 39.0, 21.0), ("Wire 12AWG 100ft", 69.0, 42.0),
                     ("GFCI Outlet 15A", 24.0, 12.5), ("Breaker 20A", 17.0, 8.5),
                     ("Conduit 10ft EMT", 14.0, 7.0), ("Work Light LED", 64.0, 36.0),
                     ("Cable Ties 500pc", 12.0, 5.0), ("Junction Box Steel", 8.0, 3.5)],
    "Plumbing":     [("PEX Pipe 100ft", 59.0, 34.0), ("Ball Valve 3/4in", 16.0, 8.0),
                     ("Pipe Wrench 14in", 44.0, 25.0), ("Sink Trap Kit", 21.0, 11.0),
                     ("Teflon Tape 12pk", 11.0, 4.5), ("Copper Elbow Pack", 29.0, 16.0),
                     ("PVC Cement 473ml", 13.0, 6.0), ("Pressure Gauge", 26.0, 14.0)],
}
CATEGORY_DEMAND = {"Power Tools": 1.0, "Fasteners": 1.6, "Safety Gear": 1.3, "Electrical": 1.2, "Plumbing": 0.9}

WAREHOUSES = {"TOR": "Toronto DC", "CAL": "Calgary DC", "VAN": "Vancouver DC"}
WAREHOUSE_NAME_VARIANTS = {
    "TOR": ["Toronto DC", "TORONTO DC", "Toronto Distribution Centre", "TOR-DC"],
    "CAL": ["Calgary DC", "CALGARY DC", "Calgary Distribution Centre", "CAL-DC"],
    "VAN": ["Vancouver DC", "VANCOUVER DC", "Vancouver Distribution Centre", "VAN-DC"],
}
REGIONS = {"ON": "TOR", "QC": "TOR", "NS": "TOR", "AB": "CAL", "SK": "CAL", "MB": "CAL", "BC": "VAN"}
REGION_NAME_VARIANTS = {"ON": ["ON", "Ontario", "ontario"], "QC": ["QC", "Quebec", "Québec"],
                        "NS": ["NS", "Nova Scotia"], "AB": ["AB", "Alberta", "alberta"],
                        "SK": ["SK", "Saskatchewan"], "MB": ["MB", "Manitoba"],
                        "BC": ["BC", "British Columbia", "b.c."]}
SEGMENTS = {"Enterprise": ["Enterprise", "enterprise", "ENT"],
            "Mid-Market": ["Mid-Market", "Mid Market", "midmarket"],
            "SMB": ["SMB", "Small Business", "smb"]}
SEGMENT_DISCOUNT = {"Enterprise": 0.12, "Mid-Market": 0.06, "SMB": 0.02}   # planted: Enterprise discounts more
COMPANY_WORDS = ["Maple", "Northern", "Summit", "Lakeshore", "Prairie", "Cedar", "Harbour", "Ridge",
                 "Pioneer", "Granite", "Timber", "Anchor", "Beacon", "Keystone", "Frontier"]
COMPANY_TYPES = ["Construction", "Contracting", "Builders", "Mechanical", "Electric", "Industrial", "Services"]
STATUSES = ["Shipped", "Delivered", "Cancelled"]

# Planted business findings (so Copilot has real insights to discover)
COST_SHOCK_CATEGORY, COST_SHOCK_START, COST_SHOCK_PCT = "Electrical", date(2026, 7, 1), 0.18
STOCKOUT_WH, STOCKOUT_CAT = "CAL", "Safety Gear"
STOCKOUT_START, STOCKOUT_END = date(2026, 8, 3), date(2026, 9, 28)


def quarter(d):
    return f"{d.year}-Q{(d.month - 1) // 3 + 1}"


def monday_of(d):
    return d - timedelta(days=d.weekday())


# ---------------------------------------------------------------- clean data
def build_products():
    products, n = [], 1001
    for cat, items in CATEGORIES.items():
        for name, price, cost in items:
            products.append({"sku": f"SKU-{n}", "product_name": name, "category": cat,
                             "list_price": price, "base_cost": cost})
            n += 1
    return products


def build_customers(n):
    cust = []
    for i in range(1, n + 1):
        seg = random.choices(list(SEGMENT_DISCOUNT), weights=[0.15, 0.30, 0.55])[0]
        region = random.choice(list(REGIONS))
        company = f"{random.choice(COMPANY_WORDS)} {random.choice(COMPANY_TYPES)}"
        cust.append({"customer_id": f"C-{i:05d}", "company": f"{company} {i % 97}",  # suffix keeps names unique
                     "segment": seg, "region": region,
                     "created": START - timedelta(days=random.randint(30, 1500))})
    return cust


def cost_at(product, d):
    c = product["base_cost"]
    if product["category"] == COST_SHOCK_CATEGORY and d >= COST_SHOCK_START:
        c *= 1 + COST_SHOCK_PCT
    return round(c, 2)


def build_orders(n_orders, customers, products):
    lines, span = [], (END - START).days
    for oid in range(1, n_orders + 1):
        c = random.choice(customers)
        od = START + timedelta(days=random.randint(0, span))
        wh = REGIONS[c["region"]]
        status = random.choices(STATUSES, weights=[0.62, 0.32, 0.06])[0]
        ship = None if status == "Cancelled" else od + timedelta(days=random.randint(1, 6))
        cats = random.choices(list(CATEGORY_DEMAND), weights=list(CATEGORY_DEMAND.values()),
                              k=random.randint(1, 4))
        for ln, cat in enumerate(cats, 1):
            p = random.choice([x for x in products if x["category"] == cat])
            disc = round(max(0, random.gauss(SEGMENT_DISCOUNT[c["segment"]], 0.02)), 2)
            lines.append({"order_id": f"SO-{100000 + oid}", "line_no": ln, "order_date": od,
                          "customer_id": c["customer_id"], "sku": p["sku"],
                          "qty": random.randint(1, 40), "unit_price": p["list_price"],
                          "discount_pct": disc, "status": status, "ship_date": ship, "warehouse": wh})
    return lines


def build_inventory(products):
    rows, d = [], START
    while d <= END:
        for p in products:
            for wh in WAREHOUSES:
                reorder = random.randint(40, 120)
                on_hand = random.randint(30, 450)
                if wh == STOCKOUT_WH and p["category"] == STOCKOUT_CAT and STOCKOUT_START <= d <= STOCKOUT_END:
                    on_hand = random.choice([0, 0, 0, random.randint(1, 10)])
                rows.append({"snapshot_date": d, "sku": p["sku"], "warehouse": wh, "on_hand": on_hand,
                             "reorder_point": reorder, "unit_cost": cost_at(p, d),
                             "product_name": p["product_name"], "category": p["category"],
                             "supplier": f"SUP-{random.randint(1, 12):02d}"})
        d += timedelta(days=7)
    return rows


# ---------------------------------------------------------------- ground truth (from CLEAN data)
def ground_truth(lines, inventory, products, customers):
    pmap = {p["sku"]: p for p in products}
    cmap = {c["customer_id"]: c for c in customers}
    cost_by_week = {(r["sku"], r["snapshot_date"]): r["unit_cost"] for r in inventory if r["warehouse"] == "TOR"}
    rev_q, rev_cq, cogs_cq = defaultdict(float), defaultdict(float), defaultdict(float)
    cust_rev, seg_disc = defaultdict(float), defaultdict(list)
    for l in lines:
        if l["status"] == "Cancelled":          # business rule: cancelled orders excluded from revenue
            continue
        rev = l["qty"] * l["unit_price"] * (1 - l["discount_pct"])
        cat, q = pmap[l["sku"]]["category"], quarter(l["order_date"])
        cogs = l["qty"] * cost_by_week[(l["sku"], monday_of(l["order_date"]))]
        rev_q[q] += rev
        rev_cq[(cat, q)] += rev
        cogs_cq[(cat, q)] += cogs
        if l["order_date"].year == 2026:
            cust_rev[l["customer_id"]] += rev
        seg_disc[cmap[l["customer_id"]]["segment"]].append(l["discount_pct"])
    stock = defaultdict(int)
    for r in inventory:
        if r["on_hand"] == 0:
            stock[f"{r['warehouse']}|{r['category']}"] += 1
    top5 = sorted(cust_rev.items(), key=lambda kv: -kv[1])[:5]
    return {
        "note": "Computed from clean data. Cancelled orders excluded. Revenue = qty*unit_price*(1-discount). "
                "COGS = qty * unit_cost on the Monday snapshot of the order week. Dates are order_date.",
        "total_order_lines_clean": len(lines),
        "revenue_by_quarter": {k: round(v, 2) for k, v in sorted(rev_q.items())},
        "gross_margin_pct_by_category_quarter": {
            f"{c}|{q}": round(100 * (rev_cq[(c, q)] - cogs_cq[(c, q)]) / rev_cq[(c, q)], 1)
            for (c, q) in sorted(rev_cq)},
        "top5_customers_2026_revenue": [{"customer_id": k, "company": cmap[k]["company"], "revenue": round(v, 2)}
                                        for k, v in top5],
        "avg_discount_pct_by_segment": {s: round(100 * sum(v) / len(v), 1) for s, v in seg_disc.items()},
        "stockout_snapshots_by_warehouse_category": dict(stock),
        "planted_findings": [
            f"{COST_SHOCK_CATEGORY} unit cost +{int(COST_SHOCK_PCT*100)}% from {COST_SHOCK_START} with unchanged prices -> margin erosion in 2026-Q3/Q4",
            f"{STOCKOUT_WH} {STOCKOUT_CAT} stockouts {STOCKOUT_START} to {STOCKOUT_END}",
            "Enterprise segment receives the deepest discounts",
        ],
    }


# ---------------------------------------------------------------- messify + write
def fmt_date(d, style):
    return {0: d.strftime("%Y-%m-%d"), 1: d.strftime("%d/%m/%Y"), 2: d.strftime("%b %d, %Y")}[style]


def write_erp(lines, path, log):
    rows = []
    for l in lines:
        style = random.choices([0, 1, 2], weights=[0.6, 0.25, 0.15])[0]
        price = f"${l['unit_price']:.2f}" if random.random() < 0.2 else l["unit_price"]
        disc = f"{int(l['discount_pct']*100)}%" if random.random() < 0.15 else l["discount_pct"]
        status = random.choice([l["status"], l["status"].lower(), l["status"].upper()])
        cust = l["customer_id"] if random.random() > 0.03 else l["customer_id"].replace("-", "").lower()
        qty = l["qty"] if random.random() > 0.01 else None
        rows.append([l["order_id"], l["line_no"], fmt_date(l["order_date"], style), cust, l["sku"], qty,
                     price, disc, status, fmt_date(l["ship_date"], style) if l["ship_date"] else "",
                     l["warehouse"]])
    dups = random.sample(rows, int(len(rows) * 0.02))
    rows.extend(dups)
    random.shuffle(rows)
    log["erp_exact_duplicate_rows"] = len(dups)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "line_no", "order_date", "customer_id", "sku", "qty", "unit_price",
                    "discount_pct", "status", "ship_date", "warehouse"])
        w.writerows(rows)
    log["erp_rows_written"] = len(rows)


def write_inventory(inv, path, log):
    nulls = 0
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["log_timestamp", "sku", "warehouse", "on_hand_qty", "reorder_point", "unit_cost",
                    "product_name", "category", "supplier_code"])
        for r in inv:
            ts = r["snapshot_date"].isoformat() + random.choice(["", " 00:00:00", "T00:00:00Z"])
            sku = random.choices([r["sku"], r["sku"].lower().replace("-", ""), r["sku"].replace("-", " ")],
                                 weights=[0.7, 0.2, 0.1])[0]
            wh = random.choice(WAREHOUSE_NAME_VARIANTS[r["warehouse"]])
            on_hand = r["on_hand"]
            if random.random() < 0.01:
                on_hand, nulls = "", nulls + 1
            cost = f"{r['unit_cost']:.2f}"
            if r["unit_cost"] >= 100 and random.random() < 0.3:
                cost = f"{r['unit_cost']:,.2f}"          # thousands separators -> parse issue
            w.writerow([ts, sku, wh, on_hand, r["reorder_point"], cost, r["product_name"], r["category"], r["supplier"]])
    log["inventory_rows_written"] = len(inv)
    log["inventory_null_on_hand"] = nulls


def write_crm(customers, path, log, skip=20):
    records = []
    for c in random.sample(customers, len(customers) - skip):     # `skip` customers missing from CRM (orphans)
        company = c["company"]
        company = random.choice([company, company.upper(), company + " Inc.", company + " Inc"])
        rec = {"crm_id": f"CRM-{random.randint(10000, 99999)}",
               "account_number": c["customer_id"].replace("-", ""),       # C00001 vs ERP C-00001
               "company_name": company,
               "email": f"purchasing@{company.lower().split()[0]}{random.randint(1, 999)}.ca",
               "segment": random.choice(SEGMENTS[c["segment"]]),
               "province": random.choice(REGION_NAME_VARIANTS[c["region"]]),
               "created_at": c["created"].isoformat(),
               "account_owner": random.choice(["A. Singh", "M. Tremblay", "J. Okafor", "L. Chen", None])}
        records.append(rec)
    dups = []
    for rec in random.sample(records, int(len(records) * 0.03)):
        d = dict(rec)
        d["crm_id"] = f"CRM-{random.randint(10000, 99999)}"
        d["account_owner"] = random.choice(["A. Singh", "M. Tremblay", "J. Okafor", "L. Chen"])
        dups.append(d)
    records.extend(dups)
    random.shuffle(records)
    log["crm_duplicate_accounts"] = len(dups)
    log["crm_customers_missing_from_crm"] = skip
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)
    log["crm_records_written"] = len(records)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--orders", type=int, default=4000, help="number of orders (each has 1-4 lines)")
    ap.add_argument("--customers", type=int, default=600)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="./data")
    a = ap.parse_args()
    random.seed(a.seed)
    os.makedirs(a.out, exist_ok=True)

    products, customers = build_products(), build_customers(a.customers)
    lines = build_orders(a.orders, customers, products)
    inventory = build_inventory(products)

    truth = ground_truth(lines, inventory, products, customers)
    log = {}
    write_erp(lines, os.path.join(a.out, "erp_orders.csv"), log)
    write_inventory(inventory, os.path.join(a.out, "inventory_log.csv"), log)
    write_crm(customers, os.path.join(a.out, "crm_customers.json"), log)
    truth["injected_defects"] = log
    with open(os.path.join(a.out, "ground_truth.json"), "w") as f:
        json.dump(truth, f, indent=2)

    print(f"Wrote 3 sources + ground_truth.json to {a.out}")
    for k, v in log.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()

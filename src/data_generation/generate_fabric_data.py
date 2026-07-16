"""
Fabric DP-700 project - synthetic data generator
Produces:
  customers.csv, products.csv            -> dimension/master data, upload to Lakehouse Files
  orders_batch_YYYYMMDD.csv              -> simulate the nightly ERP extract (Data Pipeline source)
  orders_stream_events.jsonl             -> simulate checkout events (feed to Eventstream Custom Endpoint,
                                             or just batch-load once to prove the Bronze/Silver merge)
"""

import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

OUT_DIR = Path("data")
OUT_DIR.mkdir(exist_ok=True)

N_CUSTOMERS = 200
N_PRODUCTS = 60
N_ORDERS = 500
EXTRACT_DATE = datetime.now().date()

FULFILLMENT_CENTERS = ["FC_NORTH", "FC_SOUTH", "FC_EAST", "FC_WEST"]
CATEGORIES = ["Electronics", "Apparel", "Home & Kitchen", "Sports", "Books", "Toys"]

# ---------------------------------------------------------------------------
# 1. Master data (dimensions) - load these into the Lakehouse first
# ---------------------------------------------------------------------------


def generate_customers(n):
    rows = []
    for i in range(n):
        rows.append(
            {
                "customer_id": f"CUST{i + 1:05d}",
                "customer_name": fake.name(),
                "email": fake.email(),
                "city": fake.city(),
                "country": fake.country(),
                "signup_date": fake.date_between(
                    start_date="-2y", end_date="-30d"
                ).isoformat(),
            }
        )
    return pd.DataFrame(rows)


def generate_products(n):
    rows = []
    for i in range(n):
        category = random.choice(CATEGORIES)
        rows.append(
            {
                "product_id": f"PROD{i + 1:04d}",
                "product_name": fake.catch_phrase(),
                "category": category,
                "unit_price": round(random.uniform(5, 500), 2),
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 2. Orders - generated ONCE as a shared pool, then split/mutated into
#    the batch extract and the stream events so both sources describe
#    the SAME underlying orders (this is what makes the merge meaningful).
# ---------------------------------------------------------------------------


def generate_order_pool(n, customers_df, products_df):
    customer_ids = customers_df["customer_id"].tolist()
    product_lookup = products_df.set_index("product_id")["unit_price"].to_dict()
    product_ids = list(product_lookup.keys())

    orders = []
    for i in range(n):
        order_id = f"ORD{100000 + i}"
        product_id = random.choice(product_ids)
        # order_ts spread across the last 36 hours so some are "yesterday's batch"
        # and some are "too recent for tonight's batch, stream-only for now"
        hours_ago = random.uniform(0.1, 36)
        order_ts = datetime.now() - timedelta(hours=hours_ago)

        orders.append(
            {
                "order_id": order_id,
                "customer_id": random.choice(customer_ids),
                "product_id": product_id,
                "quantity": random.randint(1, 5),
                "unit_price": product_lookup[product_id],
                "order_ts": order_ts,
                "hours_ago": hours_ago,
            }
        )
    return orders


def split_into_batch_and_stream(orders):
    """
    Scenario buckets, matching what we discussed:
      - hours_ago > 24  -> definitely in tonight's batch extract, and already
                            appeared in the stream ~a day ago (both sources, no conflict)
      - 6 < hours_ago <= 24 -> in batch, but batch OVERRIDES status
                                (stream said Created, batch says Cancelled/Refunded/Delivered)
      - hours_ago <= 6  -> too recent, STREAM-ONLY (not in tonight's batch yet)
    """
    batch_rows = []
    stream_events = []

    for o in orders:
        discount_pct = round(random.choice([0, 0, 0, 0.05, 0.1, 0.15, 0.2]), 2)
        fulfillment_center = random.choice(FULFILLMENT_CENTERS)

        # --- every order that isn't brand new emits at least one stream event ---
        create_event = {
            "event_id": str(uuid.uuid4()),
            "order_id": o["order_id"],
            "customer_id": o["customer_id"],
            "product_id": o["product_id"],
            "quantity": o["quantity"],
            "unit_price": o["unit_price"],
            "event_type": "OrderCreated",
            "event_ts": o["order_ts"].isoformat(),
            "source_system": "WEB_STREAM",
        }
        stream_events.append(create_event)

        if o["hours_ago"] <= 6:
            # STREAM-ONLY bucket: no batch row yet, this is today's "in-flight" order
            continue

        elif o["hours_ago"] <= 24:
            # BATCH OVERRIDE bucket: stream said Created, batch has the final word
            final_status = random.choice(["Cancelled", "Refunded", "Delivered"])
            payment_status = (
                "Refunded"
                if final_status == "Refunded"
                else ("Failed" if final_status == "Cancelled" else "Paid")
            )
            # also emit a matching update/cancel event in the stream, so Silver
            # can see the event *history*, even though batch is authoritative
            stream_events.append(
                {
                    "event_id": str(uuid.uuid4()),
                    "order_id": o["order_id"],
                    "customer_id": o["customer_id"],
                    "product_id": o["product_id"],
                    "quantity": o["quantity"],
                    "unit_price": o["unit_price"],
                    "event_type": "OrderCancelled"
                    if final_status in ("Cancelled", "Refunded")
                    else "OrderUpdated",
                    "event_ts": (o["order_ts"] + timedelta(hours=1)).isoformat(),
                    "source_system": "WEB_STREAM",
                }
            )
        else:
            # NORMAL bucket: batch confirms, no conflict
            final_status = random.choice(["Shipped", "Delivered", "Delivered"])
            payment_status = "Paid"

        batch_rows.append(
            {
                "order_id": o["order_id"],
                "customer_id": o["customer_id"],
                "product_id": o["product_id"],
                "quantity": o["quantity"],
                "unit_price": o["unit_price"],
                "discount_pct": discount_pct,
                "order_ts": o["order_ts"].isoformat(),
                "status": final_status,
                "payment_status": payment_status,
                "fulfillment_center": fulfillment_center,
                "extract_date": EXTRACT_DATE.isoformat(),
                "source_system": "ERP_BATCH",
            }
        )

    return batch_rows, stream_events


def main():
    customers_df = generate_customers(N_CUSTOMERS)
    products_df = generate_products(N_PRODUCTS)
    customers_df.to_csv(OUT_DIR / "customers.csv", index=False)
    products_df.to_csv(OUT_DIR / "products.csv", index=False)

    order_pool = generate_order_pool(N_ORDERS, customers_df, products_df)
    batch_rows, stream_events = split_into_batch_and_stream(order_pool)

    batch_df = pd.DataFrame(batch_rows)
    batch_filename = f"orders_batch_{EXTRACT_DATE.strftime('%Y%m%d')}.csv"
    batch_df.to_csv(OUT_DIR / batch_filename, index=False)

    with open(OUT_DIR / "orders_stream_events.jsonl", "w") as f:
        for event in stream_events:
            f.write(json.dumps(event) + "\n")

    stream_only = len(order_pool) - len(batch_rows)
    print(f"customers.csv          -> {len(customers_df)} rows")
    print(f"products.csv           -> {len(products_df)} rows")
    print(f"{batch_filename} -> {len(batch_df)} rows")
    print(f"orders_stream_events.jsonl -> {len(stream_events)} events")
    print(f"  of which stream-only (no batch row yet): {stream_only}")
    print(f"\nAll files written to {OUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
simulate_load.py
----------------
Generates realistic CDC events (INSERT / UPDATE / DELETE) against
the source PostgreSQL database to test the pipeline end-to-end.

Usage:
    python scripts/simulate_load.py --events 1000 --interval 0.05
    python scripts/simulate_load.py --events 500 --mode mixed
"""

import argparse
import logging
import os
import random
import time
import psycopg2
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger("cdc.simulate")

DB_CONFIG = {
    "host": os.getenv("SOURCE_DB_HOST", "localhost"),
    "port": int(os.getenv("SOURCE_DB_PORT", 5432)),
    "dbname": os.getenv("SOURCE_DB_NAME", "sourcedb"),
    "user": os.getenv("SOURCE_DB_USER", "postgres"),
    "password": os.getenv("SOURCE_DB_PASSWORD", "postgres"),
}

PRODUCTS = ["Laptop", "Monitor", "Keyboard", "Mouse", "Headphones", "Webcam", "Desk", "Chair"]
STATUSES = ["pending", "processing", "shipped", "completed", "cancelled"]
REGIONS = ["US-East", "US-West", "EU", "APAC", "LATAM"]


def random_order(customer_id: int) -> dict:
    return {
        "customer_id": customer_id,
        "product": random.choice(PRODUCTS),
        "quantity": random.randint(1, 5),
        "amount": round(random.uniform(9.99, 1999.99), 2),
        "status": "pending",
    }


def simulate(conn, total_events: int, interval: float, mode: str):
    inserted_ids = []
    stats = {"insert": 0, "update": 0, "delete": 0}

    with conn.cursor() as cur:
        # Ensure we have some customers
        cur.execute("SELECT id FROM customers LIMIT 10;")
        customer_ids = [row[0] for row in cur.fetchall()]
        if not customer_ids:
            for i in range(5):
                cur.execute(
                    "INSERT INTO customers (name, email, region) VALUES (%s, %s, %s) RETURNING id",
                    (f"User{i}", f"user{i}@example.com", random.choice(REGIONS))
                )
                customer_ids.append(cur.fetchone()[0])
            conn.commit()

    for i in range(total_events):
        try:
            with conn.cursor() as cur:
                op = _choose_op(mode, inserted_ids)

                if op == "insert" or not inserted_ids:
                    customer_id = random.choice(customer_ids)
                    order = random_order(customer_id)
                    cur.execute(
                        """INSERT INTO orders (customer_id, product, quantity, amount, status)
                           VALUES (%(customer_id)s, %(product)s, %(quantity)s, %(amount)s, %(status)s)
                           RETURNING id""",
                        order
                    )
                    new_id = cur.fetchone()[0]
                    inserted_ids.append(new_id)
                    stats["insert"] += 1
                    if i % 100 == 0:
                        logger.info(f"INSERT order id={new_id} ({i}/{total_events})")

                elif op == "update" and inserted_ids:
                    target_id = random.choice(inserted_ids)
                    new_status = random.choice(STATUSES)
                    cur.execute(
                        "UPDATE orders SET status = %s, updated_at = NOW() WHERE id = %s",
                        (new_status, target_id)
                    )
                    stats["update"] += 1

                elif op == "delete" and len(inserted_ids) > 10:
                    target_id = inserted_ids.pop(random.randint(0, len(inserted_ids) - 1))
                    cur.execute("DELETE FROM orders WHERE id = %s", (target_id,))
                    stats["delete"] += 1

                conn.commit()

        except psycopg2.Error as e:
            conn.rollback()
            logger.error(f"DB error at event {i}: {e}")

        time.sleep(interval)

    logger.info(f"\nSimulation complete: {stats}")


def _choose_op(mode: str, inserted_ids: list) -> str:
    if mode == "insert":
        return "insert"
    elif mode == "update":
        return "update" if inserted_ids else "insert"
    elif mode == "delete":
        return "delete" if len(inserted_ids) > 10 else "insert"
    else:  # mixed
        if not inserted_ids:
            return "insert"
        return random.choices(["insert", "update", "delete"], weights=[50, 35, 15])[0]


def main():
    parser = argparse.ArgumentParser(description="CDC Load Simulator")
    parser.add_argument("--events", type=int, default=500, help="Number of events to generate")
    parser.add_argument("--interval", type=float, default=0.05, help="Seconds between events")
    parser.add_argument("--mode", choices=["insert", "update", "delete", "mixed"], default="mixed")
    args = parser.parse_args()

    logger.info(f"Simulating {args.events} CDC events (mode={args.mode}, interval={args.interval}s)")
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        simulate(conn, args.events, args.interval, args.mode)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

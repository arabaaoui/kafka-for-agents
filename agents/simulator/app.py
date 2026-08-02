"""
Simulator — Generates synthetic retail orders and stock data.

Simulates 200 stores × 50 products, producing stock updates and orders
to Kafka topics. Periodically injects anomalies (quantity < seuil_min)
to trigger the detection→decision→execution pipeline.

Update interval: 5 seconds (divided by SIMULATION_SPEED).
"""

import json
import logging
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Producer

from common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    SIMULATION_SPEED,
    TOPIC_ORDERS,
    TOPIC_STOCKS,
)

# Logging with [SIMULATOR] prefix
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SIMULATOR] %(levelname)s %(message)s",
)
logger = logging.getLogger("simulator")

# Signal for graceful shutdown
shutdown_event = False


def handle_sigterm(signum, frame):
    """Handle SIGTERM for graceful shutdown."""
    global shutdown_event
    logger.info("SIGTERM received, shutting down gracefully...")
    shutdown_event = True


signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)

# --- Configuration ---

NUM_STORES = 200
NUM_PRODUCTS = 50
REGIONS = ["IDF", "PACA", "NORD", "SUD_OUEST", "RHONE_ALPES", "GRAND_EST", "BRETAGNE", "CENTRE"]

PRODUCT_CATEGORIES = [
    "SEC",        # Dry goods
    "FRAIS",      # Fresh
    "ULTRA_FRAIS",  # Ultra-fresh
    "SURGELE",    # Frozen
    "BOISSON",    # Beverages
]

# Perishable categories that need the 10% buffer
PERISHABLE_CATEGORIES = {"FRAIS", "ULTRA_FRAIS", "SURGELE"}


def init_stock_state() -> dict[str, dict[str, Any]]:
    """
    Initialize stock state for all store × product combinations.
    Returns a dict keyed by "store-{i}:{product_id}" with current quantity and seuil_min.
    """
    state = {}
    for store_id in range(1, NUM_STORES + 1):
        region = REGIONS[(store_id - 1) % len(REGIONS)]
        for product_id in range(1, NUM_PRODUCTS + 1):
            category = PRODUCT_CATEGORIES[(product_id - 1) % len(PRODUCT_CATEGORIES)]
            pid = f"prod-{product_id:03d}"
            key = f"store-{store_id:03d}:{pid}"

            # Random base stock: 100-500 units
            base_stock = random.randint(100, 500)
            seuil_min = random.randint(30, 80)

            state[key] = {
                "store_id": f"store-{store_id:03d}",
                "product_id": pid,
                "quantity": base_stock,
                "seuil_min": seuil_min,
                "region": region,
                "category": category,
            }
    logger.info(f"Initialized stock state: {len(state)} entries ({NUM_STORES} stores × {NUM_PRODUCTS} products)")
    return state


def update_stock(state: dict[str, dict[str, Any]]) -> None:
    """
    Simulate natural stock depletion: decrease quantities slightly.
    Also simulate occasional restocking.
    """
    for key, stock in state.items():
        # Random consumption: 0-5 units per cycle
        consumption = random.randint(0, 5)
        stock["quantity"] = max(0, stock["quantity"] - consumption)

        # Occasional restocking (2% chance per cycle)
        if random.random() < 0.02:
            stock["quantity"] += random.randint(50, 200)


def inject_anomalies(state: dict[str, dict[str, Any]], count: int = 3) -> None:
    """
    Periodically inject anomalies by setting quantity below seuil_min.
    This triggers the detection→decision→execution pipeline.
    """
    keys = list(state.keys())
    for _ in range(count):
        key = random.choice(keys)
        stock = state[key]
        # Set quantity below threshold to trigger anomaly
        stock["quantity"] = max(0, stock["seuil_min"] - random.randint(1, 20))
        logger.info(
            f"ANOMALY INJECTED: {stock['store_id']} {stock['product_id']} "
            f"qty={stock['quantity']} < seuil_min={stock['seuil_min']} "
            f"region={stock['region']}"
        )


def generate_order(store: dict[str, Any]) -> dict[str, Any] | None:
    """
    Generate a synthetic order for a store if stock is low.
    Returns an order dict or None.
    """
    # Only generate orders sometimes (15% chance per product per cycle)
    if random.random() > 0.15:
        return None

    return {
        "order_id": f"ord-{int(time.time() * 1000000)}-{random.randint(1000, 9999)}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "store_id": store["store_id"],
        "product_id": store["product_id"],
        "quantity": random.randint(10, 100),
        "region": store["region"],
        "category": store["category"],
    }


def produce_message(producer: Producer, topic: str, key: str, value: dict) -> None:
    """Produce a JSON message to a Kafka topic."""
    try:
        payload = json.dumps(value, ensure_ascii=False)
        producer.produce(
            topic,
            key=key,
            value=payload.encode("utf-8"),
            on_delivery=lambda err, msg: logger.error(f"Delivery failed to {topic}: {err}")
            if err
            else None,
        )
    except Exception as e:
        logger.error(f"Failed to produce to {topic}: {e}")


def main():
    """Main loop — generates and produces synthetic stock updates and orders."""
    global shutdown_event

    # Kafka producer
    producer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "linger.ms": 100,
        "batch.size": 16384,
    }
    producer = Producer(producer_conf)

    # Initialize stock state
    state = init_stock_state()

    # Update interval (divided by SIMULATION_SPEED)
    base_interval = 5.0
    interval = base_interval / max(SIMULATION_SPEED, 0.1)

    # Cycle counter for periodic anomaly injection
    cycle_count = 0
    anomaly_injection_interval = 10  # Inject anomalies every 10 cycles

    logger.info(f"Simulator started: {NUM_STORES} stores, {NUM_PRODUCTS} products")
    logger.info(f"Update interval: {interval:.1f}s (base={base_interval}s, speed={SIMULATION_SPEED}x)")
    logger.info(f"Kafka bootstrap: {KAFKA_BOOTSTRAP_SERVERS}")

    try:
        while not shutdown_event:
            cycle_start = time.time()
            cycle_count += 1

            # Update stock levels (decrease naturally)
            update_stock(state)

            # Produce stock updates and orders for all store×product combos
            for key, stock in state.items():
                # Add timestamp
                stock_msg = {
                    **stock,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }

                # Produce stock message
                produce_message(producer, TOPIC_STOCKS, key, stock_msg)

                # Optionally generate an order
                order = generate_order(stock)
                if order:
                    produce_message(producer, TOPIC_ORDERS, order["order_id"], order)

            # Flush all produced messages
            producer.flush(timeout=10)

            # Periodically inject anomalies
            if cycle_count % anomaly_injection_interval == 0:
                logger.info(f"Cycle {cycle_count}: injecting anomalies...")
                inject_anomalies(state, count=random.randint(2, 5))

            # Calculate sleep time to maintain the interval
            elapsed = time.time() - cycle_start
            sleep_time = max(0.1, interval - elapsed)
            time.sleep(sleep_time)

    except Exception as e:
        logger.exception(f"Fatal error in simulator loop: {e}")
    finally:
        logger.info("Closing simulator...")
        producer.flush(timeout=15)
        logger.info("Simulator stopped.")


if __name__ == "__main__":
    main()
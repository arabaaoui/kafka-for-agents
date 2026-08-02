"""
Detection Agent — Monitors the 'stocks' topic for anomalies.

For each stock message where quantity < seuil_min, produces an anomaly
JSON message to the 'anomalies' topic. Uses MCP Confluent via HTTP JSON-RPC
to consume additional context when needed.
"""

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

import httpx
from confluent_kafka import Consumer, Producer, KafkaError

from common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    MCP_CONFLUENT_URL,
    TOPIC_STOCKS,
    TOPIC_ANOMALIES,
)

# Logging with [DETECTION] prefix
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [DETECTION] %(levelname)s %(message)s",
)
logger = logging.getLogger("detection")

# Signal for graceful shutdown
shutdown_event = False


def handle_sigterm(signum, frame):
    """Handle SIGTERM for graceful shutdown."""
    global shutdown_event
    logger.info("SIGTERM received, shutting down gracefully...")
    shutdown_event = True


signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)


def call_mcp_consume(topic: str, limit: int = 10) -> list[dict]:
    """
    Call the MCP Confluent server via HTTP POST with JSON-RPC format
    to consume messages from a topic for additional context.
    """
    try:
        payload = {
            "jsonrpc": "2.0",
            "method": "tools/call",
            "params": {
                "name": "consume-messages",
                "arguments": {
                    "topic": topic,
                    "max_messages": limit,
                },
            },
            "id": int(time.time() * 1000),
        }

        with httpx.Client(timeout=30.0) as client:
            response = client.post(
                f"{MCP_CONFLUENT_URL}/mcp",
                json=payload,
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
            result = response.json()

            # Extract messages from JSON-RPC response
            if "result" in result and "content" in result["result"]:
                content = result["result"]["content"]
                if isinstance(content, list) and len(content) > 0:
                    text = content[0].get("text", "[]")
                    return json.loads(text)
            return []
    except Exception as e:
        logger.warning(f"MCP Confluent call failed: {e}")
        return []


def is_anomaly(stock_msg: dict) -> bool:
    """Check if a stock message represents an anomaly (quantity < seuil_min)."""
    quantity = stock_msg.get("quantity", 0)
    seuil_min = stock_msg.get("seuil_min", 0)
    return quantity < seuil_min


def build_anomaly_json(stock_msg: dict) -> dict:
    """Build the anomaly JSON payload from a stock message."""
    quantity = stock_msg.get("quantity", 0)
    seuil_min = stock_msg.get("seuil_min", 0)

    # Determine severity
    if quantity == 0:
        type_anomalie = "STOCK_ZERO"
        severite = "CRITIQUE"
    elif quantity < seuil_min * 0.5:
        type_anomalie = "STOCK_CRITIQUE"
        severite = "CRITIQUE"
    else:
        type_anomalie = "RUPTURE_IMMINENTE"
        severite = "HAUTE"

    return {
        "anomalie_id": f"anom-{int(time.time() * 1000000)}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "store_id": stock_msg.get("store_id"),
        "product_id": stock_msg.get("product_id"),
        "region": stock_msg.get("region"),
        "current_quantity": quantity,
        "seuil_min": seuil_min,
        "type_anomalie": type_anomalie,
        "severite": severite,
        "stock_message_original": stock_msg,
    }


def main():
    """Main loop — polls 'stocks' topic, detects anomalies, produces to 'anomalies'."""
    # Kafka consumer for stocks
    consumer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "detection-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC_STOCKS])

    # Kafka producer for anomalies
    producer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    }
    producer = Producer(producer_conf)

    logger.info(f"Detection agent started. Listening on topic '{TOPIC_STOCKS}'")

    try:
        while not shutdown_event:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                time.sleep(5)
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    time.sleep(5)
                    continue
                logger.error(f"Kafka error: {msg.error()}")
                time.sleep(5)
                continue

            # Parse the stock message
            try:
                value = msg.value()
                if value is None:
                    continue
                stock_data = json.loads(value.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.error(f"Failed to parse stock message: {e}")
                continue

            logger.debug(
                f"Received stock: store={stock_data.get('store_id')} "
                f"product={stock_data.get('product_id')} "
                f"qty={stock_data.get('quantity')}"
            )

            # Check for anomaly
            if is_anomaly(stock_data):
                logger.warning(
                    f"Anomaly detected: store={stock_data.get('store_id')} "
                    f"product={stock_data.get('product_id')} "
                    f"qty={stock_data.get('quantity')} < seuil_min={stock_data.get('seuil_min')}"
                )

                # Build anomaly message
                anomaly = build_anomaly_json(stock_data)

                # Optionally enrich with MCP Confluent context
                try:
                    extra_context = call_mcp_consume(TOPIC_STOCKS, limit=5)
                    if extra_context:
                        anomaly["contexte_mcp"] = extra_context
                except Exception:
                    pass  # MCP is best-effort

                # Produce to anomalies topic
                anomaly_json = json.dumps(anomaly, ensure_ascii=False)
                producer.produce(
                    TOPIC_ANOMALIES,
                    key=str(anomaly.get("store_id")),
                    value=anomaly_json.encode("utf-8"),
                    on_delivery=lambda err, msg: logger.error(f"Delivery failed: {err}")
                    if err
                    else None,
                )
                producer.flush(timeout=5)
                logger.info(f"Anomaly produced: {anomaly['anomalie_id']}")

            # Sleep between polls
            time.sleep(5)

    except Exception as e:
        logger.exception(f"Fatal error in detection loop: {e}")
    finally:
        logger.info("Closing detection agent...")
        consumer.close()
        producer.flush(timeout=10)
        logger.info("Detection agent stopped.")


if __name__ == "__main__":
    main()
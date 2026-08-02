"""
Detection Agent — Monitors the 'stocks' topic for anomalies.

A deterministic pre-filter (quantity < seuil_min) decides WHETHER a message is
worth escalating — the simulator produces ~10,000 stock messages per cycle
(200 stores x 50 products), and calling an LLM on every single one would be
both slow and needlessly expensive. Only pre-filtered candidates go through
the real google-adk Agent, which qualifies severity and publishes the anomaly
itself via its `produce_anomaly` tool.

The detection LLM is OPTIONAL, same principle as the Execution agent. If
DETECTION_LLM_API_KEY is empty, no ADK agent is ever instantiated: each
pre-filtered candidate is published straight to 'anomalies' as a basic,
unqualified anomaly (see `produce_simple_anomaly`) — no severity qualification.
"""

import json
import logging
import signal
import time
from datetime import datetime, timezone

import httpx
from confluent_kafka import Consumer, Producer, KafkaError

from common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    MCP_CONFLUENT_URL,
    TOPIC_STOCKS,
    TOPIC_ANOMALIES,
    DETECTION_LLM_PROVIDER,
    DETECTION_LLM_MODEL,
    DETECTION_LLM_API_KEY,
    STOCK_ALERT_THRESHOLD_RATIO,
)
from common.adk_factory import AdkAgentRunner
from detection.prompts import SYSTEM_PROMPT, DETECTION_USER_PROMPT

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


def is_anomaly(stock_msg: dict) -> bool:
    """Deterministic pre-filter: quantity < seuil_min * STOCK_ALERT_THRESHOLD_RATIO. Runs BEFORE the LLM is invoked."""
    quantity = stock_msg.get("quantity", 0)
    seuil_min = stock_msg.get("seuil_min", 0)
    return quantity < seuil_min * STOCK_ALERT_THRESHOLD_RATIO


def produce_simple_anomaly(producer: Producer, stock_data: dict) -> None:
    """No-LLM detection path: publish the pre-filtered candidate as-is, no severity qualification."""
    store_id = stock_data.get("store_id")
    product_id = stock_data.get("product_id")
    quantity = stock_data.get("quantity", 0)
    seuil_min = stock_data.get("seuil_min", 0)
    timestamp = datetime.now(timezone.utc).isoformat()

    anomaly = {
        "anomalie_id": f"anomaly-{product_id}-{store_id}-{timestamp}",
        "timestamp": timestamp,
        "type_anomalie": "rupture_stock",
        "severity": "WARNING",
        "store_id": store_id,
        "product_id": product_id,
        "region": stock_data.get("region"),
        "current_quantity": quantity,
        "seuil_min": seuil_min,
        "message": f"Stock bas détecté (qty={quantity} < seuil={seuil_min})",
    }
    anomaly_json_str = json.dumps(anomaly, ensure_ascii=False)
    producer.produce(
        TOPIC_ANOMALIES,
        key=str(store_id),
        value=anomaly_json_str.encode("utf-8"),
        on_delivery=lambda err, msg: logger.error(f"Delivery failed: {err}") if err else None,
    )
    producer.flush(timeout=5)
    logger.info(f"Anomaly produced (deterministic): {anomaly['anomalie_id']}")


class DetectionTools:
    """Tools exposed to the detection ADK agent. Bound to a live Kafka producer."""

    def __init__(self, producer: Producer):
        self._producer = producer

    def consume_stocks(self, limit: int = 5) -> list[dict]:
        """Read the most recent messages from the 'stocks' Kafka topic via MCP Confluent, for extra context."""
        return self.call_mcp("consume-messages", {"topic": TOPIC_STOCKS, "max_messages": limit})

    def call_mcp(self, tool_name: str, arguments: dict) -> dict | list:
        """Call an MCP Confluent tool (consume-messages, list-topics, get-topic-config, get-consumer-group-lag) via HTTP JSON-RPC."""
        try:
            payload = {
                "jsonrpc": "2.0",
                "method": "tools/call",
                "params": {"name": tool_name, "arguments": arguments},
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

                if "result" in result and "content" in result["result"]:
                    content = result["result"]["content"]
                    if isinstance(content, list) and len(content) > 0:
                        return json.loads(content[0].get("text", "{}"))
                return {}
        except Exception as e:
            logger.warning(f"MCP Confluent call failed: {e}")
            return {}

    def produce_anomaly(self, anomalie_json: str) -> str:
        """Publish an anomaly JSON payload to the 'anomalies' Kafka topic. Call exactly once per message."""
        try:
            anomaly = json.loads(anomalie_json)
        except json.JSONDecodeError as e:
            return f"ERROR: invalid JSON — {e}"

        anomaly.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
        anomaly_json_str = json.dumps(anomaly, ensure_ascii=False)
        self._producer.produce(
            TOPIC_ANOMALIES,
            key=str(anomaly.get("store_id")),
            value=anomaly_json_str.encode("utf-8"),
            on_delivery=lambda err, msg: logger.error(f"Delivery failed: {err}") if err else None,
        )
        self._producer.flush(timeout=5)
        logger.info(f"Anomaly produced: {anomaly.get('anomalie_id')}")
        return "OK: anomaly produced"


def main():
    """Main loop — polls 'stocks' topic, pre-filters, delegates qualification+publish to the ADK agent."""
    consumer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "detection-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC_STOCKS])

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    tools = DetectionTools(producer)
    agent_runner = None
    if DETECTION_LLM_API_KEY:
        agent_runner = AdkAgentRunner(
            name="detection-agent",
            description="Détecte et qualifie les anomalies de stock (rupture, stock critique, stock zéro).",
            instruction=SYSTEM_PROMPT,
            tools=[tools.consume_stocks, tools.call_mcp, tools.produce_anomaly],
            provider=DETECTION_LLM_PROVIDER,
            model=DETECTION_LLM_MODEL,
            api_key=DETECTION_LLM_API_KEY,
        )
        logger.info(f"LLM: provider={DETECTION_LLM_PROVIDER} model={DETECTION_LLM_MODEL}")
    else:
        logger.info("No DETECTION_LLM_API_KEY configured — running deterministic anomaly detection")

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

            try:
                value = msg.value()
                if value is None:
                    continue
                stock_data = json.loads(value.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.error(f"Failed to parse stock message: {e}")
                continue

            if not is_anomaly(stock_data):
                time.sleep(5)
                continue

            logger.warning(
                f"Candidate anomaly: store={stock_data.get('store_id')} "
                f"product={stock_data.get('product_id')} "
                f"qty={stock_data.get('quantity')} < seuil_min={stock_data.get('seuil_min')}"
            )

            if agent_runner is not None:
                user_prompt = DETECTION_USER_PROMPT.format(
                    store_id=stock_data.get("store_id"),
                    product_id=stock_data.get("product_id"),
                    quantity=stock_data.get("quantity"),
                    seuil_min=stock_data.get("seuil_min"),
                    region=stock_data.get("region"),
                    timestamp=stock_data.get("timestamp"),
                )
                try:
                    response = agent_runner.run(user_prompt)
                    logger.info(f"Agent response: {response}")
                except Exception as e:
                    logger.error(f"Detection agent LLM call failed: {e}")
            else:
                produce_simple_anomaly(producer, stock_data)

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

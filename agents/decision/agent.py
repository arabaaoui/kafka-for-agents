"""
Decision Agent — Polls the 'anomalies' topic and decides on replenishment actions.

Reads the SKILL.md procedure, calls an LLM to decide between internal transfer
or supplier order, produces tasks to 'tasks' topic and logs decisions to 'audit'.
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
    SKILL_PATH,
    LLM_PROVIDER,
    LLM_MODEL,
    LLM_API_KEY,
    TOPIC_ANOMALIES,
    TOPIC_TASKS,
    TOPIC_AUDIT,
    TOPIC_STOCKS,
)

# Logging with [DECISION] prefix
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [DECISION] %(levelname)s %(message)s",
)
logger = logging.getLogger("decision")

# Signal for graceful shutdown
shutdown_event = False


def handle_sigterm(signum, frame):
    """Handle SIGTERM for graceful shutdown."""
    global shutdown_event
    logger.info("SIGTERM received, shutting down gracefully...")
    shutdown_event = True


signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)


def load_skill_file() -> str:
    """Load the SKILL.md file content for the replenishment procedure."""
    try:
        with open(SKILL_PATH, "r", encoding="utf-8") as f:
            content = f.read()
        logger.info(f"SKILL.md loaded from {SKILL_PATH} ({len(content)} chars)")
        return content
    except FileNotFoundError:
        logger.error(f"SKILL.md not found at {SKILL_PATH}")
        return ""
    except Exception as e:
        logger.exception(f"Failed to load SKILL.md: {e}")
        return ""


def call_llm(system_prompt: str, user_prompt: str) -> dict | None:
    """
    Call an LLM (OpenAI-compatible API) with the given prompts.
    Returns the parsed JSON decision or None on failure.
    """
    if not LLM_API_KEY:
        logger.error("LLM_API_KEY not set — cannot call LLM")
        return None

    # Determine the API endpoint based on provider
    provider_endpoints = {
        "anthropic": "https://api.anthropic.com/v1/messages",
        "openai": "https://api.openai.com/v1/chat/completions",
        "deepseek": "https://api.deepseek.com/v1/chat/completions",
    }

    api_url = provider_endpoints.get(LLM_PROVIDER.lower(), "https://api.openai.com/v1/chat/completions")

    try:
        # Build request for OpenAI-compatible format
        headers = {
            "Authorization": f"Bearer {LLM_API_KEY}",
            "Content-Type": "application/json",
        }

        payload = {
            "model": LLM_MODEL,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.2,
            "max_tokens": 2000,
        }

        with httpx.Client(timeout=60.0) as client:
            response = client.post(api_url, json=payload, headers=headers)
            response.raise_for_status()
            result = response.json()

            # Extract content from response
            content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
            logger.debug(f"LLM response: {content[:200]}...")

            # Try to parse JSON from response
            # The LLM might wrap the JSON in markdown code blocks
            json_start = content.find("{")
            json_end = content.rfind("}") + 1
            if json_start >= 0 and json_end > json_start:
                json_str = content[json_start:json_end]
                return json.loads(json_str)

            logger.warning(f"Could not extract JSON from LLM response")
            return None

    except Exception as e:
        logger.error(f"LLM call failed: {e}")
        return None


def get_neighbor_stocks(consumer: Consumer, region: str, product_id: str, exclude_store: str) -> list[dict]:
    """
    Check stock levels at neighboring stores in the same region.
    Polls recent messages from the 'stocks' topic.
    """
    # We poll for recent stock messages and filter by region/product
    neighbors = []
    try:
        # Try to get up to 50 messages quickly
        for _ in range(50):
            msg = consumer.poll(timeout=0.1)
            if msg is None or msg.error():
                continue
            try:
                value = msg.value()
                if value is None:
                    continue
                stock_data = json.loads(value.decode("utf-8"))
                if (
                    stock_data.get("region") == region
                    and stock_data.get("product_id") == product_id
                    and stock_data.get("store_id") != exclude_store
                ):
                    neighbors.append(stock_data)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
    except Exception as e:
        logger.warning(f"Error polling neighbor stocks: {e}")

    return neighbors


def build_decision_prompt(anomaly: dict, skill_content: str, neighbors: list[dict]) -> str:
    """Build the full decision prompt for the LLM."""
    from .prompts import DECISION_PROMPT_TEMPLATE

    # Format neighbor stocks
    neighbor_str = json.dumps(neighbors, indent=2, ensure_ascii=False) if neighbors else "[]"

    return DECISION_PROMPT_TEMPLATE.format(
        anomalie_id=anomaly.get("anomalie_id", ""),
        store_id=anomaly.get("store_id", ""),
        product_id=anomaly.get("product_id", ""),
        region=anomaly.get("region", ""),
        current_quantity=anomaly.get("current_quantity", 0),
        seuil_min=anomaly.get("seuil_min", 0),
        type_anomalie=anomaly.get("type_anomalie", "UNKNOWN"),
        severite=anomaly.get("severite", "UNKNOWN"),
        skill_content=skill_content,
        neighbor_stocks=neighbor_str,
    )


def produce_audit_log(producer: Producer, anomaly: dict, decision: dict) -> None:
    """Log the decision to the audit topic."""
    audit_msg = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "anomalie_id": anomaly.get("anomalie_id"),
        "type_decision": decision.get("decision"),
        "product_id": decision.get("product_id"),
        "store_id": decision.get("store_id") or anomaly.get("store_id"),
        "region": anomaly.get("region"),
        "quantite_commandee": decision.get("quantite"),
        "magasin_source": decision.get("magasin_source"),
        "delai_estime_heures": decision.get("delai_estime_heures"),
        "perissable": decision.get("perissable", False),
        "raison": decision.get("raison", ""),
    }

    audit_json = json.dumps(audit_msg, ensure_ascii=False)
    producer.produce(
        TOPIC_AUDIT,
        key=str(anomaly.get("anomalie_id")),
        value=audit_json.encode("utf-8"),
    )
    logger.info(f"Audit log produced for anomaly {anomaly.get('anomalie_id')}")


def produce_task(producer: Producer, decision: dict) -> None:
    """Produce a task to the tasks topic."""
    task_msg = {
        "task_id": f"task-{int(time.time() * 1000000)}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "action": decision.get("decision", ""),
        "magasin_source": decision.get("magasin_source"),
        "magasin_destination": decision.get("magasin_destination"),
        "product_id": decision.get("product_id"),
        "quantite": decision.get("quantite"),
        "perissable": decision.get("perissable", False),
        "buffer_gaspillage_pct": decision.get("buffer_gaspillage_pct"),
        "priorite": decision.get("priorite", "HAUTE"),
        "delai_estime_heures": decision.get("delai_estime_heures"),
        "raison": decision.get("raison", ""),
    }

    task_json = json.dumps(task_msg, ensure_ascii=False)
    producer.produce(
        TOPIC_TASKS,
        key=task_msg["task_id"],
        value=task_json.encode("utf-8"),
    )
    logger.info(f"Task produced: {task_msg['task_id']} action={task_msg['action']}")


def main():
    """Main loop — polls 'anomalies', decides action, produces tasks and audit logs."""
    global shutdown_event

    # Load SKILL.md
    skill_content = load_skill_file()
    if not skill_content:
        logger.warning("SKILL.md is empty or could not be loaded — LLM decisions may be degraded")

    # Load system prompt
    try:
        from .prompts import SYSTEM_PROMPT
    except ImportError:
        SYSTEM_PROMPT_VAR = (
            "Tu es un agent de décision supply chain. "
            "Pour chaque anomalie, applique la procédure du fichier SKILL.md."
        )
        SYSTEM_PROMPT_VAR = SYSTEM_PROMPT
    else:
        SYSTEM_PROMPT_VAR = SYSTEM_PROMPT

    # Kafka consumer for anomalies
    consumer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "decision-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC_ANOMALIES])

    # Separate consumer for neighbor stock polling (different group, no offset commit)
    stocks_consumer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "decision-stocks-group",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    }
    stocks_consumer = Consumer(stocks_consumer_conf)
    stocks_consumer.subscribe([TOPIC_STOCKS])

    # Kafka producer
    producer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    }
    producer = Producer(producer_conf)

    logger.info(f"Decision agent started. Listening on topic '{TOPIC_ANOMALIES}'")
    logger.info(f"SKILL_PATH={SKILL_PATH}, LLM_PROVIDER={LLM_PROVIDER}, LLM_MODEL={LLM_MODEL}")

    try:
        while not shutdown_event:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                time.sleep(3)
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    time.sleep(3)
                    continue
                logger.error(f"Kafka error: {msg.error()}")
                time.sleep(3)
                continue

            # Parse anomaly message
            try:
                value = msg.value()
                if value is None:
                    continue
                anomaly = json.loads(value.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.error(f"Failed to parse anomaly message: {e}")
                continue

            logger.info(
                f"Processing anomaly: {anomaly.get('anomalie_id')} "
                f"store={anomaly.get('store_id')} product={anomaly.get('product_id')}"
            )

            # Get neighbor stocks
            neighbors = get_neighbor_stocks(
                stocks_consumer,
                anomaly.get("region", ""),
                anomaly.get("product_id", ""),
                anomaly.get("store_id", ""),
            )
            logger.info(f"Found {len(neighbors)} neighbor(s) in region {anomaly.get('region')}")

            # Build prompt and call LLM
            user_prompt = build_decision_prompt(anomaly, skill_content, neighbors)
            decision = call_llm(SYSTEM_PROMPT_VAR, user_prompt)

            if decision is None:
                # Fallback: if LLM fails, create a default supplier order
                logger.warning("LLM failed — using fallback decision (supplier order)")
                decision = {
                    "decision": "COMMANDE_FOURNISSEUR",
                    "magasin_source": None,
                    "magasin_destination": anomaly.get("store_id"),
                    "product_id": anomaly.get("product_id"),
                    "quantite": max(anomaly.get("seuil_min", 10) - anomaly.get("current_quantity", 0) + 5, 1),
                    "perissable": False,
                    "buffer_gaspillage_pct": None,
                    "priorite": anomaly.get("severite", "HAUTE"),
                    "delai_estime_heures": 48,
                    "raison": "Fallback : décision automatique suite à échec LLM",
                }

            # Produce task
            produce_task(producer, decision)

            # Produce audit log
            produce_audit_log(producer, anomaly, decision)

            # Flush producer
            producer.flush(timeout=5)

            time.sleep(3)

    except Exception as e:
        logger.exception(f"Fatal error in decision loop: {e}")
    finally:
        logger.info("Closing decision agent...")
        consumer.close()
        stocks_consumer.close()
        producer.flush(timeout=10)
        logger.info("Decision agent stopped.")


if __name__ == "__main__":
    main()
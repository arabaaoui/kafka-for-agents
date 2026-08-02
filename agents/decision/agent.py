"""
Decision Agent — Polls the 'anomalies' topic and decides on replenishment actions.

SKILL.md is read ONCE at startup and injected into the ADK agent's instruction
(not re-read per message). The agent applies the procedure itself through its
tools (get_neighbor_stocks, produce_task, log_audit) — the Python loop only
drives Kafka polling and provides a deterministic fallback decision if the LLM
call fails or the agent forgets to produce a task, so the demo pipeline never
stalls on an LLM outage.

The decision LLM is OPTIONAL, same principle as the Execution agent. If
DECISION_LLM_API_KEY is empty, no ADK agent is ever instantiated: every
anomaly goes straight through `fallback_decision`/`fallback_audit` — the
default rule-based outcome (commande fournisseur) — reusing the exact same
fallback path already used when the LLM fails or forgets to produce a task.
"""

import json
import logging
import signal
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer, KafkaError

from common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    SKILL_PATH,
    TOPIC_ANOMALIES,
    TOPIC_TASKS,
    TOPIC_AUDIT,
    TOPIC_STOCKS,
    DECISION_LLM_PROVIDER,
    DECISION_LLM_MODEL,
    DECISION_LLM_API_KEY,
)
from common.adk_factory import AdkAgentRunner
from decision.prompts import SYSTEM_PROMPT, DECISION_USER_PROMPT

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


def poll_neighbor_stocks(consumer: Consumer, region: str, product_id: str, exclude_store: str) -> list[dict]:
    """Poll recent messages from the 'stocks' topic and filter by region/product."""
    neighbors = []
    try:
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


def produce_task(producer: Producer, task: dict) -> None:
    """Produce a task to the tasks topic."""
    task.setdefault("task_id", f"task-{int(time.time() * 1000000)}")
    task.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
    task_json = json.dumps(task, ensure_ascii=False)
    producer.produce(TOPIC_TASKS, key=task["task_id"], value=task_json.encode("utf-8"))
    logger.info(f"Task produced: {task['task_id']} action={task.get('action')}")


def produce_audit_log(producer: Producer, audit: dict) -> None:
    """Log a decision to the audit topic."""
    audit.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
    audit_json = json.dumps(audit, ensure_ascii=False)
    producer.produce(TOPIC_AUDIT, key=str(audit.get("anomalie_id")), value=audit_json.encode("utf-8"))
    logger.info(f"Audit log produced for anomaly {audit.get('anomalie_id')}")


def fallback_decision(anomaly: dict) -> dict:
    """Deterministic fallback if the LLM fails or forgets to call produce_task: default supplier order."""
    quantite = max(anomaly.get("seuil_min", 10) - anomaly.get("current_quantity", 0) + 5, 1)
    return {
        "action": "commande_fournisseur",
        "magasin_source": None,
        "magasin_destination": anomaly.get("store_id"),
        "product_id": anomaly.get("product_id"),
        "quantite": quantite,
        "perissable": False,
        "buffer_gaspillage_pct": None,
        "priorite": anomaly.get("severite", "HAUTE"),
        "region": anomaly.get("region"),
        "delai_estime_heures": 48,
        "raison": "Fallback : décision automatique suite à échec ou absence de réponse du LLM",
    }


def fallback_audit(anomaly: dict, decision: dict) -> dict:
    """Build the audit entry matching a fallback (or agent) decision."""
    type_decision = "COMMANDE_FOURNISSEUR" if decision.get("action") == "commande_fournisseur" else "TRANSFERT_INTERNE"
    return {
        "anomalie_id": anomaly.get("anomalie_id"),
        "type_decision": type_decision,
        "product_id": decision.get("product_id"),
        "store_id": decision.get("magasin_destination") or anomaly.get("store_id"),
        "region": anomaly.get("region"),
        "quantite_commandee": decision.get("quantite"),
        "magasin_source": decision.get("magasin_source"),
        "delai_estime_heures": decision.get("delai_estime_heures"),
        "perissable": decision.get("perissable", False),
        "raison": decision.get("raison", ""),
    }


class DecisionTools:
    """Tools exposed to the decision ADK agent. Tracks side effects for the fallback logic."""

    def __init__(self, producer: Producer, stocks_consumer: Consumer, skill_content: str):
        self._producer = producer
        self._stocks_consumer = stocks_consumer
        self._skill_content = skill_content
        self.reset()

    def reset(self) -> None:
        self.task_produced = False
        self.audit_produced = False
        self.last_decision: dict | None = None

    def read_skill(self) -> str:
        """Return the full text of the SKILL.md replenishment procedure."""
        return self._skill_content

    def get_neighbor_stocks(self, region: str, product_id: str, exclude_store: str) -> list[dict]:
        """List stock levels for a product across stores in the same region, excluding the anomaly's own store."""
        return poll_neighbor_stocks(self._stocks_consumer, region, product_id, exclude_store)

    def produce_task(self, task_json: str) -> str:
        """Publish an execution task (transfert_interne or commande_fournisseur) to the 'tasks' topic."""
        try:
            task = json.loads(task_json)
        except json.JSONDecodeError as e:
            return f"ERROR: invalid JSON — {e}"
        produce_task(self._producer, task)
        self.task_produced = True
        self.last_decision = task
        return "OK: task produced"

    def log_audit(self, decision_json: str) -> str:
        """Publish an audit log entry to the 'audit' topic."""
        try:
            audit = json.loads(decision_json)
        except json.JSONDecodeError as e:
            return f"ERROR: invalid JSON — {e}"
        produce_audit_log(self._producer, audit)
        self.audit_produced = True
        return "OK: audit produced"


def main():
    """Main loop — polls 'anomalies', delegates decision to the ADK agent, guarantees task+audit output."""
    skill_content = load_skill_file()
    if not skill_content:
        logger.warning("SKILL.md is empty or could not be loaded — decisions may be degraded")

    consumer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "decision-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([TOPIC_ANOMALIES])

    # Separate consumer for neighbor stock polling (different group, no offset commit)
    stocks_consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "decision-stocks-group",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })
    stocks_consumer.subscribe([TOPIC_STOCKS])

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    tools = DecisionTools(producer, stocks_consumer, skill_content)
    agent_runner = None
    if DECISION_LLM_API_KEY:
        instruction = SYSTEM_PROMPT.format(skill_content=skill_content)
        agent_runner = AdkAgentRunner(
            name="decision-agent",
            description="Décide entre transfert interne et commande fournisseur en appliquant SKILL.md.",
            instruction=instruction,
            tools=[tools.read_skill, tools.get_neighbor_stocks, tools.produce_task, tools.log_audit],
            provider=DECISION_LLM_PROVIDER,
            model=DECISION_LLM_MODEL,
            api_key=DECISION_LLM_API_KEY,
        )
        logger.info(f"LLM: provider={DECISION_LLM_PROVIDER} model={DECISION_LLM_MODEL}")
    else:
        logger.info("No DECISION_LLM_API_KEY configured — running deterministic decision (commande fournisseur par défaut)")

    logger.info(f"Decision agent started. Listening on topic '{TOPIC_ANOMALIES}'")

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

            tools.reset()
            if agent_runner is not None:
                user_prompt = DECISION_USER_PROMPT.format(
                    anomalie_id=anomaly.get("anomalie_id", ""),
                    store_id=anomaly.get("store_id", ""),
                    product_id=anomaly.get("product_id", ""),
                    region=anomaly.get("region", ""),
                    current_quantity=anomaly.get("current_quantity", 0),
                    seuil_min=anomaly.get("seuil_min", 0),
                    type_anomalie=anomaly.get("type_anomalie", "UNKNOWN"),
                    severite=anomaly.get("severite", "UNKNOWN"),
                )
                try:
                    response = agent_runner.run(user_prompt)
                    logger.info(f"Agent response: {response}")
                except Exception as e:
                    logger.error(f"Decision agent LLM call failed: {e}")

            # Guarantee a task always gets produced, even if the LLM failed
            # or forgot to call its tools.
            if not tools.task_produced:
                logger.warning("No task produced by the agent — using fallback decision (supplier order)")
                decision = fallback_decision(anomaly)
                produce_task(producer, decision)
                tools.last_decision = decision

            # Guarantee an audit trail for every decision (SKILL.md règle: traçabilité obligatoire).
            if not tools.audit_produced:
                produce_audit_log(producer, fallback_audit(anomaly, tools.last_decision or {}))

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

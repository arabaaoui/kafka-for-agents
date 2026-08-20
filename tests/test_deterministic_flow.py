#!/usr/bin/env python3
"""
Test standalone de la logique métier — sans Docker, sans Kafka réel, sans appel LLM.

Usage : python tests/test_deterministic_flow.py

Ce script ne dépend d'aucun fichier .env ni d'aucun service externe (Kafka,
MCP Confluent, providers LLM) : toutes les variables d'environnement
nécessaires sont fixées ci-dessous, et les fonctions testées sont soit pures
(is_anomaly, fallback_decision, deterministic_execute), soit appelées avec un
producer Kafka factice (FakeProducer) qui n'ouvre aucune connexion réseau.

common.share_group_client wraps the native confluent_kafka.ShareConsumer,
which opens real C sockets via librdkafka as soon as it's instantiated — so
Test 6 below patches it with a FakeNativeShareConsumer (in-memory, no I/O)
to exercise ShareGroupClient's poll()/acknowledge()/commit_sync() mapping
offline.
"""

import json
import os
import sys
from pathlib import Path

# --- Variables d'environnement nécessaires (pas de dépendance à .env) ---
os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "localhost:9092"
os.environ["MCP_CONFLUENT_URL"] = "http://localhost:3000"
os.environ["DETECTION_LLM_PROVIDER"] = "openai"
os.environ["DETECTION_LLM_MODEL"] = "gpt-4o"
os.environ["DETECTION_LLM_API_KEY"] = ""
os.environ["DECISION_LLM_PROVIDER"] = "anthropic"
os.environ["DECISION_LLM_MODEL"] = "claude-sonnet-4-20250514"
os.environ["DECISION_LLM_API_KEY"] = ""
os.environ["EXECUTION_LLM_PROVIDER"] = ""
os.environ["EXECUTION_LLM_MODEL"] = ""
os.environ["EXECUTION_LLM_API_KEY"] = ""
os.environ["SHARE_GROUP_LOCK_DURATION_MS"] = "30000"
os.environ["SHARE_GROUP_DELIVERY_COUNT_LIMIT"] = "5"
os.environ["SIMULATION_SPEED"] = "1.0"

REPO_ROOT = Path(__file__).resolve().parent.parent
AGENTS_DIR = REPO_ROOT / "agents"
os.environ["SKILL_PATH"] = str(REPO_ROOT / "skills" / "supply-chain-replenishment" / "SKILL.md")

# Les modules testés vivent sous agents/ (common, detection, decision, execution)
# et s'importent comme en production (PYTHONPATH=/app dans le Dockerfile).
sys.path.insert(0, str(AGENTS_DIR))

RESULTS: list[tuple[str, bool]] = []


def check(label: str, condition: bool) -> bool:
    """Affiche ✅/❌ pour une assertion individuelle."""
    print(f"  {'✅' if condition else '❌'} {label}")
    return condition


def run_test(name: str, func) -> None:
    print(f"\n=== {name} ===")
    try:
        ok = bool(func())
    except Exception as e:
        print(f"  ❌ Exception non gérée : {e!r}")
        ok = False
    RESULTS.append((name, ok))


# ---------------------------------------------------------------------------
# Test 1 — Imports
# ---------------------------------------------------------------------------

def test_imports() -> bool:
    modules = [
        "common.config",
        "common.share_group_client",
        "common.adk_factory",
        "detection.agent",
        "detection.prompts",
        "decision.agent",
        "decision.prompts",
        "execution.agent",
        "execution.prompts",
    ]
    ok = True
    for mod_name in modules:
        try:
            __import__(mod_name)
            print(f"  ✅ import {mod_name}")
        except Exception as e:
            print(f"  ❌ import {mod_name} — {e}")
            ok = False
    return ok


# ---------------------------------------------------------------------------
# Test 2 — Config
# ---------------------------------------------------------------------------

def test_config() -> bool:
    try:
        from common import config
    except Exception as e:
        print(f"  ❌ Impossible d'importer common.config — {e}")
        return False

    ok = True
    for attr in (
        "DETECTION_LLM_PROVIDER", "DETECTION_LLM_MODEL", "DETECTION_LLM_API_KEY",
        "DECISION_LLM_PROVIDER", "DECISION_LLM_MODEL", "DECISION_LLM_API_KEY",
        "EXECUTION_LLM_PROVIDER", "EXECUTION_LLM_MODEL", "EXECUTION_LLM_API_KEY",
        "TOPIC_ORDERS", "TOPIC_STOCKS", "TOPIC_ANOMALIES", "TOPIC_TASKS", "TOPIC_AUDIT",
    ):
        ok &= check(f"config.{attr} défini", hasattr(config, attr))

    print("\n  Config détectée :")
    print(f"    DETECTION : provider={config.DETECTION_LLM_PROVIDER} model={config.DETECTION_LLM_MODEL} "
          f"api_key={'(vide)' if not config.DETECTION_LLM_API_KEY else '***'}")
    print(f"    DECISION  : provider={config.DECISION_LLM_PROVIDER} model={config.DECISION_LLM_MODEL} "
          f"api_key={'(vide)' if not config.DECISION_LLM_API_KEY else '***'}")
    print(f"    EXECUTION : provider={config.EXECUTION_LLM_PROVIDER or '(aucun)'} model={config.EXECUTION_LLM_MODEL or '(aucun)'} "
          f"api_key={'(vide)' if not config.EXECUTION_LLM_API_KEY else '***'}")
    print(f"    KAFKA_BOOTSTRAP_SERVERS={config.KAFKA_BOOTSTRAP_SERVERS}")
    print(f"    Topics: {config.TOPIC_ORDERS}, {config.TOPIC_STOCKS}, {config.TOPIC_ANOMALIES}, "
          f"{config.TOPIC_TASKS}, {config.TOPIC_AUDIT}")

    return ok


# ---------------------------------------------------------------------------
# Test 3 — Prompts
# ---------------------------------------------------------------------------

def test_prompts() -> bool:
    try:
        from detection.prompts import SYSTEM_PROMPT as DETECTION_SYSTEM_PROMPT, DETECTION_USER_PROMPT
        from decision.prompts import SYSTEM_PROMPT as DECISION_SYSTEM_PROMPT, DECISION_USER_PROMPT
        from execution.prompts import SYSTEM_PROMPT as EXECUTION_SYSTEM_PROMPT, EXECUTION_USER_PROMPT
    except Exception as e:
        print(f"  ❌ Impossible d'importer les prompts — {e}")
        return False

    ok = True
    ok &= check("detection.SYSTEM_PROMPT non vide", bool(DETECTION_SYSTEM_PROMPT.strip()))
    ok &= check("decision.SYSTEM_PROMPT non vide", bool(DECISION_SYSTEM_PROMPT.strip()))
    ok &= check("execution.SYSTEM_PROMPT non vide", bool(EXECUTION_SYSTEM_PROMPT.strip()))

    try:
        formatted = DECISION_SYSTEM_PROMPT.format(skill_content="CONTENU SKILL DE TEST")
        ok &= check("decision.SYSTEM_PROMPT.format(skill_content=...) se formatte", "CONTENU SKILL DE TEST" in formatted)
    except Exception as e:
        ok &= check(f"decision.SYSTEM_PROMPT.format() a levé une exception — {e}", False)

    try:
        DETECTION_USER_PROMPT.format(
            store_id="store-01", product_id="prod-01", quantity=3,
            seuil_min=10, region="idf", timestamp="2026-08-02T00:00:00Z",
        )
        ok &= check("DETECTION_USER_PROMPT.format(...) se formatte", True)
    except Exception as e:
        ok &= check(f"DETECTION_USER_PROMPT.format() a levé une exception — {e}", False)

    try:
        DECISION_USER_PROMPT.format(
            anomalie_id="anom-1", store_id="store-01", product_id="prod-01",
            region="idf", current_quantity=3, seuil_min=10,
            type_anomalie="RUPTURE_IMMINENTE", severite="HAUTE",
        )
        ok &= check("DECISION_USER_PROMPT.format(...) se formatte", True)
    except Exception as e:
        ok &= check(f"DECISION_USER_PROMPT.format() a levé une exception — {e}", False)

    try:
        EXECUTION_USER_PROMPT.format(task_json='{"task_id": "task-1"}')
        ok &= check("EXECUTION_USER_PROMPT.format(...) se formatte", True)
    except Exception as e:
        ok &= check(f"EXECUTION_USER_PROMPT.format() a levé une exception — {e}", False)

    return ok


# ---------------------------------------------------------------------------
# Test 4 — Flow déterministe (Detection → Decision → Execution)
# ---------------------------------------------------------------------------

class FakeProducer:
    """Producer Kafka factice : n'ouvre aucune connexion réseau, enregistre juste les appels."""

    def __init__(self):
        self.produced: list[dict] = []

    def produce(self, topic, key=None, value=None, on_delivery=None):
        self.produced.append({"topic": topic, "key": key, "value": value})
        if on_delivery:
            on_delivery(None, None)

    def flush(self, timeout=None):
        return 0


def test_deterministic_flow() -> bool:
    try:
        from detection.agent import is_anomaly, produce_simple_anomaly
        from decision.agent import fallback_decision
        from execution.agent import deterministic_execute
    except Exception as e:
        print(f"  ❌ Impossible d'importer les fonctions du flow — {e}")
        return False

    ok = True

    # 1. Detection — pré-filtre déterministe
    stock_data = {
        "store_id": "store-42",
        "product_id": "prod-99",
        "quantity": 2,
        "seuil_min": 10,
        "region": "idf",
        "timestamp": "2026-08-02T10:00:00Z",
    }
    ok &= check("is_anomaly(stock sous seuil) == True", is_anomaly(stock_data) is True)
    ok &= check("is_anomaly(stock au-dessus du seuil) == False", is_anomaly(dict(stock_data, quantity=50)) is False)

    # 2. Detection — publication (producer factice, aucun vrai Kafka)
    fake_producer = FakeProducer()
    anomaly = None
    try:
        produce_simple_anomaly(fake_producer, stock_data)
        ok &= check("produce_simple_anomaly() publie exactement un message", len(fake_producer.produced) == 1)
        anomaly = json.loads(fake_producer.produced[0]["value"])
        ok &= check("anomalie générée a un anomalie_id", bool(anomaly.get("anomalie_id")))
        ok &= check("anomalie générée référence le bon store_id", anomaly.get("store_id") == "store-42")
    except Exception as e:
        ok &= check(f"produce_simple_anomaly() a levé une exception — {e}", False)

    # 3. Decision — fallback déterministe (sans LLM)
    anomaly_for_decision = anomaly or {
        "anomalie_id": "anom-test-1",
        "store_id": "store-42",
        "product_id": "prod-99",
        "region": "idf",
        "current_quantity": 2,
        "seuil_min": 10,
        "severite": "HAUTE",
    }
    task = fallback_decision(anomaly_for_decision)
    ok &= check("fallback_decision().action == 'commande_fournisseur'", task.get("action") == "commande_fournisseur")
    ok &= check("fallback_decision().magasin_destination == store_id", task.get("magasin_destination") == "store-42")
    ok &= check("fallback_decision().product_id == product_id", task.get("product_id") == "prod-99")
    ok &= check(
        "fallback_decision().quantite est un nombre positif",
        isinstance(task.get("quantite"), (int, float)) and task.get("quantite") > 0,
    )

    # 4. Execution — exécution déterministe (sans LLM)
    task.setdefault("task_id", "task-test-1")
    result = deterministic_execute(task)
    ok &= check("deterministic_execute().delay_s == 2.0", result.get("delay_s") == 2.0)
    ok &= check("deterministic_execute().success == True", result.get("success") is True)
    ok &= check("deterministic_execute().task_id conservé", result.get("task_id") == task["task_id"])

    return ok


# ---------------------------------------------------------------------------
# Test 5 — ADK factory (sans appel LLM)
# ---------------------------------------------------------------------------

def test_adk_factory() -> bool:
    try:
        from common.adk_factory import create_llm, AdkAgentRunner
    except Exception as e:
        print(f"  ❌ Impossible d'importer common.adk_factory — {e}")
        return False

    ok = True

    # create_llm() doit lever ValueError pour un provider inconnu, sans appeler de LLM
    try:
        create_llm("provider-bidon", "model-bidon", "fake-key")
        ok &= check("create_llm(provider inconnu) lève ValueError", False)
    except ValueError as e:
        ok &= check("create_llm(provider inconnu) lève ValueError('Unknown LLM provider')", "Unknown LLM provider" in str(e))
    except Exception as e:
        ok &= check(f"create_llm(provider inconnu) a levé {type(e).__name__} au lieu de ValueError — {e}", False)

    # AdkAgentRunner appelle create_llm() dès la construction (avant tout appel réseau),
    # donc l'erreur doit remonter à l'instanciation, sans jamais contacter de LLM.
    try:
        AdkAgentRunner(
            name="test-agent",
            description="agent de test",
            instruction="instruction de test",
            tools=[],
            provider="provider-bidon",
            model="model-bidon",
            api_key="fake-key",
        )
        ok &= check("AdkAgentRunner(provider inconnu) lève une erreur", False)
    except ValueError as e:
        ok &= check("AdkAgentRunner(provider inconnu) lève ValueError('Unknown LLM provider')", "Unknown LLM provider" in str(e))
    except Exception as e:
        ok &= check(f"AdkAgentRunner(provider inconnu) a levé {type(e).__name__} au lieu de ValueError — {e}", False)

    return ok


# ---------------------------------------------------------------------------
# Test 6 — ShareGroupClient (native ShareConsumer mocked, no real sockets)
# ---------------------------------------------------------------------------

class FakeMessage:
    """Stand-in for confluent_kafka.Message — a plain record, no C object."""

    def __init__(self, topic, partition, offset, key, value, delivery_count=1):
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._key = key
        self._value = value
        self._delivery_count = delivery_count

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def key(self):
        return self._key.encode("utf-8") if self._key else None

    def value(self):
        return self._value.encode("utf-8") if self._value else None

    def error(self):
        return None

    def delivery_count(self):
        return self._delivery_count


class FakeMessages(list):
    """Stand-in for confluent_kafka.Messages — the batch container returned by poll()."""

    def is_empty(self):
        return len(self) == 0


class FakeNativeShareConsumer:
    """Stand-in for confluent_kafka.ShareConsumer — records calls, opens no sockets."""

    def __init__(self, conf):
        self.conf = conf
        self.subscribed: list[str] = []
        self.acknowledged: list[tuple] = []
        self.commits = 0
        self.closed = False
        self._next_batch = FakeMessages(
            [FakeMessage("tasks", 0, 42, "store-42", '{"task_id": "task-1"}', delivery_count=1)]
        )

    def subscribe(self, topics):
        self.subscribed = list(topics)

    def poll(self, timeout):
        batch, self._next_batch = self._next_batch, FakeMessages([])
        return batch

    def acknowledge(self, message, ack_type):
        self.acknowledged.append((message, ack_type))

    def commit_sync(self):
        self.commits += 1

    def close(self):
        self.closed = True


def test_share_group_client() -> bool:
    try:
        from unittest.mock import patch
        from confluent_kafka import AcknowledgeType as NativeAckType
        from common import share_group_client as sgc
    except Exception as e:
        print(f"  ❌ Impossible d'importer common.share_group_client — {e}")
        return False

    ok = True
    fake_consumer = FakeNativeShareConsumer({})

    with patch.object(sgc, "NativeShareConsumer", return_value=fake_consumer):
        client = sgc.ShareGroupClient(group_id="test-group", topics=["tasks"], consumer_id="test-consumer")

        client.start()
        ok &= check("start() souscrit au bon topic (via le ShareConsumer natif mocké)", fake_consumer.subscribed == ["tasks"])

        messages = client.poll(timeout=1.0)
        ok &= check("poll() retourne exactement 1 message", len(messages) == 1)
        if not messages:
            return False
        msg = messages[0]
        ok &= check("poll() décode value en str", msg.value == '{"task_id": "task-1"}')
        ok &= check("poll() expose delivery_count()", msg.delivery_count == 1)

        # Second poll returns an empty batch — mirrors the real broker with nothing left to acquire.
        ok &= check("poll() suivant (batch vide) renvoie []", client.poll(timeout=1.0) == [])

        client.acknowledge(msg, sgc.AcknowledgeType.ACK)
        ok &= check("acknowledge(ACK) mappe vers AcknowledgeType.ACCEPT", fake_consumer.acknowledged[-1][1] == NativeAckType.ACCEPT)

        client.acknowledge(msg, sgc.AcknowledgeType.RELEASE)
        ok &= check("acknowledge(RELEASE) mappe vers AcknowledgeType.RELEASE", fake_consumer.acknowledged[-1][1] == NativeAckType.RELEASE)

        try:
            client.acknowledge(msg, sgc.AcknowledgeType.RENEW)
            ok &= check("acknowledge(RENEW) lève NotImplementedError (non supporté côté client Python)", False)
        except NotImplementedError:
            ok &= check("acknowledge(RENEW) lève NotImplementedError (non supporté côté client Python)", True)

        client.commit_sync()
        ok &= check("commit_sync() délègue au ShareConsumer natif", fake_consumer.commits == 1)

        stats = client.get_stats()
        ok &= check("get_stats() comptabilise 1 ACK et 1 RELEASE", stats.get("acked") == 1 and stats.get("released") == 1)

        client.stop()
        ok &= check("stop() ferme le ShareConsumer natif", fake_consumer.closed is True)

    return ok


# ---------------------------------------------------------------------------
# Résultat final
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 70)
    print("TEST DÉTERMINISTE — flow métier sans Docker, sans Kafka, sans LLM")
    print("=" * 70)

    run_test("Test 1 — Imports", test_imports)
    run_test("Test 2 — Configuration", test_config)
    run_test("Test 3 — Prompts", test_prompts)
    run_test("Test 4 — Flow déterministe (Detection → Decision → Execution)", test_deterministic_flow)
    run_test("Test 5 — ADK factory (sans appel LLM)", test_adk_factory)
    run_test("Test 6 — ShareGroupClient (ShareConsumer natif mocké)", test_share_group_client)

    print("\n" + "=" * 70)
    print("RÉSUMÉ")
    print("=" * 70)
    passed = 0
    for name, ok in RESULTS:
        print(f"  {'✅' if ok else '❌'} {name}")
        passed += ok

    total = len(RESULTS)
    print(f"\nRésultat : {passed}/{total} tests passés")

    sys.exit(0 if passed == total else 1)


if __name__ == "__main__":
    main()

"""
Execution Agent — Polls the 'tasks' topic via ShareGroupClient and executes tasks.

Uses KIP-932 share group semantics: messages are locked when acquired,
acknowledged on success, and automatically retried on failure (lock expiry).

The execution LLM is OPTIONAL. If EXECUTION_LLM_API_KEY is empty, no ADK agent
is ever instantiated: execution is fully deterministic — a fixed 2s delay and
100% success, no randomness — so the pipeline stays runnable without paying
for a third LLM provider. When an execution LLM IS configured, a real ADK
agent decides which tool to call (execute_transfer / execute_order), which
simulate the real operation with a variable delay and occasional failure.
"""

import json
import logging
import os
import random
import signal
import time
from datetime import datetime, timezone

from common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_TASKS,
    SHARE_GROUP_LOCK_DURATION_MS,
    SHARE_GROUP_MAX_DELIVERY_ATTEMPTS,
    EXECUTION_LLM_PROVIDER,
    EXECUTION_LLM_MODEL,
    EXECUTION_LLM_API_KEY,
)
from common.share_group_client import ShareGroupClient, AcknowledgeType
from common.adk_factory import AdkAgentRunner
from execution.prompts import SYSTEM_PROMPT, EXECUTION_USER_PROMPT

# Logging with [EXECUTION] prefix
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [EXECUTION] %(levelname)s %(message)s",
)
logger = logging.getLogger("execution")

# Signal for graceful shutdown
shutdown_event = False


def handle_sigterm(signum, frame):
    """Handle SIGTERM for graceful shutdown."""
    global shutdown_event
    logger.info("SIGTERM received, shutting down gracefully...")
    shutdown_event = True


signal.signal(signal.SIGTERM, handle_sigterm)
signal.signal(signal.SIGINT, handle_sigterm)


def deterministic_execute(task: dict) -> dict:
    """No-LLM execution path: fixed 2s delay, always succeeds — no randomness."""
    logger.info(f"Executing task {task.get('task_id')} deterministically (no LLM configured): action={task.get('action')}")
    time.sleep(2.0)
    return {
        "task_id": task.get("task_id"),
        "action": task.get("action"),
        "delay_s": 2.0,
        "success": True,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }


class ExecutionTools:
    """Tools exposed to the execution ADK agent (only used when an LLM is configured)."""

    def __init__(self):
        self.last_result: dict | None = None

    def execute_transfer(self, task_json: str) -> dict:
        """Execute an internal store-to-store transfer task. Simulates the logistics operation."""
        return self._simulate(task_json)

    def execute_order(self, task_json: str) -> dict:
        """Execute a supplier order task. Simulates placing and confirming the order."""
        return self._simulate(task_json)

    def _simulate(self, task_json: str) -> dict:
        task = json.loads(task_json) if isinstance(task_json, str) else task_json
        delay = random.uniform(1, 10)
        logger.info(f"Executing task {task.get('task_id')}: action={task.get('action')} delay={delay:.1f}s")
        time.sleep(delay)
        success = random.random() > 0.10
        result = {
            "task_id": task.get("task_id"),
            "action": task.get("action"),
            "delay_s": round(delay, 2),
            "success": success,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
        self.last_result = result
        return result


def run_with_llm(agent_runner: AdkAgentRunner, tools: ExecutionTools, task: dict) -> dict:
    """Delegate execution to the ADK agent. Falls back to deterministic execution on any failure."""
    tools.last_result = None
    task_json = json.dumps(task, ensure_ascii=False)
    user_prompt = EXECUTION_USER_PROMPT.format(task_json=task_json)
    try:
        response = agent_runner.run(user_prompt)
        logger.info(f"Agent response: {response}")
    except Exception as e:
        logger.error(f"Execution LLM call failed for {task.get('task_id')}: {e} — falling back to deterministic execution")
        return deterministic_execute(task)

    if tools.last_result is not None:
        return tools.last_result

    logger.warning(f"Execution agent did not call an execution tool for {task.get('task_id')} — falling back")
    return deterministic_execute(task)


def process_task(client: ShareGroupClient, task_msg, agent_runner: AdkAgentRunner | None, tools: ExecutionTools | None) -> bool:
    """
    Process a single task from the share group.
    Returns True if the message was acknowledged, False if it should be retried.
    """
    task_id = "unknown"

    try:
        value = task_msg.value
        if value is None:
            logger.warning("Received task with no value — acknowledging")
            client.acknowledge(task_msg, AcknowledgeType.ACK)
            return True

        task = json.loads(value) if isinstance(value, str) else value
        task_id = task.get("task_id", "unknown")

        logger.info(
            f"Task acquired: {task_id} "
            f"action={task.get('action')} "
            f"product={task.get('product_id')} "
            f"qty={task.get('quantite')}"
        )

        # Renew the lock immediately: execution duration is unpredictable when
        # an LLM is in the loop, unlike a fixed deterministic delay.
        client.acknowledge(task_msg, AcknowledgeType.RENEW)

        if agent_runner is not None:
            result = run_with_llm(agent_runner, tools, task)
        else:
            result = deterministic_execute(task)

        if result.get("success"):
            client.acknowledge(task_msg, AcknowledgeType.ACK)
            logger.info(f"Task {task_id}: ACK — completed successfully ({result.get('delay_s')}s)")
            return True
        else:
            logger.warning(
                f"Task {task_id}: FAILED — not acknowledging, "
                f"lock will expire and message will be retried"
            )
            return False

    except json.JSONDecodeError as e:
        logger.error(f"Task {task_id}: invalid JSON — {e}")
        client.acknowledge(task_msg, AcknowledgeType.ACK)
        return True
    except Exception as e:
        logger.error(f"Task {task_id}: unexpected error — {e}")
        return False


def main():
    """Main loop — polls 'tasks' via share group, executes, acknowledges."""
    group_id = os.getenv("SHARE_GROUP", "execution-group")
    consumer_id = f"executor-{os.getpid()}"

    client = ShareGroupClient(
        group_id=group_id,
        topics=[TOPIC_TASKS],
        consumer_id=consumer_id,
        lock_duration_ms=SHARE_GROUP_LOCK_DURATION_MS,
        max_delivery_attempts=SHARE_GROUP_MAX_DELIVERY_ATTEMPTS,
    )

    agent_runner = None
    tools = None
    if EXECUTION_LLM_API_KEY:
        tools = ExecutionTools()
        agent_runner = AdkAgentRunner(
            name="execution-agent",
            description="Exécute les tâches de transfert interne et de commande fournisseur.",
            instruction=SYSTEM_PROMPT,
            tools=[tools.execute_transfer, tools.execute_order],
            provider=EXECUTION_LLM_PROVIDER,
            model=EXECUTION_LLM_MODEL,
            api_key=EXECUTION_LLM_API_KEY,
        )
        logger.info(f"LLM: provider={EXECUTION_LLM_PROVIDER} model={EXECUTION_LLM_MODEL}")
    else:
        logger.info("No EXECUTION_LLM_API_KEY configured — running fully deterministic (fixed 2s delay, 100% success)")

    client.start()
    logger.info(f"Execution agent started: consumer_id={consumer_id} group={group_id}")

    try:
        while not shutdown_event:
            messages = client.poll(timeout=1.0)

            for msg in messages:
                process_task(client, msg, agent_runner, tools)

            time.sleep(0.5)

    except Exception as e:
        logger.exception(f"Fatal error in execution loop: {e}")
    finally:
        logger.info("Closing execution agent...")
        client.stop()
        logger.info("Execution agent stopped.")


if __name__ == "__main__":
    main()

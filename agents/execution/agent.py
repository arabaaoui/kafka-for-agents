"""
Execution Agent — Polls the 'tasks' topic via ShareGroupClient and executes tasks.

Uses KIP-932 share group semantics: messages are locked when acquired,
acknowledged on success, and automatically retried on failure (lock expiry).
Simulates execution with a random delay of 1-10 seconds.
"""

import json
import logging
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone

from common.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_TASKS,
    SHARE_GROUP_LOCK_DURATION_MS,
    SHARE_GROUP_MAX_DELIVERY_ATTEMPTS,
)
from common.share_group_client import ShareGroupClient, AcknowledgeType

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


def simulate_execution(task: dict) -> dict:
    """
    Simulate the execution of a task with a random delay.
    Returns a result dict with status and timing info.
    """
    delay = random.uniform(1, 10)
    logger.info(
        f"Executing task {task.get('task_id')}: action={task.get('action')} "
        f"delay={delay:.1f}s"
    )

    time.sleep(delay)

    # Simulate occasional failures (10% chance)
    success = random.random() > 0.10

    result = {
        "task_id": task.get("task_id"),
        "action": task.get("action"),
        "delay_s": round(delay, 2),
        "success": success,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }

    if success:
        logger.info(f"Task {task.get('task_id')} completed successfully ({delay:.1f}s)")
    else:
        logger.warning(f"Task {task.get('task_id')} failed — will be retried")

    return result


def process_task(client: ShareGroupClient, task_msg) -> bool:
    """
    Process a single task from the share group.
    Returns True if the message was acknowledged, False if it should be retried.
    """
    task_id = "unknown"

    try:
        # Parse the task payload
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

        # Simulate execution
        delay = random.uniform(1, 10)

        # If delay > 5s, renew the lock to prevent expiry during execution
        if delay > 5.0:
            logger.info(f"Task {task_id}: long execution ({delay:.1f}s), renewing lock")
            client.acknowledge(task_msg, AcknowledgeType.RENEW)

        # Execute (sleep to simulate)
        time.sleep(delay)

        # Determine success (10% failure rate)
        success = random.random() > 0.10

        if success:
            client.acknowledge(task_msg, AcknowledgeType.ACK)
            logger.info(f"Task {task_id}: ACK — completed successfully ({delay:.1f}s)")
            return True
        else:
            # Don't acknowledge — lock will expire and message will be retried
            logger.warning(
                f"Task {task_id}: FAILED — not acknowledging, "
                f"lock will expire and message will be retried"
            )
            return False

    except json.JSONDecodeError as e:
        logger.error(f"Task {task_id}: invalid JSON — {e}")
        # Acknowledge bad messages to avoid poison-pill loops
        client.acknowledge(task_msg, AcknowledgeType.ACK)
        return True
    except Exception as e:
        logger.error(f"Task {task_id}: unexpected error — {e}")
        # Don't acknowledge — retry
        return False


def main():
    """Main loop — polls 'tasks' via share group, executes, acknowledges."""
    global shutdown_event

    # Create share group client
    group_id = os.getenv("SHARE_GROUP", "execution-group")
    consumer_id = f"executor-{os.getpid()}"

    client = ShareGroupClient(
        group_id=group_id,
        topics=[TOPIC_TASKS],
        consumer_id=consumer_id,
        lock_duration_ms=SHARE_GROUP_LOCK_DURATION_MS,
        max_delivery_attempts=SHARE_GROUP_MAX_DELIVERY_ATTEMPTS,
    )

    client.start()
    logger.info(f"Execution agent started: consumer_id={consumer_id} group={group_id}")

    try:
        while not shutdown_event:
            # Poll for available messages
            messages = client.poll(timeout=1.0)

            for msg in messages:
                process_task(client, msg)

            # Small sleep to avoid busy-looping
            time.sleep(0.5)

    except Exception as e:
        logger.exception(f"Fatal error in execution loop: {e}")
    finally:
        logger.info("Closing execution agent...")
        client.stop()
        logger.info("Execution agent stopped.")


if __name__ == "__main__":
    main()
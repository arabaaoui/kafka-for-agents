"""
Configuration loader for all agents.
Reads from environment variables (set via Docker Compose or .env).
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env if present (local dev)
_env_path = Path(__file__).resolve().parent.parent.parent / ".env"
if _env_path.exists():
    load_dotenv(_env_path)

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# MCP Confluent
MCP_CONFLUENT_URL = os.getenv("MCP_CONFLUENT_URL", "http://mcp-confluent:3000")

# --- LLM config, one block per agent ---
# Each agent can run a different provider/model. Supported providers: openai, anthropic, gemini
# (see agents/common/adk_factory.py — all providers route through LiteLLM).

DETECTION_LLM_PROVIDER = os.getenv("DETECTION_LLM_PROVIDER", "openai")
DETECTION_LLM_MODEL = os.getenv("DETECTION_LLM_MODEL", "gpt-4o")
DETECTION_LLM_API_KEY = os.getenv("DETECTION_LLM_API_KEY", "")

DECISION_LLM_PROVIDER = os.getenv("DECISION_LLM_PROVIDER", "anthropic")
DECISION_LLM_MODEL = os.getenv("DECISION_LLM_MODEL", "claude-sonnet-4-20250514")
DECISION_LLM_API_KEY = os.getenv("DECISION_LLM_API_KEY", "")

# Execution LLM is optional: if EXECUTION_LLM_API_KEY is empty, the execution
# agent runs fully deterministic (no ADK agent instantiated at all).
EXECUTION_LLM_PROVIDER = os.getenv("EXECUTION_LLM_PROVIDER", "")
EXECUTION_LLM_MODEL = os.getenv("EXECUTION_LLM_MODEL", "")
EXECUTION_LLM_API_KEY = os.getenv("EXECUTION_LLM_API_KEY", "")

# Share Group
SHARE_GROUP_LOCK_DURATION_MS = int(os.getenv("SHARE_GROUP_LOCK_DURATION_MS", "30000"))
SHARE_GROUP_MAX_DELIVERY_ATTEMPTS = int(os.getenv("SHARE_GROUP_MAX_DELIVERY_ATTEMPTS", "5"))

# Simulation
SIMULATION_SPEED = float(os.getenv("SIMULATION_SPEED", "1.0"))

# Detection threshold — ratio applied to each product's seuil_min (1.0 = exact threshold)
STOCK_ALERT_THRESHOLD_RATIO = float(os.getenv("STOCK_ALERT_THRESHOLD_RATIO", "1.0"))

# Skill path
SKILL_PATH = os.getenv("SKILL_PATH", "/app/skills/supply-chain-replenishment/SKILL.md")

# Topic names
TOPIC_ORDERS = "orders"
TOPIC_STOCKS = "stocks"
TOPIC_ANOMALIES = "anomalies"
TOPIC_TASKS = "tasks"
TOPIC_AUDIT = "audit"


def validate() -> list[str]:
    """Validate required config. Returns list of missing items."""
    missing = []
    if not DETECTION_LLM_API_KEY:
        missing.append("DETECTION_LLM_API_KEY (set in .env or environment)")
    if not DECISION_LLM_API_KEY:
        missing.append("DECISION_LLM_API_KEY (set in .env or environment)")
    # EXECUTION_LLM_API_KEY is optional — empty means deterministic execution agent.
    if not KAFKA_BOOTSTRAP_SERVERS:
        missing.append("KAFKA_BOOTSTRAP_SERVERS")
    return missing

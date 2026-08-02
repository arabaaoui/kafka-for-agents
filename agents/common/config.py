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

# LLM
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "anthropic")
LLM_MODEL = os.getenv("LLM_MODEL", "claude-sonnet-4-20250514")
LLM_API_KEY = os.getenv("LLM_API_KEY", "")

# Share Group
SHARE_GROUP_LOCK_DURATION_MS = int(os.getenv("SHARE_GROUP_LOCK_DURATION_MS", "30000"))
SHARE_GROUP_MAX_DELIVERY_ATTEMPTS = int(os.getenv("SHARE_GROUP_MAX_DELIVERY_ATTEMPTS", "5"))

# Simulation
SIMULATION_SPEED = float(os.getenv("SIMULATION_SPEED", "1.0"))

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
    if not LLM_API_KEY:
        missing.append("LLM_API_KEY (set in .env or environment)")
    if not KAFKA_BOOTSTRAP_SERVERS:
        missing.append("KAFKA_BOOTSTRAP_SERVERS")
    return missing
# Kafka Retail Agents PoC — Kafka as an Agent-Native Platform

[![Kafka](https://img.shields.io/badge/Kafka-4.2.1-231F20?style=flat-square&logo=apache-kafka)](https://kafka.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose%20v2-2496ED?style=flat-square&logo=docker)](https://docs.docker.com/compose/)

A proof-of-concept demonstrating how **Apache Kafka can serve as a native platform for AI agents** — enabling reliable, observable, and scalable multi-agent architectures for real-world business use cases like retail operations.

> Article: [Kafka as an Agent-Native Platform — blog.dolizone.com](https://blog.dolizone.com)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         RETAIL PIPELINE                                 │
│                                                                         │
│   ┌──────────────┐                                                      │
│   │  SIMULATOR   │  Publishes fake orders (retail events)               │
│   └──────┬───────┘                                                      │
│          │                                                              │
│          ▼                                                              │
│   ┌──────────────┐     ┌──────────────┐     ┌──────────────┐           │
│   │  DETECTION   │────▶│  DECISION    │────▶│  EXECUTION   │           │
│   │  AGENT       │     │  AGENT       │     │  AGENT ×5    │           │
│   │              │     │              │     │ (KIP-932)    │           │
│   └──────┬───────┘     └──────┬───────┘     └──────┬───────┘           │
│          │                    │                    │                    │
│   ┌──────▼────────────────────▼────────────────────▼───────┐           │
│   │                     APACHE KAFKA 4.2.1                  │           │
│   │  ┌────────┐ ┌──────────┐ ┌───────────┐ ┌─────────┐   │           │
│   │  │ orders │ │ anomalies │ │   tasks   │ │  audit  │   │           │
│   │  └────────┘ └──────────┘ └───────────┘ └─────────┘   │           │
│   │  ┌────────┐                                            │           │
│   │  │ stocks │  (reference data)                          │           │
│   │  └────────┘                                            │           │
│   └────────────────────────────────────────────────────────┘           │
│                                                                         │
│   ┌──────────────┐     ┌──────────────┐                                │
│   │  MCP         │     │  KAFKA UI    │                                │
│   │  CONFLUENT   │     │  :8080       │                                │
│   │  :3000       │     │              │                                │
│   └──────────────┘     └──────────────┘                                │
└─────────────────────────────────────────────────────────────────────────┘
```

**Pipeline flow:**

1. **Simulator** → publishes synthetic orders to `orders` topic
2. **Detection Agent** → consumes `orders`, detects anomalies, publishes to `anomalies`
3. **Decision Agent** → reads `anomalies` + `stocks`, applies business logic via `SKILL.md`, writes `tasks`
4. **Execution Agent ×N** → cooperative consumption of `tasks` (KIP-932 Share Groups), writes results to `audit`

---

## The 3 Pillars

This PoC is built around three key Kafka capabilities that make it an ideal platform for agent-native architectures:

### 1. MCP Confluent — LLMs Query Kafka Natively

The [MCP Confluent](https://github.com/confluentinc/mcp-confluent) bridge exposes Kafka as a tool that LLMs can call directly. Instead of custom API layers, agents use the same Kafka protocol to read/write topics — enabling:

- **Native integration**: LLMs discover topics, schemas, and messages through standard Kafka tooling
- **Zero-copy data access**: Agents consume/produce directly on Kafka, no intermediate services
- **Schema-aware queries**: Avro/Protobuf schemas provide type-safe interactions

### 2. Agent Skills — Business Logic via SKILL.md

The Decision Agent's behavior is driven by a plain-text `SKILL.md` file — a declarative specification of business rules:

- **Hot-reloadable**: Update `SKILL.md` and the agent adapts without redeployment
- **Human-readable**: Business stakeholders can audit and modify agent behavior
- **Version-controlled**: Skills live in Git alongside code — full change history and review

### 3. KIP-932 Share Groups — Cooperative Consumption with ACK/Retry

Execution agents use [KIP-932](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka) share groups:

- **Cooperative consumption**: Tasks are load-balanced across all agents — each task delivered to exactly one agent
- **ACK-based delivery**: Tasks are only marked complete when the agent acknowledges — no data loss on crashes
- **Auto-reassignment**: If an agent dies, its unacknowledged tasks are automatically reassigned to healthy agents
- **Linear scalability**: `docker compose up -d --scale execution-agent=N` — add capacity instantly

---

## Quickstart

### Prerequisites

- Docker & Docker Compose v2
- An LLM API key (OpenAI-compatible endpoint)

### Setup

```bash
# 1. Clone the repository
git clone <repo-url> kafka-retail-agents-poc
cd kafka-retail-agents-poc

# 2. Create your environment file
cp .env.example .env

# 3. Edit .env — set your LLM API key and endpoint
#    Required: LLM_API_KEY=sk-...
#    Optional: LLM_BASE_URL (default: https://api.openai.com/v1)
#    Optional: LLM_MODEL (default: gpt-4o-mini)

vim .env

# 4. Start the full pipeline
docker compose up -d
```

The pipeline is now running! Visit [Kafka UI](http://localhost:8080) to explore topics and messages in real time.

---

## Demo Scripts

Three self-contained demo scripts showcase the platform's capabilities:

### Demo 1 — Full Pipeline

```bash
./scripts/demo-1-full-pipeline.sh
```

Starts all services, waits for Kafka initialization, and shows the complete 3-agent pipeline flowing:

- Simulator generates orders
- Detection agent finds anomalies
- Decision agent creates tasks via SKILL.md
- Execution agent processes tasks into audit records

### Demo 2 — Horizontal Scaling

```bash
./scripts/demo-2-scale.sh
```

Demonstrates KIP-932 Share Group scaling:

- Shows the single execution agent running
- Scales to 5 agents: `docker compose up -d --scale execution-agent=5`
- Watches logs showing cooperative task consumption across all 5 agents

### Demo 3 — Crash Recovery

```bash
./scripts/demo-3-crash.sh
```

Demonstrates automatic failure recovery:

- Lists running execution agents
- Force-kills one container (`docker kill`)
- Shows unacknowledged tasks being automatically reassigned to remaining agents
- Pipeline continues without data loss or manual intervention

---

## Project Structure

```
kafka-retail-agents-poc/
├── .env.example              # Environment variable template
├── .gitignore
├── README.md                 # This file
├── docker-compose.yml        # Complete stack definition
├── scripts/
│   ├── demo-1-full-pipeline.sh   # Full pipeline startup demo
│   ├── demo-2-scale.sh           # Horizontal scaling demo
│   └── demo-3-crash.sh           # Crash recovery demo
├── kafka-init/
│   └── create-topics.sh      # Topic creation on startup
├── simulator/
│   ├── Dockerfile
│   └── main.py               # Synthetic order generator
├── detection-agent/
│   ├── Dockerfile
│   └── agent.py              # Anomaly detection logic
├── decision-agent/
│   ├── Dockerfile
│   ├── agent.py              # Decision logic engine
│   └── SKILL.md              # Business rules (Agent Skill)
├── execution-agent/
│   ├── Dockerfile
│   └── agent.py              # Task executor (Share Group consumer)
├── mcp-confluent/
│   └── ...                   # MCP Confluent bridge config
└── kafka-ui/
    └── ...                   # Kafka UI configuration
```

---

## Configuration Reference

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `LLM_API_KEY` | **Yes** | — | OpenAI-compatible API key |
| `LLM_BASE_URL` | No | `https://api.openai.com/v1` | API base URL |
| `LLM_MODEL` | No | `gpt-4o-mini` | Model name |
| `KAFKA_BOOTSTRAP_SERVERS` | No | `kafka:9092` | Kafka broker address |
| `SHARE_GROUP` | No | `execution-group` | KIP-932 share group name |
| `LOG_LEVEL` | No | `INFO` | Logging verbosity |

---

## Requirements

- **Docker** 24+ (with Docker Compose v2)
- **Python 3.11** (for local development; not required for Docker-only usage)
- **LLM API key** (OpenAI, or any OpenAI-compatible provider via `LLM_BASE_URL`)
- **~4 GB RAM** available for the full stack

---

## License

MIT
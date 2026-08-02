#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Demo 1: Full Pipeline Startup
# Starts all services and shows the complete agent pipeline in action.
# =============================================================================

# -----------------------------------------------------------------------------
# Step 1: Bring up all services
# -----------------------------------------------------------------------------
echo ""
echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║  STEP 1: Starting all services (docker compose up -d)                       ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""

docker compose up -d

echo ""
echo "  ✅ All services started."

# -----------------------------------------------------------------------------
# Step 2: Wait for Kafka to be ready
# -----------------------------------------------------------------------------
echo ""
echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║  STEP 2: Waiting 30s for Kafka broker to initialize...                      ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""

for i in $(seq 30 -1 1); do
  printf "\r  ⏳ %2d seconds remaining..." "$i"
  sleep 1
done
echo ""
echo ""
echo "  ✅ Kafka should be ready now."

# -----------------------------------------------------------------------------
# Step 3: Show running services
# -----------------------------------------------------------------------------
echo ""
echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║  STEP 3: Running services                                                  ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""

docker compose ps

# -----------------------------------------------------------------------------
# Step 4: Follow logs for 20s (full pipeline)
# -----------------------------------------------------------------------------
echo ""
echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║  STEP 4: Watching pipeline logs for 20 seconds...                          ║"
echo "║           Look for: [SIMULATOR] → [DETECTION] → [DECISION] → [EXECUTION]   ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""

timeout 20s docker compose logs -f --tail=50 || true

# -----------------------------------------------------------------------------
# Step 5: Instructions
# -----------------------------------------------------------------------------
echo ""
echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║  🎉 DEMO 1 COMPLETE!                                                       ║"
echo "╠══════════════════════════════════════════════════════════════════════════════╣"
echo "║                                                                              ║"
echo "║  What you just saw:                                                         ║"
echo "║    1. All services started (Kafka, agents, simulator, MCP, Kafka-UI)       ║"
echo "║    2. Simulator publishes stock levels → stocks topic                      ║"
echo "║    3. Detection agent (real google-adk Agent) qualifies anomalies          ║"
echo "║    4. Decision agent (real google-adk Agent) reads anomalies → writes      ║"
echo "║       tasks by applying SKILL.md as its instruction                        ║"
echo "║    5. Execution agent picks up tasks → executes → writes audit             ║"
echo "║                                                                              ║"
echo "║  Each agent can run a DIFFERENT LLM provider (openai/anthropic/gemini),    ║"
echo "║  all routed through LiteLLM — see DETECTION_LLM_* / DECISION_LLM_* /       ║"
echo "║  EXECUTION_LLM_* in .env. Execution's LLM is optional: empty API key =     ║"
echo "║  deterministic execution (fixed 2s delay, 100% success, no LLM call).      ║"
echo "║                                                                              ║"
echo "║  Useful commands:                                                           ║"
echo "║    docker compose ps                — list all services                     ║"
echo "║    docker compose logs -f           — follow all logs                       ║"
echo "║    docker compose logs execution-agent -f  — watch execution agent only    ║"
echo "║                                                                              ║"
echo "║  Web UI:                                                                   ║"
echo "║    Kafka UI → http://localhost:8080                                         ║"
echo "║    MCP Confluent → http://localhost:3000                                    ║"
echo "║                                                                              ║"
echo "║  Next demos:                                                                ║"
echo "║    ./scripts/demo-2-scale.sh        — scale execution agents               ║"
echo "║    ./scripts/demo-3-crash.sh        — demonstrate crash recovery           ║"
echo "║                                                                              ║"
echo "║  Stop everything:                                                           ║"
echo "║    docker compose down                                                      ║"
echo "║                                                                              ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""
#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Demo 2: Horizontal Scaling
# Scales execution-agent from 1 → 5 instances using KIP-932 Share Groups.
# Assumes the full pipeline is already running (demo-1).
# =============================================================================

# -----------------------------------------------------------------------------
# Step 1: Show current execution-agent status
# -----------------------------------------------------------------------------
echo ""
echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║  STEP 1: Current execution-agent containers                                ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""

docker compose ps execution-agent

echo ""
echo "  📊 You should see 1 execution-agent running."

# -----------------------------------------------------------------------------
# Step 2: Scale to 5 execution agents
# -----------------------------------------------------------------------------
echo ""
echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║  STEP 2: Scaling execution-agent to 5 instances                            ║"
echo "║           (docker compose up -d --scale execution-agent=5)                  ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""

docker compose up -d --scale execution-agent=5

echo ""
echo "  ✅ Scaling command executed."

# -----------------------------------------------------------------------------
# Step 3: Wait a moment for new instances to start
# -----------------------------------------------------------------------------
echo ""
echo "  ⏳ Waiting 5s for new instances to join the share group..."
sleep 5
echo "  ✅ Done."

# -----------------------------------------------------------------------------
# Step 4: Show scaled execution-agent containers
# -----------------------------------------------------------------------------
echo ""
echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║  STEP 3: Scaled execution-agent containers (now ×5)                        ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""

docker compose ps execution-agent

echo ""
echo "  📊 You should now see 5 execution-agent containers, all sharing the"
echo "     execution-group via KIP-932 cooperative consumption."

# -----------------------------------------------------------------------------
# Step 5: Follow execution-agent logs for 15s
# -----------------------------------------------------------------------------
echo ""
echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║  STEP 4: Watching execution-agent logs for 15 seconds...                   ║"
echo "║           Each agent picks unique tasks — no overlap (Share Groups!)       ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""

timeout 15s docker compose logs execution-agent -f --tail=30 || true

# -----------------------------------------------------------------------------
# Step 6: Instructions
# -----------------------------------------------------------------------------
echo ""
echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║  🎉 DEMO 2 COMPLETE!                                                       ║"
echo "╠══════════════════════════════════════════════════════════════════════════════╣"
echo "║                                                                              ║"
echo "║  What you just saw:                                                         ║"
echo "║    1. Single execution agent running initially                              ║"
echo "║    2. Scaled to 5 agents with a single command                              ║"
echo "║    3. All 5 agents consume tasks cooperatively (KIP-932 Share Groups)      ║"
echo "║    4. Each task is delivered to exactly ONE agent — no duplication         ║"
echo "║                                                                              ║"
echo "║  Why this matters:                                                          ║"
echo "║    • Linear scalability — add agents as workload grows                     ║"
echo "║    • Cooperative consumption — tasks are load-balanced across agents       ║"
echo "║    • ACK-based delivery — tasks are not lost if an agent crashes           ║"
echo "║                                                                              ║"
echo "║  Scale up/down on the fly:                                                  ║"
echo "║    docker compose up -d --scale execution-agent=10                          ║"
echo "║    docker compose up -d --scale execution-agent=2                           ║"
echo "║                                                                              ║"
echo "║  Next demo:                                                                 ║"
echo "║    ./scripts/demo-3-crash.sh        — crash recovery demo                  ║"
echo "║                                                                              ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""
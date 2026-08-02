#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Demo 3: Crash Recovery
# Kills an execution-agent container and demonstrates KIP-932 automatic
# task reassignment via cooperative consumption with ACK/retry.
# Assumes the full pipeline is already running (demo-1, optionally demo-2).
# =============================================================================

# -----------------------------------------------------------------------------
# Step 1: Show current execution-agent containers
# -----------------------------------------------------------------------------
echo ""
echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║  STEP 1: Current execution-agent containers                                ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""

docker compose ps execution-agent

# -----------------------------------------------------------------------------
# Step 2: Pick and kill one execution-agent container
# -----------------------------------------------------------------------------
echo ""
echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║  STEP 2: Selecting one execution-agent to kill...                          ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""

# Get the first execution-agent container ID
VICTIM=$(docker compose ps -q execution-agent | head -1)

if [ -z "$VICTIM" ]; then
  echo "  ❌ ERROR: No execution-agent containers found."
  echo "     Make sure the pipeline is running (run demo-1 first)."
  exit 1
fi

VICTIM_NAME=$(docker inspect --format '{{.Name}}' "$VICTIM" | sed 's|^/||')
echo ""
echo "  🎯 Killing container: $VICTIM_NAME ($VICTIM)"
echo ""

docker kill "$VICTIM"

echo ""
echo "  💥 Container killed! Tasks that were in-flight will be reassigned."

# -----------------------------------------------------------------------------
# Step 3: Wait for reassignment
# -----------------------------------------------------------------------------
echo ""
echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║  STEP 3: Waiting 5s for KIP-932 reassignment...                            ║"
echo "║           The share group detects the dead member and redistributes tasks. ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""

for i in $(seq 5 -1 1); do
  printf "\r  ⏳ %d seconds..." "$i"
  sleep 1
done
echo ""
echo ""
echo "  ✅ Reassignment window elapsed."

# -----------------------------------------------------------------------------
# Step 4: Show remaining containers
# -----------------------------------------------------------------------------
echo ""
echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║  STEP 4: Remaining execution-agent containers after crash                  ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""

docker compose ps execution-agent

echo ""
echo "  📊 The killed container is gone. Remaining agents pick up the slack."

# -----------------------------------------------------------------------------
# Step 5: Watch logs for 15s — see tasks being retried
# -----------------------------------------------------------------------------
echo ""
echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║  STEP 5: Watching execution-agent logs for 15 seconds...                   ║"
echo "║           Look for tasks being reassigned and retried by other agents.     ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""

timeout 15s docker compose logs execution-agent -f --tail=50 || true

# -----------------------------------------------------------------------------
# Step 6: Instructions
# -----------------------------------------------------------------------------
echo ""
echo "╔══════════════════════════════════════════════════════════════════════════════╗"
echo "║  🎉 DEMO 3 COMPLETE!                                                       ║"
echo "╠══════════════════════════════════════════════════════════════════════════════╣"
echo "║                                                                              ║"
echo "║  What you just saw:                                                         ║"
echo "║    1. Running execution agents listed                                       ║"
echo "║    2. One agent forcefully killed (simulating a crash)                      ║"
echo "║    3. KIP-932 Share Group detects the dead member                          ║"
echo "║    4. Unacknowledged tasks are automatically reassigned to healthy agents  ║"
echo "║    5. Pipeline continues without data loss or manual intervention          ║"
echo "║                                                                              ║"
echo "║  Why this matters:                                                          ║"
echo "║    • Self-healing — no manual task reassignment needed                     ║"
echo "║    • No data loss — unacknowledged tasks are redelivered                   ║"
echo "║    • At-least-once semantics — tasks will be processed                     ║"
echo "║                                                                              ║"
echo "║  Recovery:                                                                  ║"
echo "║    The dead agent won't restart automatically in this demo. To restore:     ║"
echo "║      docker compose up -d --scale execution-agent=5                         ║"
echo "║                                                                              ║"
echo "║  Clean up:                                                                  ║"
echo "║    docker compose down                                                      ║"
echo "║                                                                              ║"
echo "╚══════════════════════════════════════════════════════════════════════════════╝"
echo ""
#!/bin/bash
#
# Deploy Sandboxed Trading Agents
# Builds Docker image and spawns sandboxed agents for generated strategies
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

echo "🐳 Deploying Sandboxed Trading Agents"
echo "======================================="
echo "Project: $PROJECT_ROOT"
echo

# Check Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker not installed. Install from https://docker.com"
    exit 1
fi

echo "✅ Docker available: $(docker --version)"
echo

# Build image
echo "🔨 Building sandbox image..."
cd "$PROJECT_ROOT"

python3 agents/sandbox_manager.py build

if [ $? -ne 0 ]; then
    echo "❌ Image build failed"
    exit 1
fi

echo "✅ Image built successfully"
echo

# Get generated strategies
echo "📋 Finding generated strategies..."

STRATEGIES_DB="$PROJECT_ROOT/data/strategies.db"

if [ ! -f "$STRATEGIES_DB" ]; then
    echo "⚠️  No strategies database found. Generate strategies first:"
    echo "   python3 agents/strategy_synthesizer.py"
    exit 1
fi

# Query top strategies
STRATEGIES=$(python3 -c "
import sqlite3
conn = sqlite3.connect('$STRATEGIES_DB')
c = conn.cursor()
c.execute('''
    SELECT strategy_id, name
    FROM generated_strategies
    WHERE backtest_status = 'passed'
      AND backtest_sharpe > 1.0
    ORDER BY backtest_sharpe DESC
    LIMIT 10
''')
rows = c.fetchall()
conn.close()

for row in rows:
    print(f'{row[0]}:{row[1]}')
")

if [ -z "$STRATEGIES" ]; then
    echo "⚠️  No passing strategies found. Run strategy synthesizer first."
    exit 1
fi

echo "Found $(echo "$STRATEGIES" | wc -l) strategies to deploy"
echo

# Spawn sandboxes
echo "🚀 Spawning sandboxed agents..."
echo

DEPLOYED=0

while IFS=: read -r strategy_id name; do
    echo "  Spawning: $name ($strategy_id)"

    container_id=$(python3 agents/sandbox_manager.py spawn \
        --strategy-id "$name" \
        --pair BTC-USD 2>&1 | grep 'Spawned:' | awk '{print $2}')

    if [ -n "$container_id" ]; then
        echo "    ✅ Container: ${container_id:0:12}"
        DEPLOYED=$((DEPLOYED + 1))
    else
        echo "    ❌ Failed to spawn"
    fi

done <<< "$STRATEGIES"

echo
echo "======================================="
echo "✅ Deployment complete"
echo "   Deployed: $DEPLOYED sandboxes"
echo

# List running sandboxes
echo "📊 Active sandboxes:"
python3 agents/sandbox_manager.py list

echo
echo "💡 Management commands:"
echo "   List:    python3 agents/sandbox_manager.py list"
echo "   Logs:    python3 agents/sandbox_manager.py logs --container-id <id>"
echo "   Stop:    python3 agents/sandbox_manager.py stop --container-id <id>"
echo "   Cleanup: python3 agents/sandbox_manager.py cleanup"
echo

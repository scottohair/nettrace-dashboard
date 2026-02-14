#!/bin/bash
# Real-time fill watcher for Phase 1 agents
# Usage: ./watch_fills.sh [region] [agent]
# Examples:
#   ./watch_fills.sh ewr            # Watch all fills in ewr
#   ./watch_fills.sh ewr sentiment  # Watch sentiment_leech fills in ewr
#   ./watch_fills.sh                # Watch all regions

REGION="${1:-ewr}"
AGENT="${2:-}"

echo "🔍 Watching fills in region: $REGION"
if [ -n "$AGENT" ]; then
  echo "   Agent filter: $AGENT"
fi
echo ""
echo "Press Ctrl+C to stop"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

/Users/scott/.fly/bin/flyctl logs -r "$REGION" --no-tail 2>&1 | while read line; do
  # Look for FILLED, PLACE, APPROVED, or DENIED entries
  if echo "$line" | grep -E "(✓ FILLED|→ PLACE|APPROVED|DENIED|ERROR|WARNING)" > /dev/null; then
    # Filter by agent if specified
    if [ -n "$AGENT" ]; then
      if echo "$line" | grep "$AGENT" > /dev/null; then
        echo "$line"
      fi
    else
      echo "$line"
    fi
  fi
done

# NetTrace Agent Playbook

## For AI Agents, Developers, and Autonomous Systems

This playbook teaches any agent how to participate in the NetTrace compute pool,
generate trading signals, and contribute to the money-making pipeline.

**Read this entire document before writing any code or making any trades.**

---

## 1. Architecture Overview

```
                    +------------------+
                    |  Fly.io API      |
                    |  (nettrace-      |
                    |   dashboard)     |
                    +--------+---------+
                             |
                    +--------+---------+
                    |   Orchestrator   |
                    |  orchestrator.py |
                    +--------+---------+
                             |
              +--------------+--------------+
              |              |              |
     +--------+---+  +------+------+  +----+--------+
     |  Scanner   |  | ML Signal   |  | Live Trader |
     | worker_    |  | Agent       |  | live_       |
     | scanner.py |  | ml_signal_  |  | trader.py   |
     |            |  | agent.py    |  |             |
     +------+-----+  +------+------+  +------+------+
            |               |                |
            +-------+-------+                |
                    |                        |
           +--------+--------+      +--------+--------+
           |  Compute Pool   |      |  Coinbase API   |
           |  compute_pool.py|      |  exchange_       |
           |  :9090          |      |  connector.py    |
           +--------+--------+      +-----------------+
                    |
     +--------------+--------------+
     |              |              |
 +---+----+   +----+---+   +-----+-----+
 | Local  |   | M1 Max |   | M2 Ultra  |
 | M3 Air |   | :110   |   | :106      |
 | :9090  |   | :9091  |   | :9091     |
 +--------+   +--------+   +-----------+
```

### Component Roles

| Component | File | Purpose |
|-----------|------|---------|
| **Compute Pool** | `compute_pool.py` | Manages GPU/CPU nodes, dispatches ML inference tasks |
| **ML Signal Agent** | `ml_signal_agent.py` | Generates trading signals from latency data using ML |
| **Live Trader** | `live_trader.py` | Executes real trades on Coinbase based on signals |
| **Quant Trader** | `quant_trader.py` | Paper trading engine and backtesting |
| **Scanner** | `worker_scanner.py` | Runs traceroutes, feeds data into the pipeline |
| **Exchange Connector** | `exchange_connector.py` | Coinbase API wrapper, price feeds |
| **Orchestrator** | `orchestrator.py` | Manages agent lifecycle, hires/fires based on KPIs |

### Data Flow

```
Traceroute scans -> ML Signal Agent -> Trading Signals -> Live Trader -> Coinbase
                         |                                     |
                   Compute Pool                          Exchange Connector
                   (GPU inference)                       (order execution)
```

---

## 2. The Compute Pool

### Starting the Pool

```bash
# Start the pool manager (runs HTTP server on port 9090)
cd /Users/scott/traceroute-dashboard
python agents/compute_pool.py
```

The pool automatically:
1. Detects the local machine's GPU (Apple Metal, CUDA, or CPU)
2. Registers the local node
3. Pre-registers known Apple Silicon nodes (M1 Max, M2 Ultra)
4. Starts health monitoring
5. Opens the REST API on port 9090

### How to Join the Compute Pool

#### From Python (same machine)

```python
from compute_pool import ComputePoolManager

pool = ComputePoolManager()
pool.start()

# Dispatch a task
result = pool.dispatch_task(
    task_type="anomaly_detection",
    data={"host": "api.coinbase.com", "rtt_ms": 45.2},
    requirements={"framework": "mlx"},
    priority=3,
)
print(result)  # {"task_id": "abc123", "assigned_node": "local", "status": "running"}
```

#### From a Remote Node (HTTP)

```bash
# Register your node
curl -X POST http://192.168.1.100:9090/register \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-gpu-box",
    "host": "192.168.1.200",
    "port": 9091,
    "gpu_type": "apple_metal",
    "gpu_name": "Apple M1 Max",
    "gpu_cores": 32,
    "memory_gb": 64,
    "frameworks": ["numpy", "mlx", "pytorch", "mps"],
    "metal_support": true,
    "mlx_available": true,
    "cuda_available": false
  }'

# Send heartbeats (every 30s to stay online)
curl -X POST http://192.168.1.100:9090/heartbeat \
  -H "Content-Type: application/json" \
  -d '{"node_id": "your-node-id", "load": 0.2}'
```

#### Using the Client Library

```python
from compute_pool import ComputePoolClient

client = ComputePoolClient("http://192.168.1.100:9090")

# Register
resp = client.register({
    "name": "my-node",
    "host": "192.168.1.200",
    "gpu_type": "apple_metal",
    "frameworks": ["mlx", "pytorch"],
    "memory_gb": 64,
})
node_id = resp["node_id"]

# Dispatch work
task = client.dispatch(
    task_type="batch_analysis",
    data={"scans": [...]},
    requirements={"framework": "mlx", "min_memory_gb": 16},
)

# Wait for result
result = client.wait_for_result(task["task_id"], timeout=30)
```

---

## 3. Adding CUDA Nodes

Any machine with an NVIDIA GPU can join the pool. Here is how.

### Prerequisites on the CUDA Machine

```bash
# Verify CUDA is available
python3 -c "import torch; print(torch.cuda.is_available(), torch.cuda.get_device_name(0))"
```

### Registration

```python
import torch
import platform
from compute_pool import ComputePoolClient

client = ComputePoolClient("http://<pool-host>:9090")

client.register({
    "name": f"cuda-{platform.node()}",
    "host": "<this-machines-ip>",
    "port": 9091,
    "gpu_type": "cuda",
    "gpu_name": torch.cuda.get_device_name(0),
    "gpu_cores": torch.cuda.get_device_properties(0).multi_processor_count,
    "memory_gb": round(torch.cuda.get_device_properties(0).total_mem / 1e9, 1),
    "chip": platform.processor(),
    "frameworks": ["numpy", "pytorch", "cuda"],
    "metal_support": False,
    "mlx_available": False,
    "cuda_available": True,
})
```

### Running as a Worker

```bash
# On the CUDA machine, run the ML signal agent as a pool worker
python ml_signal_agent.py --mode worker --pool-url http://<pool-host>:9090
```

### Cloud GPU Integration (Lambda, RunPod, etc.)

```bash
# From a cloud GPU instance
curl -X POST http://<your-tailscale-ip>:9090/register \
  -H "Content-Type: application/json" \
  -d '{
    "name": "cloud-a100",
    "host": "<instance-ip>",
    "port": 9091,
    "gpu_type": "cuda",
    "gpu_name": "NVIDIA A100",
    "gpu_cores": 6912,
    "memory_gb": 80,
    "frameworks": ["numpy", "pytorch", "cuda", "tensorflow"],
    "cuda_available": true
  }'
```

---

## 4. Generating and Validating Trading Signals

### Signal Types

| Type | Method | What It Detects |
|------|--------|-----------------|
| `latency_anomaly` | Z-score on RTT timeseries | Sudden spike/drop in exchange latency |
| `trend_shift` | Linear regression slope change | Gradual degradation or recovery |
| `route_change` | Hop-path diff vs. baseline | Network path changes (BGP, CDN) |
| `composite` | Weighted ensemble | Consensus of multiple signal types |

### How Signals Become Trades

1. **Scanner** runs traceroutes to exchange hosts (coinbase, binance, etc.)
2. **ML Signal Agent** processes RTT data through anomaly detectors
3. Signals with `confidence >= 0.55` are stored in `signals.db`
4. **Live Trader** polls for active signals mapped to trading pairs
5. If a signal's exchange host maps to a known pair (e.g., `api.coinbase.com` -> `BTC-USD`), a trade is considered
6. Risk rules are applied (see section 6)
7. Order is placed via `exchange_connector.py`

### Signal Direction Logic

```
Latency spike on exchange host  ->  Exchange is degraded
  ->  Users may panic sell  ->  SHORT signal (bearish)

Latency drop (recovery)  ->  Exchange returning to normal
  ->  Confidence returns  ->  LONG signal (bullish)

Route change (more hops)  ->  Longer path  ->  Degradation  ->  SHORT
Route change (fewer hops)  ->  Shorter path  ->  Improvement  ->  LONG
```

### Validating Signal Quality

```python
from ml_signal_agent import MLSignalAgent

agent = MLSignalAgent()

# Check historical accuracy
accuracy = agent.get_accuracy(hours=24)
print(f"Win rate: {accuracy['accuracy']:.1%}")
print(f"Total P&L: ${accuracy['total_pnl']:.2f}")

# A good signal agent maintains > 55% accuracy
# Below 50% means the signals are worse than random
```

### Signal Confidence Levels

| Confidence | Meaning | Action |
|------------|---------|--------|
| 0.40 - 0.54 | Weak | Discard, do not trade |
| 0.55 - 0.64 | Marginal | Trade with minimum size |
| 0.65 - 0.74 | Moderate | Trade with normal size |
| 0.75 - 0.89 | Strong | Trade with increased size |
| 0.90 - 0.95 | Very strong | Multiple signals agree, trade aggressively within risk limits |

---

## 5. Using Apple Metal / MLX for Acceleration

### Why MLX?

MLX is Apple's native ML framework for Apple Silicon. It runs directly on the
Metal GPU and unified memory, giving 2-10x speedups over CPU-only NumPy for
matrix operations and ML inference.

### Detection Order

The ML Signal Agent tries frameworks in this order:

1. **MLX** (`import mlx.core as mx`) -- fastest on Apple Silicon
2. **PyTorch with MPS** (`torch.backends.mps.is_available()`) -- good fallback
3. **PyTorch CPU** -- works everywhere
4. **NumPy** -- universal fallback, no GPU

### Installing MLX

```bash
# On Apple Silicon Macs (M1/M2/M3)
pip install mlx

# Verify
python -c "import mlx.core as mx; print(mx.array([1,2,3]))"
```

### How MLX Is Used

The agent uses MLX for the numerical core operations:

- **Array creation**: `mx.array(data)` instead of `np.array(data)`
- **Mean/std**: `mx.mean(arr)`, manual std via `mx.sqrt(mx.mean((arr - m)**2))`
- **Linear regression**: Matrix operations on MLX arrays

All operations automatically run on the Metal GPU without explicit device
management. MLX uses lazy evaluation -- computations are only executed when
results are needed, which enables automatic optimization.

### Performance Expectations

| Operation | NumPy (CPU) | MLX (Metal) | Speedup |
|-----------|-------------|-------------|---------|
| Z-score (50 samples) | ~0.1ms | ~0.05ms | 2x |
| Linear regression (20 pts) | ~0.2ms | ~0.08ms | 2.5x |
| Batch 1000 scans | ~150ms | ~40ms | 3.7x |
| Large matrix ops (1M+) | ~500ms | ~50ms | 10x |

For our scan volumes (hundreds per cycle), the speedup is modest but
consistent. The real win comes when processing historical backtests or
running more complex models.

---

## 6. Risk Management Rules

**MANDATORY. Every agent MUST follow these rules. No exceptions.**

### Hard Limits

| Rule | Value | Consequence of Violation |
|------|-------|--------------------------|
| Max trade size | $5.00 | Order rejected |
| Min trade size | $1.00 | Below Coinbase minimum |
| Max daily loss | $2.00 | Trading halted for the day |
| Min signal confidence | 0.55 | Signal discarded |
| Max open positions | 3 | New trades blocked |
| Position hold time | 5 minutes min | No scalping below this |
| Take profit | 0.5% | Auto-close at target |
| Stop loss | 0.3% | Auto-close at loss limit |
| Cooldown per pair | 30 minutes | Prevents overtrading |

### Rules for Agents

1. **NEVER** increase position sizes beyond the limits above
2. **NEVER** override stop losses -- they exist to protect capital
3. **NEVER** trade without a signal -- no "gut feelings" or momentum chasing
4. **ALWAYS** check daily P&L before placing a trade
5. **ALWAYS** log every trade decision (including decisions NOT to trade)
6. **ALWAYS** respect cooldown periods between trades on the same pair
7. If the daily loss limit ($2) is hit, **STOP ALL TRADING** immediately
8. If signal accuracy drops below 45% over 24 hours, pause and investigate
9. **NEVER** store API keys in code -- use environment variables only
10. **NEVER** commit `.env` files or credentials to git

### Position Sizing Formula

```python
# Conservative Kelly-inspired sizing
def position_size(confidence, capital, max_trade=5.0):
    base_pct = 0.02  # 2% of capital as base
    confidence_multiplier = max(0, (confidence - 0.55)) * 5  # Scale with confidence
    size = capital * base_pct * (1 + confidence_multiplier)
    return max(1.0, min(size, max_trade))  # Clamp to [$1, $5]
```

---

## 7. API Endpoints

### Compute Pool (port 9090)

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Health check (returns `{"status": "ok"}`) |
| `GET` | `/status` | Pool statistics (nodes, tasks, GPU capacity) |
| `GET` | `/nodes` | List all nodes (optional `?status=online`) |
| `GET` | `/nodes/{id}` | Single node details |
| `POST` | `/register` | Register a new compute node |
| `POST` | `/heartbeat` | Node heartbeat (`{"node_id": "...", "load": 0.3}`) |
| `POST` | `/dispatch` | Submit a task for execution |
| `GET` | `/results/{task_id}` | Get task result |
| `POST` | `/results/{task_id}` | Worker submits completed result |
| `GET` | `/tasks` | List queued/running tasks (optional `?status=queued`) |

### Dispatch Request Format

```json
{
  "task_type": "anomaly_detection",
  "data": {
    "host": "api.coinbase.com",
    "rtt_ms": 45.2,
    "timestamp": 1707700000.0
  },
  "requirements": {
    "gpu_type": "any",
    "framework": "mlx",
    "min_memory_gb": 8,
    "prefer": "low_latency"
  },
  "priority": 3
}
```

### Task Types

| Type | Data Fields | Returns |
|------|-------------|---------|
| `anomaly_detection` | `host`, `rtt_ms`, `timestamp`, `hops` | `{"signals": [...]}` |
| `batch_analysis` | `scans` (list of scan dicts) | `{"signals": [...], "count": N}` |
| `get_signals` | `min_confidence` | `{"signals": [...]}` |
| `get_accuracy` | `hours` | `{"accuracy": 0.62, "total_pnl": 1.50}` |

### Result Submit Format (from workers)

```json
{
  "result": {"signals": [...]},
  "error": null,
  "execution_ms": 42.5,
  "gpu_utilization": 0.65,
  "memory_used_mb": 512
}
```

---

## 8. Inter-Agent Communication

### Agent-to-Pool

```python
# Any agent can dispatch ML work to the pool
from compute_pool import ComputePoolClient

client = ComputePoolClient("http://127.0.0.1:9090")
task = client.dispatch("batch_analysis", {"scans": scan_data})
result = client.wait_for_result(task["task_id"], timeout=30)
```

### Signal Agent to Trader

```python
# The trader polls for signals
from ml_signal_agent import MLSignalAgent

agent = MLSignalAgent()
signals = agent.get_trading_signals(min_confidence=0.55)

for signal in signals:
    if signal["confidence"] >= 0.65:
        # Execute trade
        pass
    agent.signal_db.mark_acted(signal["signal_id"])
```

### Fly.io API Integration

The scanner posts results to the Fly.io API, which stores them in the
central database. The ML Signal Agent reads from the local traceroute.db
(synced or populated by the scanner).

```python
# Scanner -> Fly.io
POST https://nettrace-dashboard.fly.dev/api/v1/scans
{"host": "...", "hops": [...], "total_rtt": 45.2}

# Trader -> Fly.io (signal reporting)
POST https://nettrace-dashboard.fly.dev/api/v1/signals
{"type": "latency_anomaly", "confidence": 0.72, "pair": "BTC-USD"}
```

---

## 9. Money-Making Strategy Summary

### The Edge

We run multi-region traceroute scans against crypto exchange infrastructure.
When we detect latency anomalies (spikes, route changes, degradation), we know
something is happening at the network layer **before** it shows up in price action.

### Why This Works

1. **Latency spikes precede outages** -- when Coinbase's API latency spikes,
   their matching engine may be degrading. Users experience slow fills, leading
   to panic selling on that exchange while prices on other exchanges remain stable.

2. **Route changes signal infrastructure events** -- BGP changes, CDN failovers,
   and datacenter migrations cause observable route changes that precede service
   quality changes.

3. **15-minute information advantage** -- our multi-region scanning detects these
   events 10-15 minutes before they become visible to retail traders or make news.

### Execution

1. **Scan continuously** -- traceroutes every 15 minutes from multiple vantage points
2. **Detect anomalies** -- Z-score, trend analysis, route diffing via the ML agent
3. **Generate signals** -- map exchange host anomalies to trading pairs
4. **Trade conservatively** -- micro positions ($1-5), tight stops (0.3%), quick exits
5. **Compound wins** -- reinvest profits, grow position sizes as capital allows
6. **Track everything** -- every signal, every trade, every P&L in SQLite

### Current Capital: ~$13

With $13, we trade the minimum sizes on Coinbase (BTC-USD, ETH-USD, SOL-USD).
The goal is to compound to $100+ through consistent small wins on high-confidence
signals.

### Key Metrics to Watch

- **Signal accuracy**: Must stay above 55% to be profitable after fees
- **Win/loss ratio**: Target 1.5:1 (take profit 0.5% vs stop loss 0.3%)
- **Daily P&L**: Stop at -$2/day, no exceptions
- **Signals per day**: 5-20 is healthy; fewer means scans are stale, more means
  thresholds may be too loose

---

## 10. Quick Reference

### Start Everything

```bash
# Terminal 1: Compute pool
python agents/compute_pool.py

# Terminal 2: ML Signal Agent (scan mode)
python agents/ml_signal_agent.py --mode scan --interval 120

# Terminal 3: Live Trader
python agents/live_trader.py

# Terminal 4: Scanner (optional, feeds local data)
python agents/worker_scanner.py
```

### Check Pool Status

```bash
curl http://localhost:9090/status | python -m json.tool
curl http://localhost:9090/nodes?status=online | python -m json.tool
```

### One-Shot Signal Analysis

```bash
python agents/ml_signal_agent.py --mode oneshot
```

### Environment Variables

| Variable | Purpose | Default |
|----------|---------|---------|
| `POOL_HOST` | Compute pool bind address | `0.0.0.0` |
| `POOL_PORT` | Compute pool port | `9090` |
| `POOL_URL` | Pool URL for clients | `http://127.0.0.1:9090` |
| `DB_PATH` | Traceroute scan database | `../traceroute.db` |
| `COINBASE_API_KEY_ID` | Coinbase CDP key ID | (required for trading) |
| `COINBASE_API_KEY_SECRET` | Coinbase CDP secret | (required for trading) |
| `NETTRACE_API_KEY` | Fly.io API key | (required for API access) |

### File Locations

| File | Purpose |
|------|---------|
| `agents/compute_pool.py` | Compute pool manager + HTTP server |
| `agents/compute_pool.db` | Pool state (nodes, tasks, metrics) |
| `agents/compute_pool.log` | Pool server logs |
| `agents/ml_signal_agent.py` | ML signal generation agent |
| `agents/signals.db` | Signal storage and performance tracking |
| `agents/ml_signal.log` | Signal agent logs |
| `agents/live_trader.py` | Live trading engine |
| `agents/trader.db` | Trade history and positions |
| `agents/live_trader.log` | Trader logs |
| `agents/exchange_connector.py` | Exchange API wrapper |
| `agents/orchestrator.py` | Agent lifecycle manager |
| `agents/agents.db` | Agent registry and performance |

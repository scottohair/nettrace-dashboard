# NetTrace Multi-Region Expansion Plan

## Phase 1: Validate from EWR (current — first 48 hours)
- Scanner is live, collecting data on 100 targets every 15 min
- After 48 hours: ~19,200 data points to validate baseline RTTs
- Key metrics to validate:
  - NYSE/NASDAQ: should see 1-5ms from EWR
  - Crypto exchanges: Binance (40-80ms to Singapore), Coinbase (5-15ms)
  - Can we detect route changes in real-time? (route_changes table)

## Phase 2: Deploy Financial Center Nodes
Each node = same Docker image on Fly.io, different region.

| Priority | Fly Region | City | Why | Cost |
|----------|-----------|------|-----|------|
| 1 | `ord` | Chicago | CME Group, CBOE — futures/options capital | $3.19/mo |
| 2 | `lhr` | London | LSE, ICE Futures, LSEG — European finance | $3.19/mo |
| 3 | `sin` | Singapore | SGX, Binance HQ, SE Asian crypto hub | $3.19/mo |
| 4 | `nrt` | Tokyo | TSE, Japanese exchanges, Asian finance | $3.19/mo |
| 5 | `gru` | São Paulo | B3, LatAm largest exchange | $3.19/mo |
| 6 | `bom` | Mumbai | NSE, BSE India — massive volume | $3.19/mo |
| 7 | `hkg` | Hong Kong | HKEX, gateway to China | $3.19/mo |
| 8 | `fra` | Frankfurt | Deutsche Börse, Eurex | $3.19/mo |
| 9 | `syd` | Sydney | ASX, Oceania gateway | $3.19/mo |
| 10 | `jnb` | Johannesburg | JSE, African markets | $3.19/mo |

Total: ~$32/mo for 10 global scanner nodes

## Phase 3: Odd/Underserved Regions (Alpha Opportunity)
These are regions where latency data is scarce = premium pricing.

| Region | Fly Region | Opportunity |
|--------|-----------|-------------|
| Dubai | `dxb` | DFSA, crypto-friendly regulation, growing exchange hub |
| Lagos | `lag` (if avail) | Nigerian naira forex, massive remittance market |
| Istanbul | `ist` | Borsa Istanbul, Turkey forex volatility |
| Bangkok | `bkk` | Thai exchanges, SE Asian expansion |
| Nairobi | N/A | M-Pesa digital wallet hub, East African finance |
| Buenos Aires | `eze` | Argentine peso forex, crypto adoption from inflation |

## Phase 4: Hardware (Local Resources)
Use local hardware for:
- Historical data backup/archive (cheaper than cloud storage)
- ML model training (regression, anomaly detection)
- Strategy backtesting on full dataset dumps
- Metal/GPU for future ML inference

## Data Product Per Node
Each node generates:
- Unique latency perspective (different BGP paths, different ISPs)
- Cross-region comparison data (how fast is NY→Singapore vs Singapore→NY)
- Triangulation: detect which leg of a route is degraded
- Multi-region route change correlation

## Transaction Speed Validation
To validate our ability to transact rapidly:
1. Measure RTT to exchange API endpoints (already doing this)
2. Measure actual API response time (not just ICMP — TCP handshake + HTTP)
3. Build latency leaderboard: which exchange is fastest from which region
4. Detect when a fast exchange becomes slow (= opportunity for others)

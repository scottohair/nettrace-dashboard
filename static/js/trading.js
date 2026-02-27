/* NetTrace Quant Platform — Trading Page Controller */
/* MVC Controller Layer: trading dashboard logic */
/* Depends on: core.js (WS, Toast, Theme, Fmt, api, Poller, copyToClipboard) */
/* Depends on: wallet.js (Wallet) */

'use strict';

// ── State ──────────────────────────────────────────────────────────────
let currentOrgSlug = null;
let portfolioChart = null;
let metaScatterChart = null;
let worldMap = null;
let lastPrices = {};
let tradedPairs = ['BTC-USD', 'ETH-USD', 'SOL-USD', 'DOGE-USD', 'XRP-USD', 'AVAX-USD', 'LINK-USD'];

const SERVERS = [
  { region: 'EWR', location: 'Newark, NJ', lat: 40.69, lng: -74.17, role: 'primary', status: 'active' },
  { region: 'ORD', location: 'Chicago, IL', lat: 41.88, lng: -87.63, role: 'compute', status: 'active' },
  { region: 'LHR', location: 'London, UK', lat: 51.51, lng: -0.13, role: 'scout', status: 'active' },
  { region: 'FRA', location: 'Frankfurt, DE', lat: 50.11, lng: 8.68, role: 'scout', status: 'active' },
  { region: 'NRT', location: 'Tokyo, JP', lat: 35.68, lng: 139.69, role: 'scout', status: 'active' },
  { region: 'SIN', location: 'Singapore', lat: 1.35, lng: 103.82, role: 'scout', status: 'active' },
  { region: 'BOM', location: 'Mumbai, IN', lat: 19.08, lng: 72.88, role: 'future', status: 'active' },
  { region: 'LOCAL', location: 'M3 Air (host)', lat: 40.75, lng: -73.99, role: 'local', status: 'active' },
  { region: 'M1MAX', location: '192.168.1.110', lat: 40.74, lng: -74.00, role: 'local', status: 'standby' },
  { region: 'M2ULTRA', location: '192.168.1.106', lat: 40.73, lng: -74.01, role: 'local', status: 'standby' },
];

// ── Init ───────────────────────────────────────────────────────────────
function init() {
  connectWS();
  initPortfolioChart();
  renderServers();
  loadOrgs();
  loadCredentials();
  refreshData();

  // Start all pollers
  Poller.start('ticker', fetchTicker, 10000);
  Poller.start('trading-data', refreshData, 60000);
  Poller.start('system-status', fetchSystemStatus, 10000);
  Poller.start('asset-pools', refreshAssetPools, 30000);
  Poller.start('venue-comparison', fetchVenueComparison, 30000);
  Poller.start('opportunities', fetchOpportunities, 60000);
  Poller.start('regimes', fetchRegimes, 300000);
  Poller.start('treasury', fetchTreasury, 60000);
  Poller.start('proposals', loadProposals, 15000);
  Poller.start('agents', loadAgents, 30000);
  Poller.start('leaderboard', loadLeaderboard, 60000);
  Poller.start('meta-engine', loadMetaEngine, 30000);

  // Initial SCADA fetch
  fetchSystemStatus();
}

// ── WebSocket ──────────────────────────────────────────────────────────
function connectWS() {
  WS.connect();

  WS.on('trading_update', function(d) {
    if (d.portfolio_value) {
      _setText('portfolioValue', Fmt.usd(d.portfolio_value, 2));
    }
    if (d.daily_pnl !== undefined) {
      const el = document.getElementById('dailyPnl');
      if (el) {
        el.textContent = Fmt.pnl(d.daily_pnl);
        el.className = 'big ' + (d.daily_pnl >= 0 ? 'green' : 'red');
      }
    }
    if (d.trades_today !== undefined) _setText('tradesToday', d.trades_today);
    if (d.trades_total !== undefined) _setText('tradesTotal', 'total: ' + d.trades_total);

    // Update holdings from WS heartbeat
    if (d.holdings && Object.keys(d.holdings).length > 0) {
      _renderHoldingsTable(d.holdings);
    }

    // Trade notification
    if (d.latest_trade && d.latest_trade.pair) {
      const t = d.latest_trade;
      Toast.show('Trade: ' + t.side + ' ' + t.pair + ' @ ' + Fmt.usd(t.price, 2), 'trade');
    }
    if (d.trade) {
      Toast.show('Trade: ' + d.trade.side + ' ' + d.trade.pair + ' @ $' + d.trade.price, 'trade');
    }

    const src = d.source === 'heartbeat' ? 'heartbeat' : 'snapshot';
    _setText('lastUpdate', 'LIVE (' + src + ') | ' + new Date().toLocaleTimeString());

    // WS is working; slow poller
    Poller.stop('trading-data');
    Poller.start('trading-data', refreshData, 60000);
  });

  WS.on('price_update', function(d) {
    if (d.prices) updateTicker(d.prices);
  });

  WS.on('disconnect', function() {
    // WS down — poll faster
    Poller.stop('trading-data');
    Poller.start('trading-data', refreshData, 15000);
  });
}

// ── Refresh Data (primary HTTP) ────────────────────────────────────────
async function refreshData() {
  const qs = currentOrgSlug ? '?org_slug=' + encodeURIComponent(currentOrgSlug) : '';
  const data = await api('/api/trading-data' + qs);
  if (data.error) {
    _setText('lastUpdate', 'Error: ' + data.error);
    return;
  }

  const val = data.portfolio_value || 0;
  const cbVal = data.coinbase_value || 0;
  const walVal = data.wallet_value || 0;
  const stuckVal = data.stuck_value || 0;
  const transitVal = data.in_transit_value || 0;
  const pnl = data.daily_pnl || 0;

  _setText('portfolioValue', Fmt.usd(val, 2));

  // Portfolio breakdown
  const breakdown = [];
  if (cbVal > 0) breakdown.push('CB: ' + Fmt.usd(cbVal, 2));
  if (walVal > 0) breakdown.push('Wallet: ' + Fmt.usd(walVal, 2));
  if (stuckVal > 0) breakdown.push('Stuck: ' + Fmt.usd(stuckVal, 2));
  if (transitVal > 0) breakdown.push('Transit: ' + Fmt.usd(transitVal, 2));
  _setText('portfolioChange', breakdown.length ? breakdown.join(' + ') : '--');

  // P&L
  const pnlEl = document.getElementById('dailyPnl');
  if (pnlEl) {
    pnlEl.textContent = Fmt.pnl(pnl);
    pnlEl.className = 'big ' + (pnl >= 0 ? 'green' : 'red');
  }

  // Trades count
  _setText('tradesToday', data.trades_today || 0);
  _setText('tradesTotal', 'total: ' + (data.trades_total || 0));
  _setText('traderPnl', Fmt.pnl(pnl));

  // Win rate
  const winRate = data.win_rate;
  if (winRate !== undefined) {
    _setText('winRateValue', Fmt.pct(winRate));
  }

  // Sharpe ratio
  if (data.sharpe_ratio !== undefined) {
    _setText('sharpeValue', Number(data.sharpe_ratio).toFixed(2));
  }

  // Wallet addresses in footer
  if (data.evm_address) _setText('footerEvmAddr', data.evm_address.slice(0, 6) + '...' + data.evm_address.slice(-4));
  if (data.solana_address) _setText('footerSolAddr', data.solana_address.slice(0, 6) + '...' + data.solana_address.slice(-4));

  // Holdings
  if (data.holdings) _renderHoldingsTable(data.holdings);

  // Update traded pairs list dynamically
  if (data.holdings && Object.keys(data.holdings).length > 0) {
    const holdingPairs = Object.keys(data.holdings)
      .filter(k => k !== 'USD' && k !== 'USDC')
      .map(k => k + '-USD');
    if (holdingPairs.length > 0) {
      tradedPairs = [...new Set([...holdingPairs, ...tradedPairs])];
    }
  }

  // Recent Trades
  const trades = data.trades || [];
  const tradesBody = document.getElementById('tradesBody');
  const noTrades = document.getElementById('noTrades');
  if (trades.length > 0 && tradesBody) {
    if (noTrades) noTrades.style.display = 'none';
    tradesBody.innerHTML = trades.slice(0, 20).map(function(t) {
      const time = t.created_at ? Fmt.shortTime(t.created_at) : '--';
      return '<tr>' +
        '<td>' + time + '</td>' +
        '<td>' + t.pair + '</td>' +
        '<td><span class="badge ' + (t.side === 'BUY' ? 'b-buy' : 'b-sell') + '">' + t.side + '</span></td>' +
        '<td>' + Fmt.usd(t.price, 2) + '</td>' +
        '<td>' + Fmt.usd(t.total_usd, 2) + '</td>' +
        '<td style="font-size:9px">' + (t.signal_type || '--') + '</td>' +
        '<td><span class="badge ' + (t.status === 'filled' ? 'b-filled' : 'b-failed') + '">' + t.status + '</span></td>' +
        '</tr>';
    }).join('');
  }

  // Claude Insights
  renderClaudeInsights(data.claude_insights || {});

  // Quant 100 status
  _renderQuant100Status(data.quant100_summary || {}, data.quant100_agent || {});

  // Claude Ingest
  _renderClaudeIngest(data.claude_ingest || {}, data.claude_stager || {}, data.claude_duplex || {});

  // AmiCoin
  renderAmiCoin(data.amicoin || {}, data.amicoin_agent || {});

  // Chart update
  const snapshots = data.snapshots || [];
  if (portfolioChart && snapshots.length > 0) {
    const sorted = snapshots.sort(function(a, b) {
      return new Date(a.recorded_at) - new Date(b.recorded_at);
    });
    portfolioChart.data.labels = sorted.map(function(s) {
      return new Date(s.recorded_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    });
    portfolioChart.data.datasets[0].data = sorted.map(function(s) { return s.total_value_usd; });
    portfolioChart.update('none');
  }

  _setText('lastUpdate', 'Live | ' + new Date().toLocaleTimeString() + (data.snapshot_age ? ' | data: ' + data.snapshot_age : ''));
}

// ── Ticker ─────────────────────────────────────────────────────────────
let _tickerPairsInitialized = false;

async function fetchTicker() {
  const tickerBar = document.getElementById('tickerBar');
  if (!tickerBar) return;

  // On the first ticker fetch, try to get live holdings from the trading
  // data endpoint so the ticker reflects actual positions rather than
  // only the hardcoded fallback list.
  if (!_tickerPairsInitialized) {
    _tickerPairsInitialized = true;
    try {
      const qs = currentOrgSlug ? '?org_slug=' + encodeURIComponent(currentOrgSlug) : '';
      const td = await api('/api/trading-data' + qs);
      if (!td.error && td.holdings && Object.keys(td.holdings).length > 0) {
        const holdingPairs = Object.keys(td.holdings)
          .filter(function(k) { return k !== 'USD' && k !== 'USDC'; })
          .map(function(k) { return k + '-USD'; });
        if (holdingPairs.length > 0) {
          tradedPairs = [...new Set([...holdingPairs, ...tradedPairs])];
        }
      }
    } catch (e) { /* use existing hardcoded pairs */ }
  }

  const results = [];
  for (const pair of tradedPairs) {
    try {
      const r = await fetch('https://api.exchange.coinbase.com/products/' + pair + '/ticker');
      const d = await r.json();
      const sym = pair.split('-')[0];
      const price = parseFloat(d.price || 0);
      const prev = lastPrices[sym] || price;
      const change = ((price - prev) / prev * 100);
      lastPrices[sym] = price;
      results.push({ sym: sym, price: price, change: change });
    } catch (e) { /* skip unavailable pairs */ }
  }

  if (results.length === 0) return;

  tickerBar.innerHTML = results.map(function(r) {
    const priceStr = Fmt.price(r.price);
    const changeColor = r.change >= 0 ? 'var(--green)' : 'var(--red)';
    const changeStr = Fmt.pct(r.change);
    return '<div class="ticker-item">' +
      '<span class="symbol">' + r.sym + '</span>' +
      '<span class="price">$' + priceStr + '</span>' +
      '<span class="change" style="color:' + changeColor + '">' + changeStr + '</span>' +
      '</div>';
  }).join('');
}

function updateTicker(prices) {
  // WS price updates — patch ticker items if present
  for (const sym in prices) {
    const data = prices[sym];
    if (data && data.price) lastPrices[sym] = data.price;
  }
}

// ── Portfolio Chart ────────────────────────────────────────────────────
function initPortfolioChart() {
  const canvas = document.getElementById('portfolioChart');
  if (!canvas) return;

  // Try TradingView Lightweight Charts first
  if (typeof LightweightCharts !== 'undefined') {
    _initTVChart(canvas.parentElement);
    return;
  }

  // Fallback to Chart.js
  const ctx = canvas.getContext('2d');
  portfolioChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels: [],
      datasets: [{
        label: 'Portfolio ($)',
        data: [],
        borderColor: '#00ff88',
        backgroundColor: 'rgba(0,255,136,0.08)',
        fill: true,
        tension: 0.3,
        pointRadius: 1,
        borderWidth: 2
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      interaction: { intersect: false, mode: 'index' },
      scales: {
        x: {
          ticks: { color: '#5a6c7d', font: { size: 9, family: "'SF Mono','Fira Code',monospace" }, maxTicksLimit: 10 },
          grid: { color: 'rgba(30,58,95,0.13)' }
        },
        y: {
          ticks: {
            color: '#5a6c7d',
            font: { size: 9, family: "'SF Mono','Fira Code',monospace" },
            callback: function(v) { return '$' + v.toFixed(2); }
          },
          grid: { color: 'rgba(30,58,95,0.13)' }
        }
      }
    }
  });
}

function _initTVChart(container) {
  // Replace canvas with TradingView Lightweight Chart
  container.innerHTML = '';
  const chart = LightweightCharts.createChart(container, {
    width: container.clientWidth,
    height: 240,
    layout: {
      background: { color: 'transparent' },
      textColor: '#5a6c7d',
      fontFamily: "'SF Mono','Fira Code','Consolas',monospace",
      fontSize: 10
    },
    grid: {
      vertLines: { color: 'rgba(30,58,95,0.13)' },
      horzLines: { color: 'rgba(30,58,95,0.13)' }
    },
    crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
    rightPriceScale: { borderColor: '#1e3a5f' },
    timeScale: { borderColor: '#1e3a5f', timeVisible: true, secondsVisible: false }
  });

  const areaSeries = chart.addAreaSeries({
    topColor: 'rgba(0, 255, 136, 0.2)',
    bottomColor: 'rgba(0, 255, 136, 0.02)',
    lineColor: '#00ff88',
    lineWidth: 2
  });

  // Store for updates
  portfolioChart = {
    _tv: chart,
    _series: areaSeries,
    _isTV: true,
    data: { labels: [], datasets: [{ data: [] }] },
    update: function() {
      // TV chart gets data via setData
    }
  };

  // Resize handler
  const ro = new ResizeObserver(function() {
    chart.applyOptions({ width: container.clientWidth });
  });
  ro.observe(container);
}

function _updateTVChart(snapshots) {
  if (!portfolioChart || !portfolioChart._isTV) return;
  const data = snapshots.map(function(s) {
    return { time: Math.floor(new Date(s.recorded_at).getTime() / 1000), value: s.total_value_usd };
  });
  if (data.length > 0) {
    portfolioChart._series.setData(data);
  }
}

// ── Server Infrastructure & World Map ──────────────────────────────────
function renderServers() {
  const grid = document.getElementById('serverGrid');
  if (grid) {
    grid.innerHTML = SERVERS.map(function(s) {
      const primaryClass = s.role === 'primary' ? ' primary' : '';
      const dotStyle = s.status === 'standby' ? 'background:var(--gold);animation:none' : '';
      return '<div class="server-node' + primaryClass + '">' +
        '<div class="region"><span class="status-dot" style="' + dotStyle + '"></span>' + s.region + '</div>' +
        '<div class="location">' + s.location + '</div>' +
        '<div class="latency">' + (s.status === 'active' ? 'online' : 'standby') +
        (s.role === 'primary' ? ' (PRIMARY)' : '') + '</div>' +
        '</div>';
    }).join('');
  }

  // Initialize world map if Leaflet available
  _initWorldMap();
}

function _initWorldMap() {
  const container = document.getElementById('worldMap');
  if (!container || typeof L === 'undefined') return;

  worldMap = L.map(container, {
    center: [30, 10],
    zoom: 2,
    zoomControl: false,
    attributionControl: false,
    scrollWheelZoom: false,
    dragging: true
  });

  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 6,
    minZoom: 1
  }).addTo(worldMap);

  // Add server markers
  const ewrCoords = [40.69, -74.17];
  SERVERS.forEach(function(s) {
    if (!s.lat || !s.lng) return;
    const isLocal = s.role === 'local';
    if (isLocal) return; // Skip local machines on world map

    const color = s.role === 'primary' ? '#00ff88' :
                  s.status === 'standby' ? '#fbbf24' : '#00d4ff';
    const radius = s.role === 'primary' ? 8 : 5;

    const marker = L.circleMarker([s.lat, s.lng], {
      radius: radius,
      fillColor: color,
      color: color,
      weight: 1,
      opacity: 0.9,
      fillOpacity: 0.6
    }).addTo(worldMap);

    marker.bindPopup(
      '<strong>' + s.region + '</strong><br>' +
      s.location + '<br>' +
      '<span style="color:' + color + '">' + s.status.toUpperCase() + '</span>' +
      (s.role === 'primary' ? '<br>TRADING BRAIN' : '')
    );

    // Draw connection lines from scouts to EWR (primary)
    if (s.role === 'scout' || s.role === 'compute') {
      const line = L.polyline([ewrCoords, [s.lat, s.lng]], {
        color: color,
        weight: 1,
        opacity: 0.4,
        dashArray: '6,4',
        className: 'connection-line'
      }).addTo(worldMap);
    }
  });
}

// ── Claude Insights ────────────────────────────────────────────────────
function renderClaudeInsights(insights) {
  const feed = document.getElementById('claudeFeed');
  if (!feed) return;

  if (!insights || Object.keys(insights).length === 0) {
    feed.innerHTML = '<div style="text-align:center;padding:16px;color:var(--muted);font-size:11px">Waiting for optimizer cycles...</div>';
    return;
  }

  const posture = (insights.risk_posture || 'BALANCED').toUpperCase();
  const postureClass = posture === 'DEFENSIVE' ? 'claude-def' : (posture === 'OFFENSIVE' ? 'claude-off' : 'claude-bal');

  function mkItems(arr) {
    if (arr && arr.length) {
      return arr.slice(0, 4).map(function(x) { return '<div class="item">' + _escapeHtml(x) + '</div>'; }).join('');
    }
    return '<div class="item">No updates</div>';
  }

  const focus = (insights.focus_pairs && insights.focus_pairs.length) ? insights.focus_pairs.join(', ') : 'none';
  const snap = insights.learning_snapshot || {};

  feed.innerHTML =
    '<div class="claude-grid">' +
      '<div class="claude-col"><h4>Algorithm</h4><div class="claude-list">' + mkItems(insights.algorithm_recommendations) + '</div></div>' +
      '<div class="claude-col"><h4>Quant</h4><div class="claude-list">' + mkItems(insights.quant_recommendations) + '</div></div>' +
      '<div class="claude-col"><h4>Dashboard</h4><div class="claude-list">' + mkItems(insights.dashboard_recommendations) + '</div></div>' +
    '</div>' +
    '<div class="claude-meta">' +
      '<span>Posture: <span class="claude-pill ' + postureClass + '">' + posture + '</span></span>' +
      '<span>Focus: <span style="color:var(--accent)">' + _escapeHtml(focus) + '</span></span>' +
      '<span>Sharpe: <span style="color:var(--green)">' + Number(snap.sharpe_ratio || 0).toFixed(2) + '</span></span>' +
      '<span>Drawdown: <span style="color:var(--gold)">' + Number(snap.max_drawdown_pct || 0).toFixed(2) + '%</span></span>' +
    '</div>';

  // Update agent statuses
  var statusMap = {};
  (insights.agent_status || []).forEach(function(s) { statusMap[s.agent] = s; });
  if (statusMap.algorithm_optimizer) _setText('algoAgentState', statusMap.algorithm_optimizer.detail || statusMap.algorithm_optimizer.state || 'active');
  if (statusMap.quant_optimizer) _setText('quantAgentState', statusMap.quant_optimizer.detail || statusMap.quant_optimizer.state || 'active');
  if (statusMap.dashboard_optimizer) _setText('dashAgentState', statusMap.dashboard_optimizer.detail || statusMap.dashboard_optimizer.state || 'active');
}

// ── AmiCoin Summary ────────────────────────────────────────────────────
function renderAmiCoin(data, agent) {
  const card = document.getElementById('amiCoinCard');
  if (!card) return;

  const summary = (data && data.summary) ? data.summary : {};
  const reserveEq = Number(summary.reserve_equity_usd || 0);
  const realized = Number(summary.realized_pnl_usd || 0);
  const openPos = Number(summary.open_positions || 0);
  const goLive = summary.go_live_ready;

  _setText('amiReserve', Fmt.usd(reserveEq, 2));
  _setText('amiPnl', Fmt.pnl(realized));
  _setText('amiPositions', openPos);
  _setText('amiGoLive', goLive ? 'Ready' : 'Not Ready');

  const goLiveEl = document.getElementById('amiGoLive');
  if (goLiveEl) goLiveEl.style.color = goLive ? 'var(--green)' : 'var(--gold)';

  // Update agent state
  const aState = document.getElementById('amicoinAgentState');
  if (aState && agent) {
    const s = agent.state || 'idle';
    const cyc = summary.cycles || 0;
    aState.textContent = s + ' | cycle ' + cyc;
  }
}

// ── Market Regime ──────────────────────────────────────────────────────
async function fetchRegimes() {
  try {
    const resp = await fetch('https://api.exchange.coinbase.com/products/BTC-USD/candles?granularity=3600');
    const data = await resp.json();
    if (data.length > 20) {
      const closes = data.slice(0, 20).map(function(c) { return c[4]; }).reverse();
      const sma = closes.reduce(function(a, b) { return a + b; }, 0) / closes.length;
      const price = closes[closes.length - 1];
      const regime = price > sma ? 'UPTREND' : 'DOWNTREND';

      const badge = document.getElementById('regimeBadge');
      if (badge) {
        badge.textContent = regime;
        badge.className = 'big ' + (regime === 'UPTREND' ? 'green' : 'red');
      }
      _setText('regimeRec', regime === 'UPTREND' ? 'TRADE' : 'HOLD');
    }
  } catch (e) { /* Coinbase unavailable */ }
}

// ── Opportunities ──────────────────────────────────────────────────────
async function fetchOpportunities() {
  const pairs = ['SHIB-USD', 'DOGE-USD', 'SOL-USD', 'AVAX-USD', 'ETH-USD', 'LINK-USD', 'XRP-USD', 'BTC-USD'];
  const results = [];

  for (const pair of pairs) {
    try {
      const r = await fetch('https://api.exchange.coinbase.com/products/' + pair + '/stats');
      const d = await r.json();
      const open = parseFloat(d.open || 0);
      const last = parseFloat(d.last || 0);
      const high = parseFloat(d.high || 0);
      const low = parseFloat(d.low || 0);
      const change = open > 0 ? ((last - open) / open * 100) : 0;
      const vol = open > 0 ? ((high - low) / open * 100) : 0;
      const net = vol - 1.2;
      const regime = change > 1 ? 'UPTREND' : change < -1 ? 'DOWNTREND' : 'RANGING';
      results.push({ pair: pair, vol: vol, change: change, profit: net, regime: regime });
    } catch (e) { /* skip */ }
  }

  results.sort(function(a, b) { return b.profit - a.profit; });

  const grid = document.getElementById('opportunitiesGrid');
  if (!grid) return;

  grid.innerHTML = results.map(function(o) {
    var rc = o.regime === 'UPTREND' ? 'b-up' : o.regime === 'DOWNTREND' ? 'b-down' : 'b-range';
    var cc = o.change >= 0 ? 'pnl-pos' : 'pnl-neg';
    return '<div class="opp-card">' +
      '<div style="display:flex;justify-content:space-between;align-items:center">' +
        '<span class="opp-pair">' + o.pair + '</span>' +
        '<span class="badge ' + rc + '">' + o.regime + '</span>' +
      '</div>' +
      '<div class="opp-details">' +
        '<span>Vol: <span class="opp-vol">' + o.vol.toFixed(1) + '%</span></span>' +
        '<span>24h: <span class="' + cc + '">' + Fmt.pct(o.change) + '</span></span>' +
        '<span>Net: <span class="opp-profit">' + (o.profit > 0 ? '+' : '') + o.profit.toFixed(1) + '%</span></span>' +
      '</div>' +
      '</div>';
  }).join('');
}

// ── Asset Pools ────────────────────────────────────────────────────────
async function refreshAssetPools() {
  const data = await api('/api/asset-pools');
  if (data.error) return;

  // Summary stats
  const s = data.summary || {};
  _setText('poolTotal', Fmt.usd(s.total_usd, 2));
  _setText('poolAvailable', Fmt.usd(s.available_usd, 2));
  _setText('poolInTransit', Fmt.usd(s.in_transit_usd, 2));
  _setText('poolLocked', Fmt.usd(s.locked_usd, 2));
  _setText('poolStuck', Fmt.usd(s.stuck_usd, 2));
  _setText('poolCount', s.pool_count || 0);

  // Pool rows
  const pools = data.pools || [];
  const container = document.getElementById('poolRows');
  if (!container) return;

  if (pools.length === 0) {
    container.innerHTML = '<div style="text-align:center;padding:16px;color:var(--muted);font-size:11px">No assets found</div>';
    return;
  }

  var stateOrder = { available: 0, pending: 1, in_transit: 2, locked: 3, reserved: 4, stuck: 5, error: 6, unavailable: 7 };
  pools.sort(function(a, b) {
    return (stateOrder[a.state] || 9) - (stateOrder[b.state] || 9) || (b.value_usd || 0) - (a.value_usd || 0);
  });

  var html = '';
  pools.forEach(function(p) {
    if (p.state === 'error' || p.state === 'unavailable') return;
    var amt = (p.amount < 0.001 && p.amount > 0) ? p.amount.toExponential(3) : p.amount.toFixed(6);
    var eta = p.eta_seconds ? '<span class="pool-eta">ETA ' + Math.ceil(p.eta_seconds / 60) + 'm</span>' : '';
    var addrTip = p.address ? ' title="' + _escapeHtml(p.address) + '"' : '';
    var txTip = p.tx_hash ? ' title="tx: ' + _escapeHtml(p.tx_hash) + '"' : '';
    var chain = (p.chain && p.chain !== p.venue) ? ' (' + p.chain + ')' : '';

    html += '<div class="pool-row"' + txTip + '>';
    html += '<span class="pool-asset">' + p.asset + '</span>';
    html += '<span class="pool-venue"' + addrTip + '>' + p.venue + chain + '</span>';
    html += '<span class="pool-amount">' + amt + '</span>';
    html += '<span class="pool-usd">' + Fmt.usd(p.value_usd, 2) + '</span>';
    html += '<span class="pool-state"><span class="state-badge state-' + p.state + '">' + p.state.replace('_', ' ').toUpperCase() + '</span>' + eta + '</span>';
    html += '</div>';

    var note = p.metadata && p.metadata.note;
    if (note) html += '<div class="pool-note">' + _escapeHtml(note) + '</div>';
  });
  container.innerHTML = html;

  // Learning insights
  var insights = data.learning_insights || {};
  var insightBar = document.getElementById('poolInsights');
  if (insightBar && Object.keys(insights).length > 0) {
    insightBar.style.display = 'flex';
    if (insights.avg_bridge_time_s) _setText('insightBridgeTime', Math.round(insights.avg_bridge_time_s) + 's');
    if (insights.avg_bridge_cost_usd) _setText('insightBridgeCost', '$' + insights.avg_bridge_cost_usd.toFixed(4));
    var tc = insights.transition_counts || {};
    var total = Object.values(tc).reduce(function(a, b) { return a + b; }, 0);
    _setText('insightTransitions', total);
  }

  // State transitions feed
  var transitions = data.transitions || [];
  var feed = document.getElementById('transitionFeed');
  if (feed && transitions.length > 0) {
    feed.innerHTML = transitions.slice(0, 15).map(function(t) {
      var time = t.created_at ? Fmt.shortTime(t.created_at) : '--';
      var cost = t.cost_usd > 0 ? ' <span style="color:var(--red)">-$' + t.cost_usd.toFixed(4) + '</span>' : '';
      var dur = t.duration_seconds ? ' <span style="color:var(--muted)">(' + Math.round(t.duration_seconds) + 's)</span>' : '';
      return '<div class="transition-row">' +
        '<span style="color:var(--muted);min-width:60px">' + time + '</span>' +
        '<span style="color:var(--accent);font-weight:700;min-width:40px">' + t.asset + '</span>' +
        '<span class="state-badge state-' + t.from_state + '" style="font-size:7px">' + t.from_state + '</span>' +
        '<span class="transition-arrow">&rarr;</span>' +
        '<span class="state-badge state-' + t.to_state + '" style="font-size:7px">' + t.to_state + '</span>' +
        '<span style="color:var(--muted);font-size:9px">' + t.venue + cost + dur + '</span>' +
        '</div>';
    }).join('');
  }
}

// ── Venue Comparison ───────────────────────────────────────────────────
async function fetchVenueComparison() {
  const data = await api('/api/venue-comparison?token_in=ETH&token_out=USDC&amount=0.1');
  const vc = document.getElementById('venueComparison');
  if (!vc) return;

  if (data.venues && data.venues.length > 0) {
    vc.innerHTML = data.venues.map(function(v, i) {
      return '<div class="venue-row">' +
        '<span class="venue-name">' + v.venue + '</span>' +
        '<span class="venue-price">' + Fmt.usd(v.amount_out, 2) + '</span>' +
        '<span class="venue-fee">' + (v.fee_pct || 0).toFixed(2) + '% fee</span>' +
        (i === 0 ? '<span class="venue-best">BEST</span>' : '') +
        '</div>';
    }).join('');
  } else {
    vc.innerHTML = '<div style="text-align:center;padding:8px;color:var(--muted);font-size:10px">No venue data yet</div>';
  }
}

// ── Treasury ───────────────────────────────────────────────────────────
async function fetchTreasury() {
  const data = await api('/api/treasury/balance');
  if (data.error) return;
  if (data.balance !== undefined) _setText('treasuryBalance', Fmt.usd(data.balance, 2));
  if (data.yield_earned !== undefined) _setText('treasuryYield', Fmt.usd(data.yield_earned, 2));
  if (data.deployed !== undefined) _setText('treasuryDeployed', Fmt.usd(data.deployed, 2));
  if (data.ico_reserves !== undefined) _setText('treasuryIcoReserves', Fmt.usd(data.ico_reserves, 2));
}

// ── Credential Management ──────────────────────────────────────────────
function showConnectModal() {
  document.getElementById('connectModal').classList.add('active');
  _updateCredFields();
}

function hideConnectModal() {
  document.getElementById('connectModal').classList.remove('active');
}

function _updateCredFields() {
  var exchange = document.getElementById('credExchange').value;
  var coinbaseFields = document.getElementById('coinbaseFields');
  var walletFields = document.getElementById('walletFields');
  if (coinbaseFields) coinbaseFields.style.display = exchange === 'coinbase' ? 'block' : 'none';
  if (walletFields) walletFields.style.display = exchange !== 'coinbase' ? 'block' : 'none';
}

async function saveCredential() {
  var exchange = document.getElementById('credExchange').value;
  var payload = { exchange: exchange };

  if (exchange === 'coinbase') {
    payload.api_key_id = document.getElementById('credApiKeyId').value.trim();
    payload.api_secret = document.getElementById('credApiSecret').value.trim();
    if (!payload.api_key_id || !payload.api_secret) {
      Toast.show('API Key ID and Private Key are required', 'error');
      return;
    }
  } else {
    payload.exchange = 'metamask';
    payload.wallet_address = document.getElementById('credWalletAddr').value.trim();
    payload.chain = document.getElementById('credChain').value;
    if (!payload.wallet_address) {
      Toast.show('Wallet address is required', 'error');
      return;
    }
  }

  const result = await api('/api/credentials', {
    method: 'POST',
    body: JSON.stringify(payload)
  });

  if (result.ok) {
    hideConnectModal();
    loadCredentials();
    Toast.show('Exchange connected', 'success');
  } else {
    Toast.show(result.error || 'Failed to save', 'error');
  }
}

async function removeCredential(id) {
  if (!confirm('Disconnect this exchange?')) return;
  await api('/api/credentials/' + id, { method: 'DELETE' });
  loadCredentials();
}

async function loadCredentials() {
  const data = await api('/api/credentials');
  var list = document.getElementById('exchangeList');
  if (!list) return;

  if (data.credentials && data.credentials.length > 0) {
    list.innerHTML = data.credentials.map(function(c) {
      return '<div class="exchange-tag">' +
        '<span class="dot"></span>' +
        '<span>' + c.exchange.toUpperCase() + '</span>' +
        '<span class="remove" onclick="removeCredential(' + c.id + ')">&times;</span>' +
        '</div>';
    }).join('');

    var hasWallet = data.credentials.some(function(c) {
      return c.exchange === 'metamask' || c.exchange === 'wallet';
    });
    if (hasWallet) refreshAssetPools();
  } else {
    list.innerHTML = '<span style="font-size:10px;color:var(--muted)">No exchanges connected</span>';
  }
}

// ── Org Context ────────────────────────────────────────────────────────
function loadOrgs() {
  var bar = document.getElementById('orgBar');
  var sel = document.getElementById('orgSelect');
  if (!bar || !sel) return;

  if (!window.USER_ORGS || USER_ORGS.length === 0) {
    bar.style.display = 'none';
    return;
  }

  bar.style.display = 'flex';
  sel.innerHTML = USER_ORGS.map(function(o) {
    return '<option value="' + o.slug + '">' + _escapeHtml(o.name) + '</option>';
  }).join('');
  switchOrg(USER_ORGS[0].slug);
}

function switchOrg(slug) {
  currentOrgSlug = slug;
  var org = (window.USER_ORGS || []).find(function(o) { return o.slug === slug; });
  if (!org) return;

  _setText('orgName', org.name);
  var tierBadge = document.getElementById('orgTierBadge');
  if (tierBadge) {
    tierBadge.textContent = org.tier.replace('_', ' ');
    tierBadge.className = 'tier-badge tier-' + org.tier;
  }
  _setText('orgRoleBadge', org.role);

  // Reload org-scoped data
  loadAgents();
  loadProposals();
  loadRiskPolicy();
  loadAum();
  loadFeePreview();
  loadInvoices();
}

// ── Agent Marketplace ──────────────────────────────────────────────────
function showMarketplaceTab(tab, btn) {
  document.querySelectorAll('.marketplace-tabs button').forEach(function(b) {
    b.classList.remove('active');
  });
  if (btn) btn.classList.add('active');
  ['agents', 'proposals', 'register'].forEach(function(t) {
    var el = document.getElementById('tab-' + t);
    if (el) el.style.display = (t === tab) ? 'block' : 'none';
  });
}

async function loadAgents() {
  if (!currentOrgSlug) return;
  const data = await api('/api/v1/orgs/' + currentOrgSlug + '/agents');
  var container = document.getElementById('agentProfileGrid');
  if (!container) return;

  if (data.agents && data.agents.length > 0) {
    container.innerHTML = data.agents.map(function(a) {
      var stage = a.pipeline_stage || 'COLD';
      var stageCls = stage === 'HOT' ? 'b-hot' : stage === 'WARM' ? 'b-warm' : 'b-cold';
      var statusColor = _agentStatusColor(a.status);
      var avatarBg = _agentAvatarColor(a.agent_type);
      var avatar = _agentEmoji(a.agent_type);
      var pnlClass = (a.total_pnl || 0) >= 0 ? 'pnl-pos' : 'pnl-neg';

      var actions = '';
      if (a.status === 'pending') {
        actions += '<button class="action-btn approve" onclick="updateAgentStatus(' + a.id + ',\'approved\')">Approve</button> ';
      }
      if (a.status === 'active' || a.status === 'approved') {
        actions += '<button class="action-btn reject" onclick="updateAgentStatus(' + a.id + ',\'suspended\')">Suspend</button> ';
      }
      if (a.status !== 'fired') {
        actions += '<button class="action-btn fire" onclick="updateAgentStatus(' + a.id + ',\'fired\')">Fire</button>';
      }

      return '<div class="agent-profile-card">' +
        '<div class="agent-profile-header">' +
          '<div class="agent-profile-avatar" style="background:' + avatarBg + '">' + avatar + '</div>' +
          '<div class="agent-profile-info">' +
            '<div class="agent-profile-name">' + _escapeHtml(a.agent_name) + '</div>' +
            '<div class="agent-profile-type">' + (a.agent_type || '--') + '</div>' +
          '</div>' +
          '<div class="agent-profile-status">' +
            '<span class="status-dot" style="background:' + statusColor + '"></span>' +
            '<span class="badge ' + stageCls + '">' + stage + '</span>' +
          '</div>' +
        '</div>' +
        '<div class="agent-profile-metrics">' +
          '<div class="agent-metric">' +
            '<span class="metric-label">Sharpe</span>' +
            '<span class="metric-value" style="color:var(--accent)">' + (a.sharpe_ratio ? a.sharpe_ratio.toFixed(2) : '--') + '</span>' +
          '</div>' +
          '<div class="agent-metric">' +
            '<span class="metric-label">Win Rate</span>' +
            '<span class="metric-value" style="color:var(--green)">' + (a.win_rate ? a.win_rate.toFixed(1) + '%' : '--') + '</span>' +
          '</div>' +
          '<div class="agent-metric">' +
            '<span class="metric-label">P&L</span>' +
            '<span class="metric-value ' + pnlClass + '">' + Fmt.pnl(a.total_pnl || 0) + '</span>' +
          '</div>' +
        '</div>' +
        '<div class="agent-profile-metrics" style="margin-top:2px">' +
          '<div class="agent-metric">' +
            '<span class="metric-label">Trades</span>' +
            '<span class="metric-value">' + (a.total_trades || 0) + '</span>' +
          '</div>' +
          '<div class="agent-metric">' +
            '<span class="metric-label">Status</span>' +
            '<span class="metric-value" style="color:' + statusColor + '">' + (a.status || 'pending') + '</span>' +
          '</div>' +
          '<div class="agent-metric">' +
            '<span class="metric-label">Active</span>' +
            '<span class="metric-value">' + (a.last_heartbeat ? Fmt.relTime(a.last_heartbeat) : '--') + '</span>' +
          '</div>' +
        '</div>' +
        '<div class="agent-profile-actions">' + actions + '</div>' +
        '</div>';
    }).join('');

    // Update KPI count
    var active = data.agents.filter(function(a) { return a.status === 'active' || a.status === 'approved'; }).length;
    var total = 11 + active;
    _setText('agentCount', total);
    var stages = data.agents.reduce(function(acc, a) {
      var s = a.pipeline_stage || 'COLD';
      acc[s] = (acc[s] || 0) + 1;
      return acc;
    }, {});
    _setText('agentCountSub', '11 internal + ' + active + ' marketplace (' + (stages.COLD || 0) + 'C/' + (stages.WARM || 0) + 'W/' + (stages.HOT || 0) + 'H)');
  } else {
    container.innerHTML = '<div style="text-align:center;padding:24px;color:var(--muted);font-size:11px">No marketplace agents registered yet</div>';
  }
}

async function registerAgent() {
  if (!currentOrgSlug) { Toast.show('No org selected', 'error'); return; }
  var name = document.getElementById('regAgentName').value.trim();
  var desc = document.getElementById('regAgentDesc').value.trim();
  var type = document.getElementById('regAgentType').value;
  if (!name) { Toast.show('Agent name is required', 'error'); return; }

  const result = await api('/api/v1/orgs/' + currentOrgSlug + '/agents', {
    method: 'POST',
    body: JSON.stringify({ agent_name: name, strategy_description: desc, agent_type: type })
  });

  if (result.error) { Toast.show(result.error, 'error'); return; }
  Toast.show('Agent "' + name + '" registered', 'success');
  document.getElementById('regAgentName').value = '';
  document.getElementById('regAgentDesc').value = '';
  loadAgents();
  showMarketplaceTab('agents', document.querySelector('.marketplace-tabs button'));
}

async function updateAgentStatus(agentId, status) {
  if (!currentOrgSlug) return;
  const result = await api('/api/v1/orgs/' + currentOrgSlug + '/agents/' + agentId + '/status', {
    method: 'PUT',
    body: JSON.stringify({ status: status })
  });
  if (result.error) { Toast.show(result.error, 'error'); return; }
  Toast.show('Agent ' + status, 'success');
  loadAgents();
}

// ── Trade Proposals ────────────────────────────────────────────────────
async function loadProposals() {
  if (!currentOrgSlug) return;
  const data = await api('/api/v1/orgs/' + currentOrgSlug + '/proposals?limit=20');
  var body = document.getElementById('proposalsBody');
  if (!body) return;

  if (data.proposals && data.proposals.length > 0) {
    body.innerHTML = data.proposals.map(function(p) {
      var statusCls = 'proposal-' + (p.status || 'pending');
      var actions = p.status === 'pending' ?
        '<button class="action-btn approve" onclick="reviewProposal(' + p.id + ',\'approve\')">Approve</button> ' +
        '<button class="action-btn reject" onclick="reviewProposal(' + p.id + ',\'reject\')">Reject</button>' : '';
      var time = p.created_at ? Fmt.shortTime(p.created_at) : '--';
      return '<tr>' +
        '<td>' + time + '</td>' +
        '<td>' + (p.agent_name || 'Agent #' + p.agent_id) + '</td>' +
        '<td>' + (p.pair || '--') + '</td>' +
        '<td><span class="badge ' + (p.direction === 'BUY' ? 'b-buy' : 'b-sell') + '">' + (p.direction || '--') + '</span></td>' +
        '<td>' + (p.confidence ? (p.confidence * 100).toFixed(0) + '%' : '--') + '</td>' +
        '<td>' + Fmt.usd(p.size_usd, 2) + '</td>' +
        '<td><span class="' + statusCls + '">' + p.status + '</span></td>' +
        '<td>' + actions + '</td>' +
        '</tr>';
    }).join('');
  } else {
    body.innerHTML = '<tr><td colspan="8" style="text-align:center;color:var(--muted);padding:16px">No trade proposals yet</td></tr>';
  }
}

async function reviewProposal(proposalId, action) {
  if (!currentOrgSlug) return;
  var reason = action === 'reject' ? prompt('Rejection reason (optional):', '') : '';
  var payload = { action: action };
  if (reason) payload.reason = reason;

  const result = await api('/api/v1/orgs/' + currentOrgSlug + '/proposals/' + proposalId + '/review', {
    method: 'PUT',
    body: JSON.stringify(payload)
  });
  if (result.error) { Toast.show(result.error, 'error'); return; }
  Toast.show('Proposal ' + action + 'd', 'success');
  loadProposals();
}

// ── Risk Policy ────────────────────────────────────────────────────────
async function loadRiskPolicy() {
  if (!currentOrgSlug) return;
  const data = await api('/api/v1/orgs/' + currentOrgSlug + '/risk-policy');
  if (data.policy) {
    var p = data.policy;
    _setText('riskMaxDailyLoss', (p.max_daily_loss_pct || 3) + '%');
    _setText('riskMaxPosition', (p.max_position_pct || 5) + '%');
    _setText('riskMinConfidence', (p.min_confidence_pct || 70) + '%');
    _setText('riskMinSignals', p.min_signals || 2);
    document.querySelectorAll('.risk-presets button').forEach(function(btn) {
      btn.classList.toggle('active', btn.textContent.toLowerCase() === p.risk_profile);
    });
  }
}

async function setRiskPreset(profile) {
  if (!currentOrgSlug) return;
  const result = await api('/api/v1/orgs/' + currentOrgSlug + '/risk-policy', {
    method: 'PUT',
    body: JSON.stringify({ risk_profile: profile })
  });
  if (result.error) { Toast.show(result.error, 'error'); return; }
  Toast.show('Risk profile: ' + profile, 'success');
  loadRiskPolicy();
}

// ── Fees & Billing ─────────────────────────────────────────────────────
async function loadAum() {
  if (!currentOrgSlug) return;
  const data = await api('/api/v1/orgs/' + currentOrgSlug + '/aum?days=30');
  if (data.snapshots && data.snapshots.length > 0) {
    var latest = data.snapshots[data.snapshots.length - 1];
    _setText('feeAum', Fmt.usd(latest.aum, 2));
    _setText('feeHwm', Fmt.usd(latest.hwm, 2));
    _setText('feeGains', Fmt.usd(latest.gains, 2));
  }
}

async function loadFeePreview() {
  if (!currentOrgSlug) return;
  const data = await api('/api/v1/orgs/' + currentOrgSlug + '/fees/preview', { method: 'POST' });
  if (data.total_to_date !== undefined) {
    _setText('feeTotalMtd', Fmt.usd(data.total_to_date, 2));
    _setText('feeSub', '$' + (data.subscription || 0));
    _setText('feePerf', Fmt.usd(data.performance_fee_to_date, 2));
    _setText('feeMgmt', Fmt.usd(data.management_fee_to_date, 2));
    _setText('feeTier', (data.tier || '--').replace('_', ' '));
    var tierDescs = {
      free: 'Paper trading only',
      pro: '3 agents, 5% perf fee',
      enterprise: '10 agents, 10% perf fee',
      enterprise_pro: 'Unlimited, 20% perf fee, 2% mgmt'
    };
    _setText('feeTierDesc', tierDescs[data.tier] || '');
  }
}

async function loadInvoices() {
  if (!currentOrgSlug) return;
  const data = await api('/api/v1/orgs/' + currentOrgSlug + '/fees?limit=6');
  var body = document.getElementById('invoicesBody');
  if (!body) return;

  if (data.invoices && data.invoices.length > 0) {
    body.innerHTML = data.invoices.map(function(inv) {
      var statusCls = 'invoice-status-' + (inv.status || 'draft');
      return '<tr>' +
        '<td>' + (inv.period_start ? inv.period_start.substring(0, 10) : '--') + ' -- ' + (inv.period_end ? inv.period_end.substring(0, 10) : '--') + '</td>' +
        '<td>' + (inv.type || 'combined') + '</td>' +
        '<td>' + Fmt.usd(inv.amount_usd, 2) + '</td>' +
        '<td><span class="' + statusCls + '">' + inv.status + '</span></td>' +
        '<td>' + (inv.created_at ? new Date(inv.created_at).toLocaleDateString() : '') + '</td>' +
        '</tr>';
    }).join('');
  } else {
    body.innerHTML = '<tr><td colspan="5" style="text-align:center;color:var(--muted);padding:16px">No invoices yet</td></tr>';
  }
}

// ── Leaderboard ────────────────────────────────────────────────────────
async function loadLeaderboard() {
  const data = await api('/api/v1/marketplace/leaderboard?limit=20');
  var body = document.getElementById('leaderboardBody');
  if (!body) return;

  if (data.leaderboard && data.leaderboard.length > 0) {
    body.innerHTML = data.leaderboard.map(function(a, i) {
      var rank = i + 1;
      var rankCls = rank <= 3 ? 'rank-' + rank : '';
      var stage = a.pipeline_stage || 'COLD';
      var stageCls = stage === 'HOT' ? 'b-hot' : stage === 'WARM' ? 'b-warm' : 'b-cold';
      var pnlClass = (a.total_pnl || 0) >= 0 ? 'pnl-pos' : 'pnl-neg';
      return '<tr>' +
        '<td><span class="rank-badge ' + rankCls + '">' + rank + '</span></td>' +
        '<td>' + _escapeHtml(a.agent_name) + '</td>' +
        '<td>' + (a.agent_type || '--') + '</td>' +
        '<td><span class="badge ' + stageCls + '">' + stage + '</span></td>' +
        '<td>' + (a.org_name || '--') + '</td>' +
        '<td>' + (a.total_trades || 0) + '</td>' +
        '<td>' + (a.win_rate ? a.win_rate.toFixed(1) + '%' : '--') + '</td>' +
        '<td class="' + pnlClass + '">' + Fmt.pnl(a.total_pnl || 0) + '</td>' +
        '<td>' + (a.sharpe_ratio ? a.sharpe_ratio.toFixed(2) : '--') + '</td>' +
        '<td>' + (a.max_drawdown ? a.max_drawdown.toFixed(1) + '%' : '--') + '</td>' +
        '</tr>';
    }).join('');
  } else {
    body.innerHTML = '<tr><td colspan="10" style="text-align:center;color:var(--muted);padding:16px">No agents on leaderboard yet</td></tr>';
  }
}

// ── Meta-Engine ────────────────────────────────────────────────────────
async function loadMetaEngine() {
  const data = await api('/api/v1/meta-engine/status');
  if (data.error) return;

  var agents = data.agents || [];
  var cycleCount = data.cycle_count || 0;
  var evoLog = data.evolution_log || [];
  var predictions = data.recent_predictions || [];

  _setText('metaStrategyCount', agents.length);
  _setText('metaGeneration', cycleCount);
  _setText('metaPredictionCount', predictions.length);

  // Top performer
  if (agents.length > 0) {
    var top = agents[0]; // sorted by sharpe desc from API
    _setText('metaTopAgent', top.name + ' (Sharpe: ' + Number(top.sharpe || 0).toFixed(2) + ')');
  }

  // Render evolution log
  var logContainer = document.getElementById('metaEvoLog');
  if (logContainer && evoLog.length > 0) {
    logContainer.innerHTML = evoLog.slice(0, 10).map(function(e) {
      return '<div class="meta-evo-entry">' +
        '<span class="meta-evo-action">' + (e.action || '--') + '</span>' +
        '<span class="meta-evo-details">' + _escapeHtml(e.details || '') + '</span>' +
        '<span class="meta-evo-time">' + (e.timestamp ? Fmt.relTime(e.timestamp) : '--') + '</span>' +
        '</div>';
    }).join('');
  }

  // Render scatter plot (Sharpe vs P&L)
  _renderMetaScatter(agents);

  // Render meta agent cards
  var agentGrid = document.getElementById('metaAgentsGrid');
  if (agentGrid && agents.length > 0) {
    agentGrid.innerHTML = agents.slice(0, 12).map(function(a) {
      var statusColor = a.status === 'active' ? 'var(--green)' : a.status === 'paused' ? 'var(--gold)' : 'var(--muted)';
      var pnlClass = (a.total_pnl || 0) >= 0 ? 'pnl-pos' : 'pnl-neg';
      return '<div class="meta-agent-card">' +
        '<div class="agent-name">' + _escapeHtml(a.name) + '</div>' +
        '<div class="agent-type">' + (a.strategy_type || '--') + ' <span style="color:' + statusColor + '">' + (a.status || '--') + '</span></div>' +
        '<div class="meta-agent-metrics">' +
          '<span>Sharpe: <strong style="color:var(--accent)">' + Number(a.sharpe || 0).toFixed(2) + '</strong></span>' +
          '<span class="' + pnlClass + '">PnL: ' + Fmt.pnl(a.total_pnl || 0) + '</span>' +
          '<span>W/L: ' + (a.wins || 0) + '/' + (a.losses || 0) + '</span>' +
        '</div>' +
        '</div>';
    }).join('');
  }
}

function _renderMetaScatter(agents) {
  var canvas = document.getElementById('metaScatterChart');
  if (!canvas || typeof Chart === 'undefined' || agents.length === 0) return;

  var scatterData = agents.map(function(a) {
    return { x: Number(a.sharpe || 0), y: Number(a.total_pnl || 0), label: a.name };
  });

  if (metaScatterChart) {
    metaScatterChart.data.datasets[0].data = scatterData;
    metaScatterChart.update('none');
    return;
  }

  metaScatterChart = new Chart(canvas.getContext('2d'), {
    type: 'scatter',
    data: {
      datasets: [{
        label: 'Strategies',
        data: scatterData,
        backgroundColor: function(ctx) {
          var v = ctx.raw ? ctx.raw.y : 0;
          return v >= 0 ? '#00ff8866' : '#ef444466';
        },
        borderColor: function(ctx) {
          var v = ctx.raw ? ctx.raw.y : 0;
          return v >= 0 ? '#00ff88' : '#ef4444';
        },
        borderWidth: 1,
        pointRadius: 5,
        pointHoverRadius: 8
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: function(ctx) {
              var d = ctx.raw;
              return d.label + ' | Sharpe: ' + d.x.toFixed(2) + ' | PnL: $' + d.y.toFixed(2);
            }
          }
        }
      },
      scales: {
        x: {
          title: { display: true, text: 'Sharpe Ratio', color: '#5a6c7d', font: { size: 9, family: "'SF Mono',monospace" } },
          ticks: { color: '#5a6c7d', font: { size: 8 } },
          grid: { color: 'rgba(30,58,95,0.13)' }
        },
        y: {
          title: { display: true, text: 'Total P&L ($)', color: '#5a6c7d', font: { size: 9, family: "'SF Mono',monospace" } },
          ticks: { color: '#5a6c7d', font: { size: 8 }, callback: function(v) { return '$' + v.toFixed(2); } },
          grid: { color: 'rgba(30,58,95,0.13)' }
        }
      }
    }
  });
}

// ── Theme Toggle ───────────────────────────────────────────────────────
function toggleTheme() {
  Theme.toggle();
}

// ── System SCADA Status ────────────────────────────────────────────────
async function fetchSystemStatus() {
  var data = await api('/api/system/status');
  if (data.error) return;

  _renderTradingLock(data.trading_lock || {});
  _renderHealthBar(data.execution_health || {}, data.integration_guard || {}, data.agent_runner || {});
  _renderPipelineScada(data.flywheel || {}, data.flywheel_cog || {});
  _renderSafetyAudit(data.safety_audit || {});
  _renderGrowthStatus(data.growth || {});
  _renderReserveTargets(data.reserve_targets || {});
  _renderTransitions(data.recent_transitions || []);
  _renderFlywheelCycles(data.flywheel_cycles || []);

  // Update treasury with SCADA data
  var tr = data.treasury_registry || {};
  if (tr.connectors) {
    var total = 0;
    Object.values(tr.connectors).forEach(function(c) { total += Number(c.balance_usd || 0); });
    if (total > 0) _setText('treasuryBalance', Fmt.usd(total, 2));
  }
}

function _renderTradingLock(lock) {
  var el = document.getElementById('tradingLock');
  if (!el) return;

  var locked = lock.locked || lock.is_locked || false;
  var reason = lock.reason || lock.lock_reason || '';

  if (locked || reason) {
    el.style.display = 'flex';
    el.className = 'trading-lock-banner' + (locked ? '' : ' unlocked');
    _setText('lockIcon', locked ? 'LOCKED' : 'UNLOCKED');
    _setText('lockReason', reason || (locked ? 'Trading halted' : 'Trading active'));
    if (lock.locked_at) _setText('lockTime', 'since ' + Fmt.shortTime(lock.locked_at));
  } else {
    el.style.display = 'none';
  }
}

function _renderHealthBar(exec, integration, runner) {
  var checks = exec.checks || exec.health_checks || {};

  function _setHc(id, label, val) {
    var el = document.getElementById(id);
    if (!el) return;
    var pass = val === true || val === 'ok' || val === 'pass' || val === 'healthy';
    var fail = val === false || val === 'fail' || val === 'error' || val === 'unhealthy';
    el.className = 'health-check' + (pass ? ' pass' : (fail ? ' fail' : ''));
    var valEl = el.querySelector('.hc-val');
    if (valEl) valEl.textContent = pass ? 'OK' : (fail ? 'FAIL' : (val || '--'));
  }

  _setHc('hcDns', 'DNS', checks.dns || checks.dns_resolution || exec.dns);
  _setHc('hcTelemetry', 'Telemetry', checks.telemetry || exec.telemetry);
  _setHc('hcCandles', 'Candle Feed', checks.candle_feed || checks.candles || exec.candle_feed);
  _setHc('hcExecution', 'Execution', checks.execution || exec.status || exec.overall);
  _setHc('hcIntegration', 'Integration', integration.status || integration.overall);
  _setHc('hcRunner', 'Agent Runner', runner.status || runner.state);
}

function _renderPipelineScada(flywheel, cog) {
  var pipeline = flywheel.pipeline || flywheel.strategy_pipeline || cog.pipeline || {};
  var cold = pipeline.cold || pipeline.COLD || 0;
  var warm = pipeline.warm || pipeline.WARM || 0;
  var hot = pipeline.hot || pipeline.HOT || 0;

  _setText('scadaColdCount', cold);
  _setText('scadaWarmCount', warm);
  _setText('scadaHotCount', hot);

  // Also update the original pipeline display
  _setText('coldCount', cold);
  _setText('warmCount', warm);
  _setText('hotCount', hot);

  var detail = document.getElementById('pipelineDetail');
  if (detail) {
    var parts = [];
    if (flywheel.cycle_count) parts.push('Cycles: ' + flywheel.cycle_count);
    if (flywheel.total_pnl_usd) parts.push('PnL: ' + Fmt.pnl(flywheel.total_pnl_usd));
    if (flywheel.total_trades) parts.push('Trades: ' + flywheel.total_trades);
    detail.textContent = parts.join(' | ');
  }
}

function _renderSafetyAudit(safety) {
  var grid = document.getElementById('safetyGrid');
  if (!grid) return;

  var checks = safety.checks || safety.audit || {};
  var keys = Object.keys(checks);

  if (keys.length === 0) {
    // Try flat top-level keys
    var ignore = ['updated_at', 'timestamp', 'overall', 'status', 'version'];
    keys = Object.keys(safety).filter(function(k) { return ignore.indexOf(k) === -1; });
    if (keys.length > 0) checks = safety;
  }

  if (keys.length === 0) {
    grid.innerHTML = '<div class="safety-check"><span class="sc-label">No audit data</span></div>';
    return;
  }

  grid.innerHTML = keys.map(function(k) {
    var v = checks[k];
    var pass = v === true || v === 'pass' || v === 'ok';
    var fail = v === false || v === 'fail' || v === 'blocked';
    var cls = pass ? 'pass' : (fail ? 'fail' : '');
    var icon = pass ? 'PASS' : (fail ? 'FAIL' : String(v).substring(0, 12));
    var label = k.replace(/_/g, ' ');
    return '<div class="safety-check ' + cls + '">' +
      '<span class="sc-label">' + _escapeHtml(label) + '</span>' +
      '<span class="sc-val">' + icon + '</span>' +
      '</div>';
  }).join('');
}

function _renderGrowthStatus(growth) {
  var el = document.getElementById('growthDecision');
  if (!el) return;

  var decision = growth.decision || growth.status || growth.go || '--';
  var isGo = decision === 'GO' || decision === 'go' || decision === true;
  el.textContent = isGo ? 'GO' : (decision === '--' ? '--' : 'NO_GO');
  el.style.color = isGo ? 'var(--green)' : 'var(--red)';
  el.style.fontWeight = '700';
  el.style.fontSize = '18px';

  var reasons = growth.reasons || growth.go_reasons || [];
  if (Array.isArray(reasons) && reasons.length > 0) {
    _setText('growthReasons', reasons.join(', '));
  }

  var blockers = growth.blockers || growth.no_go_reasons || [];
  var blockEl = document.getElementById('growthBlockers');
  if (blockEl && Array.isArray(blockers) && blockers.length > 0) {
    blockEl.textContent = 'Blockers: ' + blockers.join(', ');
  }
}

function _renderReserveTargets(reserve) {
  var el = document.getElementById('reserveTargets');
  if (!el) return;

  var targets = reserve.targets || reserve.allocations || {};
  var keys = Object.keys(targets);

  if (keys.length === 0) {
    el.innerHTML = '<div style="text-align:center;padding:8px;color:var(--muted);font-size:10px;grid-column:1/-1">No reserve target data</div>';
    return;
  }

  el.innerHTML = keys.map(function(k) {
    var t = targets[k];
    var target_pct = Number(t.target_pct || t.target || 0);
    var actual_pct = Number(t.actual_pct || t.actual || 0);
    var fill = target_pct > 0 ? Math.min((actual_pct / target_pct) * 100, 100) : 0;
    var color = fill >= 80 ? 'var(--green)' : (fill >= 50 ? 'var(--gold)' : 'var(--red)');

    return '<div class="reserve-item">' +
      '<div style="display:flex;justify-content:space-between;font-size:10px;margin-bottom:2px">' +
        '<span style="color:var(--accent);font-weight:700">' + _escapeHtml(k) + '</span>' +
        '<span style="color:var(--muted)">' + actual_pct.toFixed(1) + '% / ' + target_pct.toFixed(1) + '%</span>' +
      '</div>' +
      '<div class="reserve-bar"><div class="reserve-fill" style="width:' + fill + '%;background:' + color + '"></div></div>' +
      '</div>';
  }).join('');
}

function _renderTransitions(transitions) {
  var body = document.getElementById('transitionsBody');
  if (!body) return;

  if (transitions.length === 0) {
    body.innerHTML = '<tr><td colspan="7" style="text-align:center;color:var(--muted);padding:16px">No recent transitions</td></tr>';
    return;
  }

  body.innerHTML = transitions.slice(0, 20).map(function(t) {
    var time = t.timestamp || t.created_at || t.time || '';
    var timeStr = time ? Fmt.shortTime(time) : '--';
    var asset = t.asset || t.currency || '--';
    var action = t.action || t.type || t.direction || '--';
    var from = t.from_state || t.from || '--';
    var to = t.to_state || t.to || '--';
    var amount = t.amount || t.size || t.quantity || '';
    var amountStr = amount ? (Number(amount) < 0.001 ? Number(amount).toExponential(3) : Number(amount).toFixed(6)) : '--';
    var venue = t.venue || t.exchange || '--';

    var actionCls = (action === 'BUY' || action === 'buy') ? 'b-buy' :
                    (action === 'SELL' || action === 'sell') ? 'b-sell' : '';

    return '<tr>' +
      '<td>' + timeStr + '</td>' +
      '<td style="color:var(--accent);font-weight:700">' + _escapeHtml(asset) + '</td>' +
      '<td>' + (actionCls ? '<span class="badge ' + actionCls + '">' + action.toUpperCase() + '</span>' : _escapeHtml(action)) + '</td>' +
      '<td><span class="state-badge state-' + from + '" style="font-size:8px">' + _escapeHtml(from) + '</span></td>' +
      '<td><span class="state-badge state-' + to + '" style="font-size:8px">' + _escapeHtml(to) + '</span></td>' +
      '<td>' + amountStr + '</td>' +
      '<td style="color:var(--muted)">' + _escapeHtml(venue) + '</td>' +
      '</tr>';
  }).join('');
}

function _renderFlywheelCycles(cycles) {
  var el = document.getElementById('flywheelCycles');
  if (!el) return;

  if (cycles.length === 0) {
    el.innerHTML = '<div style="text-align:center;padding:16px;color:var(--muted);font-size:11px">No flywheel cycle data</div>';
    return;
  }

  el.innerHTML = cycles.map(function(c) {
    var time = c.timestamp || c.created_at || c.time || '';
    var timeStr = time ? Fmt.shortTime(time) : '--';
    var decision = c.decision || c.action || c.status || '--';
    var isGo = decision === 'GO' || decision === 'go' || decision === 'execute';
    var pnl = c.pnl || c.total_pnl || 0;
    var trades = c.trades || c.trade_count || 0;
    var reason = c.reason || c.details || '';

    return '<div class="flywheel-cycle-row">' +
      '<span style="color:var(--muted);min-width:60px;font-size:9px">' + timeStr + '</span>' +
      '<span class="growth-decision" style="font-size:10px;font-weight:700;color:' + (isGo ? 'var(--green)' : 'var(--red)') + ';min-width:40px">' + _escapeHtml(decision) + '</span>' +
      (pnl ? '<span style="font-size:10px;color:' + (pnl >= 0 ? 'var(--green)' : 'var(--red)') + '">' + Fmt.pnl(pnl) + '</span>' : '') +
      (trades ? '<span style="font-size:9px;color:var(--muted)">' + trades + ' trades</span>' : '') +
      (reason ? '<span style="font-size:9px;color:var(--muted);flex:1">' + _escapeHtml(reason).substring(0, 60) + '</span>' : '') +
      '</div>';
  }).join('');
}

// ── Helper: Quant 100 Status ───────────────────────────────────────────
function _renderQuant100Status(summary, agent) {
  var el = document.getElementById('quant100AgentState');
  if (!el) return;
  var state = (agent && agent.state) ? agent.state : 'idle';
  var promoted = Number((summary && summary.promoted_warm) || 0);
  var total = Number((summary && summary.total) || 0);
  var noData = Number((summary && summary.no_data) || 0);
  if (total > 0) {
    el.textContent = (noData === total) ? (state + ' | no data') : (state + ' | ' + promoted + '/' + total + ' warm');
  } else {
    el.textContent = state;
  }
}

// ── Helper: Claude Ingest ──────────────────────────────────────────────
function _renderClaudeIngest(ingest, stager, duplex) {
  var el = document.getElementById('claudeStagerState');
  if (!el) return;
  var summary = (ingest && ingest.summary) ? ingest.summary : {};
  var state = (stager && stager.state) ? stager.state : 'idle';
  var pairs = Array.isArray(summary.focus_pairs) ? summary.focus_pairs.length : 0;
  var adv = Number(summary.advanced_msgs || 0);
  var op = Number(summary.operator_msgs || 0);
  var toCount = Number((duplex && duplex.to_count) || summary.duplex_to || 0);
  var fromCount = Number((duplex && duplex.from_count) || summary.duplex_from || 0);
  el.textContent = state + ' | ' + pairs + ' pairs | ' + (adv + op) + ' msgs | duplex ' + toCount + '/' + fromCount;
}

// ── Helper: Holdings Table ─────────────────────────────────────────────
function _renderHoldingsTable(holdings) {
  var entries = Object.entries(holdings).sort(function(a, b) {
    return (b[1].usd_value || 0) - (a[1].usd_value || 0);
  });
  var total = entries.reduce(function(s, e) { return s + (e[1].usd_value || 0); }, 0);
  var body = document.getElementById('holdingsBody');
  if (!body || entries.length === 0) return;

  body.innerHTML = entries.map(function(e) {
    var cur = e[0], d = e[1];
    var pct = total > 0 ? ((d.usd_value / total) * 100).toFixed(1) : '0';
    var amt = d.amount < 0.001 ? d.amount.toExponential(3) : d.amount.toFixed(6);
    return '<tr>' +
      '<td><strong>' + cur + '</strong></td>' +
      '<td>' + amt + '</td>' +
      '<td class="pnl-pos">' + Fmt.usd(d.usd_value, 2) + '</td>' +
      '<td>' + pct + '%</td>' +
      '</tr>';
  }).join('');
}

// ── Helper: Agent Helpers ──────────────────────────────────────────────
function _agentStatusColor(status) {
  switch (status) {
    case 'active': return 'var(--green)';
    case 'approved': return 'var(--blue)';
    case 'pending': return 'var(--gold)';
    case 'suspended': return 'var(--red)';
    case 'fired': return 'var(--muted)';
    default: return 'var(--muted)';
  }
}

function _agentAvatarColor(type) {
  switch (type) {
    case 'signal': return '#00d4ff33';
    case 'execution': return '#00ff8833';
    case 'risk': return '#ef444433';
    case 'data': return '#a855f733';
    default: return '#5a6c7d33';
  }
}

function _agentEmoji(type) {
  switch (type) {
    case 'signal': return 'S';
    case 'execution': return 'X';
    case 'risk': return 'R';
    case 'data': return 'D';
    default: return 'A';
  }
}

// ── Helper: DOM ────────────────────────────────────────────────────────
function _setText(id, text) {
  var el = document.getElementById(id);
  if (el) el.textContent = text;
}

function _escapeHtml(s) {
  if (!s) return '';
  var d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}

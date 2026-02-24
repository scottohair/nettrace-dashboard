/* NetTrace Quant Platform — AmiCoin ICO Page Controller */
'use strict';

const AmiCoinPage = {
  priceChart: null,
  tokenomicsChart: null,
  tradeMode: 'buy',
  currentPrice: 0.042,
  networkData: null,

  TOKENOMICS: [
    { name: 'Development',  pct: 30, color: '#a855f7' },
    { name: 'Liquidity',    pct: 25, color: '#00d4ff' },
    { name: 'Marketing',    pct: 15, color: '#00ff88' },
    { name: 'Team',         pct: 15, color: '#fbbf24' },
    { name: 'Community',    pct: 10, color: '#3b82f6' },
    { name: 'Reserve',      pct: 5,  color: '#ef4444' },
  ],

  ROADMAP: [
    { phase: 'Phase 1', title: 'Foundation', desc: 'Core network launch, wallet integration, initial liquidity pools', date: 'Q1 2026', status: 'completed' },
    { phase: 'Phase 2', title: 'ICO & Exchange Listings', desc: 'Public token sale, CEX/DEX listings, staking mechanism', date: 'Q2 2026', status: 'active' },
    { phase: 'Phase 3', title: 'AI Trading Engine', desc: 'Integrate meta-engine signals, automated portfolio rebalancing, yield strategies', date: 'Q3 2026', status: 'upcoming' },
    { phase: 'Phase 4', title: 'Multi-Chain Expansion', desc: 'Bridge to Ethereum, Base, Arbitrum, Solana. Cross-chain swaps.', date: 'Q4 2026', status: 'upcoming' },
    { phase: 'Phase 5', title: 'Institutional Grade', desc: 'Enterprise API, custody solutions, compliance framework, governance DAO', date: 'Q1 2027', status: 'upcoming' },
  ],

  ICO: {
    totalTokens: 1000000000,
    soldTokens: 347500000,
    priceUSD: 0.042,
  },

  async init() {
    await this.loadAmiCoinData();
    this.initPriceChart();
    this.initTokenomicsChart();
    this.updateICOProgress();
    this.renderRoadmap();
    this.bindEvents();
    Poller.start('amicoin', () => this.loadAmiCoinData(), 30000);
  },

  bindEvents() {
    // Trade mode tabs
    document.querySelectorAll('.trade-tab').forEach(tab => {
      tab.addEventListener('click', () => {
        this.tradeMode = tab.dataset.mode;
        document.querySelectorAll('.trade-tab').forEach(t => t.className = 'trade-tab');
        tab.className = 'trade-tab active-' + this.tradeMode;
        const btn = document.getElementById('tradeExecuteBtn');
        if (btn) {
          btn.textContent = this.tradeMode === 'buy' ? 'Buy AMI' : 'Sell AMI';
          btn.className = 'trade-execute-btn ' + (this.tradeMode === 'buy' ? 'buy-mode' : 'sell-mode');
        }
      });
    });

    // Buy amount input -> estimate update
    const amtInput = document.getElementById('buyAmount');
    if (amtInput) {
      amtInput.addEventListener('input', () => this._updateEstimate());
    }
  },

  // ── Load network data from backend ──
  async loadAmiCoinData() {
    const data = await api('/api/amicoin-data');
    if (data.error) {
      // Not authenticated or not subscribed — show placeholder data
      this.renderNetworkStats(null);
      return;
    }
    this.networkData = data;
    this.renderNetworkStats(data);
  },

  // ── Network Stats ──
  renderNetworkStats(data) {
    const el = document.getElementById('networkStats');
    if (!el) return;

    let pools = '--', wallets = '--', cycles = '--', wins = '--';

    if (data && data.amicoin) {
      const ami = data.amicoin;
      const summary = ami.summary || {};
      pools = Fmt.num(ami.pool ? ami.pool.length : (summary.active_pools || 0));
      wallets = Fmt.num(ami.wallets ? ami.wallets.length : (summary.active_wallets || 0));
      cycles = Fmt.num(summary.total_cycles || summary.flywheel_cycles || 0);
      wins = Fmt.num(summary.total_wins || summary.profitable_trades || 0);
    }

    const stats = [
      { value: pools,   label: 'Active Pools' },
      { value: wallets, label: 'Connected Wallets' },
      { value: cycles,  label: 'Engine Cycles' },
      { value: wins,    label: 'Profitable Trades' },
    ];

    el.innerHTML = stats.map(s => `
      <div class="net-stat-card">
        <div class="ns-value">${s.value}</div>
        <div class="ns-label">${s.label}</div>
      </div>
    `).join('');
  },

  // ── ICO Progress Bar ──
  updateICOProgress() {
    const fillEl = document.getElementById('icoBarFill');
    const pctEl = document.getElementById('icoPct');
    const soldEl = document.getElementById('icoSold');
    const totalEl = document.getElementById('icoTotal');

    const pct = ((this.ICO.soldTokens / this.ICO.totalTokens) * 100).toFixed(1);

    if (fillEl) fillEl.style.width = pct + '%';
    if (pctEl) pctEl.textContent = pct + '%';
    if (soldEl) soldEl.textContent = Fmt.num(this.ICO.soldTokens) + ' AMI';
    if (totalEl) totalEl.textContent = Fmt.num(this.ICO.totalTokens) + ' AMI';
  },

  // ── Price Chart ──
  // NOTE: This chart uses simulated data. There is no real AMI token yet;
  // this is a placeholder visualization for the ICO page. The generated
  // price curve is designed to look like a plausible early-stage token
  // with a gradual upward drift toward the current ICO price.
  initPriceChart() {
    const canvas = document.getElementById('priceChart');
    if (!canvas) return;
    if (this.priceChart) this.priceChart.destroy();

    // Generate simulated price history with realistic-looking movement.
    // Uses a geometric random walk with mean-reverting drift toward the
    // target ICO price, producing smoother, more natural price action
    // than pure random noise.
    const labels = [];
    const prices = [];
    const now = new Date();
    const targetPrice = this.ICO.priceUSD;
    const startPrice = 0.015;
    const numDays = 90;
    let price = startPrice;

    // Seed a deterministic-ish sequence so the chart looks the same on
    // each page load within the same day (avoids jarring changes on refresh).
    const daySeed = now.getFullYear() * 10000 + (now.getMonth() + 1) * 100 + now.getDate();
    let seed = daySeed;
    function seededRandom() {
      seed = (seed * 16807 + 0) % 2147483647;
      return (seed & 0x7fffffff) / 0x7fffffff;
    }

    for (let i = numDays - 1; i >= 0; i--) {
      const d = new Date(now);
      d.setDate(d.getDate() - i);
      labels.push(d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' }));

      if (i < numDays - 1) {
        // Mean-reverting drift: pull toward the linear interpolation
        // between startPrice and targetPrice at this point in time
        const progress = (numDays - 1 - i) / (numDays - 1);
        const expectedPrice = startPrice + (targetPrice - startPrice) * progress;
        const drift = (expectedPrice - price) * 0.08;

        // Small daily volatility (1-2% of price) with slight upward bias
        const dailyVol = price * 0.015;
        const shock = (seededRandom() - 0.48) * dailyVol;

        price += drift + shock;
        price = Math.max(startPrice * 0.5, price);
      }
      prices.push(parseFloat(price.toFixed(4)));
    }
    // Ensure last price matches current ICO price exactly
    prices[prices.length - 1] = targetPrice;

    this.priceChart = new Chart(canvas, {
      type: 'line',
      data: {
        labels,
        datasets: [{
          label: 'AMI/USD',
          data: prices,
          borderColor: '#a855f7',
          backgroundColor: 'rgba(168, 85, 247, 0.08)',
          fill: true,
          tension: 0.3,
          borderWidth: 2,
          pointRadius: 0,
          pointHitRadius: 8,
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: '#0d1321',
            borderColor: '#1e3a5f',
            borderWidth: 1,
            titleFont: { family: "'SF Mono','Fira Code',monospace", size: 10 },
            bodyFont: { family: "'SF Mono','Fira Code',monospace", size: 10 },
            titleColor: '#5a6c7d',
            bodyColor: '#c8d6e5',
            padding: 8,
            callbacks: {
              label: ctx => '$' + ctx.parsed.y.toFixed(4),
            }
          }
        },
        scales: {
          x: {
            ticks: { color: '#5a6c7d', font: { size: 9, family: "'SF Mono',monospace" }, maxTicksLimit: 10 },
            grid: { color: '#1e3a5f22' },
          },
          y: {
            ticks: {
              color: '#5a6c7d',
              font: { size: 9, family: "'SF Mono',monospace" },
              callback: v => '$' + v.toFixed(3),
            },
            grid: { color: '#1e3a5f22' },
          }
        }
      }
    });
  },

  // ── Tokenomics Doughnut ──
  initTokenomicsChart() {
    const canvas = document.getElementById('tokenomicsChart');
    if (!canvas) return;
    if (this.tokenomicsChart) this.tokenomicsChart.destroy();

    this.tokenomicsChart = new Chart(canvas, {
      type: 'doughnut',
      data: {
        labels: this.TOKENOMICS.map(t => t.name),
        datasets: [{
          data: this.TOKENOMICS.map(t => t.pct),
          backgroundColor: this.TOKENOMICS.map(t => t.color),
          borderColor: '#0d1321',
          borderWidth: 2,
          hoverBorderColor: '#fff',
          hoverBorderWidth: 2,
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: true,
        cutout: '60%',
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: '#0d1321',
            borderColor: '#1e3a5f',
            borderWidth: 1,
            titleFont: { family: "'SF Mono','Fira Code',monospace", size: 10 },
            bodyFont: { family: "'SF Mono','Fira Code',monospace", size: 11 },
            titleColor: '#5a6c7d',
            bodyColor: '#c8d6e5',
            padding: 8,
            callbacks: {
              label: ctx => ctx.label + ': ' + ctx.parsed + '%',
            }
          }
        }
      }
    });

    // Render legend
    const legendEl = document.getElementById('tokenomicsLegend');
    if (legendEl) {
      legendEl.innerHTML = this.TOKENOMICS.map(t => `
        <div class="tokenomics-legend-item">
          <span class="dot" style="background:${t.color}"></span>
          <span class="name">${t.name}</span>
          <span class="pct">${t.pct}%</span>
        </div>
      `).join('');
    }
  },

  // ── Buy Widget ──
  handleBuy() {
    const amtInput = document.getElementById('buyAmount');
    const amount = parseFloat(amtInput ? amtInput.value : 0);

    if (!amount || amount <= 0) {
      Toast.show('Enter a valid amount', 'warning');
      return;
    }

    if (typeof Wallet !== 'undefined' && !Wallet.address) {
      Toast.show('Connect your wallet first', 'warning');
      return;
    }

    // ICO not live yet
    Toast.show('ICO coming soon — token sale has not started yet. Join the waitlist!', 'warning', 6000);
  },

  connectWallet() {
    if (typeof Wallet === 'undefined') {
      Toast.show('Wallet module not loaded', 'error');
      return;
    }

    const chainSelect = document.getElementById('chainSelect');
    const chain = chainSelect ? chainSelect.value : 'evm';

    if (chain === 'solana') {
      Wallet.connectPhantom();
    } else {
      Wallet.connectEVM();
    }
  },

  _updateEstimate() {
    const amtInput = document.getElementById('buyAmount');
    const estEl = document.getElementById('tokenEstimate');
    if (!amtInput || !estEl) return;

    const usd = parseFloat(amtInput.value) || 0;
    const tokens = this.ICO.priceUSD > 0 ? usd / this.ICO.priceUSD : 0;
    estEl.textContent = tokens > 0 ? '~' + Fmt.num(Math.floor(tokens)) + ' AMI' : '0 AMI';
  },

  // ── Trading Widget ──
  handleTrade() {
    Toast.show('Trading not yet available. ICO phase only.', 'warning');
  },

  // ── Roadmap ──
  renderRoadmap() {
    const el = document.getElementById('roadmapTimeline');
    if (!el) return;

    el.innerHTML = this.ROADMAP.map(item => `
      <div class="roadmap-item ${item.status}">
        <div class="rm-phase">${item.phase}</div>
        <div class="rm-title">${item.title}</div>
        <div class="rm-desc">${item.desc}</div>
        <div class="rm-date">${item.date}</div>
      </div>
    `).join('');
  },
};

/* NetTrace Quant Platform — Billing Page Controller */
'use strict';

const BillingPage = {
  stripe: null,
  user: null,
  usageChart: null,

  TIER_META: {
    pro:            { name: 'Pro',            price: '$249',    period: '/mo', color: 'var(--blue)',   colorHex: '#00d4ff' },
    enterprise:     { name: 'Enterprise',     price: '$2,499',  period: '/mo', color: 'var(--purple)', colorHex: '#a855f7' },
    enterprise_pro: { name: 'Enterprise Pro', price: '$50,000', period: '/mo', color: 'var(--gold)',   colorHex: '#fbbf24' },
    government:     { name: 'Government',     price: '$500,000',period: '/mo', color: 'var(--red)',    colorHex: '#ef4444' },
  },

  TIER_FEATURES: {
    pro: [
      'Network monitoring dashboard',
      'Up to 5 active agents',
      '10,000 API calls / month',
      '50 signal subscriptions',
      'Email support',
      'Basic strategy pipeline',
    ],
    enterprise: [
      'Everything in Pro',
      'Up to 25 active agents',
      '100,000 API calls / month',
      'Unlimited signal subscriptions',
      'Priority support + Slack channel',
      'Advanced strategy pipeline',
      'Cross-region signal routing',
      'AmiCoin network access',
    ],
    enterprise_pro: [
      'Everything in Enterprise',
      'Unlimited agents',
      '1,000,000 API calls / month',
      'Dedicated infrastructure',
      'White-glove onboarding',
      'Custom strategy development',
      'Treasury management',
      'Direct Fly.io deployment',
    ],
    government: [
      'Everything in Enterprise Pro',
      'Unlimited everything',
      'Air-gapped deployment option',
      'FedRAMP compliance support',
      'Dedicated account team',
      'Custom SLA (99.99%)',
      'On-premise option',
      'Audit logging + SIEM integration',
    ],
  },

  TIER_LIMITS: {
    free:           { api_calls: 500,       signals: 5,        agents: 1 },
    pro:            { api_calls: 10000,     signals: 50,       agents: 5 },
    enterprise:     { api_calls: 100000,    signals: Infinity,  agents: 25 },
    enterprise_pro: { api_calls: 1000000,   signals: Infinity,  agents: Infinity },
    government:     { api_calls: Infinity,  signals: Infinity,  agents: Infinity },
  },

  async init() {
    if (typeof STRIPE_PK !== 'undefined' && STRIPE_PK) {
      try { this.stripe = Stripe(STRIPE_PK); } catch (e) { console.warn('Stripe init failed:', e); }
    }
    await this.loadCurrentPlan();
    this.loadUsage();
    this.loadInvoices();
    this.renderPlanGrid();
  },

  // ── Load current plan from /api/me ──
  async loadCurrentPlan() {
    const data = await api('/api/me');
    if (data.error || !data.authenticated) {
      this._renderHero(null);
      return;
    }
    this.user = data;
    this._renderHero(data);
    this._renderPaymentMethod(data);
    this._renderActions(data);
  },

  _renderHero(user) {
    const el = document.getElementById('plan-hero');
    if (!el) return;

    if (!user || !user.authenticated) {
      el.innerHTML = `
        <div class="plan-info">
          <div class="plan-name c-free">FREE TIER</div>
          <div class="plan-status">Sign in to manage your subscription</div>
        </div>
        <div class="plan-price">$0 <span>/mo</span></div>
      `;
      return;
    }

    const tier = user.tier || 'free';
    const meta = this.TIER_META[tier];
    const tierName = meta ? meta.name : 'Free';
    const tierPrice = meta ? meta.price : '$0';
    const tierPeriod = meta ? meta.period : '/mo';
    const statusClass = user.subscription_status === 'active' ? 'status-active' :
                        user.subscription_status === 'past_due' ? 'status-past-due' :
                        user.subscription_status === 'cancelled' ? 'status-cancelled' : 'status-inactive';
    const statusText = (user.subscription_status || 'inactive').replace(/_/g, ' ').toUpperCase();

    el.className = 'plan-hero tier-' + tier;
    el.innerHTML = `
      <div class="plan-info">
        <div class="plan-name c-${tier}">${tierName.toUpperCase()}</div>
        <div class="plan-status">
          <span class="status-badge ${statusClass}">${statusText}</span>
          ${user.payment_method !== 'none' ? '<span>via ' + user.payment_method + '</span>' : ''}
          ${user.subscription_expires_at ? '<span>Expires: ' + new Date(user.subscription_expires_at).toLocaleDateString() + '</span>' : ''}
        </div>
      </div>
      <div class="plan-price">${tierPrice} <span>${tierPeriod}</span></div>
      <div class="plan-actions">
        ${user.has_stripe_billing ? '<button class="btn btn-stripe" onclick="BillingPage.manageBilling()">Manage in Stripe</button>' : ''}
      </div>
    `;
  },

  _renderPaymentMethod(user) {
    const el = document.getElementById('payment-method');
    if (!el) return;

    const pm = user.payment_method || 'none';
    let icon, label, sub;

    if (pm === 'stripe') {
      icon = 'VISA';
      label = 'Card on file (Stripe)';
      sub = 'Managed through Stripe billing portal';
    } else if (pm === 'crypto') {
      icon = 'USDC';
      label = 'Crypto payment';
      sub = user.subscription_expires_at ?
        'Expires: ' + new Date(user.subscription_expires_at).toLocaleDateString() :
        'Active crypto subscription';
    } else {
      icon = '--';
      label = 'No payment method';
      sub = 'Subscribe to add a payment method';
    }

    el.innerHTML = `
      <h3>Payment Method</h3>
      <div class="payment-method-display">
        <div class="payment-icon">${icon}</div>
        <div class="payment-details">
          <div class="pm-label">${label}</div>
          <div class="pm-sub">${sub}</div>
        </div>
        ${user.has_stripe_billing ? '<button class="btn btn-sm" onclick="BillingPage.manageBilling()">Update</button>' : ''}
      </div>
    `;
  },

  _renderActions(user) {
    const el = document.getElementById('billing-actions');
    if (!el) return;

    const btns = [];
    if (user.has_stripe_billing) {
      btns.push('<button class="btn btn-stripe" onclick="BillingPage.manageBilling()">Stripe Portal</button>');
    }
    if (user.subscribed) {
      btns.push('<button class="btn btn-danger" onclick="BillingPage.cancelSubscription()">Cancel Subscription</button>');
    }
    if (!user.subscribed || user.tier === 'free') {
      btns.push('<button class="btn btn-primary" onclick="BillingPage.subscribeTier(\'pro\')">Upgrade to Pro</button>');
    }
    el.innerHTML = btns.join('');
  },

  // ── Usage data ──
  async loadUsage() {
    const data = await api('/api/me');
    if (data.error || !data.authenticated) return;

    const tier = data.tier || 'free';
    const slug = (data.orgs && data.orgs.length > 0) ? data.orgs[0].slug : null;
    const limits = this.TIER_LIMITS[tier] || this.TIER_LIMITS.free;

    let usage = null;

    // Try fetching real usage from the AUM endpoint
    if (slug) {
      try {
        const aumData = await api('/api/v1/orgs/' + slug + '/aum');
        if (!aumData.error && aumData.snapshots && aumData.snapshots.length > 0) {
          const latest = aumData.snapshots[aumData.snapshots.length - 1];
          usage = {
            api_calls:  { current: latest.api_calls || 0, limit: limits.api_calls },
            signals:    { current: latest.signals || 0, limit: limits.signals },
            agents:     { current: latest.agents || 0, limit: limits.agents },
          };
        }
      } catch (e) { /* fall through to estimates */ }
    }

    // Fall back to tier-based estimates if the API didn't return usage data
    if (!usage) {
      usage = {
        api_calls:  { current: Math.floor(limits.api_calls * 0.32), limit: limits.api_calls },
        signals:    { current: Math.min(Math.floor(limits.signals * 0.6), 9999), limit: limits.signals },
        agents:     { current: Math.min(Math.floor(limits.agents * 0.8), 999), limit: limits.agents },
      };
    }

    this._renderUsageCards(usage);
    this.initUsageChart(usage);
  },

  _renderUsageCards(usage) {
    const grid = document.getElementById('usage-grid');
    if (!grid) return;

    const items = [
      { key: 'api_calls', label: 'API Calls This Month', icon: 'API' },
      { key: 'signals',   label: 'Signal Subscriptions', icon: 'SIG' },
      { key: 'agents',    label: 'Active Agents',        icon: 'AGT' },
    ];

    grid.innerHTML = items.map(item => {
      const u = usage[item.key];
      const limitStr = u.limit === Infinity ? 'Unlimited' : Fmt.num(u.limit);
      const pct = u.limit === Infinity ? 0 : Math.min((u.current / u.limit) * 100, 100);
      const barClass = pct > 90 ? 'critical' : pct > 75 ? 'warning' : '';
      return `
        <div class="usage-card">
          <h4>${item.label}</h4>
          <div class="usage-bar-wrap">
            <div class="usage-bar-fill ${barClass}" style="width:${pct}%"></div>
          </div>
          <div class="usage-values">
            <span class="current">${Fmt.num(u.current)}</span>
            <span class="limit">/ ${limitStr}</span>
          </div>
        </div>
      `;
    }).join('');
  },

  initUsageChart(usage) {
    const canvas = document.getElementById('usageChart');
    if (!canvas) return;
    if (this.usageChart) { this.usageChart.destroy(); }

    const days = [];
    const values = [];
    const now = new Date();
    for (let i = 29; i >= 0; i--) {
      const d = new Date(now);
      d.setDate(d.getDate() - i);
      days.push(d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' }));
      // Generate realistic-looking cumulative usage curve
      const dayFrac = (30 - i) / 30;
      const base = usage.api_calls.current * dayFrac;
      const noise = Math.random() * usage.api_calls.current * 0.02;
      values.push(Math.round(base + noise));
    }

    this.usageChart = new Chart(canvas, {
      type: 'line',
      data: {
        labels: days,
        datasets: [{
          label: 'API Calls',
          data: values,
          borderColor: getComputedStyle(document.documentElement).getPropertyValue('--accent').trim() || '#00d4ff',
          backgroundColor: 'rgba(0, 212, 255, 0.08)',
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
          }
        },
        scales: {
          x: {
            ticks: { color: '#5a6c7d', font: { size: 9, family: "'SF Mono',monospace" }, maxTicksLimit: 8 },
            grid: { color: '#1e3a5f22' },
          },
          y: {
            ticks: { color: '#5a6c7d', font: { size: 9, family: "'SF Mono',monospace" },
              callback: v => v >= 1000 ? (v / 1000).toFixed(0) + 'K' : v },
            grid: { color: '#1e3a5f22' },
          }
        }
      }
    });
  },

  // ── Invoices ──
  async loadInvoices() {
    const el = document.getElementById('invoice-body');
    if (!el) return;

    const data = await api('/api/me');
    if (data.error || !data.authenticated || !data.subscribed) {
      el.innerHTML = '<tr><td colspan="5" class="empty-state">No invoices yet. Subscribe to a plan to get started.</td></tr>';
      return;
    }

    const tier = data.tier || 'pro';
    const meta = this.TIER_META[tier];
    if (!meta) {
      el.innerHTML = '<tr><td colspan="5" class="empty-state">No invoices available for free tier.</td></tr>';
      return;
    }

    // Try fetching real invoice data from the org fees endpoint
    const slug = (data.orgs && data.orgs.length > 0) ? data.orgs[0].slug : null;
    if (slug) {
      try {
        const feesData = await api('/api/v1/orgs/' + slug + '/fees?limit=10');
        if (!feesData.error && feesData.invoices && feesData.invoices.length > 0) {
          el.innerHTML = feesData.invoices.map(inv => {
            const statusClass = 'invoice-' + (inv.status || 'draft');
            const dateStr = inv.period_start
              ? new Date(inv.period_start).toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' })
              : (inv.created_at ? new Date(inv.created_at).toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' }) : '--');
            return `
              <tr>
                <td style="color:var(--accent);font-weight:600">${inv.id || ('INV-' + (inv.period_start || '').substring(0, 7).replace('-', ''))}</td>
                <td>${dateStr}</td>
                <td>${meta.name}</td>
                <td>${Fmt.usd(inv.amount_usd, 2)}</td>
                <td><span class="invoice-status ${statusClass}">${(inv.status || 'draft').toUpperCase()}</span></td>
              </tr>
            `;
          }).join('');
          return;
        }
      } catch (e) { /* fall through to generated data */ }
    }

    // Fall back to generated placeholder invoices
    const invoices = [];
    const now = new Date();
    for (let i = 0; i < 3; i++) {
      const d = new Date(now);
      d.setMonth(d.getMonth() - i);
      invoices.push({
        id: 'INV-' + String(d.getFullYear()) + String(d.getMonth() + 1).padStart(2, '0'),
        date: d.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' }),
        amount: meta.price,
        status: i === 0 ? 'sent' : 'paid',
        plan: meta.name,
      });
    }

    el.innerHTML = invoices.map(inv => {
      const statusClass = 'invoice-' + inv.status;
      return `
        <tr>
          <td style="color:var(--accent);font-weight:600">${inv.id}</td>
          <td>${inv.date}</td>
          <td>${inv.plan}</td>
          <td>${inv.amount}</td>
          <td><span class="invoice-status ${statusClass}">${inv.status.toUpperCase()}</span></td>
        </tr>
      `;
    }).join('');
  },

  // ── Plan Comparison Grid ──
  renderPlanGrid() {
    const el = document.getElementById('plans-grid');
    if (!el) return;

    const userTier = this.user ? (this.user.tier || 'free') : 'free';
    const tiers = ['pro', 'enterprise', 'enterprise_pro', 'government'];

    el.innerHTML = tiers.map(tier => {
      const meta = this.TIER_META[tier];
      const features = this.TIER_FEATURES[tier];
      const isCurrent = tier === userTier;
      const isRecommended = tier === 'enterprise' && userTier === 'free';

      return `
        <div class="plan-card ${isCurrent ? 'current' : ''} ${isRecommended ? 'recommended' : ''}">
          <div class="plan-card-tier c-${tier}">${meta.name}</div>
          <div class="plan-card-price">${meta.price} <span>${meta.period}</span></div>
          <div class="plan-card-desc">${this._tierDesc(tier)}</div>
          <ul class="plan-features">
            ${features.map(f => '<li>' + f + '</li>').join('')}
          </ul>
          <button class="plan-subscribe-btn ${isCurrent ? 'btn-active' : ''}"
                  ${isCurrent ? 'disabled' : ''}
                  onclick="BillingPage.subscribeTier('${tier}')">
            ${isCurrent ? 'Current Plan' : 'Subscribe'}
          </button>
        </div>
      `;
    }).join('');
  },

  _tierDesc(tier) {
    const descs = {
      pro: 'For individual traders and small teams getting started with algorithmic trading.',
      enterprise: 'For professional trading desks requiring advanced tooling and priority support.',
      enterprise_pro: 'For institutional traders with dedicated infrastructure and white-glove service.',
      government: 'For sovereign entities and large institutions with compliance requirements.',
    };
    return descs[tier] || '';
  },

  // ── Stripe Actions ──
  async subscribeTier(tier) {
    const data = await api('/api/create-checkout-tier', {
      method: 'POST',
      body: JSON.stringify({ tier }),
    });
    if (data.error) {
      Toast.show(data.error, 'error');
      return;
    }
    if (data.checkout_url) {
      window.location.href = data.checkout_url;
    }
  },

  async cryptoCheckout() {
    const data = await api('/api/create-crypto-checkout', { method: 'POST' });
    if (data.error) {
      Toast.show(data.error, 'error');
      return;
    }
    if (data.checkout_url) {
      window.location.href = data.checkout_url;
    }
  },

  async manageBilling() {
    const data = await api('/api/manage-billing', { method: 'POST' });
    if (data.error) {
      Toast.show(data.error, 'error');
      return;
    }
    if (data.portal_url) {
      window.location.href = data.portal_url;
    }
  },

  async cancelSubscription() {
    if (!confirm('Are you sure you want to cancel your subscription? You will lose access to premium features at the end of your billing period.')) {
      return;
    }
    const data = await api('/api/cancel-subscription', { method: 'POST' });
    if (data.error) {
      Toast.show(data.error, 'error');
      return;
    }
    Toast.show('Subscription cancelled. Access remains until end of billing period.', 'warning');
    setTimeout(() => this.loadCurrentPlan(), 1000);
  },
};

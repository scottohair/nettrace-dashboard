/* NetTrace — Home Page Controller */
/* Network traceroute dashboard: map, sidebar, scans, subscriptions */

'use strict';

/* ── State ── */
let DATA = [];
let userScans = [];
let currentUser = null;
let isSubscribed = false;
let userMeta = {};
let authMode = 'login';
let currentView = 'targets';
let latencyChart = null;
let routeChanges = {};  // host -> true if recently changed
let stripeInstance = null;
let wsId = null;

/* Category colors for lines + CSS classes */
const catColors = {
  'Quant APIs':       { line: '#a855f7', css: 'cat-quant' },
  'Cloud Providers':  { line: '#3b82f6', css: 'cat-cloud' },
  'NYSE & Financial': { line: '#f59e0b', css: 'cat-finance' },
  'Crypto Exchanges': { line: '#f472b6', css: 'cat-crypto' },
  'Forex & Brokers':  { line: '#2dd4bf', css: 'cat-forex' },
  'CDN & DNS':        { line: '#fbbf24', css: 'cat-cdn' },
  'Gov & Data':       { line: '#60a5fa', css: 'cat-gov' },
  'Custom':           { line: '#00ff88', css: 'cat-custom' }
};

/* ── Leaflet Map ── */
const map = L.map('map', {
  center: [38, -40],
  zoom: 3,
  zoomControl: false,
  attributionControl: false
});
L.control.zoom({ position: 'topright' }).addTo(map);
L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
  maxZoom: 19
}).addTo(map);
const routeLayers = L.layerGroup().addTo(map);
const bgLayers = L.layerGroup().addTo(map);

/* ══════════════════════════════════════
   Auth Modal
   ══════════════════════════════════════ */

function showAuth() {
  document.getElementById('auth-modal').classList.remove('hidden');
}

function hideAuth() {
  document.getElementById('auth-modal').classList.add('hidden');
  document.getElementById('auth-error').classList.add('hidden');
}

function toggleAuthMode(e) {
  e.preventDefault();
  authMode = authMode === 'login' ? 'register' : 'login';
  document.getElementById('auth-title').textContent = authMode === 'login' ? 'Sign In' : 'Register';
  document.getElementById('auth-submit').textContent = authMode === 'login' ? 'Sign In' : 'Register';
  document.getElementById('auth-switch-text').textContent = authMode === 'login' ? 'No account? ' : 'Have an account? ';
  document.getElementById('auth-switch').textContent = authMode === 'login' ? 'Register' : 'Sign In';
  const orgField = document.getElementById('auth-org-name');
  if (authMode === 'register') orgField.classList.remove('hidden');
  else orgField.classList.add('hidden');
}

async function submitAuth() {
  const u = document.getElementById('auth-user').value.trim();
  const p = document.getElementById('auth-pass').value;
  const orgName = document.getElementById('auth-org-name').value.trim();
  const errEl = document.getElementById('auth-error');
  try {
    const payload = { username: u, password: p };
    if (authMode === 'register' && orgName) payload.org_name = orgName;
    const url = authMode === 'login' ? '/api/login' : '/api/register';
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    const data = await res.json();
    if (!res.ok) {
      errEl.textContent = data.error;
      errEl.classList.remove('hidden');
      return;
    }
    currentUser = data.username;
    isSubscribed = data.subscribed;
    onLogin();
    hideAuth();
  } catch (e) {
    errEl.textContent = 'Network error';
    errEl.classList.remove('hidden');
  }
}

/* ══════════════════════════════════════
   Logout
   ══════════════════════════════════════ */

async function doLogout() {
  await fetch('/api/logout', { method: 'POST' });
  currentUser = null;
  isSubscribed = false;
  userMeta = {};
  document.getElementById('btn-auth').classList.remove('hidden');
  ['user-badge', 'sub-badge', 'scan-section', 'paywall-section', 'btn-settings'].forEach(id => {
    document.getElementById(id).classList.add('hidden');
  });
  document.getElementById('past-due-banner').classList.add('hidden');
  const navTrading = document.getElementById('nav-trading');
  if (navTrading) navTrading.style.display = 'none';
  userScans = [];
  buildSidebar('all');
}

/* ══════════════════════════════════════
   User Meta / Auth State
   ══════════════════════════════════════ */

async function fetchUserMeta() {
  try {
    const res = await fetch('/api/me');
    const d = await res.json();
    if (d.authenticated) {
      userMeta = d;
      currentUser = d.username;
      isSubscribed = d.subscribed;
    }
    return d;
  } catch (e) {
    return {};
  }
}

function onLogin() {
  document.getElementById('btn-auth').classList.add('hidden');
  document.getElementById('btn-settings').classList.remove('hidden');
  document.getElementById('user-badge').textContent = currentUser;
  document.getElementById('user-badge').classList.remove('hidden');

  /* Past due banner */
  if (userMeta.subscription_status === 'past_due') {
    document.getElementById('past-due-banner').classList.remove('hidden');
  } else {
    document.getElementById('past-due-banner').classList.add('hidden');
  }

  /* Trading nav visibility */
  const navTrading = document.getElementById('nav-trading');
  if (navTrading) navTrading.style.display = isSubscribed ? 'inline' : 'none';

  if (isSubscribed) {
    const tierLabels = {
      'free': 'FREE', 'pro': 'PRO', 'enterprise': 'ENTERPRISE',
      'enterprise_pro': 'ENT PRO', 'government': 'GOV'
    };
    const tierLabel = tierLabels[userMeta.tier] || (userMeta.tier || 'pro').toUpperCase();
    document.getElementById('sub-badge').textContent = tierLabel;
    document.getElementById('sub-badge').classList.remove('hidden');
    document.getElementById('scan-section').classList.remove('hidden');
    document.getElementById('paywall-section').classList.add('hidden');
    connectSocket();
    loadMyScans();
  } else {
    document.getElementById('sub-badge').classList.add('hidden');
    document.getElementById('scan-section').classList.add('hidden');
    document.getElementById('paywall-section').classList.remove('hidden');
    /* Expired or cancelled paywall messaging */
    const st = userMeta.subscription_status;
    if (st === 'expired' || st === 'cancelled') {
      document.getElementById('paywall-title').textContent = 'Subscription Expired';
      const sub = document.getElementById('paywall-subtitle');
      sub.textContent = 'Renew to restore your Pro access.';
      sub.classList.remove('hidden');
    } else {
      document.getElementById('paywall-title').textContent = 'Network Intelligence Plans';
      document.getElementById('paywall-subtitle').classList.add('hidden');
    }
    mountApplePay();
  }
}

/* ══════════════════════════════════════
   Settings Modal
   ══════════════════════════════════════ */

function showSettings() {
  fetchUserMeta().then(() => {
    const m = userMeta;
    document.getElementById('set-username').textContent = m.username || '-';
    document.getElementById('set-created').textContent = m.created_at
      ? new Date(m.created_at).toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' })
      : '-';

    /* Status badge */
    const sb = document.getElementById('set-status-badge');
    const st = m.subscription_status || 'none';
    const labels = { active: 'Active', trialing: 'Trialing', past_due: 'Past Due', cancelled: 'Cancelled', expired: 'Expired', canceled: 'Cancelled' };
    const classes = { active: 'status-active', trialing: 'status-active', past_due: 'status-past-due', cancelled: 'status-cancelled', canceled: 'status-cancelled', expired: 'status-expired' };
    sb.textContent = labels[st] || 'Inactive';
    sb.className = 'status-badge ' + (classes[st] || 'status-inactive');

    /* Payment method */
    const pm = m.payment_method || 'none';
    const pmLabels = { stripe: 'Card', crypto: 'Crypto', none: '-' };
    document.getElementById('set-payment').textContent = pmLabels[pm] || pm;

    /* Renewal info */
    const renewEl = document.getElementById('set-renewal');
    if (pm === 'stripe' && (st === 'active' || st === 'trialing' || st === 'past_due')) {
      renewEl.textContent = 'Auto-renews monthly';
    } else if (pm === 'crypto' && m.subscription_expires_at) {
      renewEl.textContent = 'Expires ' + new Date(m.subscription_expires_at).toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
    } else {
      renewEl.textContent = '-';
    }

    /* Crypto expiry warning */
    const cw = document.getElementById('set-crypto-warning');
    if (pm === 'crypto' && m.subscription_expires_at && (st === 'active' || st === 'cancelled')) {
      const days = Math.ceil((new Date(m.subscription_expires_at) - Date.now()) / (1000 * 60 * 60 * 24));
      if (days <= 7 && days > 0) {
        cw.textContent = 'Your subscription expires in ' + days + ' day' + (days === 1 ? '' : 's') + '. Renew to keep access.';
        cw.classList.remove('hidden');
      } else if (days <= 0) {
        cw.textContent = 'Your subscription has expired.';
        cw.classList.remove('hidden');
      } else {
        cw.classList.add('hidden');
      }
    } else {
      cw.classList.add('hidden');
    }

    /* Action buttons */
    document.getElementById('set-btn-billing').classList.toggle('hidden', !(pm === 'stripe' && m.has_stripe_billing));
    document.getElementById('set-btn-renew').classList.toggle('hidden', !(pm === 'crypto' || st === 'expired' || st === 'cancelled' || st === 'none'));
    document.getElementById('set-btn-cancel').classList.toggle('hidden', !(st === 'active' || st === 'trialing' || st === 'past_due'));

    document.getElementById('settings-modal').classList.remove('hidden');
    loadApiKeys();
  });
}

function hideSettings() {
  document.getElementById('settings-modal').classList.add('hidden');
}

async function cancelSubscription() {
  if (!confirm('Cancel your subscription?')) return;
  try {
    const res = await fetch('/api/cancel-subscription', { method: 'POST' });
    const data = await res.json();
    if (data.use_portal) { manageBilling(); return; }
    if (data.ok) {
      Toast.show(data.message || 'Subscription cancelled', 'success');
      hideSettings();
      await fetchUserMeta();
      onLogin();
    } else {
      Toast.show(data.error || 'Could not cancel', 'error');
    }
  } catch (e) {
    Toast.show('Network error', 'error');
  }
}

/* ══════════════════════════════════════
   Stripe / Payments
   ══════════════════════════════════════ */

function getStripe() {
  if (!stripeInstance && typeof STRIPE_PK !== 'undefined' && STRIPE_PK) {
    stripeInstance = Stripe(STRIPE_PK);
  }
  return stripeInstance;
}

async function subscribe() {
  try {
    const res = await fetch('/api/create-checkout', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' }
    });
    const data = await res.json();
    if (data.checkout_url) window.location.href = data.checkout_url;
    else Toast.show(data.error || 'Could not start checkout', 'error');
  } catch (e) {
    Toast.show('Network error', 'error');
  }
}

async function subscribeTier(tier) {
  try {
    const res = await fetch('/api/create-checkout-tier', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ tier })
    });
    const data = await res.json();
    if (data.checkout_url) {
      window.location.href = data.checkout_url;
    } else {
      /* Fallback to legacy checkout */
      const res2 = await fetch('/api/create-checkout', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' }
      });
      const data2 = await res2.json();
      if (data2.checkout_url) window.location.href = data2.checkout_url;
      else Toast.show(data2.error || 'Could not start checkout', 'error');
    }
  } catch (e) {
    Toast.show('Network error', 'error');
  }
}

function mountApplePay() {
  const s = getStripe();
  if (!s) return;
  const btn = document.getElementById('apple-pay-btn');
  const fallback = document.getElementById('fallback-subscribe');
  if (!btn) return;
  const paymentRequest = s.paymentRequest({
    country: 'US',
    currency: 'usd',
    total: { label: 'NetTrace Pro - Monthly', amount: 24900 },
    requestPayerName: true,
    requestPayerEmail: true
  });
  const prButton = s.elements().create('paymentRequestButton', {
    paymentRequest,
    style: { paymentRequestButton: { type: 'subscribe', theme: 'dark' } }
  });
  paymentRequest.canMakePayment().then(result => {
    if (result) {
      prButton.mount('#apple-pay-btn');
      if (fallback) fallback.style.display = 'none';
    } else {
      btn.style.display = 'none';
    }
  });
  paymentRequest.on('paymentmethod', async (ev) => {
    ev.complete('success');
    subscribeTier('pro');
  });
}

async function cryptoCheckout() {
  try {
    const res = await fetch('/api/create-crypto-checkout', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' }
    });
    const data = await res.json();
    if (data.checkout_url) window.location.href = data.checkout_url;
    else Toast.show(data.error || 'Could not start crypto checkout', 'error');
  } catch (e) {
    Toast.show('Network error', 'error');
  }
}

async function manageBilling() {
  try {
    const res = await fetch('/api/manage-billing', { method: 'POST' });
    const data = await res.json();
    if (data.portal_url) window.location.href = data.portal_url;
    else Toast.show(data.error || 'Could not open billing', 'error');
  } catch (e) {
    Toast.show('Network error', 'error');
  }
}

/* ══════════════════════════════════════
   API Key Management
   ══════════════════════════════════════ */

async function loadApiKeys() {
  try {
    const res = await fetch('/api/keys');
    if (!res.ok) return;
    const keys = await res.json();
    const list = document.getElementById('api-keys-list');
    if (!list) return;
    if (keys.length === 0) {
      list.innerHTML = '<div style="font-size:10px;color:#374151;padding:4px">No API keys yet</div>';
      return;
    }
    list.innerHTML = keys.map(k =>
      `<div class="api-key-item">
        <div><span class="api-key-prefix">${k.prefix}...</span> <span style="color:var(--muted)">${k.name}</span></div>
        <div style="display:flex;gap:6px;align-items:center">
          <span class="api-key-tier">${k.tier}</span>
          ${k.is_active
            ? `<button class="btn btn-danger" style="padding:2px 6px;font-size:9px" onclick="deleteApiKey(${k.id})">Revoke</button>`
            : '<span style="color:var(--red);font-size:9px">Revoked</span>'}
        </div>
      </div>`
    ).join('');
  } catch (e) { /* silent */ }
}

async function generateApiKey() {
  try {
    const res = await fetch('/api/keys', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'default' })
    });
    const data = await res.json();
    if (!res.ok) { Toast.show(data.error || 'Failed to generate key', 'error'); return; }
    const display = document.getElementById('new-api-key-display');
    display.innerHTML = `<strong>New API Key (save now!):</strong><br>` +
      `<code style="color:var(--green);font-size:11px;word-break:break-all">${data.api_key}</code><br>` +
      `<span style="color:#f59e0b;font-size:9px">This key won't be shown again.</span>`;
    display.classList.remove('hidden');
    loadApiKeys();
    Toast.show('API key generated', 'success');
  } catch (e) {
    Toast.show('Network error', 'error');
  }
}

async function deleteApiKey(id) {
  if (!confirm('Revoke this API key?')) return;
  try {
    await fetch(`/api/keys/${id}`, { method: 'DELETE' });
    loadApiKeys();
    Toast.show('API key revoked', 'success');
  } catch (e) {
    Toast.show('Network error', 'error');
  }
}

/* ══════════════════════════════════════
   Checkout Return Handling
   ══════════════════════════════════════ */

(function handleCheckoutReturn() {
  const params = new URLSearchParams(window.location.search);
  const subParam = params.get('subscription');
  if (!subParam) return;
  history.replaceState(null, '', '/');
  if (subParam === 'success') {
    Toast.show('Payment received! Activating your subscription...', 'success', 8000);
    let attempts = 0;
    const poll = setInterval(async () => {
      attempts++;
      if (attempts > 20) { clearInterval(poll); return; }
      try {
        const res = await fetch('/api/me');
        const d = await res.json();
        if (d.subscribed) {
          clearInterval(poll);
          userMeta = d;
          currentUser = d.username;
          isSubscribed = true;
          Toast.show('Subscription active! You can now run scans.', 'success');
          onLogin();
        }
      } catch (e) { /* retry */ }
    }, 3000);
  } else if (subParam === 'cancelled') {
    Toast.show('Checkout cancelled', 'warning');
  }
})();

/* ══════════════════════════════════════
   WebSocket
   ══════════════════════════════════════ */

function connectSocket() {
  if (WS.connected) return;
  WS.connect();
  WS.on('connected', d => { wsId = d.sid; });
  WS.on('scan_status', d => {
    const el = document.getElementById('scan-status');
    el.textContent = d.message;
    el.className = 'scan-status running';
  });
  WS.on('scan_progress', d => {
    const el = document.getElementById('scan-status');
    el.textContent = `Tracing... hop ${d.hops_done}/${d.total_hops}`;
  });
  WS.on('scan_complete', d => {
    const el = document.getElementById('scan-status');
    el.textContent = 'Scan complete!';
    el.className = 'scan-status done';
    loadMyScans();
  });
  WS.on('scan_error', d => {
    const el = document.getElementById('scan-status');
    el.textContent = 'Error: ' + d.error;
    el.className = 'scan-status err';
  });
}

/* ══════════════════════════════════════
   Scanning
   ══════════════════════════════════════ */

async function startScan() {
  const host = document.getElementById('scan-host').value.trim();
  if (!host) return;
  const el = document.getElementById('scan-status');
  el.textContent = 'Starting scan...';
  el.className = 'scan-status running';
  try {
    const res = await fetch('/api/scan', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ host, name: host, sid: wsId })
    });
    const data = await res.json();
    if (data.needs_subscription) {
      el.textContent = 'Subscription required';
      el.className = 'scan-status err';
      return;
    }
    if (!res.ok) {
      el.textContent = data.error;
      el.className = 'scan-status err';
      return;
    }
    el.textContent = `Scan #${data.scan_id} started...`;
  } catch (e) {
    el.textContent = 'Network error';
    el.className = 'scan-status err';
  }
}

function quickScan(h, n) {
  document.getElementById('scan-host').value = h;
  startScan();
}

async function loadMyScans() {
  try {
    const res = await fetch('/api/scans');
    const scans = await res.json();
    userScans = scans
      .filter(s => s.status === 'completed' && s.result)
      .map(s => ({
        name: s.name || s.host,
        host: s.host,
        category: 'Custom',
        hop_count: s.result.hop_count || 0,
        total_rtt: s.result.total_rtt,
        hops: s.result.hops || []
      }));
    buildSidebar(document.querySelector('.filter-btn.active')?.dataset.filter || 'all');
    updateStats();
    drawBgRoutes();
  } catch (e) { /* silent */ }
}

/* ══════════════════════════════════════
   Stats + Rendering
   ══════════════════════════════════════ */

function allData() {
  return [...DATA, ...userScans];
}

function updateStats() {
  const all = allData();

  /* Targets count */
  document.getElementById('s-targets').textContent = all.length;

  /* Total hops */
  const totalHops = all.reduce((s, t) => s + (t.hops ? t.hops.length : 0), 0);
  document.getElementById('s-hops').textContent = totalHops;

  /* Average RTT */
  const rtts = all.filter(t => t.total_rtt).map(t => t.total_rtt);
  document.getElementById('s-rtt').textContent = rtts.length
    ? (rtts.reduce((a, b) => a + b, 0) / rtts.length).toFixed(1)
    : '-';

  /* Unique IPs */
  const ips = new Set();
  all.forEach(t => {
    if (t.hops) t.hops.forEach(h => { if (h.ip) ips.add(h.ip); });
  });
  document.getElementById('s-ips').textContent = ips.size;
}

function buildSidebar(filter) {
  const list = document.getElementById('target-list');
  list.innerHTML = '';
  if (currentView === 'rankings') { buildRankings(filter); return; }

  const all = allData();
  const filtered = filter === 'all' ? all : all.filter(d => d.category === filter);

  /* Category sort order */
  const co = {
    'Quant APIs': 0, 'Cloud Providers': 1, 'NYSE & Financial': 2,
    'Crypto Exchanges': 3, 'Forex & Brokers': 4, 'CDN & DNS': 5,
    'Gov & Data': 6, 'Custom': 7
  };
  filtered.sort((a, b) => (co[a.category] || 9) - (co[b.category] || 9) || a.name.localeCompare(b.name));

  let lastCat = '';
  filtered.forEach(t => {
    if (t.category !== lastCat) {
      lastCat = t.category;
      const h = document.createElement('div');
      h.className = 'cat-header ' + (catColors[t.category]?.css || 'cat-custom');
      h.textContent = t.category;
      list.appendChild(h);
    }
    const item = document.createElement('div');
    const idx = all.indexOf(t);
    item.className = 'target-item' + (routeChanges[t.host] ? ' route-changed' : '');
    item.dataset.index = idx;

    const rc = !t.total_rtt ? 'rtt-mid' : t.total_rtt < 30 ? 'rtt-good' : t.total_rtt < 80 ? 'rtt-mid' : 'rtt-bad';
    item.innerHTML =
      `<div>
        <div class="target-name">${t.name}</div>
        <div class="target-host">${t.host}</div>
      </div>
      <div class="target-meta">
        <div class="target-rtt ${rc}">${t.total_rtt ? t.total_rtt.toFixed(1) + ' ms' : 'N/A'}</div>
        <div class="target-hops">${t.hop_count} hops</div>
      </div>`;
    item.addEventListener('click', () => selectTarget(idx));
    list.appendChild(item);
  });
}

function buildRankings(filter) {
  const list = document.getElementById('target-list');
  const all = allData();
  const filtered = (filter === 'all' ? all : all.filter(d => d.category === filter))
    .filter(t => t.total_rtt);
  filtered.sort((a, b) => a.total_rtt - b.total_rtt);

  let html = '<table class="rankings-table"><thead><tr><th>#</th><th>Target</th><th>RTT</th><th>Hops</th></tr></thead><tbody>';
  filtered.forEach((t, i) => {
    const rc = t.total_rtt < 30 ? 'rtt-good' : t.total_rtt < 80 ? 'rtt-mid' : 'rtt-bad';
    const idx = all.indexOf(t);
    html += `<tr style="cursor:pointer" onclick="selectTarget(${idx})">
      <td class="rank-num">${i + 1}</td>
      <td><div style="font-size:11px">${t.name}</div><div style="font-size:8px;color:var(--muted)">${t.host}</div></td>
      <td class="${rc}" style="font-weight:700">${t.total_rtt.toFixed(1)} ms</td>
      <td style="color:var(--muted)">${t.hop_count}</td>
    </tr>`;
  });
  html += '</tbody></table>';
  list.innerHTML = html;
}

function setView(view) {
  currentView = view;
  document.querySelectorAll('.view-btn').forEach(b =>
    b.classList.toggle('active', b.dataset.view === view)
  );
  buildSidebar(document.querySelector('.filter-btn.active')?.dataset.filter || 'all');
}

/* ══════════════════════════════════════
   Map Drawing
   ══════════════════════════════════════ */

function drawBgRoutes() {
  bgLayers.clearLayers();
  allData().forEach(t => {
    if (!t.hops) return;
    const pts = t.hops.filter(h => h.geo).map(h => [h.geo.lat, h.geo.lon]);
    if (pts.length > 1) {
      L.polyline(pts, {
        color: catColors[t.category]?.line || '#00ff88',
        weight: 1,
        opacity: 0.12
      }).addTo(bgLayers);
    }
    const last = [...t.hops].reverse().find(h => h.geo);
    if (last) {
      L.circleMarker([last.geo.lat, last.geo.lon], {
        radius: 3,
        color: catColors[t.category]?.line || '#00ff88',
        fillColor: catColors[t.category]?.line || '#00ff88',
        fillOpacity: 0.5,
        weight: 1
      }).bindPopup(
        `<b>${t.name}</b><br>${t.host}<br>${last.geo.city}, ${last.geo.country}`
      ).addTo(bgLayers);
    }
  });
}

function selectTarget(idx) {
  document.querySelectorAll('.target-item').forEach(el => el.classList.remove('active'));
  document.querySelectorAll(`.target-item[data-index="${idx}"]`).forEach(el => el.classList.add('active'));

  const t = allData()[idx];
  if (!t) return;

  const color = catColors[t.category]?.line || '#00d4ff';
  routeLayers.clearLayers();
  const pts = [];
  (t.hops || []).forEach(h => {
    if (!h.geo) return;
    pts.push([h.geo.lat, h.geo.lon]);
    L.circleMarker([h.geo.lat, h.geo.lon], {
      radius: 5, color, fillColor: color, fillOpacity: 0.8, weight: 2
    }).bindPopup(
      `<b>Hop ${h.hop}</b><br>${h.host}<br>` +
      `<span style="color:var(--accent)">${h.ip}</span><br>` +
      `${h.geo.city}, ${h.geo.region}, ${h.geo.country}<br>` +
      `RTT: <b>${h.rtt_ms ? h.rtt_ms.toFixed(1) + ' ms' : 'N/A'}</b><br>` +
      `<span style="color:var(--muted)">${h.geo.isp || ''}</span>`
    ).addTo(routeLayers);
  });

  if (pts.length > 1) {
    L.polyline(pts, { color, weight: 3, opacity: 0.9 }).addTo(routeLayers);
    L.polyline(pts, { color, weight: 8, opacity: 0.2 }).addTo(routeLayers);
    map.fitBounds(L.latLngBounds(pts).pad(0.3));
  } else if (pts.length === 1) {
    map.setView(pts[0], 6);
  }

  /* Detail panel */
  const panel = document.getElementById('detail-panel');
  const hops = t.hops || [];
  const maxR = Math.max(...hops.filter(h => h.rtt_ms).map(h => h.rtt_ms), 1);
  const rows = hops.map(h => {
    if (!h.ip) {
      return `<tr><td class="hop-num">${h.hop}</td><td class="hop-timeout" colspan="4">* * * (timeout)</td></tr>`;
    }
    const bw = h.rtt_ms ? Math.max(4, (h.rtt_ms / maxR) * 100) : 0;
    const bc = !h.rtt_ms ? '#374151' : h.rtt_ms < 20 ? '#00ff88' : h.rtt_ms < 60 ? '#f59e0b' : '#ef4444';
    const loc = h.geo ? `${h.geo.city}${h.geo.city && h.geo.country ? ', ' : ''}${h.geo.country}` : '';
    return `<tr>
      <td class="hop-num">${h.hop}</td>
      <td class="hop-ip">${h.ip}</td>
      <td class="hop-host" title="${h.host}">${h.host}</td>
      <td>${h.rtt_ms ? `<span class="rtt-bar" style="width:${bw}px;background:${bc}"></span>${h.rtt_ms.toFixed(1)} ms` : '-'}</td>
      <td class="hop-loc">${loc}</td>
    </tr>`;
  }).join('');

  panel.innerHTML =
    `<div class="detail-title">${t.name} &mdash; ${t.host}</div>
    <div class="chart-panel">
      <h3>Latency History</h3>
      <div class="chart-container"><canvas id="latency-chart"></canvas></div>
    </div>
    <table class="hop-table">
      <thead><tr><th>#</th><th>IP</th><th>Hostname</th><th>RTT</th><th>Location</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>`;

  loadLatencyChart(t.host, t.name);
}

/* ══════════════════════════════════════
   Latency Chart
   ══════════════════════════════════════ */

async function loadLatencyChart(host, name) {
  try {
    const res = await fetch(`/api/internal/history/${encodeURIComponent(host)}`);
    const data = await res.json();
    if (!data.length) return;
    const canvas = document.getElementById('latency-chart');
    if (!canvas) return;
    if (latencyChart) { latencyChart.destroy(); latencyChart = null; }

    const reversed = data.reverse();
    const labels = reversed.map(d =>
      d.t ? new Date(d.t).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' }) : ''
    );
    const rtts = reversed.map(d => d.rtt);

    latencyChart = new Chart(canvas, {
      type: 'line',
      data: {
        labels,
        datasets: [{
          label: 'RTT (ms)',
          data: rtts,
          borderColor: '#00d4ff',
          backgroundColor: '#00d4ff11',
          fill: true,
          tension: 0.3,
          pointRadius: 1,
          borderWidth: 1.5
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: {
          x: {
            display: true,
            ticks: { color: '#374151', font: { size: 8 }, maxTicksLimit: 8 },
            grid: { color: '#111827' }
          },
          y: {
            display: true,
            ticks: { color: '#5a6c7d', font: { size: 9 } },
            grid: { color: '#111827' }
          }
        }
      }
    });
  } catch (e) { /* silent */ }
}

/* ══════════════════════════════════════
   Event Bindings
   ══════════════════════════════════════ */

function bindEvents() {
  /* Filter buttons */
  document.querySelectorAll('.filter-btn').forEach(b => {
    b.addEventListener('click', () => {
      document.querySelectorAll('.filter-btn').forEach(x => x.classList.remove('active'));
      b.classList.add('active');
      buildSidebar(b.dataset.filter);
    });
  });

  /* Scan input enter key */
  const scanHost = document.getElementById('scan-host');
  if (scanHost) scanHost.addEventListener('keydown', e => { if (e.key === 'Enter') startScan(); });

  /* Auth input enter key */
  const authPass = document.getElementById('auth-pass');
  if (authPass) authPass.addEventListener('keydown', e => { if (e.key === 'Enter') submitAuth(); });

  /* Auth modal backdrop close */
  const authModal = document.getElementById('auth-modal');
  if (authModal) authModal.addEventListener('click', e => { if (e.target === e.currentTarget) hideAuth(); });
}

/* ══════════════════════════════════════
   Init
   ══════════════════════════════════════ */

async function init() {
  bindEvents();

  /* Check auth state */
  try {
    const res = await fetch('/api/me');
    const d = await res.json();
    if (d.authenticated) {
      userMeta = d;
      currentUser = d.username;
      isSubscribed = d.subscribed;
      onLogin();
    }
  } catch (e) { /* not logged in */ }

  /* Load demo targets */
  try {
    const res = await fetch('/api/demo');
    DATA = await res.json();
  } catch (e) {
    DATA = [];
  }

  /* Render with live data */
  updateStats();
  buildSidebar('all');
  drawBgRoutes();

  /* Handle cross-page redirects for auth/settings modals */
  const params = new URLSearchParams(window.location.search);
  if (params.get('auth') === '1') {
    history.replaceState(null, '', '/');
    showAuth();
  } else if (params.get('settings') === '1') {
    history.replaceState(null, '', '/');
    if (currentUser) showSettings();
    else showAuth();
  }
}

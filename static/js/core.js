/* NetTrace Quant Platform — Core JavaScript Utilities */
/* MVC Controller Layer: shared across all pages */

'use strict';

// ─── WebSocket Manager ───
const WS = {
  socket: null,
  connected: false,
  handlers: {},
  reconnectTimer: null,
  reconnectDelay: 2000,

  connect() {
    if (typeof io === 'undefined') return;
    this.socket = io({ transports: ['websocket', 'polling'] });
    this.socket.on('connect', () => {
      this.connected = true;
      this.reconnectDelay = 2000;
      this._updateStatus(true);
      this._fire('connect');
    });
    this.socket.on('disconnect', () => {
      this.connected = false;
      this._updateStatus(false);
      this._fire('disconnect');
    });
    this.socket.on('connected', (d) => { this._fire('connected', d); });
    this.socket.on('trading_update', (d) => { this._fire('trading_update', d); });
    this.socket.on('price_update', (d) => { this._fire('price_update', d); });
    this.socket.on('scan_status', (d) => { this._fire('scan_status', d); });
    this.socket.on('scan_progress', (d) => { this._fire('scan_progress', d); });
    this.socket.on('scan_complete', (d) => { this._fire('scan_complete', d); });
    this.socket.on('scan_error', (d) => { this._fire('scan_error', d); });
  },

  on(event, fn) {
    if (!this.handlers[event]) this.handlers[event] = [];
    this.handlers[event].push(fn);
  },

  off(event, fn) {
    if (!this.handlers[event]) return;
    this.handlers[event] = this.handlers[event].filter(h => h !== fn);
  },

  _fire(event, data) {
    (this.handlers[event] || []).forEach(fn => { try { fn(data); } catch (e) { console.error('WS handler error:', e); } });
  },

  _updateStatus(connected) {
    const el = document.getElementById('wsStatus');
    if (!el) return;
    if (connected) {
      el.className = 'ws-status connected';
      el.textContent = 'LIVE';
    } else {
      el.className = 'ws-status disconnected';
      el.textContent = 'WS: offline';
    }
  },

  emit(event, data) {
    if (this.socket) this.socket.emit(event, data);
  }
};

// ─── Toast System with History ───
const Toast = {
  history: [],
  maxHistory: 50,
  unreadCount: 0,

  show(msg, type = 'success', duration = 5000) {
    const c = document.getElementById('toast-container');
    if (!c) return;
    const t = document.createElement('div');
    t.className = 'toast toast-' + type;
    t.innerHTML = `<div>${this._escape(msg)}</div><div class="toast-time">${this._timeStr()}</div>`;
    t.onclick = () => t.remove();
    c.appendChild(t);

    // Auto dismiss (unless critical)
    if (duration > 0) {
      setTimeout(() => { t.style.opacity = '0'; t.style.transition = 'opacity .3s'; setTimeout(() => t.remove(), 300); }, duration);
    }

    // Add to history
    this.history.unshift({ msg, type, time: new Date() });
    if (this.history.length > this.maxHistory) this.history.pop();
    this.unreadCount++;
    this._updateBadge();
    this._renderDrawer();
  },

  _escape(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; },
  _timeStr() { return new Date().toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit' }); },

  _updateBadge() {
    const el = document.querySelector('.notif-count');
    if (!el) return;
    if (this.unreadCount > 0) {
      el.textContent = this.unreadCount > 99 ? '99+' : this.unreadCount;
      el.style.display = 'block';
    } else {
      el.style.display = 'none';
    }
  },

  _renderDrawer() {
    const list = document.getElementById('notifList');
    if (!list) return;
    const colors = { success: 'var(--green)', error: 'var(--red)', warning: '#f59e0b', trade: 'var(--blue)', system: 'var(--muted)' };
    list.innerHTML = this.history.map(h => `
      <div class="notif-item">
        <span class="notif-dot" style="background:${colors[h.type] || colors.system}"></span>
        <span class="notif-msg">${this._escape(h.msg)}</span>
        <span class="notif-time">${this._relTime(h.time)}</span>
      </div>
    `).join('');
  },

  _relTime(d) {
    const s = Math.floor((Date.now() - d.getTime()) / 1000);
    if (s < 60) return s + 's ago';
    if (s < 3600) return Math.floor(s / 60) + 'm ago';
    return Math.floor(s / 3600) + 'h ago';
  },

  toggleDrawer() {
    const drawer = document.getElementById('notifDrawer');
    if (!drawer) return;
    drawer.classList.toggle('open');
    if (drawer.classList.contains('open')) {
      this.unreadCount = 0;
      this._updateBadge();
    }
  }
};

// ─── Theme Manager ───
const Theme = {
  init() {
    const theme = localStorage.getItem('nt-theme') || 'dark';
    const accent = localStorage.getItem('nt-accent') || 'cyan';
    document.documentElement.setAttribute('data-theme', theme);
    document.documentElement.setAttribute('data-accent', accent);
    this._updateAccentDots(accent);
  },

  toggle() {
    const current = document.documentElement.getAttribute('data-theme');
    const next = current === 'light' ? 'dark' : 'light';
    document.documentElement.setAttribute('data-theme', next);
    localStorage.setItem('nt-theme', next);
  },

  setAccent(accent) {
    document.documentElement.setAttribute('data-accent', accent);
    localStorage.setItem('nt-accent', accent);
    this._updateAccentDots(accent);
  },

  _updateAccentDots(accent) {
    document.querySelectorAll('.accent-dot').forEach(d => {
      d.classList.toggle('active', d.getAttribute('data-accent') === accent);
    });
  }
};

// ─── API Fetch Helper ───
async function api(url, options = {}) {
  try {
    const defaults = { headers: { 'Content-Type': 'application/json' } };
    const res = await fetch(url, { ...defaults, ...options });
    const data = await res.json();
    if (!res.ok) {
      if (res.status === 401) {
        Toast.show('Session expired — please sign in', 'warning');
      } else {
        Toast.show(data.error || 'Request failed', 'error');
      }
      return { error: data.error || 'Request failed', status: res.status };
    }
    return data;
  } catch (e) {
    Toast.show('Network error', 'error');
    return { error: 'Network error' };
  }
}

// ─── Format Helpers ───
const Fmt = {
  usd(n, decimals) {
    if (n == null || isNaN(n)) return '$--';
    const num = Number(n);
    if (decimals !== undefined) return '$' + num.toFixed(decimals);
    if (Math.abs(num) >= 1000000) return '$' + (num / 1000000).toFixed(2) + 'M';
    if (Math.abs(num) >= 1000) return '$' + (num / 1000).toFixed(1) + 'K';
    return '$' + num.toFixed(2);
  },

  pnl(n) {
    if (n == null || isNaN(n)) return '$0.00';
    const num = Number(n);
    const prefix = num >= 0 ? '+$' : '-$';
    return prefix + Math.abs(num).toFixed(2);
  },

  pct(n) {
    if (n == null || isNaN(n)) return '--';
    const num = Number(n);
    return (num >= 0 ? '+' : '') + num.toFixed(2) + '%';
  },

  price(n) {
    if (n == null || isNaN(n)) return '--';
    const num = Number(n);
    if (num >= 100) return num.toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
    if (num >= 1) return num.toFixed(2);
    return num.toFixed(4);
  },

  num(n) {
    if (n == null || isNaN(n)) return '0';
    return Number(n).toLocaleString();
  },

  relTime(iso) {
    if (!iso) return '--';
    const s = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
    if (s < 60) return s + 's ago';
    if (s < 3600) return Math.floor(s / 60) + 'm ago';
    if (s < 86400) return Math.floor(s / 3600) + 'h ago';
    return Math.floor(s / 86400) + 'd ago';
  },

  shortTime(iso) {
    if (!iso) return '--';
    return new Date(iso).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  }
};

// ─── Clipboard ───
function copyToClipboard(text) {
  navigator.clipboard.writeText(text).then(() => {
    Toast.show('Copied to clipboard', 'success', 2000);
  });
}

// ─── Polling Manager ───
const Poller = {
  _intervals: {},

  start(name, fn, ms) {
    this.stop(name);
    fn(); // run immediately
    this._intervals[name] = setInterval(fn, ms);
  },

  stop(name) {
    if (this._intervals[name]) {
      clearInterval(this._intervals[name]);
      delete this._intervals[name];
    }
  },

  stopAll() {
    Object.keys(this._intervals).forEach(k => this.stop(k));
  }
};

// ─── Global navigation stubs ───
// showAuth() and showSettings() are defined in index.js but referenced
// by onclick handlers in base.html which is shared across all pages.
// If we're not on the index page, redirect there with a query param so
// the index page can open the correct modal on load.
if (typeof window.showAuth === 'undefined') {
  window.showAuth = function() { window.location.href = '/?auth=1'; };
}
if (typeof window.showSettings === 'undefined') {
  window.showSettings = function() { window.location.href = '/?settings=1'; };
}

// ─── Init on load ───
document.addEventListener('DOMContentLoaded', () => {
  Theme.init();
});

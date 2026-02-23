/* NetTrace Quant Platform — WalletConnect / MetaMask / Phantom Integration */
'use strict';

const Wallet = {
  provider: null,
  address: null,
  chainId: null,
  balances: {},
  type: null, // 'metamask' | 'phantom' | 'walletconnect'

  CHAINS: {
    1:     { name: 'Ethereum', symbol: 'ETH', rpc: 'https://eth.llamarpc.com' },
    8453:  { name: 'Base', symbol: 'ETH', rpc: 'https://mainnet.base.org' },
    42161: { name: 'Arbitrum', symbol: 'ETH', rpc: 'https://arb1.arbitrum.io/rpc' },
    137:   { name: 'Polygon', symbol: 'MATIC', rpc: 'https://polygon-rpc.com' },
  },

  // Check available wallets
  detect() {
    const wallets = [];
    if (typeof window.ethereum !== 'undefined') {
      if (window.ethereum.isMetaMask) wallets.push('metamask');
      else wallets.push('injected');
    }
    if (typeof window.solana !== 'undefined' && window.solana.isPhantom) {
      wallets.push('phantom');
    }
    return wallets;
  },

  // Connect MetaMask / injected EVM wallet
  async connectEVM() {
    if (typeof window.ethereum === 'undefined') {
      Toast.show('No EVM wallet detected. Install MetaMask.', 'warning');
      return false;
    }
    try {
      const accounts = await window.ethereum.request({ method: 'eth_requestAccounts' });
      if (!accounts.length) return false;
      this.provider = window.ethereum;
      this.address = accounts[0];
      this.type = window.ethereum.isMetaMask ? 'metamask' : 'injected';
      this.chainId = parseInt(await window.ethereum.request({ method: 'eth_chainId' }), 16);

      // Listen for changes
      window.ethereum.on('accountsChanged', (accs) => {
        this.address = accs[0] || null;
        this._updateUI();
      });
      window.ethereum.on('chainChanged', (id) => {
        this.chainId = parseInt(id, 16);
        this._updateUI();
      });

      await this._fetchBalance();
      this._updateUI();
      Toast.show('Wallet connected: ' + this._shortAddr(this.address), 'success');
      return true;
    } catch (e) {
      Toast.show('Wallet connection rejected', 'error');
      return false;
    }
  },

  // Connect Phantom (Solana)
  async connectPhantom() {
    if (!window.solana || !window.solana.isPhantom) {
      Toast.show('Phantom wallet not detected', 'warning');
      return false;
    }
    try {
      const resp = await window.solana.connect();
      this.address = resp.publicKey.toString();
      this.type = 'phantom';
      this.chainId = 'solana';
      this._updateUI();
      Toast.show('Phantom connected: ' + this._shortAddr(this.address), 'success');
      return true;
    } catch (e) {
      Toast.show('Phantom connection rejected', 'error');
      return false;
    }
  },

  // Disconnect
  disconnect() {
    this.provider = null;
    this.address = null;
    this.chainId = null;
    this.balances = {};
    this.type = null;
    this._updateUI();
    Toast.show('Wallet disconnected', 'success');
  },

  // Switch EVM chain
  async switchChain(chainId) {
    if (!this.provider) return;
    try {
      await this.provider.request({
        method: 'wallet_switchEthereumChain',
        params: [{ chainId: '0x' + chainId.toString(16) }]
      });
    } catch (e) {
      if (e.code === 4902) {
        Toast.show('Chain not added to wallet', 'warning');
      }
    }
  },

  // Fetch ETH balance
  async _fetchBalance() {
    if (!this.provider || !this.address) return;
    try {
      const bal = await this.provider.request({
        method: 'eth_getBalance',
        params: [this.address, 'latest']
      });
      this.balances[this.chainId] = parseInt(bal, 16) / 1e18;
    } catch (e) {
      console.error('Balance fetch error:', e);
    }
  },

  // Update UI elements
  _updateUI() {
    const addrEl = document.getElementById('walletAddress');
    const statusEl = document.getElementById('walletStatus');
    const connectBtn = document.getElementById('walletConnectBtn');
    const disconnectBtn = document.getElementById('walletDisconnectBtn');
    const chainEl = document.getElementById('walletChain');
    const balEl = document.getElementById('walletBalance');

    if (this.address) {
      if (addrEl) addrEl.textContent = this._shortAddr(this.address);
      if (statusEl) { statusEl.textContent = 'Connected'; statusEl.className = 'badge b-buy'; }
      if (connectBtn) connectBtn.style.display = 'none';
      if (disconnectBtn) disconnectBtn.style.display = 'inline';
      if (chainEl) {
        const chain = this.CHAINS[this.chainId];
        chainEl.textContent = chain ? chain.name : (this.chainId === 'solana' ? 'Solana' : 'Chain ' + this.chainId);
      }
      if (balEl && this.balances[this.chainId] != null) {
        balEl.textContent = this.balances[this.chainId].toFixed(4) + ' ' + (this.CHAINS[this.chainId]?.symbol || 'ETH');
      }
    } else {
      if (addrEl) addrEl.textContent = 'Not connected';
      if (statusEl) { statusEl.textContent = 'Disconnected'; statusEl.className = 'badge'; }
      if (connectBtn) connectBtn.style.display = 'inline';
      if (disconnectBtn) disconnectBtn.style.display = 'none';
      if (chainEl) chainEl.textContent = '--';
      if (balEl) balEl.textContent = '--';
    }
  },

  _shortAddr(addr) {
    if (!addr) return '--';
    return addr.slice(0, 6) + '...' + addr.slice(-4);
  }
};

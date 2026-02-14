# Port Forwarding Setup - Serve Web Directly

## Overview
This guide explains how to expose your local Flask app (or any web service) to the internet **without Fly.io**, allowing direct web serving from your M3 MacBook Air.

## Option 1: ngrok (Easiest - Recommended for Development)

### Install ngrok
```bash
# Install via Homebrew
brew install ngrok

# Or download from https://ngrok.com/download
```

### Authenticate (Free tier)
```bash
# Sign up at https://dashboard.ngrok.com/signup
# Get your auth token from https://dashboard.ngrok.com/get-started/your-authtoken

ngrok config add-authtoken YOUR_AUTH_TOKEN
```

### Start Tunnel
```bash
# Forward local Flask app (port 5000) to public URL
ngrok http 5000

# With custom subdomain (paid plan required)
ngrok http --domain=clawd-bot.ngrok.app 5000

# With basic auth (protect your site)
ngrok http 5000 --basic-auth="username:password"
```

### Output
```
ngrok

Session Status                online
Account                       scott@example.com (Plan: Free)
Version                       3.5.0
Region                        United States (us)
Latency                       20ms
Web Interface                 http://127.0.0.1:4040
Forwarding                    https://abc123.ngrok.app -> http://localhost:5000

Connections                   ttl     opn     rt1     rt5     p50     p90
                              0       0       0.00    0.00    0.00    0.00
```

**Your app is now live at**: `https://abc123.ngrok.app`

### Pros
- ✅ Setup in 60 seconds
- ✅ HTTPS by default
- ✅ Web inspector (http://127.0.0.1:4040)
- ✅ No router configuration needed

### Cons
- ❌ URL changes on each restart (free tier)
- ❌ 40 requests/minute limit (free tier)
- ❌ Session expires after 8 hours

### Pricing
- **Free**: Random URLs, 40 req/min, 8h session
- **Personal ($10/mo)**: Custom subdomains, unlimited sessions
- **Pro ($29/mo)**: Reserved domains, IP whitelisting, multiple tunnels

---

## Option 2: Cloudflare Tunnel (Free, Production-Ready)

### Install cloudflared
```bash
# Install via Homebrew
brew install cloudflare/cloudflare/cloudflared

# Or download from https://developers.cloudflare.com/cloudflare-one/connections/connect-apps/install-and-setup/installation/
```

### Authenticate
```bash
# Login to Cloudflare (opens browser)
cloudflared tunnel login
```

### Create Tunnel
```bash
# Create a named tunnel
cloudflared tunnel create clawd-bot

# Output will show tunnel ID and credentials location:
# Created tunnel clawd-bot with id abc123-def456-...
# Credentials written to: /Users/scott/.cloudflared/abc123-def456-....json
```

### Configure DNS
```bash
# Route a domain to your tunnel
# (requires you own a domain on Cloudflare)
cloudflared tunnel route dns clawd-bot clawd.bot

# Or use a subdomain
cloudflared tunnel route dns clawd-bot app.clawd.bot
```

### Run Tunnel
```bash
# Forward traffic to local Flask app
cloudflared tunnel run --url http://localhost:5000 clawd-bot

# Or use config file (create ~/.cloudflared/config.yml):
```

**~/.cloudflared/config.yml**:
```yaml
tunnel: abc123-def456-ghi789
credentials-file: /Users/scott/.cloudflared/abc123-def456-ghi789.json

ingress:
  - hostname: clawd.bot
    service: http://localhost:5000
  - hostname: api.clawd.bot
    service: http://localhost:8000
  - service: http_status:404
```

Then run:
```bash
cloudflared tunnel run clawd-bot
```

### Run as Background Service (Auto-start on boot)
```bash
# Install as a service
sudo cloudflared service install

# Start service
sudo launchctl start com.cloudflare.cloudflared

# Check status
sudo launchctl list | grep cloudflare
```

### Pros
- ✅ **100% FREE** (unlimited bandwidth, no time limits)
- ✅ Custom domain support (bring your own)
- ✅ DDoS protection
- ✅ Auto HTTPS
- ✅ Production-ready reliability

### Cons
- ❌ Requires domain on Cloudflare (free to transfer)
- ❌ Slightly more complex setup

---

## Option 3: Port Forwarding via Router (Full Control)

### Step 1: Get Local IP
```bash
ifconfig | grep "inet " | grep -v 127.0.0.1
```

Example output: `inet 192.168.1.108`

### Step 2: Configure Static IP (Optional but Recommended)

#### macOS System Settings
1. Open **System Settings** → **Network**
2. Select your connection (Wi-Fi or Ethernet)
3. Click **Details**
4. Go to **TCP/IP** tab
5. Change **Configure IPv4** to **Manually**
6. Set:
   - **IP Address**: `192.168.1.108` (your current IP)
   - **Subnet Mask**: `255.255.255.0`
   - **Router**: `192.168.1.1` (your router IP)
7. Go to **DNS** tab and set DNS servers:
   - `1.1.1.1` (Cloudflare)
   - `8.8.8.8` (Google)
8. Click **OK** and **Apply**

### Step 3: Access Router Admin Panel

```bash
# Find your router IP (usually 192.168.1.1 or 192.168.0.1)
netstat -nr | grep default
```

Open router admin panel:
- Common IPs: `http://192.168.1.1` or `http://192.168.0.1`
- Common logins:
  - **Comcast/Xfinity**: admin / password (check sticker on router)
  - **Netgear**: admin / password
  - **TP-Link**: admin / admin
  - **Asus**: admin / admin
  - **Linksys**: admin / admin

### Step 4: Configure Port Forwarding

Each router is different, but generally:

1. Navigate to **Advanced** → **Port Forwarding** (or **NAT Forwarding**)
2. Add new rule:
   - **Service Name**: Flask App
   - **External Port**: 80 (HTTP) or 443 (HTTPS)
   - **Internal IP**: `192.168.1.108` (your Mac's static IP)
   - **Internal Port**: 5000 (Flask default)
   - **Protocol**: TCP
3. Save and enable the rule

### Step 5: Get Public IP
```bash
curl ifconfig.me
```

Example output: `73.112.45.67`

### Step 6: Test Access
```bash
# From external network (mobile hotspot):
curl http://73.112.45.67
```

### Step 7: Set Up Dynamic DNS (Optional)

Your ISP likely changes your public IP periodically. Use a Dynamic DNS service:

**Free DDNS Providers**:
- **DuckDNS** (https://www.duckdns.org) - Free, no ads
- **No-IP** (https://www.noip.com) - Free tier: 3 hostnames
- **Dynu** (https://www.dynu.com) - Free, unlimited hostnames

**Example: DuckDNS Setup**
```bash
# 1. Sign up at https://www.duckdns.org
# 2. Create domain: clawd-bot.duckdns.org
# 3. Install update client:

curl "https://www.duckdns.org/update?domains=clawd-bot&token=YOUR_TOKEN&ip=" | cron

# Add to crontab (update every 5 minutes):
*/5 * * * * curl -s "https://www.duckdns.org/update?domains=clawd-bot&token=YOUR_TOKEN&ip=" >/dev/null 2>&1
```

Now access your site at: **http://clawd-bot.duckdns.org**

### Pros
- ✅ Full control
- ✅ No third-party dependencies
- ✅ Free (if you have static IP or DDNS)

### Cons
- ❌ Exposes your home IP
- ❌ ISP may block port 80/443 (check Terms of Service)
- ❌ No DDoS protection
- ❌ Manual HTTPS setup required (Let's Encrypt)

---

## Option 4: LocalTunnel (Quick & Dirty)

```bash
# Install
npm install -g localtunnel

# Run tunnel
lt --port 5000

# Output:
# your url is: https://fast-panda-42.loca.lt
```

### Pros
- ✅ Instant setup (no signup)
- ✅ Free

### Cons
- ❌ Random URLs
- ❌ Rate limited
- ❌ Not reliable for production

---

## Recommended Setup for clawd.bot

### For Development
```bash
# Use ngrok for quick testing
ngrok http 5000
```

### For Production
```bash
# Use Cloudflare Tunnel with custom domain
# 1. Buy clawd.bot domain ($10-15/year at Namecheap, Cloudflare Registrar, etc.)
# 2. Transfer DNS to Cloudflare (free)
# 3. Set up tunnel:

cloudflared tunnel create clawd-bot
cloudflared tunnel route dns clawd-bot clawd.bot
cloudflared tunnel run --url http://localhost:5000 clawd-bot
```

---

## Security Considerations

### 1. Firewall Rules
```bash
# macOS: Enable firewall
sudo /usr/libexec/ApplicationFirewall/socketfilterfw --setglobalstate on

# Allow only necessary ports
sudo /usr/libexec/ApplicationFirewall/socketfilterfw --add /usr/local/bin/python3
```

### 2. Rate Limiting (Add to Flask)
```python
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

limiter = Limiter(
    app,
    key_func=get_remote_address,
    default_limits=["200 per day", "50 per hour"]
)

@app.route("/api/endpoint")
@limiter.limit("10 per minute")
def api_endpoint():
    return {"status": "ok"}
```

### 3. Authentication
```python
from flask_httpauth import HTTPBasicAuth
auth = HTTPBasicAuth()

@auth.verify_password
def verify_password(username, password):
    # In production: use bcrypt + database
    if username == "admin" and password == "SECRET_PASSWORD":
        return username

@app.route("/admin")
@auth.login_required
def admin_panel():
    return "Admin Dashboard"
```

### 4. HTTPS (Production)
```bash
# Use Cloudflare Tunnel (auto HTTPS)
# OR Let's Encrypt with certbot:

sudo certbot certonly --standalone -d clawd.bot
# Certificates saved to: /etc/letsencrypt/live/clawd.bot/
```

Then update Flask:
```python
if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=443,
        ssl_context=(
            "/etc/letsencrypt/live/clawd.bot/fullchain.pem",
            "/etc/letsencrypt/live/clawd.bot/privkey.pem"
        )
    )
```

---

## Testing Your Setup

```bash
# 1. Start Flask app
cd ~/src/quant
python3 app.py

# 2. In another terminal, start tunnel:
# Option A: ngrok
ngrok http 5000

# Option B: Cloudflare
cloudflared tunnel run --url http://localhost:5000 clawd-bot

# 3. Test from external device (phone, friend's computer)
curl https://YOUR_PUBLIC_URL/api/v1/targets
```

---

## Next Steps
1. Choose tunneling method (Cloudflare Tunnel recommended)
2. Set up custom domain (clawd.bot)
3. Deploy company website (see `/websites/clawd_bot/`)
4. Add rate limiting + authentication
5. Set up monitoring (Uptime Robot, Pingdom)
6. Configure auto-restart (systemd, launchd, or PM2)

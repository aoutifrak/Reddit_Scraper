# Reddit NSFW Subreddit Metadata Scraper

A Python scraper that collects metadata (name, subscriber count) of NSFW subreddits from Reddit, routing all traffic through a Gluetun VPN proxy. Automatically rotates IP by restarting the Gluetun container when rate-limited or blocked.

## Features

- **Raw Reddit API** - Uses `requests` with OAuth2 (no PRAW dependency)
- **Gluetun VPN Proxy** - All traffic routed through VPN for privacy
- **Automatic IP Rotation** - Restarts Gluetun container on 429/403 errors to get a fresh IP
- **Multi-method Discovery** - Keyword search, popular/new listings, related subreddit traversal
- **Deduplication** - Prevents duplicate entries across all discovery methods
- **Resume Support** - Checkpoint-based state saving for graceful shutdown/resume
- **Dual Export** - Outputs to both JSON and CSV formats

## Prerequisites

- Python 3.10+
- Docker (for Gluetun VPN only)
- Reddit API credentials (create app at https://www.reddit.com/prefs/apps)
- VPN provider credentials (Mullvad, NordVPN, Surfshark, etc.)

## Quick Start

### 1. Configure Environment

```bash
cd /home/kali/Desktop/scraper
cp .env.example .env
```

Edit `.env` with your credentials:

```env
# Reddit API (required)
REDDIT_CLIENT_ID=your_client_id
REDDIT_CLIENT_SECRET=your_client_secret

# VPN Provider (example: NordVPN)
VPN_SERVICE_PROVIDER=nordvpn
VPN_TYPE=openvpn
OPENVPN_USER=your_nordvpn_service_username
OPENVPN_PASSWORD=your_nordvpn_service_password
```

### 2. Create Reddit App

1. Go to https://www.reddit.com/prefs/apps
2. Click "Create App" or "Create Another App"
3. Select **script** type
4. Name: `NSFWSubredditScraper`
5. Redirect URI: `http://localhost:8080` (not used but required)
6. Copy the client ID (under app name) and secret

### 3. Start Gluetun VPN Proxy

```bash
# Start Gluetun container (runs in background)
docker-compose up -d

# Verify it's running and healthy
docker-compose ps

# Check logs if needed
docker-compose logs -f gluetun
```

### 4. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 5. Run the Scraper

```bash
# Load environment variables and run
set -a && source .env && set +a && python src/main.py
```

### 6. Output

Results are saved to:
- `data/subreddits.json` - Full JSON with all metadata
- `data/subreddits.csv` - CSV format for spreadsheet import

## VPN Provider Configuration

### NordVPN (OpenVPN)

```env
VPN_SERVICE_PROVIDER=nordvpn
VPN_TYPE=openvpn
OPENVPN_USER=your_nordvpn_service_username
OPENVPN_PASSWORD=your_nordvpn_service_password
SERVER_COUNTRIES=Switzerland
```

### Mullvad (WireGuard)

```env
VPN_SERVICE_PROVIDER=mullvad
VPN_TYPE=wireguard
WIREGUARD_PRIVATE_KEY=your_private_key
WIREGUARD_ADDRESSES=10.x.x.x/32
SERVER_COUNTRIES=Switzerland
```

### Surfshark (OpenVPN)

```env
VPN_SERVICE_PROVIDER=surfshark
VPN_TYPE=openvpn
OPENVPN_USER=your_surfshark_username
OPENVPN_PASSWORD=your_surfshark_password
SERVER_COUNTRIES=Netherlands
```

See [Gluetun Wiki](https://github.com/qdm12/gluetun-wiki) for all supported providers.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                         Host Machine                          │
│  ┌─────────────┐       ┌─────────────────────────────────┐   │
│  │   Python    │ :8888 │      Docker: Gluetun (VPN)      │   │
│  │   Scraper   │──────▶│   HTTP Proxy → VPN Tunnel       │──▶ Internet
│  └─────────────┘       └─────────────────────────────────┘   │
│         │                            ▲                        │
│         │      Docker SDK            │                        │
│         └────────────────────────────┘                        │
│              (restart on 429/403)                             │
└──────────────────────────────────────────────────────────────┘
```

## Rate Limit Handling

1. **429 Too Many Requests** → Restart Gluetun → New IP → Retry
2. **403 Forbidden** → Restart Gluetun → New IP → Retry
3. **Max retries exceeded** → Save checkpoint → Exit gracefully

The scraper saves state to `data/checkpoint.json` on shutdown. Run again to resume.

## Commands Reference

```bash
# Start VPN proxy
docker-compose up -d

# Stop VPN proxy
docker-compose down

# View VPN logs
docker-compose logs -f gluetun

# Check current IP through proxy
curl -x http://localhost:8888 https://httpbin.org/ip

# Run scraper
set -a && source .env && set +a && python src/main.py
```

## Troubleshooting

### Gluetun won't start
- Check VPN credentials in `.env`
- Verify your VPN subscription is active
- Try a different server country

### "Proxy verification failed"
- Ensure Gluetun container is healthy: `docker-compose ps`
- Check Gluetun logs: `docker-compose logs gluetun`
- Test proxy: `curl -x http://localhost:8888 https://httpbin.org/ip`

### Permission denied on Docker socket
```bash
sudo chmod 666 /var/run/docker.sock
# Or add user to docker group
sudo usermod -aG docker $USER
```

## License

MIT License - Use responsibly and in compliance with Reddit's Terms of Service.
# Reddit_Scraper

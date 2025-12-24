# Reddit NSFW Subreddit Scraper - AI Coding Instructions

## Architecture Overview

This is a Python scraper that collects NSFW subreddit metadata via Reddit's OAuth API, routing all traffic through a Gluetun VPN proxy container. When rate-limited (429) or blocked (403), it automatically restarts the Gluetun container to get a fresh IP.

### Key Components

- `src/main.py` - Entry point, orchestrates discovery phases and handles recovery
- `src/reddit_client.py` - Raw `requests`-based Reddit OAuth client (no PRAW)
- `src/gluetun_controller.py` - Docker SDK integration for container restart/IP rotation
- `src/discovery.py` - Subreddit discovery via keyword search + related traversal
- `src/checkpoint.py` - State persistence for resume support
- `src/exporter.py` - JSON/CSV output

### Data Flow

```
main.py → reddit_client.py → Gluetun proxy → Reddit API
    ↓ (on 429/403)
gluetun_controller.py → Docker socket → restart Gluetun → new IP → retry
```

## Critical Patterns

### Rate Limit Recovery
All API calls should be wrapped with `_run_with_recovery()` in `main.py` which catches `RedditRateLimitError`/`RedditBlockedError` and triggers Gluetun restart:

```python
for info in self._run_with_recovery(self.discovery.search_by_keyword, keyword):
    self._process_subreddit(info)
```

### Proxy Enforcement
The scraper MUST refuse to run without an active proxy. Check in `main.py`:
```python
if not self.verify_proxy():
    logger.error("Aborting: Proxy not active")
    sys.exit(1)
```

### Deduplication
`SubredditDiscovery.discovered` (Set) tracks seen subreddit names across all discovery methods to prevent duplicates.

## Environment Variables

Required:
- `REDDIT_CLIENT_ID`, `REDDIT_CLIENT_SECRET` - Reddit OAuth credentials
- `HTTP_PROXY`/`HTTPS_PROXY` - Gluetun proxy URL (e.g., `http://gluetun:8888`)

Optional:
- `GLUETUN_CONTAINER_NAME` - Container to restart (default: `gluetun`)
- `GLUETUN_RESTART_COOLDOWN` - Seconds to wait after restart (default: `15`)

## Testing Changes

```bash
# Run locally (requires Gluetun running separately)
export HTTPS_PROXY=http://localhost:8888
python src/main.py

# Run with Docker
docker-compose up --build
```

## Adding New Discovery Methods

1. Add method to `SubredditDiscovery` class in `src/discovery.py`
2. Method should be a generator yielding `SubredditInfo` objects
3. Use `self._extract_subreddit_info()` to filter for NSFW public subreddits
4. Add deduplication check: `if name in self.discovered: continue`
5. Call from `main.py` using `_run_with_recovery()` wrapper

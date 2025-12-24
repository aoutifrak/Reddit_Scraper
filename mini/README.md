## Mini Reddit Commenters Project

This mini project demonstrates how to use [YARS (Yet Another Reddit Scraper)](http://github.com/datavorous/yars) to collect every unique commenter that appears in the most recent posts of one or more subreddits.

### What this does

- Calls YARS to fetch posts from the subreddits you specify.
- Uses YARS again to pull the full post details (including comments and nested replies).
- Builds a unique set of usernames who commented on each post and aggregates them per subreddit.
- Stores the structured results as JSON for later processing.

### Requirements

- Python 3.10+
- Reddit is accessed without official API keys, but heavy scraping should still be rate-limited or routed through proxies as YARS recommends.

### Setup

```bash
cd mini
python -m venv .venv
source .venv/bin/activate
# Install YARS in editable mode from the vendored repo
pip install -r requirements.txt
```

> **Note:** The `requirements.txt` installs the copy of [datavorous/yars](http://github.com/datavorous/yars) vendored under `mini/yars/`.  
> If you update that folder (e.g., `git pull`), re-run `pip install -r requirements.txt` so the editable install picks up the changes.

> **Requirements:** Docker must be installed and the user must have permission to manage containers because this tool automatically starts/restarts a Gluetun VPN container to rotate IPs.

### Usage

```bash
python scrape_commenters.py \
  generative python \
  --limit 5 \
  --category top \
  --time-filter week \
  --output-dir results
```

- `subreddits`: space-separated list of subreddit names (without `r/`).
- `--limit`: how many posts per subreddit to inspect (default `3`).
- `--category`: one of `hot`, `new`, `top`, `rising`.
- `--time-filter`: when category is `top`, choose from `hour`, `day`, `week`, `month`, `year`, `all`.
- `--output-dir`: directory where JSON summaries will be stored.

Each run prints a short summary and writes `<subreddit>_commenters.json` under the output directory. The JSON contains:

```json
{
  "subreddit": "generative",
  "category": "top",
  "time_filter": "week",
  "post_count": 5,
  "unique_commenters": ["userA", "userB"],
  "posts": [
    {
      "permalink": "/r/generative/comments/...",
      "title": "...",
      "commenters": ["userA", "userC"]
    }
  ]
}
```

Additionally, a `<subreddit>_commenters.csv` file is generated containing a single column with every unique username (one per line) for quick import into spreadsheets or downstream tooling.

### Gluetun auto-rotation

- The script automatically (re)starts a Gluetun VPN container (default name `gluetun_mini`) using the credentials found in `../config.json` (or `RedditDMBot/rsrc/config.json` as fallback).
- All Reddit traffic is routed through the Gluetun HTTP proxy exposed on the host.
- After every 100 posts scraped (configurable via `--restart-after`), the container is restarted to obtain a fresh IP.
- If Reddit replies with HTTP 429 “too many requests”, the scraper pauses that specific post, restarts Gluetun, waits for the proxy to become healthy, and then resumes right where it left off (no data is discarded).
- Any other scraping error also triggers a Gluetun restart before retrying the subreddit.
- Use `--gluetun-container NAME` to point at a different container if desired.

### Notes

- YARS talks to Reddit’s public JSON endpoints. Respect Reddit’s rules and consider using rotating proxies for heavy workloads as the YARS README advises.
- If Reddit changes its HTML/JSON structure the upstream YARS project might need updates. Pull the latest version from the repo to stay current.


#!/usr/bin/env python3
"""
Use YARS (Yet Another Reddit Scraper) to gather every unique commenter
on the latest posts from one or more subreddits.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Iterable, List, Set
import csv
import json
import socket
import os
import sys
import time
import random

import docker
import requests


def _ensure_local_yars_on_path() -> None:
    """
    Add the vendored yars/src directory to sys.path so we can import it
    even if it isn't installed system-wide.
    """
    current_dir = Path(__file__).resolve().parent
    yars_src = current_dir / "yars" / "src"
    if yars_src.exists():
        sys.path.insert(0, str(yars_src))


_ensure_local_yars_on_path()

from yars.yars import YARS, TooManyRequestsError

# Configuration constants
DEFAULT_SUBREDDIT_FILE = "subreddits.txt"  # Default file to load subreddits from


def find_available_port(start_port: int) -> int:
    for port in range(start_port, start_port + 100):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    raise RuntimeError("Unable to find available port for Gluetun proxy")


def load_gluetun_env() -> Dict[str, str]:
    base = Path(__file__).resolve().parent
    parent = base.parent
    config_paths = [
        parent / "config.json",
        parent / "RedditDMBot" / "rsrc" / "config.json",
        base / "config.json",
    ]

    env = {
        "VPN_SERVICE_PROVIDER": "nordvpn",
        "HTTPPROXY": "on",
        "SOCKS5PROXY": "on",
    }

    # First try to load from .env file
    env_file = parent / ".env"
    if env_file.exists():
        try:
            with env_file.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" in line:
                        key, _, value = line.partition("=")
                        key = key.strip()
                        value = value.strip()
                        # Map environment variable names to Gluetun expected names
                        if key in ("VPN_SERVICE_PROVIDER", "OPENVPN_USER", "OPENVPN_PASSWORD", 
                                   "SERVER_COUNTRIES", "SERVER_CITIES", "VPN_TYPE"):
                            env[key] = value
        except OSError:
            pass

    # Then check config.json files (these override .env)
    for path in config_paths:
        if not path.exists():
            continue
        try:
            with path.open("r", encoding="utf-8") as fh:
                config = json.load(fh)
                gluetun_cfg = config.get("gluetun", {})
                if gluetun_cfg:
                    env["VPN_SERVICE_PROVIDER"] = gluetun_cfg.get(
                        "vpn_service_provider", env["VPN_SERVICE_PROVIDER"]
                    )
                    user = gluetun_cfg.get("openvpn_user")
                    password = gluetun_cfg.get("openvpn_password")
                    if user:
                        env["OPENVPN_USER"] = user
                    if password:
                        env["OPENVPN_PASSWORD"] = password
                    city = gluetun_cfg.get("server_city")
                    country = gluetun_cfg.get("server_countries")
                    if city:
                        env["SERVER_CITY"] = city
                    elif country:
                        env["SERVER_COUNTRIES"] = country
                    break
        except (json.JSONDecodeError, OSError):
            continue
    return env


def test_proxy_basic(http_proxy: str, timeout: int = 5) -> bool:
    """Quick test if proxy is accepting connections."""
    proxies = {"http": http_proxy}
    try:
        resp = requests.get("http://httpbin.org/ip", proxies=proxies, timeout=timeout)
        return resp.status_code == 200
    except requests.RequestException:
        return False


def test_proxy(http_proxy: str, timeout: int = 15) -> bool:
    """Test if proxy is working and Reddit is accessible."""
    proxies = {"http": http_proxy}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    try:
        # First test basic connectivity with HTTP
        resp = requests.get("http://httpbin.org/ip", proxies=proxies, timeout=timeout, headers=headers)
        if resp.status_code != 200:
            return False
        # Then verify Reddit JSON access (not blocked)
        reddit_resp = requests.get(
            "http://old.reddit.com/r/pics.json?limit=1",
            proxies=proxies,
            timeout=timeout,
            headers=headers
        )
        # Check if we got JSON (not blocked HTML page)
        if reddit_resp.status_code == 200:
            try:
                data = reddit_resp.json()
                if "data" in data:
                    return True
            except Exception:
                pass
        return False
    except requests.RequestException:
        return False


class GluetunManager:
    def __init__(self, container_name: str = "gluetun_mini"):
        self.container_name = container_name
        self.client = docker.from_env()
        self.env = load_gluetun_env()
        self.info: Dict | None = None
        # Docker network to attach the gluetun container to. Allows multiple
        # app containers to share the same internal network while keeping
        # outbound traffic routed through the gluetun container.
        # Configure via environment var `GLUETUN_NETWORK` if needed.
        self.network = os.environ.get("GLUETUN_NETWORK", "gluetun_shared_net")

    def get_proxy(self) -> str:
        info = self.ensure_running()
        return info["http_proxy"]

    def ensure_running(self) -> Dict:
        if self.info and test_proxy(self.info["http_proxy"]):
            return self.info
        self.info = self._start_container()
        return self.info

    def restart(self) -> Dict:
        """Recreate the container to get a fresh IP."""
        print(f"[GLUETUN] Recreating container {self.container_name} to rotate IP...")
        try:
            container = self.client.containers.get(self.container_name)
            print(f"[GLUETUN] Stopping and removing old container...")
            container.stop(timeout=10)
            container.remove(force=True)
            time.sleep(2)
        except docker.errors.NotFound:
            print(f"[GLUETUN] Container not found, will create fresh")
        except docker.errors.APIError as exc:
            print(f"[GLUETUN] Error removing container: {exc}")
            try:
                container.remove(force=True)
            except Exception:
                pass
        self.info = None
        return self.ensure_running()

    def new_miner(self) -> YARS:
        proxy = self.get_proxy()
        print(f"[GLUETUN] Using HTTP proxy {proxy}")
        return YARS(proxy=proxy)

    def _start_container(self) -> Dict:
        http_port = None
        socks_port = None
        try:
            existing = self.client.containers.get(self.container_name)
            print(f"[GLUETUN] Container {self.container_name} already exists.")
            existing.reload()
            ports = existing.attrs.get("NetworkSettings", {}).get("Ports", {}) or {}
            if ports.get("8888/tcp"):
                http_port = int(ports["8888/tcp"][0]["HostPort"])
            if ports.get("8388/tcp"):
                socks_port = int(ports["8388/tcp"][0]["HostPort"])
            if existing.status != "running":
                print(f"[GLUETUN] Container not running, starting it...")
                existing.start()
                time.sleep(5)
                existing.reload()
                ports = existing.attrs.get("NetworkSettings", {}).get("Ports", {}) or {}
                if ports.get("8888/tcp"):
                    http_port = int(ports["8888/tcp"][0]["HostPort"])
                if ports.get("8388/tcp"):
                    socks_port = int(ports["8388/tcp"][0]["HostPort"])

            http_proxy = f"http://127.0.0.1:{http_port}" if http_port else "http://127.0.0.1:8888"
            
            # Wait for proxy to become ready (up to 30 seconds)
            print(f"[GLUETUN] Waiting for proxy {http_proxy} to become ready...")
            start_time = time.time()
            max_wait = 30
            while time.time() - start_time < max_wait:
                if test_proxy(http_proxy):
                    print(f"[GLUETUN] Reusing existing container on port {http_port}")
                    return {
                        "container": existing,
                        "http_port": http_port,
                        "socks5_port": socks_port,
                        "http_proxy": http_proxy,
                    }
                time.sleep(3)
            
            # If still not ready, remove and recreate the container
            print("[GLUETUN] Proxy not responding, removing container to recreate...")
            try:
                existing.stop(timeout=10)
                existing.remove(force=True)
                time.sleep(2)
            except Exception as e:
                print(f"[GLUETUN] Error removing container: {e}")
            
        except docker.errors.NotFound:
            print(f"[GLUETUN] Container {self.container_name} not found, creating new one...")

        # Retry loop: create container, wait 10s, if not ready destroy and retry
        max_attempts = 10
        for attempt in range(1, max_attempts + 1):
            # Clean up any existing container first
            try:
                old = self.client.containers.get(self.container_name)
                print(f"[GLUETUN] Removing existing container before attempt {attempt}...")
                old.stop(timeout=5)
                old.remove(force=True)
                time.sleep(2)
            except docker.errors.NotFound:
                pass
            except Exception as e:
                print(f"[GLUETUN] Cleanup error: {e}")
                try:
                    self.client.containers.get(self.container_name).remove(force=True)
                except Exception:
                    pass
                time.sleep(2)

            http_port = find_available_port(8888)
            socks_port = find_available_port(8388)

            ports = {
                "8888/tcp": ("0.0.0.0", http_port),
                "8388/tcp": ("0.0.0.0", socks_port),
                "8388/udp": ("0.0.0.0", socks_port),
            }

            # Ensure network exists or use bridge
            network_to_use = self.network
            try:
                self.client.networks.get(self.network)
            except docker.errors.NotFound:
                try:
                    self.client.networks.create(self.network, driver="bridge")
                    print(f"[GLUETUN] Created network {self.network}")
                except docker.errors.APIError:
                    network_to_use = "bridge"
                    print(f"[GLUETUN] Using default bridge network")

            print(
                f"[GLUETUN] Attempt {attempt}/{max_attempts}: Starting container {self.container_name} on "
                f"http:{http_port}, socks5:{socks_port}"
            )
            
            try:
                container = self.client.containers.run(
                    image="qmcgaw/gluetun:latest",
                    name=self.container_name,
                    cap_add=["NET_ADMIN"],
                    devices=["/dev/net/tun:/dev/net/tun"],
                    environment=self.env,
                    ports=ports,
                    network=network_to_use,
                    detach=True,
                    restart_policy={"Name": "unless-stopped"},
                )
            except docker.errors.APIError as e:
                print(f"[GLUETUN] Failed to create container: {e}")
                time.sleep(3)
                continue

            # Wait up to 20 seconds for proxy to become ready
            http_proxy = f"http://127.0.0.1:{http_port}"
            start_time = time.time()
            # First wait for basic connectivity (VPN to connect)
            while time.time() - start_time < 12:
                if test_proxy_basic(http_proxy):
                    print(f"[GLUETUN] Basic proxy connectivity OK, testing Reddit...")
                    break
                time.sleep(1)
            # Then test full Reddit access
            while time.time() - start_time < 20:
                if test_proxy(http_proxy):
                    print(f"[GLUETUN] Proxy ready on {http_proxy} (attempt {attempt})")
                    return {
                        "container": container,
                        "http_port": http_port,
                        "socks5_port": socks_port,
                        "http_proxy": http_proxy,
                    }
                time.sleep(2)

            print(f"[GLUETUN] Attempt {attempt} failed - proxy not ready in 20s, destroying and retrying...")
            try:
                container.stop(timeout=5)
                container.remove(force=True)
            except Exception:
                pass
            time.sleep(2)

        # If all attempts failed, return last attempt info anyway
        print(f"[GLUETUN] All {max_attempts} attempts failed, continuing with last container...")
        return {
            "container": container,
            "http_port": http_port,
            "socks5_port": socks_port,
            "http_proxy": http_proxy,
        }

def load_subreddits_from_file(filepath: str) -> List[str]:
    """
    Load subreddit names from a file (one per line).
    Strips whitespace and filters out empty lines and comments.
    """
    subreddits = []
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                # Skip empty lines and comments
                if line and not line.startswith("#"):
                    subreddits.append(line)
        print(f"[INFO] Loaded {len(subreddits)} subreddits from {filepath}")
        return subreddits
    except FileNotFoundError:
        print(f"[ERROR] Subreddit file not found: {filepath}")
        return []


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggregate Reddit commenters for posts in subreddits using YARS."
    )
    parser.add_argument(
        "subreddits",
        nargs="*",
        help="List of subreddit names (without the leading r/). If omitted, use --subreddit-file.",
    )
    parser.add_argument(
        "--subreddit-file",
        type=str,
        default=DEFAULT_SUBREDDIT_FILE,
        help=f"Path to a file containing subreddit names (one per line). Default: {DEFAULT_SUBREDDIT_FILE}",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Number of posts per subreddit to inspect (default: 1000). Max ~1000 per category.",
    )
    parser.add_argument(
        "--category",
        choices=["hot", "new", "top", "rising"],
        default="new",
        help="Which listing to use when fetching subreddit posts (default: new).",
    )
    parser.add_argument(
        "--time-filter",
        choices=["hour", "day", "week", "month", "year", "all"],
        default="all",
        help="Time filter to use when category is 'top' (ignored otherwise). Default: all for maximum posts.",
    )
    parser.add_argument(
        "--output-dir",
        default="results",
        help="Directory where JSON summaries will be stored (default: results).",
    )
    parser.add_argument(
        "--gluetun-container",
        default="gluetun_mini",
        help="Name of the Gluetun container to manage (default: gluetun_mini).",
    )
    parser.add_argument(
        "--restart-after",
        type=int,
        default=250,
        help="Restart Gluetun after scraping this many posts to rotate IP (default: 250).",
    )
    return parser.parse_args()


def extract_commenters(comments: Iterable[Dict]) -> Set[str]:
    """
    Recursively collect commenter usernames from the comment structure returned by YARS.
    """
    commenters: Set[str] = set()

    for comment in comments or []:
        author = comment.get("author")
        if author and author not in {"[deleted]", "AutoModerator"}:
            commenters.add(author)

        replies = comment.get("replies") or []
        commenters.update(extract_commenters(replies))

    return commenters


def normalize_permalink(post: Dict) -> str | None:
    """
    Ensure we have a Reddit permalink that YARS can feed into scrape_post_details.
    """
    permalink = post.get("permalink")
    if permalink:
        return permalink

    url = post.get("url") or post.get("link")
    if url and "reddit.com" in url:
        return url.split("reddit.com", 1)[1]
    return None


def gather_commenters_for_subreddit(
    miner: YARS,
    subreddit: str,
    limit: int,
    category: str,
    time_filter: str,
    refresh_callback=None,
) -> tuple[Dict, YARS]:
    print(f"\n[INFO] Fetching up to {limit} posts from r/{subreddit} ({category}, {time_filter})")
    
    # Helper function to fetch posts with retry logic
    def fetch_with_retry(miner, subreddit, limit, category, time_filter, max_retries=5):
        """Fetch posts with automatic container restart on 403/429 errors"""
        for attempt in range(max_retries):
            try:
                posts = miner.fetch_subreddit_posts(
                    subreddit,
                    limit=limit,
                    category=category,
                    time_filter=time_filter,
                )
                return posts, miner
            except TooManyRequestsError as exc:
                if attempt < max_retries - 1:
                    print(f"[RETRY] {exc} - Restarting container...")
                    if refresh_callback:
                        miner = refresh_callback()
                    time.sleep(random.uniform(1, 2))
                else:
                    print(f"[WARN] Failed to fetch posts after {max_retries} retries")
                    return [], miner
        return [], miner
    
    posts, miner = fetch_with_retry(miner, subreddit, limit, category, time_filter, max_retries=5)
    
    # If we got fewer posts than requested and using 'top', try 'hot' as fallback
    if len(posts) < limit * 0.5 and category == "top":
        print(f"[INFO] Got {len(posts)} posts - Trying 'hot' category...")
        hot_posts, miner = fetch_with_retry(miner, subreddit, limit // 2, "hot", "all", max_retries=3)
        posts.extend(hot_posts)
        posts = list({p['permalink']: p for p in posts}.values())
    
    # If still low, try 'new' category
    if len(posts) < limit * 0.75:
        print(f"[INFO] Got {len(posts)} posts - Trying 'new' category...")
        new_posts, miner = fetch_with_retry(miner, subreddit, limit // 2, "new", "all", max_retries=3)
        posts.extend(new_posts)
        posts = list({p['permalink']: p for p in posts}.values())

    post_summaries: List[Dict] = []
    unique_commenters: Set[str] = set()

    for idx, post in enumerate(posts, start=1):
        permalink = normalize_permalink(post)
        if not permalink:
            continue

        # Less verbose output - only show every 50 posts
        if idx % 50 == 1 or idx % 50 == 0:
            print(f"[PROGRESS] ({idx}/{len(posts)}) Scraping posts...")
        
        retry = 0
        details = None
        max_retries = 8  # Slightly reduced
        container_restart_count = 0
        
        while retry < max_retries:
            try:
                details = miner.scrape_post_details(permalink)
                break
            except TooManyRequestsError as exc:
                retry += 1
                container_restart_count += 1
                print(
                    f"[429] Too many requests while fetching {permalink}. "
                    f"Attempt {retry}/{max_retries}. Restarting Gluetun (#{container_restart_count})..."
                )
                if refresh_callback:
                    miner = refresh_callback()
                else:
                    time.sleep(2)
            except Exception as exc:
                error_msg = str(exc)
                # Check if this is a recoverable error (403, 429, connection error)
                is_recoverable = any(code in error_msg for code in ['403', '429', 'Connection', 'timeout', 'Temporary'])
                
                if is_recoverable:
                    retry += 1
                    container_restart_count += 1
                    print(
                        f"[ERROR] {error_msg} while fetching {permalink}. "
                        f"Attempt {retry}/{max_retries}. Restarting Gluetun (#{container_restart_count})..."
                    )
                    if refresh_callback:
                        miner = refresh_callback()
                    time.sleep(random.uniform(1, 2))  # Wait before retrying
                else:
                    # Permanent error, skip this post
                    print(f"[WARN] Permanent error fetching {permalink}: {error_msg}")
                    details = None
                    break
                    
        if not details:
            print(f"[WARN] Could not fetch post details for {permalink} after {retry} retries.")
            continue

        commenters = extract_commenters(details.get("comments", []))
        unique_commenters.update(commenters)
        post_summaries.append(
            {
                "permalink": permalink,
                "title": details.get("title") or post.get("title"),
                "commenters": sorted(commenters),
            }
        )

    summary = {
        "subreddit": subreddit,
        "category": category,
        "time_filter": time_filter if category == "top" else None,
        "post_count": len(post_summaries),
        "unique_commenter_count": len(unique_commenters),
        "unique_commenters": sorted(unique_commenters),
        "posts": post_summaries,
    }
    return summary, miner


def main() -> None:
    args = parse_args()
    
    # Load subreddits from file or command-line arguments
    # Prioritize: subreddits from command line > file argument > default file
    subreddits = []
    
    if args.subreddits:
        # Command-line subreddits take priority
        subreddits = args.subreddits
        print(f"[INFO] Using {len(subreddits)} subreddits from command-line arguments")
    elif args.subreddit_file and Path(args.subreddit_file).exists():
        # Load from specified or default file
        subreddits = load_subreddits_from_file(args.subreddit_file)
        if not subreddits:
            print("[ERROR] No subreddits loaded from file. Exiting.")
            sys.exit(1)
    else:
        print("[ERROR] No subreddits provided. Use positional arguments or --subreddit-file. Exiting.")
        sys.exit(1)
    
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("\n" + "="*70)
    print("[INFO] Reddit Commenter Scraper - Maximum Collection Mode")
    print("="*70)
    print(f"[INFO] Posts per subreddit: {args.limit} (maximized)")
    print(f"[INFO] Comments per post: 500 (Reddit API max with limit=500)")
    print(f"[INFO] Primary category: {args.category}")
    print(f"[INFO] Time filter: {args.time_filter}")
    print(f"[INFO] Restart IP after: {args.restart_after} posts")
    print(f"[INFO] Multi-category fallback enabled (top→hot→new)")
    print(f"[INFO] Total subreddits to scrape: {len(subreddits)}")
    print(f"[INFO] Output directory: {output_dir}")
    total_expected_posts = min(args.limit, 1000) * len(subreddits)
    total_expected_comments = total_expected_posts * 500
    print(f"[INFO] Expected total posts: ~{total_expected_posts:,}")
    print(f"[INFO] Expected total comments (per post): ~500")
    print(f"[INFO] Potential total commenters: ~{total_expected_comments:,} (before dedup)")
    print("="*70 + "\n")

    gluetun = GluetunManager(container_name=args.gluetun_container)
    miner = gluetun.new_miner()
    posts_since_restart = 0
    
    total_stats = {
        "total_subreddits": len(subreddits),
        "total_posts_scraped": 0,
        "total_unique_commenters": set(),
        "subreddits_completed": 0,
        "subreddits_failed": 0,
    }

    def refresh_miner() -> YARS:
        gluetun.restart()
        return gluetun.new_miner()

    for idx, subreddit in enumerate(subreddits, start=1):
        try:
            print(f"\n[PROGRESS] Processing subreddit {idx}/{len(subreddits)}: r/{subreddit}")
            summary, miner = gather_commenters_for_subreddit(
                miner,
                subreddit=subreddit,
                limit=1,
                category="top",
                time_filter='all',
                refresh_callback=refresh_miner,
            )
        except Exception as exc:
            print(f"[ERROR] Fatal error scraping r/{subreddit}: {exc}")
            total_stats["subreddits_failed"] += 1
            gluetun.restart()
            miner = gluetun.new_miner()
            continue

        output_path = output_dir / f"{subreddit}_commenters.json"
        with output_path.open("w", encoding="utf-8") as fp:
            json.dump(summary, fp, ensure_ascii=False, indent=2)

        print(
            f"[DONE] r/{subreddit}: {summary['unique_commenter_count']} unique commenters "
            f"from {summary['post_count']} posts -> {output_path}"
        )
        csv_path = output_dir / f"{subreddit}_commenters.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as csvfile:
            writer = csv.writer(csvfile)
            for username in summary["unique_commenters"]:
                if username:
                    writer.writerow([username])
        print(f"[DONE] CSV saved to {csv_path}")

        # Update statistics
        total_stats["subreddits_completed"] += 1
        total_stats["total_posts_scraped"] += summary["post_count"]
        total_stats["total_unique_commenters"].update(summary["unique_commenters"])
        
        posts_since_restart += summary["post_count"]
        if posts_since_restart >= args.restart_after:
            print(f"[INFO] Restarting Gluetun after {posts_since_restart} posts to rotate IP...")
            gluetun.restart()
            miner = gluetun.new_miner()
            posts_since_restart = 0

    # Print final statistics
    print("\n" + "="*70)
    print("[INFO] Scraping Complete - Final Statistics")
    print("="*70)
    print(f"[STATS] Total subreddits: {total_stats['total_subreddits']}")
    print(f"[STATS] Successfully scraped: {total_stats['subreddits_completed']}")
    print(f"[STATS] Failed: {total_stats['subreddits_failed']}")
    print(f"[STATS] Total posts scraped: {total_stats['total_posts_scraped']:,}")
    print(f"[STATS] Total unique commenters: {len(total_stats['total_unique_commenters']):,}")
    print(f"[STATS] Results saved to: {output_dir}")
    print("="*70 + "\n")

if __name__ == "__main__":
    main()

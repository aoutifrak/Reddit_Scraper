#!/usr/bin/env python3
"""
Integrated Pipeline: Subreddit Discovery → Commenter Scraping → User Export

Flow:
1. Discovers NSFW subreddits using the main scraper
2. After every 100 subreddits, feeds them to YARS commenter scraper
3. Scrapes all comments from each subreddit (time_filter: all + year)
4. Collects unique commenters and splits into 5k chunk files

Deduplication:
- Persistent hash sets for subreddits (discovered/processed) and users
- In-memory caching with periodic persistence
- Prevents re-processing same subreddits or collecting duplicate users
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import signal
import sys
import time
from pathlib import Path
from typing import Dict, List, Set, Optional

import requests
import subprocess
import shutil
from dotenv import load_dotenv

# Add paths for imports
SCRIPT_DIR = Path(__file__).resolve().parent
MINI_DIR = SCRIPT_DIR / "mini"
SRC_DIR = SCRIPT_DIR / "src"

sys.path.insert(0, str(SRC_DIR))
sys.path.insert(0, str(MINI_DIR))
sys.path.insert(0, str(MINI_DIR / "yars" / "src"))

# Import from src/
from reddit_client import RedditClient, RedditRateLimitError, RedditBlockedError, create_client_from_env
from discovery import SubredditDiscovery, SubredditInfo, NSFW_SEARCH_KEYWORDS

# Import from mini/scrape_commenters.py - reuse existing implementations
from scrape_commenters import (
    GluetunManager,
    extract_commenters,
    normalize_permalink,
    gather_commenters_for_subreddit,
)
from yars.yars import YARS, TooManyRequestsError

# ============================================================================
# Configuration
# ============================================================================

SUBREDDITS_BATCH_SIZE = 100      # Feed to commenter scraper after this many subs
USERS_CHUNK_SIZE = 5000          # Split users into files of this size
USERS_EXPORT_THRESHOLD = 5000    # Export when we hit this many users (same as chunk size)
POSTS_PER_SUBREDDIT = 500        # Posts to scrape per subreddit for commenters
RESTART_AFTER_POSTS = 100        # Restart Gluetun after this many posts
DEDUP_PERSIST_INTERVAL = 100     # Save dedup state every N operations

# SSH Upload Configuration (set via environment variables)
SSH_HOST = os.environ.get("SSH_HOST", "")           # e.g., "192.168.1.100" or "myserver.com"
SSH_PORT = int(os.environ.get("SSH_PORT", "22"))
SSH_USER = os.environ.get("SSH_USER", "")           # e.g., "admin"
SSH_KEY_PATH = os.environ.get("SSH_KEY_PATH", "")   # e.g., "/home/user/.ssh/id_rsa" (optional)
SSH_PASSWORD = os.environ.get("SSH_PASSWORD", "")   # (optional, key preferred)
SSH_REMOTE_DIR = os.environ.get("SSH_REMOTE_DIR", "/data/users")  # Remote destination directory


# ============================================================================
# Deduplication Manager - Persistent Hash Set Storage
# ============================================================================

class DeduplicationManager:
    """
    Manages deduplication for subreddits and users with persistent storage.
    Uses hash sets for O(1) lookup and periodic persistence to disk.
    """
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Subreddit tracking (in-memory hash sets)
        self.discovered_subreddits: Set[str] = set()  # All seen subreddits
        self.processed_subreddits: Set[str] = set()   # Fully scraped subreddits
        self.queued_subreddits: Set[str] = set()      # In current batch queue
        
        # User tracking (in-memory hash set)
        self.seen_users: Set[str] = set()             # All collected users (global dedup)
        
        # Persistence tracking
        self._ops_since_save = 0
        self._user_ops_since_save = 0
        
        # File paths
        self._subreddit_file = output_dir / "dedup_subreddits.json"
        self._users_file = output_dir / "dedup_users.txt"  # Line-based for large sets
        self._users_hash_file = output_dir / "dedup_users_hash.bin"  # Binary hash storage
        
        # Load existing state
        self._load_state()
    
    def _load_state(self) -> None:
        """Load persisted deduplication state."""
        # Load subreddit state
        if self._subreddit_file.exists():
            try:
                with self._subreddit_file.open("r") as f:
                    data = json.load(f)
                    self.discovered_subreddits = set(data.get("discovered", []))
                    self.processed_subreddits = set(data.get("processed", []))
                    print(f"[DEDUP] Loaded: {len(self.discovered_subreddits):,} discovered, "
                          f"{len(self.processed_subreddits):,} processed subreddits")
            except Exception as e:
                print(f"[DEDUP] Failed to load subreddit state: {e}")
        
        # Load user state (line-based file for large datasets)
        if self._users_file.exists():
            try:
                with self._users_file.open("r") as f:
                    self.seen_users = set(line.strip() for line in f if line.strip())
                print(f"[DEDUP] Loaded: {len(self.seen_users):,} seen users")
            except Exception as e:
                print(f"[DEDUP] Failed to load user state: {e}")
    
    def save_state(self, force: bool = False) -> None:
        """Persist deduplication state to disk."""
        # Save subreddit state
        if force or self._ops_since_save >= DEDUP_PERSIST_INTERVAL:
            try:
                with self._subreddit_file.open("w") as f:
                    json.dump({
                        "discovered": list(self.discovered_subreddits),
                        "processed": list(self.processed_subreddits),
                    }, f)
                self._ops_since_save = 0
            except Exception as e:
                print(f"[DEDUP] Failed to save subreddit state: {e}")
        
        # Save user state (append new users to file)
        if force or self._user_ops_since_save >= DEDUP_PERSIST_INTERVAL * 10:
            try:
                # Full rewrite for consistency
                with self._users_file.open("w") as f:
                    for user in sorted(self.seen_users):
                        f.write(f"{user}\n")
                self._user_ops_since_save = 0
                print(f"[DEDUP] Saved {len(self.seen_users):,} users to disk")
            except Exception as e:
                print(f"[DEDUP] Failed to save user state: {e}")
    
    # -------------------------------------------------------------------------
    # Subreddit Deduplication
    # -------------------------------------------------------------------------
    
    def is_subreddit_seen(self, name: str) -> bool:
        """Check if subreddit was already discovered."""
        return name.lower() in self.discovered_subreddits
    
    def is_subreddit_processed(self, name: str) -> bool:
        """Check if subreddit was already fully processed."""
        return name.lower() in self.processed_subreddits
    
    def is_subreddit_queued(self, name: str) -> bool:
        """Check if subreddit is in current processing queue."""
        return name.lower() in self.queued_subreddits
    
    def should_process_subreddit(self, name: str) -> bool:
        """Check if subreddit should be processed (not seen, processed, or queued)."""
        name_lower = name.lower()
        return (name_lower not in self.discovered_subreddits and 
                name_lower not in self.processed_subreddits and
                name_lower not in self.queued_subreddits)
    
    def mark_subreddit_discovered(self, name: str) -> bool:
        """
        Mark subreddit as discovered. Returns True if it was new.
        """
        name_lower = name.lower()
        if name_lower in self.discovered_subreddits:
            return False
        
        self.discovered_subreddits.add(name_lower)
        self._ops_since_save += 1
        
        if self._ops_since_save >= DEDUP_PERSIST_INTERVAL:
            self.save_state()
        
        return True
    
    def mark_subreddit_queued(self, name: str) -> None:
        """Mark subreddit as queued for processing."""
        self.queued_subreddits.add(name.lower())
    
    def mark_subreddit_processed(self, name: str) -> None:
        """Mark subreddit as fully processed."""
        name_lower = name.lower()
        self.processed_subreddits.add(name_lower)
        self.queued_subreddits.discard(name_lower)
        self._ops_since_save += 1
        
        if self._ops_since_save >= DEDUP_PERSIST_INTERVAL:
            self.save_state()
    
    def clear_queue(self) -> None:
        """Clear the current processing queue."""
        self.queued_subreddits.clear()
    
    def set_queue(self, subreddits: List[str]) -> None:
        """Set the current processing queue."""
        self.queued_subreddits = set(s.lower() for s in subreddits)
    
    # -------------------------------------------------------------------------
    # User Deduplication
    # -------------------------------------------------------------------------
    
    def is_user_seen(self, username: str) -> bool:
        """Check if user was already collected."""
        return username.lower() in self.seen_users
    
    def filter_new_users(self, users: Set[str]) -> Set[str]:
        """
        Filter out already-seen users. Returns only new users.
        Also marks new users as seen.
        """
        # Normalize to lowercase for comparison
        users_lower = {u.lower() for u in users}
        new_users_lower = users_lower - self.seen_users
        
        if not new_users_lower:
            return set()
        
        # Get original case versions of new users
        new_users = {u for u in users if u.lower() in new_users_lower}
        
        # Mark as seen
        self.seen_users.update(new_users_lower)
        self._user_ops_since_save += len(new_users_lower)
        
        # Periodic save
        if self._user_ops_since_save >= DEDUP_PERSIST_INTERVAL * 10:
            self.save_state()
        
        return new_users
    
    def add_users(self, users: Set[str]) -> int:
        """
        Add users to seen set. Returns count of new users added.
        """
        users_lower = {u.lower() for u in users}
        new_count = len(users_lower - self.seen_users)
        self.seen_users.update(users_lower)
        self._user_ops_since_save += new_count
        
        if self._user_ops_since_save >= DEDUP_PERSIST_INTERVAL * 10:
            self.save_state()
        
        return new_count
    
    # -------------------------------------------------------------------------
    # Statistics
    # -------------------------------------------------------------------------
    
    def get_stats(self) -> Dict:
        """Get deduplication statistics."""
        return {
            "subreddits_discovered": len(self.discovered_subreddits),
            "subreddits_processed": len(self.processed_subreddits),
            "subreddits_queued": len(self.queued_subreddits),
            "users_seen": len(self.seen_users),
        }


# ============================================================================
# SSH Uploader - Send chunk files to remote server
# ============================================================================

class SSHUploader:
    """
    Uploads files to a remote server via SCP/SSH.
    Supports both key-based and password-based authentication.
    """
    
    def __init__(self, host: str = "", port: int = 22, user: str = "",
                 key_path: str = "", password: str = "", remote_dir: str = "/data/users"):
        self.host = host or SSH_HOST
        self.port = port if port != 22 else SSH_PORT
        self.user = user or SSH_USER
        self.key_path = key_path or SSH_KEY_PATH
        self.password = password or SSH_PASSWORD
        self.remote_dir = remote_dir or SSH_REMOTE_DIR
        self.enabled = bool(self.host and self.user)
        self.uploaded_files: List[str] = []
        
        if self.enabled:
            print(f"[SSH] Upload enabled: {self.user}@{self.host}:{self.port}{self.remote_dir}")
        else:
            print("[SSH] Upload disabled (SSH_HOST or SSH_USER not set)")
    
    def _build_scp_command(self, local_path: Path) -> List[str]:
        """Build SCP command with appropriate authentication."""
        cmd = ["scp", "-P", str(self.port)]
        
        # Add key file if specified
        if self.key_path and Path(self.key_path).exists():
            cmd.extend(["-i", self.key_path])
        
        # Disable strict host key checking for automation
        cmd.extend(["-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes"])
        
        # Source and destination
        cmd.append(str(local_path))
        cmd.append(f"{self.user}@{self.host}:{self.remote_dir}/")
        
        return cmd
    
    def _build_ssh_mkdir_command(self) -> List[str]:
        """Build SSH command to create remote directory."""
        cmd = ["ssh", "-p", str(self.port)]
        
        if self.key_path and Path(self.key_path).exists():
            cmd.extend(["-i", self.key_path])
        
        cmd.extend(["-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes"])
        cmd.append(f"{self.user}@{self.host}")
        cmd.append(f"mkdir -p {self.remote_dir}")
        
        return cmd
    
    def ensure_remote_dir(self) -> bool:
        """Create remote directory if it doesn't exist."""
        if not self.enabled:
            return False
        
        try:
            cmd = self._build_ssh_mkdir_command()
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            return result.returncode == 0
        except Exception as e:
            print(f"[SSH] Warning: Could not create remote dir: {e}")
            return False
    
    def upload_file(self, local_path: Path) -> bool:
        """
        Upload a single file to the remote server.
        Returns True on success, False on failure.
        """
        if not self.enabled:
            return False
        
        if not local_path.exists():
            print(f"[SSH] Error: File not found: {local_path}")
            return False
        
        try:
            cmd = self._build_scp_command(local_path)
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            
            if result.returncode == 0:
                print(f"[SSH] Uploaded: {local_path.name} -> {self.host}:{self.remote_dir}/")
                self.uploaded_files.append(str(local_path))
                return True
            else:
                print(f"[SSH] Failed to upload {local_path.name}: {result.stderr.strip()}")
                return False
                
        except subprocess.TimeoutExpired:
            print(f"[SSH] Timeout uploading {local_path.name}")
            return False
        except Exception as e:
            print(f"[SSH] Error uploading {local_path.name}: {e}")
            return False
    
    def upload_files(self, paths: List[Path]) -> int:
        """Upload multiple files. Returns count of successful uploads."""
        if not self.enabled:
            return 0
        
        # Ensure remote directory exists
        self.ensure_remote_dir()
        
        success_count = 0
        for path in paths:
            if self.upload_file(path):
                success_count += 1
        
        return success_count
    
    def get_stats(self) -> Dict:
        """Get upload statistics."""
        return {
            "enabled": self.enabled,
            "host": self.host if self.enabled else None,
            "uploaded_count": len(self.uploaded_files),
        }


# ============================================================================
# User Export with Chunking (uses DeduplicationManager for global dedup)
# ============================================================================

class UserExporter:
    """Manages user collection and chunked export with deduplication and SSH upload."""
    
    def __init__(self, output_dir: str, dedup_manager: DeduplicationManager, 
                 ssh_uploader: Optional[SSHUploader] = None):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.dedup = dedup_manager  # Reference to global dedup manager
        self.ssh = ssh_uploader     # Optional SSH uploader
        self.pending_users: Set[str] = set()  # Users pending export (not yet in chunk files)
        self.chunk_count = 0
        self.total_exported = 0
        self.total_uploaded = 0
        
        # Load existing state if available
        self._load_state()
        
    def _load_state(self):
        """Load existing state file if present."""
        state_file = self.output_dir / "exporter_state.json"
        if state_file.exists():
            try:
                with state_file.open("r") as f:
                    state = json.load(f)
                    self.chunk_count = state.get("chunk_count", 0)
                    self.total_exported = state.get("total_exported", 0)
                    self.total_uploaded = state.get("total_uploaded", 0)
                    # Load pending users
                    pending = state.get("pending_users", [])
                    self.pending_users = set(pending)
                    print(f"[EXPORTER] Resumed: {self.total_exported:,} exported, "
                          f"{self.total_uploaded:,} uploaded, {len(self.pending_users):,} pending, chunk {self.chunk_count}")
            except Exception:
                pass
    
    def _save_state(self):
        """Save current state."""
        state_file = self.output_dir / "exporter_state.json"
        with state_file.open("w") as f:
            json.dump({
                "chunk_count": self.chunk_count,
                "total_exported": self.total_exported,
                "total_uploaded": self.total_uploaded,
                "pending_users": list(self.pending_users),
            }, f)
        
    def add_users(self, users: Set[str]) -> int:
        """
        Add users to collection after deduplication.
        Returns count of truly new users added.
        """
        # Filter through global dedup (removes already-seen users across all batches)
        new_users = self.dedup.filter_new_users(users)
        
        if new_users:
            self.pending_users.update(new_users)
            print(f"  [USERS] Added {len(new_users):,} new (filtered {len(users) - len(new_users):,} duplicates), "
                  f"pending: {len(self.pending_users):,}, global seen: {len(self.dedup.seen_users):,}")
        else:
            print(f"  [USERS] All {len(users):,} users were duplicates, pending: {len(self.pending_users):,}")
        
        return len(new_users)
        
    def should_export(self, threshold: int = USERS_EXPORT_THRESHOLD) -> bool:
        """Check if we've reached export threshold (default: 5000)."""
        return len(self.pending_users) >= threshold
    
    def export_chunks(self, force: bool = False, threshold: int = USERS_EXPORT_THRESHOLD) -> int:
        """
        Export users in 5k chunks and upload to SSH server.
        Returns number of files created.
        """
        if not force and len(self.pending_users) < threshold:
            return 0
        
        if not self.pending_users:
            return 0
            
        users_list = sorted(self.pending_users)
        files_created = 0
        chunk_files: List[Path] = []
        
        for i in range(0, len(users_list), USERS_CHUNK_SIZE):
            chunk = users_list[i:i + USERS_CHUNK_SIZE]
            self.chunk_count += 1
            
            # Export CSV
            csv_path = self.output_dir / f"users_chunk_{self.chunk_count:04d}.csv"
            with csv_path.open("w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["username"])
                for user in chunk:
                    writer.writerow([user])
            
            # Export JSON
            json_path = self.output_dir / f"users_chunk_{self.chunk_count:04d}.json"
            with json_path.open("w", encoding="utf-8") as f:
                json.dump({"users": chunk, "count": len(chunk)}, f, indent=2)
            
            chunk_files.extend([csv_path, json_path])
            files_created += 1
            print(f"  [EXPORT] Chunk {self.chunk_count}: {len(chunk):,} users -> {csv_path.name}")
        
        self.total_exported += len(self.pending_users)
        self.pending_users.clear()
        
        # Upload chunk files to SSH server
        if self.ssh and self.ssh.enabled and chunk_files:
            uploaded = self.ssh.upload_files(chunk_files)
            self.total_uploaded += uploaded
            print(f"  [SSH] Uploaded {uploaded}/{len(chunk_files)} files to server")
        
        self._save_state()
        
        # Also save dedup state
        self.dedup.save_state(force=True)
        
        return files_created
    
    def get_stats(self) -> Dict:
        return {
            "pending_users": len(self.pending_users),
            "total_exported": self.total_exported,
            "total_uploaded": self.total_uploaded,
            "chunks_created": self.chunk_count,
            "global_users_seen": len(self.dedup.seen_users),
            "ssh_enabled": self.ssh.enabled if self.ssh else False,
        }


# ============================================================================
# Pipeline State Management (uses DeduplicationManager)
# ============================================================================

class PipelineState:
    """Manages pipeline checkpoint/resume state using DeduplicationManager."""
    
    def __init__(self, output_dir: Path, dedup_manager: DeduplicationManager):
        self.state_file = output_dir / "pipeline_state.json"
        self.dedup = dedup_manager
        self.current_batch: List[str] = []
        self._load()
    
    @property
    def discovered_subreddits(self) -> Set[str]:
        return self.dedup.discovered_subreddits
    
    @property
    def processed_subreddits(self) -> Set[str]:
        return self.dedup.processed_subreddits
    
    def _load(self):
        if self.state_file.exists():
            try:
                with self.state_file.open("r") as f:
                    data = json.load(f)
                    self.current_batch = data.get("current_batch", [])
                    # Set the queue in dedup manager
                    if self.current_batch:
                        self.dedup.set_queue(self.current_batch)
                    print(f"[STATE] Resumed: {len(self.current_batch)} in current batch")
            except Exception as e:
                print(f"[STATE] Failed to load state: {e}")
    
    def save(self):
        with self.state_file.open("w") as f:
            json.dump({
                "current_batch": self.current_batch,
            }, f)
        # Also save dedup state
        self.dedup.save_state(force=True)
    
    def add_discovered(self, name: str) -> bool:
        """Mark subreddit as discovered. Returns True if new."""
        return self.dedup.mark_subreddit_discovered(name)
    
    def is_processed(self, name: str) -> bool:
        """Check if subreddit was already processed."""
        return self.dedup.is_subreddit_processed(name)
    
    def should_add_to_batch(self, name: str) -> bool:
        """Check if subreddit should be added to batch (not processed, not queued)."""
        return (not self.dedup.is_subreddit_processed(name) and 
                not self.dedup.is_subreddit_queued(name))
    
    def mark_processed(self, name: str):
        self.dedup.mark_subreddit_processed(name)
        if name in self.current_batch:
            self.current_batch.remove(name)
        self.save()
    
    def set_batch(self, batch: List[str]):
        self.current_batch = batch
        self.dedup.set_queue(batch)
        self.save()


# ============================================================================
# Main Pipeline
# ============================================================================

class IntegratedPipeline:
    """Main pipeline orchestrating subreddit discovery and commenter scraping."""
    
    def __init__(self, output_dir: str = "pipeline_output"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize deduplication manager first (shared across components)
        self.dedup = DeduplicationManager(self.output_dir)
        
        # State management (uses dedup manager)
        self.state = PipelineState(self.output_dir, self.dedup)
        
        # Subreddit scraper components
        self.reddit_client: Optional[RedditClient] = None
        self.discovery: Optional[SubredditDiscovery] = None
        
        # Gluetun for commenter scraping
        self.gluetun = GluetunManager(container_name=os.environ.get("GLUETUN_CONTAINER_NAME", "gluetun"))
        self.miner: Optional[YARS] = None
        
        # SSH Uploader for sending chunks to remote server
        self.ssh_uploader = SSHUploader()
        
        # User export (uses dedup manager for global user dedup, SSH uploader for uploads)
        self.user_exporter = UserExporter(
            output_dir=str(self.output_dir / "users"),
            dedup_manager=self.dedup,
            ssh_uploader=self.ssh_uploader
        )
        
        # State
        self.running = True
        self.posts_since_restart = 0
        
        # Stats
        self.stats = {
            "subreddits_discovered": len(self.dedup.discovered_subreddits),
            "subreddits_scraped": len(self.dedup.processed_subreddits),
            "total_commenters": len(self.dedup.seen_users),
            "duplicates_filtered": 0,
        }
        
        # Signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        print("\n[PIPELINE] Shutdown signal received, saving state...")
        self.state.save()
        self.dedup.save_state(force=True)
        self.user_exporter.export_chunks(force=True)
        self.running = False
        sys.exit(0)
    
    def _refresh_miner(self) -> YARS:
        """Restart Gluetun and get new YARS instance."""
        self.gluetun.restart()
        self.miner = self.gluetun.new_miner()
        self.posts_since_restart = 0
        return self.miner
    
    def _init_subreddit_scraper(self) -> None:
        """Initialize subreddit discovery components."""
        print("[PIPELINE] Initializing subreddit discovery...")
        self.reddit_client = create_client_from_env()
        self.discovery = SubredditDiscovery(self.reddit_client)
        # Load previously discovered to avoid duplicates in discovery module
        self.discovery.load_discovered(self.dedup.discovered_subreddits)
    
    def _process_subreddit_batch(self, subreddits: List[str]) -> None:
        """Process a batch of subreddits to extract commenters."""
        print(f"\n{'='*70}")
        print(f"[BATCH] Processing {len(subreddits)} subreddits for commenters")
        print(f"{'='*70}")
        
        self.state.set_batch(subreddits)
        
        if not self.miner:
            self.miner = self.gluetun.new_miner()
        
        for idx, subreddit in enumerate(subreddits, 1):
            if not self.running:
                break
                
            # Skip if already processed (dedup check)
            if self.dedup.is_subreddit_processed(subreddit):
                print(f"\n[BATCH {idx}/{len(subreddits)}] SKIPPING r/{subreddit} (already processed)")
                continue
            
            print(f"\n[BATCH {idx}/{len(subreddits)}] Processing r/{subreddit}")
            
            # Scrape with both 'all' and 'year' time filters
            all_commenters: Set[str] = set()
            
            for time_filter in ["all", "year"]:
                try:
                    # Use gather_commenters_for_subreddit from scrape_commenters.py
                    summary, self.miner = gather_commenters_for_subreddit(
                        self.miner,
                        subreddit=subreddit,
                        limit=POSTS_PER_SUBREDDIT,
                        category="top",
                        time_filter=time_filter,
                        refresh_callback=self._refresh_miner,
                    )
                    all_commenters.update(summary.get("unique_commenters", []))
                    
                except Exception as e:
                    print(f"  [ERROR] Failed to scrape r/{subreddit} ({time_filter}): {e}")
                    self._refresh_miner()
            
            if all_commenters:
                # Add users with deduplication (filters out already-seen users)
                raw_count = len(all_commenters)
                new_count = self.user_exporter.add_users(all_commenters)
                self.stats["duplicates_filtered"] += (raw_count - new_count)
                
                # Mark subreddit as processed
                self.state.mark_processed(subreddit)
                self.stats["subreddits_scraped"] += 1
                self.stats["total_commenters"] = len(self.dedup.seen_users)
                
                self.posts_since_restart += POSTS_PER_SUBREDDIT * 2  # Two time filters
                if self.posts_since_restart >= RESTART_AFTER_POSTS:
                    self._refresh_miner()
                
                # Export when we have 5k+ users, upload to SSH server
                if self.user_exporter.should_export():
                    print(f"\n[PIPELINE] Reached {USERS_EXPORT_THRESHOLD:,} pending users, exporting & uploading...")
                    self.user_exporter.export_chunks()
            else:
                # Still mark as processed even if no commenters found
                self.state.mark_processed(subreddit)
                self.stats["subreddits_scraped"] += 1
        
        # Print batch stats
        stats = self.user_exporter.get_stats()
        dedup_stats = self.dedup.get_stats()
        print(f"\n[BATCH COMPLETE] Scraped: {self.stats['subreddits_scraped']}, "
              f"Users pending: {stats['pending_users']:,}, "
              f"Total exported: {stats['total_exported']:,}, "
              f"Global unique users: {dedup_stats['users_seen']:,}")
    
    def run(self) -> None:
        """Run the integrated pipeline."""
        print("\n" + "="*70)
        print("[PIPELINE] Integrated Reddit Scraping Pipeline")
        print("="*70)
        print(f"  Subreddits batch size: {SUBREDDITS_BATCH_SIZE}")
        print(f"  Posts per subreddit: {POSTS_PER_SUBREDDIT}")
        print(f"  Time filters: all, year")
        print(f"  User chunk size: {USERS_CHUNK_SIZE:,}")
        print(f"  Export threshold: {USERS_EXPORT_THRESHOLD:,} users")
        print(f"  Output directory: {self.output_dir}")
        ssh_status = f"{SSH_USER}@{SSH_HOST}:{SSH_REMOTE_DIR}" if self.ssh_uploader.enabled else "DISABLED"
        print(f"  SSH upload: {ssh_status}")
        print(f"  Resumed state: {len(self.state.discovered_subreddits)} discovered, "
              f"{len(self.state.processed_subreddits)} processed")
        print("="*70 + "\n")
        
        # Initialize subreddit scraper
        self._init_subreddit_scraper()
        
        # Resume incomplete batch if any
        if self.state.current_batch:
            remaining = [s for s in self.state.current_batch 
                        if s not in self.state.processed_subreddits]
            if remaining:
                print(f"[PIPELINE] Resuming incomplete batch of {len(remaining)} subreddits")
                self._process_subreddit_batch(remaining)
        
        # Discovery loop
        batch_buffer: List[str] = []
        
        for keyword in NSFW_SEARCH_KEYWORDS:
            if not self.running:
                break
                
            print(f"\n[DISCOVERY] Searching keyword: {keyword}")
            
            try:
                for info in self.discovery.search_by_keyword(keyword, max_pages=5):
                    if not self.running:
                        break
                    
                    subreddit_name = info.subreddit_name
                    
                    # Skip if already processed
                    if self.dedup.is_subreddit_processed(subreddit_name):
                        continue
                    
                    # Mark as discovered if new
                    if self.state.add_discovered(subreddit_name):
                        self.stats["subreddits_discovered"] += 1
                    
                    # Add to batch if not already queued
                    if self.state.should_add_to_batch(subreddit_name) and subreddit_name not in batch_buffer:
                        batch_buffer.append(subreddit_name)
                        self.dedup.mark_subreddit_queued(subreddit_name)
                        print(f"  [+] r/{subreddit_name} ({info.subscribers:,} subs) - batch: {len(batch_buffer)}/{SUBREDDITS_BATCH_SIZE}")
                    
                    # Process batch when we have enough
                    if len(batch_buffer) >= SUBREDDITS_BATCH_SIZE:
                        self._process_subreddit_batch(batch_buffer)
                        batch_buffer.clear()
                        self.state.save()
                        
            except (RedditRateLimitError, RedditBlockedError) as e:
                print(f"  [WARN] Rate limit/block on discovery: {e}")
                # Rotate IP via Gluetun restart
                self._refresh_miner()
                # Also refresh reddit client session
                self.reddit_client.rotate_user_agent()
                self.reddit_client.session = requests.Session()
                proxy_url = self.gluetun.get_proxy()
                self.reddit_client.session.proxies = {
                    "http": proxy_url,
                    "https": proxy_url
                }
                time.sleep(5)
            except Exception as e:
                print(f"  [ERROR] Discovery error: {e}")
                time.sleep(2)
        
        # Process remaining subreddits
        if batch_buffer:
            self._process_subreddit_batch(batch_buffer)
        
        # Final export
        if self.user_exporter.pending_users:
            print("\n[PIPELINE] Final export of remaining users...")
            self.user_exporter.export_chunks(force=True)
        
        # Print final stats
        final_stats = self.user_exporter.get_stats()
        dedup_stats = self.dedup.get_stats()
        print("\n" + "="*70)
        print("[PIPELINE] Complete - Final Statistics")
        print("="*70)
        print(f"  Subreddits discovered: {dedup_stats['subreddits_discovered']}")
        print(f"  Subreddits processed: {dedup_stats['subreddits_processed']}")
        print(f"  Global unique users: {dedup_stats['users_seen']:,}")
        print(f"  Total users exported: {final_stats['total_exported']:,}")
        print(f"  Duplicates filtered: {self.stats['duplicates_filtered']:,}")
        print(f"  Chunk files created: {final_stats['chunks_created']}")
        print(f"  Output directory: {self.output_dir}")
        print("="*70 + "\n")


def main():
    load_dotenv()
    
    parser = argparse.ArgumentParser(description="Integrated Reddit Scraping Pipeline")
    parser.add_argument("--output-dir", default="pipeline_output", help="Output directory")
    parser.add_argument("--batch-size", type=int, default=100, help="Subreddits per batch")
    parser.add_argument("--posts-per-sub", type=int, default=500, help="Posts per subreddit")
    parser.add_argument("--chunk-size", type=int, default=5000, help="Users per chunk file")
    parser.add_argument("--restart-after", type=int, default=100, help="Restart Gluetun after N posts")
    args = parser.parse_args()
    
    # Update config from args
    global SUBREDDITS_BATCH_SIZE, POSTS_PER_SUBREDDIT, USERS_CHUNK_SIZE, RESTART_AFTER_POSTS
    SUBREDDITS_BATCH_SIZE = args.batch_size
    POSTS_PER_SUBREDDIT = args.posts_per_sub
    USERS_CHUNK_SIZE = args.chunk_size
    RESTART_AFTER_POSTS = args.restart_after
    
    pipeline = IntegratedPipeline(output_dir=args.output_dir)
    pipeline.run()


if __name__ == "__main__":
    main()

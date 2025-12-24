"""
Main entry point for NSFW Subreddit Scraper.
Orchestrates discovery, handles rate limits, and manages Gluetun restarts.
"""
import os
import sys
import signal
import logging
import time
from typing import Optional
from dotenv import load_dotenv

from reddit_client import (
    RedditClient,
    RedditRateLimitError,
    RedditBlockedError,
    create_client_from_env
)
from gluetun_controller import (
    GluetunController,
    GluetunControllerError,
    create_controller_from_env
)
from discovery import SubredditDiscovery, SubredditInfo, NSFW_SEARCH_KEYWORDS
from exporter import Exporter, create_exporter_from_env
from checkpoint import (
    CheckpointManager,
    ScraperState,
    create_checkpoint_manager_from_env
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class NSFWSubredditScraper:
    """Main scraper orchestrator."""
    
    def __init__(
        self,
        reddit_client: RedditClient,
        gluetun_controller: GluetunController,
        exporter: Exporter,
        checkpoint_manager: CheckpointManager
    ):
        self.reddit_client = reddit_client
        self.gluetun = gluetun_controller
        self.exporter = exporter
        self.checkpoint = checkpoint_manager
        
        self.discovery: Optional[SubredditDiscovery] = None
        self.state: ScraperState = ScraperState()
        self.running = True
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        logger.info("Shutdown signal received, saving checkpoint...")
        self.running = False
    
    def _save_checkpoint(self) -> None:
        """Save current state to checkpoint."""
        if self.discovery:
            self.state.discovered_names = self.discovery.discovered.copy()
            self.state.explore_queue = self.discovery.to_explore.copy()
        self.checkpoint.save(self.state)
    
    def _handle_rate_limit_or_block(self, error: Exception) -> bool:
        """
        Handle rate limit or block by restarting Gluetun.
        
        Returns:
            True if successfully recovered, False otherwise
        """
        logger.warning(f"Rate limit/block encountered: {error}")
        
        # Save checkpoint before attempting recovery
        self._save_checkpoint()
        
        try:
            if self.gluetun.restart_for_new_ip():
                logger.info("Successfully obtained new IP, resuming...")
                # Rotate user agent along with IP
                self.reddit_client.rotate_user_agent()
                # Create new session to clear cookies
                self.reddit_client.session = __import__('requests').Session()
                if self.reddit_client.proxy_url:
                    self.reddit_client.session.proxies = {
                        "http": self.reddit_client.proxy_url,
                        "https": self.reddit_client.proxy_url
                    }
                return True
            else:
                logger.error("Failed to obtain new IP")
                return False
        except GluetunControllerError as e:
            logger.error(f"Gluetun controller error: {e}")
            return False
    
    def _process_subreddit(self, info: SubredditInfo) -> None:
        """Process a discovered subreddit."""
        self.state.discovered_subreddits.append(info.to_dict())
        logger.info(
            f"Discovered: r/{info.subreddit_name} "
            f"({info.subscribers:,} subscribers)"
        )
    
    def _run_with_recovery(self, generator_func, *args, **kwargs):
        """
        Run a generator function with rate limit recovery.
        
        Wraps generator and handles rate limits by restarting Gluetun.
        """
        while self.running:
            try:
                gen = generator_func(*args, **kwargs)
                for item in gen:
                    if not self.running:
                        break
                    yield item
                return  # Generator completed successfully
                
            except (RedditRateLimitError, RedditBlockedError) as e:
                if not self._handle_rate_limit_or_block(e):
                    raise
                # Retry from the beginning (deduplication prevents duplicates)
                continue
    
    def verify_proxy(self) -> bool:
        """Verify proxy is working before starting."""
        logger.info("Verifying proxy connection...")
        
        try:
            self.gluetun.connect()
        except GluetunControllerError as e:
            logger.warning(f"Could not connect to Docker (non-fatal): {e}")
        
        if not self.gluetun.verify_proxy_active():
            logger.error("Proxy verification failed - cannot proceed without proxy")
            return False
        
        logger.info(f"Proxy verified. Public IP: {self.gluetun.current_ip}")
        return True
    
    def run(self) -> None:
        """Run the scraper."""
        # Verify proxy first
        if not self.verify_proxy():
            logger.error("Aborting: Proxy not active")
            sys.exit(1)
        
        # Load checkpoint if exists
        saved_state = self.checkpoint.load()
        if saved_state:
            self.state = saved_state
            logger.info(f"Resuming from checkpoint with {len(self.state.discovered_subreddits)} subreddits")
        
        # Initialize discovery (no auth needed - using public JSON endpoints)
        logger.info("Using Reddit public JSON endpoints (no authentication required)")
        self.discovery = SubredditDiscovery(self.reddit_client)
        self.discovery.load_discovered(self.state.discovered_names)
        self.discovery.to_explore = self.state.explore_queue.copy()
        
        # Phase 1: Keyword search
        if self.state.current_phase in ("init", "keyword_search"):
            self.state.current_phase = "keyword_search"
            logger.info("Phase 1: Keyword-based search")
            
            remaining_keywords = [
                k for k in NSFW_SEARCH_KEYWORDS
                if k not in self.state.completed_keywords
            ]
            
            for keyword in remaining_keywords:
                if not self.running:
                    break
                
                try:
                    for info in self._run_with_recovery(
                        self.discovery.search_by_keyword,
                        keyword,
                        max_pages=5
                    ):
                        self._process_subreddit(info)
                    
                    self.state.completed_keywords.append(keyword)
                    self._save_checkpoint()
                    
                except Exception as e:
                    logger.error(f"Error during keyword search '{keyword}': {e}")
                    self._save_checkpoint()
                    break
        
        # Phase 2: Popular subreddits
        if self.running and self.state.current_phase in ("keyword_search", "popular"):
            self.state.current_phase = "popular"
            logger.info("Phase 2: Discovering from popular subreddits")
            
            try:
                for info in self._run_with_recovery(
                    self.discovery.discover_from_popular,
                    max_pages=10
                ):
                    self._process_subreddit(info)
                
                self._save_checkpoint()
                
            except Exception as e:
                logger.error(f"Error during popular discovery: {e}")
                self._save_checkpoint()
        
        # Phase 3: New subreddits
        if self.running and self.state.current_phase in ("popular", "new"):
            self.state.current_phase = "new"
            logger.info("Phase 3: Discovering from new subreddits")
            
            try:
                for info in self._run_with_recovery(
                    self.discovery.discover_from_new,
                    max_pages=10
                ):
                    self._process_subreddit(info)
                
                self._save_checkpoint()
                
            except Exception as e:
                logger.error(f"Error during new discovery: {e}")
                self._save_checkpoint()
        
        # Phase 4: Related subreddit traversal
        if self.running and self.state.current_phase in ("new", "related"):
            self.state.current_phase = "related"
            logger.info("Phase 4: Exploring related subreddits")
            
            try:
                for info in self._run_with_recovery(
                    self.discovery.explore_related_queue,
                    max_subreddits=200
                ):
                    self._process_subreddit(info)
                
                self._save_checkpoint()
                
            except Exception as e:
                logger.error(f"Error during related discovery: {e}")
                self._save_checkpoint()
        
        # Export results
        self.state.current_phase = "export"
        logger.info(f"Exporting {len(self.state.discovered_subreddits)} subreddits...")
        
        # Sort by subscribers descending
        sorted_data = sorted(
            self.state.discovered_subreddits,
            key=lambda x: x.get("subscribers", 0),
            reverse=True
        )
        
        files = self.exporter.export_all(sorted_data)
        logger.info(f"Export complete: {files}")
        
        # Clean up checkpoint on successful completion
        if self.running:
            self.checkpoint.delete()
            logger.info("Scraping completed successfully!")
        else:
            logger.info("Scraping interrupted. Resume with same command.")


def main():
    """Main entry point."""
    # Load environment variables
    load_dotenv()
    
    # Check proxy configuration (required)
    proxy_url = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
    if not proxy_url:
        logger.error("No proxy configured (HTTP_PROXY or HTTPS_PROXY required)")
        sys.exit(1)
    
    # Create components
    reddit_client = create_client_from_env()
    gluetun_controller = create_controller_from_env()
    exporter = create_exporter_from_env()
    checkpoint_manager = create_checkpoint_manager_from_env()
    
    # Create and run scraper
    scraper = NSFWSubredditScraper(
        reddit_client=reddit_client,
        gluetun_controller=gluetun_controller,
        exporter=exporter,
        checkpoint_manager=checkpoint_manager
    )
    
    scraper.run()


if __name__ == "__main__":
    main()

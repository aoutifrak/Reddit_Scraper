"""
Reddit client using public JSON endpoints (no authentication required).
Accesses Reddit data via .json suffix on URLs.
Uses old.reddit.com which is less restrictive.
"""
import os
import time
import logging
import random
from typing import Optional, Dict, Any
import requests

logger = logging.getLogger(__name__)


class RedditRateLimitError(Exception):
    """Raised when Reddit rate limit is hit (429)."""
    def __init__(self, retry_after: int = 60):
        self.retry_after = retry_after
        super().__init__(f"Rate limited. Retry after {retry_after}s")


class RedditBlockedError(Exception):
    """Raised when request is blocked (403)."""
    pass


class RedditClient:
    """Reddit client using public JSON endpoints (no auth required)."""
    
    # Use old.reddit.com - less restrictive than www.reddit.com
    BASE_URL = "https://old.reddit.com"
    
    # Realistic browser user agents
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    ]
    
    def __init__(
        self,
        proxy_url: Optional[str] = None,
        request_delay: float = 3.0
    ):
        self.proxy_url = proxy_url
        self.request_delay = request_delay
        self.last_request_time = 0
        
        self.session = requests.Session()
        
        # Set a consistent user agent for this session
        self.user_agent = random.choice(self.USER_AGENTS)
        
        # Configure proxy
        if proxy_url:
            self.session.proxies = {
                "http": proxy_url,
                "https": proxy_url
            }
            logger.info(f"Proxy configured: {proxy_url}")
    
    def _get_headers(self) -> Dict[str, str]:
        """Get realistic browser headers."""
        return {
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        }
    
    def _rate_limit_delay(self) -> None:
        """Enforce delay between requests with randomization."""
        elapsed = time.time() - self.last_request_time
        # Add randomization to appear more human
        delay = self.request_delay + random.uniform(0.5, 2.0)
        if elapsed < delay:
            sleep_time = delay - elapsed
            time.sleep(sleep_time)
        self.last_request_time = time.time()
    
    def _handle_response_errors(self, response: requests.Response) -> None:
        """Check response for errors and raise appropriate exceptions."""
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            raise RedditRateLimitError(retry_after)
        
        if response.status_code == 403:
            raise RedditBlockedError(f"Request blocked: {response.status_code}")
        
        if response.status_code == 503:
            raise RedditBlockedError("Reddit service unavailable (503)")
        
        response.raise_for_status()
    
    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Make a request to Reddit's JSON endpoint."""
        self._rate_limit_delay()
        
        url = f"{self.BASE_URL}{endpoint}.json"
        
        try:
            response = self.session.get(
                url,
                params=params,
                headers=self._get_headers(),
                timeout=30
            )
            self._handle_response_errors(response)
            return response.json()
            
        except requests.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
    
    def search_subreddits(
        self,
        query: str,
        limit: int = 25,
        after: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Search for subreddits by query.
        
        Args:
            query: Search term
            limit: Max results per page (use smaller values to avoid detection)
            after: Pagination cursor
            
        Returns:
            Reddit listing response with subreddit data
        """
        params = {
            "q": query,
            "limit": min(limit, 25),  # Smaller batches
            "include_over_18": "on",
            "type": "sr",
            "sort": "relevance"
        }
        if after:
            params["after"] = after
        
        return self._make_request("/subreddits/search", params=params)
    
    def get_subreddit_about(self, subreddit_name: str) -> Dict[str, Any]:
        """
        Get detailed information about a subreddit.
        
        Args:
            subreddit_name: Name of subreddit (without r/)
            
        Returns:
            Subreddit about data
        """
        return self._make_request(f"/r/{subreddit_name}/about")
    
    def get_popular_subreddits(
        self,
        limit: int = 25,
        after: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get popular subreddits.
        
        Args:
            limit: Max results per page
            after: Pagination cursor
            
        Returns:
            Reddit listing response
        """
        params = {
            "limit": min(limit, 25),
        }
        if after:
            params["after"] = after
        
        return self._make_request("/subreddits/popular", params=params)
    
    def get_new_subreddits(
        self,
        limit: int = 25,
        after: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get new subreddits.
        
        Args:
            limit: Max results per page
            after: Pagination cursor
            
        Returns:
            Reddit listing response
        """
        params = {
            "limit": min(limit, 25),
        }
        if after:
            params["after"] = after
        
        return self._make_request("/subreddits/new", params=params)
    
    def get_subreddit_sidebar(self, subreddit_name: str) -> Optional[str]:
        """
        Get subreddit sidebar/description for related subreddit discovery.
        
        Args:
            subreddit_name: Name of subreddit
            
        Returns:
            Sidebar markdown text or None
        """
        try:
            data = self.get_subreddit_about(subreddit_name)
            return data.get("data", {}).get("description", "")
        except Exception as e:
            logger.warning(f"Failed to get sidebar for r/{subreddit_name}: {e}")
            return None
    
    def rotate_user_agent(self) -> None:
        """Rotate to a new random user agent."""
        self.user_agent = random.choice(self.USER_AGENTS)
        logger.debug(f"Rotated user agent")


def create_client_from_env() -> RedditClient:
    """Create RedditClient from environment variables."""
    proxy_url = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
    request_delay = float(os.environ.get("REQUEST_DELAY", "3.0"))
    
    return RedditClient(
        proxy_url=proxy_url,
        request_delay=request_delay
    )

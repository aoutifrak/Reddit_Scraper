"""
NSFW Subreddit discovery logic.
Implements keyword search and related subreddit traversal.
"""
import re
import logging
from typing import Set, Dict, Any, List, Optional, Generator
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)


@dataclass
class SubredditInfo:
    """Subreddit metadata."""
    subreddit_name: str
    subscribers: int
    over18: bool
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# Keywords for discovering NSFW subreddits
NSFW_SEARCH_KEYWORDS = [
    "nsfw",
    "adult",
    "18plus",
    "18+",
    "xxx",
    "porn",
    "gonewild",
    "nude",
    "naked",
    "sexy",
    "hot",
    "erotic",
    "onlyfans",
    "lewd",
    "hentai",
    "rule34",
    "boobs",
    "ass",
    "milf",
    "teen",
    "amateur",
    "homemade",
    "selfie",
    "pawg",
    "bbc",
    "petite",
    "thick",
    "curvy",
    "busty",
    "blonde",
    "brunette",
    "redhead",
    "asian",
    "latina",
    "ebony",
    "indian",
]


class SubredditDiscovery:
    """Discovers NSFW subreddits through various methods."""
    
    # Regex to find subreddit references in text
    SUBREDDIT_PATTERN = re.compile(r'/r/([a-zA-Z0-9_]+)', re.IGNORECASE)
    
    def __init__(self, reddit_client):
        """
        Initialize discovery.
        
        Args:
            reddit_client: RedditClient instance
        """
        self.client = reddit_client
        self.discovered: Set[str] = set()  # Track discovered subreddit names
        self.to_explore: List[str] = []     # Queue of subreddits to explore for related
    
    def _extract_subreddit_info(self, data: Dict[str, Any]) -> Optional[SubredditInfo]:
        """
        Extract SubredditInfo from Reddit API response data.
        
        Args:
            data: Subreddit data from API
            
        Returns:
            SubredditInfo if valid NSFW public subreddit, None otherwise
        """
        try:
            name = data.get("display_name", "").lower()
            over18 = data.get("over18", False)
            subreddit_type = data.get("subreddit_type", "")
            subscribers = data.get("subscribers", 0) or 0
            
            # Only include public NSFW subreddits
            if not over18:
                return None
            
            if subreddit_type not in ("public", "restricted"):
                # Skip private subreddits
                return None
            
            if not name:
                return None
            
            return SubredditInfo(
                subreddit_name=name,
                subscribers=subscribers,
                over18=over18
            )
        except Exception as e:
            logger.debug(f"Failed to extract subreddit info: {e}")
            return None
    
    def _process_listing(
        self,
        response: Dict[str, Any]
    ) -> Generator[SubredditInfo, None, Optional[str]]:
        """
        Process a Reddit listing response.
        
        Args:
            response: Reddit API listing response
            
        Yields:
            SubredditInfo for each valid NSFW subreddit
            
        Returns:
            Next pagination cursor or None
        """
        data = response.get("data", {})
        children = data.get("children", [])
        
        for child in children:
            child_data = child.get("data", {})
            info = self._extract_subreddit_info(child_data)
            
            if info and info.subreddit_name not in self.discovered:
                self.discovered.add(info.subreddit_name)
                self.to_explore.append(info.subreddit_name)
                yield info
        
        return data.get("after")
    
    def search_by_keyword(
        self,
        keyword: str,
        max_pages: int = 10
    ) -> Generator[SubredditInfo, None, None]:
        """
        Search for NSFW subreddits by keyword.
        
        Args:
            keyword: Search term
            max_pages: Maximum pages to fetch
            
        Yields:
            SubredditInfo for each discovered subreddit
        """
        logger.info(f"Searching for subreddits with keyword: {keyword}")
        after = None
        pages = 0
        
        while pages < max_pages:
            response = self.client.search_subreddits(
                query=keyword,
                limit=100,
                after=after
            )
            
            # Process results
            gen = self._process_listing(response)
            try:
                while True:
                    yield next(gen)
            except StopIteration as e:
                after = e.value
            
            if not after:
                break
            
            pages += 1
    
    def search_all_keywords(
        self,
        keywords: Optional[List[str]] = None,
        max_pages_per_keyword: int = 5
    ) -> Generator[SubredditInfo, None, None]:
        """
        Search for subreddits using all keywords.
        
        Args:
            keywords: List of search terms (uses defaults if None)
            max_pages_per_keyword: Max pages per keyword
            
        Yields:
            SubredditInfo for each discovered subreddit
        """
        if keywords is None:
            keywords = NSFW_SEARCH_KEYWORDS
        
        for keyword in keywords:
            yield from self.search_by_keyword(keyword, max_pages_per_keyword)
    
    def discover_from_popular(
        self,
        max_pages: int = 20
    ) -> Generator[SubredditInfo, None, None]:
        """
        Discover NSFW subreddits from popular listings.
        
        Args:
            max_pages: Maximum pages to fetch
            
        Yields:
            SubredditInfo for each discovered subreddit
        """
        logger.info("Discovering from popular subreddits")
        after = None
        pages = 0
        
        while pages < max_pages:
            response = self.client.get_popular_subreddits(
                limit=100,
                after=after
            )
            
            gen = self._process_listing(response)
            try:
                while True:
                    yield next(gen)
            except StopIteration as e:
                after = e.value
            
            if not after:
                break
            
            pages += 1
    
    def discover_from_new(
        self,
        max_pages: int = 20
    ) -> Generator[SubredditInfo, None, None]:
        """
        Discover NSFW subreddits from new listings.
        
        Args:
            max_pages: Maximum pages to fetch
            
        Yields:
            SubredditInfo for each discovered subreddit
        """
        logger.info("Discovering from new subreddits")
        after = None
        pages = 0
        
        while pages < max_pages:
            response = self.client.get_new_subreddits(
                limit=100,
                after=after
            )
            
            gen = self._process_listing(response)
            try:
                while True:
                    yield next(gen)
            except StopIteration as e:
                after = e.value
            
            if not after:
                break
            
            pages += 1
    
    def discover_related(
        self,
        subreddit_name: str
    ) -> Generator[SubredditInfo, None, None]:
        """
        Discover related subreddits from a subreddit's sidebar.
        
        Args:
            subreddit_name: Name of subreddit to explore
            
        Yields:
            SubredditInfo for each discovered related subreddit
        """
        sidebar = self.client.get_subreddit_sidebar(subreddit_name)
        if not sidebar:
            return
        
        # Find all subreddit references in sidebar
        matches = self.SUBREDDIT_PATTERN.findall(sidebar)
        related_names = set(m.lower() for m in matches)
        
        logger.debug(f"Found {len(related_names)} related subreddits in r/{subreddit_name}")
        
        for name in related_names:
            if name in self.discovered:
                continue
            
            try:
                response = self.client.get_subreddit_about(name)
                info = self._extract_subreddit_info(response.get("data", {}))
                
                if info:
                    self.discovered.add(info.subreddit_name)
                    self.to_explore.append(info.subreddit_name)
                    yield info
                    
            except Exception as e:
                logger.debug(f"Failed to get info for r/{name}: {e}")
    
    def explore_related_queue(
        self,
        max_subreddits: int = 100
    ) -> Generator[SubredditInfo, None, None]:
        """
        Explore related subreddits from the queue.
        
        Args:
            max_subreddits: Maximum subreddits to explore
            
        Yields:
            SubredditInfo for each discovered subreddit
        """
        explored = 0
        
        while self.to_explore and explored < max_subreddits:
            name = self.to_explore.pop(0)
            logger.info(f"Exploring related subreddits from r/{name}")
            
            yield from self.discover_related(name)
            explored += 1
    
    def get_discovered_count(self) -> int:
        """Get count of discovered subreddits."""
        return len(self.discovered)
    
    def load_discovered(self, names: Set[str]) -> None:
        """Load previously discovered subreddit names."""
        self.discovered.update(names)

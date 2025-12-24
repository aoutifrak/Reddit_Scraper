"""
Checkpoint manager for resume support.
Saves and loads scraper state for graceful shutdown/resume.
"""
import os
import json
import logging
from typing import Set, Dict, Any, Optional, List
from pathlib import Path
from dataclasses import dataclass, asdict, field

logger = logging.getLogger(__name__)


@dataclass
class ScraperState:
    """Scraper state for checkpointing."""
    discovered_subreddits: List[Dict[str, Any]] = field(default_factory=list)
    discovered_names: Set[str] = field(default_factory=set)
    explore_queue: List[str] = field(default_factory=list)
    completed_keywords: List[str] = field(default_factory=list)
    current_phase: str = "init"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "discovered_subreddits": self.discovered_subreddits,
            "discovered_names": list(self.discovered_names),
            "explore_queue": self.explore_queue,
            "completed_keywords": self.completed_keywords,
            "current_phase": self.current_phase
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ScraperState":
        return cls(
            discovered_subreddits=data.get("discovered_subreddits", []),
            discovered_names=set(data.get("discovered_names", [])),
            explore_queue=data.get("explore_queue", []),
            completed_keywords=data.get("completed_keywords", []),
            current_phase=data.get("current_phase", "init")
        )


class CheckpointManager:
    """Manages saving and loading scraper state."""
    
    def __init__(self, checkpoint_file: str = "data/checkpoint.json"):
        """
        Initialize checkpoint manager.
        
        Args:
            checkpoint_file: Path to checkpoint file
        """
        self.checkpoint_file = Path(checkpoint_file)
        self.checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
    
    def save(self, state: ScraperState) -> None:
        """
        Save scraper state to checkpoint file.
        
        Args:
            state: Current scraper state
        """
        try:
            with open(self.checkpoint_file, "w", encoding="utf-8") as f:
                json.dump(state.to_dict(), f, indent=2)
            logger.debug(f"Checkpoint saved: {len(state.discovered_subreddits)} subreddits")
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")
    
    def load(self) -> Optional[ScraperState]:
        """
        Load scraper state from checkpoint file.
        
        Returns:
            ScraperState if checkpoint exists, None otherwise
        """
        if not self.checkpoint_file.exists():
            logger.info("No checkpoint file found, starting fresh")
            return None
        
        try:
            with open(self.checkpoint_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            state = ScraperState.from_dict(data)
            logger.info(
                f"Loaded checkpoint: {len(state.discovered_subreddits)} subreddits, "
                f"phase: {state.current_phase}"
            )
            return state
        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}")
            return None
    
    def delete(self) -> None:
        """Delete checkpoint file."""
        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()
            logger.info("Checkpoint file deleted")
    
    def exists(self) -> bool:
        """Check if checkpoint file exists."""
        return self.checkpoint_file.exists()


def create_checkpoint_manager_from_env() -> CheckpointManager:
    """Create CheckpointManager from environment variables."""
    checkpoint_file = os.environ.get(
        "SCRAPER_CHECKPOINT_FILE",
        "data/checkpoint.json"
    )
    return CheckpointManager(checkpoint_file=checkpoint_file)

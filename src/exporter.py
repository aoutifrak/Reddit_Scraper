"""
Data exporter for subreddit metadata.
Exports to JSON and CSV formats.
"""
import os
import json
import csv
import logging
from typing import List, Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class Exporter:
    """Exports subreddit data to JSON and CSV files."""
    
    def __init__(self, output_dir: str = "data"):
        """
        Initialize exporter.
        
        Args:
            output_dir: Directory to save output files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def export_json(
        self,
        data: List[Dict[str, Any]],
        filename: str = "subreddits.json"
    ) -> str:
        """
        Export data to JSON file.
        
        Args:
            data: List of subreddit dictionaries
            filename: Output filename
            
        Returns:
            Path to created file
        """
        filepath = self.output_dir / filename
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Exported {len(data)} subreddits to {filepath}")
        return str(filepath)
    
    def export_csv(
        self,
        data: List[Dict[str, Any]],
        filename: str = "subreddits.csv"
    ) -> str:
        """
        Export data to CSV file.
        
        Args:
            data: List of subreddit dictionaries
            filename: Output filename
            
        Returns:
            Path to created file
        """
        filepath = self.output_dir / filename
        
        if not data:
            logger.warning("No data to export to CSV")
            return str(filepath)
        
        fieldnames = ["subreddit_name", "subscribers", "over18"]
        
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for item in data:
                # Ensure only expected fields are written
                row = {k: item.get(k, "") for k in fieldnames}
                writer.writerow(row)
        
        logger.info(f"Exported {len(data)} subreddits to {filepath}")
        return str(filepath)
    
    def export_all(
        self,
        data: List[Dict[str, Any]],
        json_filename: str = "subreddits.json",
        csv_filename: str = "subreddits.csv"
    ) -> Dict[str, str]:
        """
        Export data to both JSON and CSV.
        
        Args:
            data: List of subreddit dictionaries
            json_filename: JSON output filename
            csv_filename: CSV output filename
            
        Returns:
            Dictionary with paths to created files
        """
        return {
            "json": self.export_json(data, json_filename),
            "csv": self.export_csv(data, csv_filename)
        }


def create_exporter_from_env() -> Exporter:
    """Create Exporter from environment variables."""
    output_dir = os.environ.get("SCRAPER_OUTPUT_DIR", "data")
    return Exporter(output_dir=output_dir)

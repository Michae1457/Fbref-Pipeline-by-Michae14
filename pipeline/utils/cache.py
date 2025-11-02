import sqlite3
import os
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime
from pipeline.utils.logging import get_logger

logger = get_logger()

class CacheManager:
    """Manages HTML caching using separate SQLite files for each pipeline."""
    
    def __init__(self, pipeline_name: str = "competition"):
        """
        Initialize cache manager for a specific pipeline.
        
        Args:
            pipeline_name: Name of the pipeline (e.g., 'competition', 'matches', etc.)
        """
        self.pipeline_name = pipeline_name
        self.cache_dir = Path("cache")
        self.cache_dir.mkdir(exist_ok=True)
        
        self.cache_file = self.cache_dir / f"{pipeline_name}_cache.db"
        self._init_cache_db()
    
    def _init_cache_db(self):
        """Initialize the cache database."""
        try:
            conn = sqlite3.connect(str(self.cache_file))
            cursor = conn.cursor()
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS html_cache (
                    url TEXT PRIMARY KEY,
                    html_content TEXT NOT NULL,
                    cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.commit()
            conn.close()
            logger.debug(f"Initialized cache database: {self.cache_file}")
            
        except Exception as e:
            logger.error(f"Failed to initialize cache database: {e}")
            raise
    
    def cache_html(self, url: str, html_content: str):
        """
        Cache HTML content for a URL.
        
        Args:
            url: URL that was scraped
            html_content: HTML content to cache
        """
        try:
            conn = sqlite3.connect(str(self.cache_file))
            cursor = conn.cursor()
            
            # Insert or replace cache entry
            cursor.execute("""
                INSERT OR REPLACE INTO html_cache (url, html_content, cached_at, last_accessed)
                VALUES (?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """, (url, html_content))
            
            conn.commit()
            conn.close()
            
            logger.debug(f"Cached HTML for URL: {url}")
            
        except Exception as e:
            logger.error(f"Failed to cache HTML for {url}: {e}")
            raise
    
    def get_cached_html(self, url: str) -> Optional[str]:
        """
        Retrieve cached HTML content for a URL.
        
        Args:
            url: URL to look up
            
        Returns:
            Cached HTML content or None if not found
        """
        try:
            conn = sqlite3.connect(str(self.cache_file))
            cursor = conn.cursor()
            
            cursor.execute("SELECT html_content FROM html_cache WHERE url = ?", (url,))
            result = cursor.fetchone()
            
            if result:
                # Update last accessed timestamp
                cursor.execute("""
                    UPDATE html_cache SET last_accessed = CURRENT_TIMESTAMP WHERE url = ?
                """, (url,))
                
                conn.commit()
                conn.close()
                
                logger.debug(f"Retrieved cached HTML for URL: {url}")
                return result[0]
            
            conn.close()
            return None
            
        except Exception as e:
            logger.error(f"Failed to retrieve cached HTML for {url}: {e}")
            return None
    
    def clear_cache(self, older_than_days: int = None):
        """
        Clear HTML cache entries.
        
        Args:
            older_than_days: Clear entries older than this many days (optional)
        """
        try:
            conn = sqlite3.connect(str(self.cache_file))
            cursor = conn.cursor()
            
            if older_than_days:
                cursor.execute("""
                    DELETE FROM html_cache 
                    WHERE cached_at < datetime('now', '-{} days')
                """.format(older_than_days))
                logger.info(f"Cleared HTML cache entries older than {older_than_days} days")
            else:
                cursor.execute("DELETE FROM html_cache")
                logger.info("Cleared all HTML cache entries")
            
            conn.commit()
            conn.close()
                
        except Exception as e:
            logger.error(f"Failed to clear HTML cache: {e}")
            raise
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get HTML cache statistics."""
        try:
            conn = sqlite3.connect(str(self.cache_file))
            cursor = conn.cursor()
            
            # Get total entries
            cursor.execute("SELECT COUNT(*) FROM html_cache")
            total_entries = cursor.fetchone()[0]
            
            # Get oldest and newest cache entries
            cursor.execute("SELECT MIN(cached_at) FROM html_cache")
            oldest = cursor.fetchone()[0]
            
            cursor.execute("SELECT MAX(cached_at) FROM html_cache")
            newest = cursor.fetchone()[0]
            
            conn.close()
            
            return {
                'pipeline_name': self.pipeline_name,
                'cache_file': str(self.cache_file),
                'total_entries': total_entries,
                'oldest_entry': oldest,
                'newest_entry': newest
            }
            
        except Exception as e:
            logger.error(f"Failed to get cache stats: {e}")
            return {
                'pipeline_name': self.pipeline_name,
                'cache_file': str(self.cache_file),
                'total_entries': 0,
                'oldest_entry': None,
                'newest_entry': None
            }
    
    def get_cache_size(self) -> int:
        """Get the size of the cache file in bytes."""
        try:
            return self.cache_file.stat().st_size if self.cache_file.exists() else 0
        except Exception as e:
            logger.error(f"Failed to get cache file size: {e}")
            return 0
    
    def list_cached_urls(self) -> list:
        """List all cached URLs."""
        try:
            conn = sqlite3.connect(str(self.cache_file))
            cursor = conn.cursor()
            
            cursor.execute("SELECT url, cached_at FROM html_cache ORDER BY cached_at DESC")
            results = cursor.fetchall()
            
            conn.close()
            return results
            
        except Exception as e:
            logger.error(f"Failed to list cached URLs: {e}")
            return []

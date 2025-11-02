import requests
from bs4 import BeautifulSoup
import re
import time
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin
from pipeline.utils.logging import get_logger
from pipeline.utils.cache import CacheManager

logger = get_logger()

class CompetitionScraper:
    """Base class for scraping FBref competition data."""
    
    def __init__(self, base_url: str = "https://fbref.com", pipeline_name: str = "competition"):
        """
        Initialize the scraper.
        
        Args:
            base_url: Base URL for FBref
            pipeline_name: Name of the pipeline for cache management
        """
        self.base_url = base_url
        self.pipeline_name = pipeline_name
        self.cache_manager = CacheManager(pipeline_name)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
    
    def get_page(self, url: str, use_cache: bool = True) -> Optional[BeautifulSoup]:
        """
        Fetch and parse a web page with optional caching.
        
        Args:
            url: URL to fetch
            use_cache: Whether to use cached HTML if available
            
        Returns:
            BeautifulSoup object or None if failed
        """
        try:
            # Try to get cached HTML first if cache is enabled
            if use_cache:
                cached_html = self.cache_manager.get_cached_html(url)
                if cached_html:
                    logger.info(f"[CACHED] {url}")
                    soup = BeautifulSoup(cached_html, 'html.parser')
                    return soup
            
            # Fetch fresh HTML if not cached or cache disabled
            logger.info(f"Fetching fresh HTML: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            html_content = response.text
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Cache the HTML if cache is enabled
            if use_cache:
                self.cache_manager.cache_html(url, html_content)
            
            time.sleep(1)  # Be respectful to the server
            return soup
            
        except requests.RequestException as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None
    
    def extract_competition_id(self, link: str) -> Optional[int]:
        """
        Extract competition ID from competition link.
        
        Args:
            link: Competition link (e.g., "/en/comps/9/history/Premier-League-Seasons")
            
        Returns:
            Competition ID or None if not found
        """
        match = re.search(r'/comps/(\d+)/', link)
        return int(match.group(1)) if match else None
    
    def parse_awards(self, awards_cell) -> List[Dict[str, str]]:
        """
        Parse awards from a table cell.
        
        Args:
            awards_cell: BeautifulSoup element containing awards
            
        Returns:
            List of award dictionaries with name and link
        """
        awards = []
        if awards_cell:
            award_links = awards_cell.find_all('a')
            for link in award_links:
                award_name = link.get_text(strip=True)
                award_link = link.get('href', '')
                if award_name and award_link:
                    awards.append({
                        'award_name': award_name,
                        'award_link': award_link
                    })
        return awards
    
    def scrape_competition_table(self, url: str, table_selector: str = "table") -> List[Dict[str, Any]]:
        """
        Scrape competition data from a table.
        
        Args:
            url: URL to scrape
            table_selector: CSS selector for the table
            
        Returns:
            List of competition dictionaries
        """
        soup = self.get_page(url)
        if not soup:
            return []
        
        table = soup.select_one(table_selector)
        if not table:
            logger.error(f"No table found at {url}")
            return []
        
        competitions = []
        rows = table.find_all('tr')[1:]  # Skip header row
        
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) < 6:  # Minimum expected columns
                continue
            
            try:
                competition_data = self.parse_competition_row(cells)
                if competition_data:
                    competitions.append(competition_data)
            except Exception as e:
                logger.error(f"Error parsing row: {e}")
                continue
        
        logger.info(f"Scraped {len(competitions)} competitions from {url}")
        return competitions
    
    def parse_competition_row(self, cells) -> Optional[Dict[str, Any]]:
        """
        Parse a single competition row. Override in subclasses for specific formats.
        
        Args:
            cells: List of table cells
            
        Returns:
            Competition dictionary or None if parsing failed
        """
        raise NotImplementedError("Subclasses must implement parse_competition_row")

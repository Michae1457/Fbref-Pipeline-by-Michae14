import requests
from bs4 import BeautifulSoup
import time
from typing import Optional
from urllib.parse import urljoin
from pipeline.utils.logging import get_logger
from pipeline.utils.cache import CacheManager

logger = get_logger()

class UniversalScraper:
    """Class for scraping FBref universal HTML content."""
    
    def __init__(self, base_url: str = "https://fbref.com", pipeline_name: str = "universal"):
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
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        })
    
    def get_page(self, url: str, use_cache: bool = True) -> Optional[BeautifulSoup]:
        """
        Fetch a page and return BeautifulSoup object.
        
        Args:
            url: URL to fetch
            use_cache: Whether to use cached HTML if available
            
        Returns:
            BeautifulSoup object or None if failed
        """
        try:
            if use_cache:
                cached_html = self.cache_manager.get_cached_html(url)
                if cached_html:
                    logger.info(f"[CACHE] - {url}")
                    return BeautifulSoup(cached_html, 'html.parser')
            
            logger.info(f"Fetching: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            html_content = response.text
            soup = BeautifulSoup(html_content, 'html.parser')
            
            if use_cache:
                self.cache_manager.cache_html(url, html_content)
            
            time.sleep(1)  # Be respectful to the server
            return soup
            
        except requests.RequestException as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None
    
    def scrape_season_page(self, link: str, season: str, competition_name: str, competition_id: int, use_cache: bool = True) -> Optional[BeautifulSoup]:
        """
        Scrape the HTML content for a specific page.
        
        Args:
            link: Link to scrape
            season: Season string (e.g., "2024-2025")
            competition_name: Name of the competition
            competition_id: ID of the competition
            use_cache: Whether to use cached HTML if available
            
        Returns:
            BeautifulSoup object or None if failed
        """
        logger.info(f"Scraping {competition_name} {season} page...")
        
        # Construct full URL
        full_url = urljoin(self.base_url, link)
        
        soup = self.get_page(full_url, use_cache=use_cache)
        if not soup:
            logger.error(f"Failed to fetch page: {full_url}")
            return None
        
        return soup

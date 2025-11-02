import requests
from bs4 import BeautifulSoup
import re
import time
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin
from pipeline.utils.logging import get_logger
from pipeline.utils.cache import CacheManager

logger = get_logger()

class SeasonClubTournamentParser:
    """Base class for scraping FBref season data."""
    
    def __init__(self, base_url: str = "https://fbref.com", pipeline_name: str = "season"):
        """
        Initialize the parser.
        
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
            
            logger.info(f"Fetching fresh HTML: {url}")
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
    
    def extract_competition_id(self, competition_link: str) -> Optional[int]:
        """
        Extract competition ID from competition link.
        
        Args:
            competition_link: Link like '/en/comps/9/history/Premier-League-Seasons'
            
        Returns:
            Competition ID or None if not found
        """
        try:
            # Pattern: /en/comps/{id}/...
            match = re.search(r'/en/comps/(\d+)/', competition_link)
            if match:
                return int(match.group(1))
            return None
        except Exception as e:
            logger.error(f"Failed to extract competition ID from {competition_link}: {e}")
            return None
    
    def parse_season_row(self, cells, competition_name: str, competition_id: int) -> Optional[Dict[str, Any]]:
        """
        Parse a season row from the club tournament table.
        Expected columns: Season, Competition Name, # Squads, Champion, Runner-Up, Final, Top Scorer
        
        Args:
            cells: List of table cells
            competition_name: Name of the competition
            competition_id: ID of the competition
            
        Returns:
            Dictionary with season data or None if parsing failed
        """
        try:
            if len(cells) < 7:
                logger.warning(f"Expected at least 7 columns for club tournament, got {len(cells)}")
                return None
            
            # Extract season
            season_cell = cells[0]
            season_link = season_cell.find('a')
            if not season_link:
                logger.warning("No season link found")
                return None
            
            season = season_link.get_text(strip=True)
            season_link_href = season_link.get('href', '')
            
            # Extract number of squads (column 2)
            squads_cell = cells[2]
            squads_text = squads_cell.get_text(strip=True)
            try:
                num_squads = int(squads_text) if squads_text.isdigit() else None
            except ValueError:
                num_squads = None
            
            # Extract champion (column 3)
            champion_cell = cells[3]
            champion_text = champion_cell.get_text(strip=True)
            champion = champion_text if champion_text else None
            
            # Extract runner-up (column 4)
            runner_up_cell = cells[4]
            runner_up_text = runner_up_cell.get_text(strip=True)
            runner_up = runner_up_text if runner_up_text else None
            
            # Extract top scorer and goals (column 6)
            top_scorer_cell = cells[6]
            top_scorer_text = top_scorer_cell.get_text(strip=True)
            
            # Parse top scorer (can be multiple players)
            top_scorer = None
            top_goals = None
            
            if top_scorer_text and top_scorer_text != '':
                # Handle format like "Kylian Mbappé - 5" or "Raphinha, Serhou Guirassy - 13"
                if '-' in top_scorer_text and not top_scorer_text.startswith('-'):
                    # Split on the last dash to separate names from goals
                    parts = top_scorer_text.rsplit('-', 1)
                    if len(parts) == 2:
                        scorer_names = parts[0].strip()
                        try:
                            top_goals = int(parts[1].strip())
                        except ValueError:
                            top_goals = None
                        
                        # Handle multiple scorers
                        if ',' in scorer_names:
                            top_scorer = [name.strip() for name in scorer_names.split(',')]
                        else:
                            top_scorer = scorer_names
                    else:
                        top_scorer = top_scorer_text
                else:
                    # No goals info or starts with dash
                    top_scorer = top_scorer_text
            
            return {
                'season': season,
                'season_link': season_link_href,
                'champion': champion,
                'runner_up': runner_up,
                'top_scorer': top_scorer,
                'top_goals': top_goals,
                'num_squads': num_squads,
                'points': None  # Not applicable for tournaments
            }
            
        except Exception as e:
            logger.error(f"Failed to parse club tournament season row: {e}")
            return None
    
    def scrape_competition_seasons(self, competition_link: str, competition_name: str, competition_id: int) -> List[Dict[str, Any]]:
        """
        Scrape seasons for a specific competition.
        
        Args:
            competition_link: Link to competition page
            competition_name: Name of the competition
            competition_id: ID of the competition
            
        Returns:
            List of season data dictionaries
        """
        
        # Construct full URL
        full_url = urljoin(self.base_url, competition_link)
        
        soup = self.get_page(full_url)
        if not soup:
            logger.error(f"Failed to fetch page: {full_url}")
            return []
        
        # Find the seasons table
        seasons_table = soup.find('table', {'id': 'seasons'})
        if not seasons_table:
            logger.warning(f"Seasons table not found for {competition_name}")
            return []
        
        seasons_data = []
        tbody = seasons_table.find('tbody')
        if not tbody:
            logger.warning(f"Seasons table body not found for {competition_name}")
            return []
        
        rows = tbody.find_all('tr')
        logger.info(f"Found {len(rows)} seasons")
        
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 5:  # Ensure we have enough columns
                season_data = self.parse_season_row(cells, competition_name, competition_id)
                if season_data:
                    seasons_data.append(season_data)
        
        logger.info(f"✅ Season Scraping Completed: {len(seasons_data)} seasons")
        return seasons_data

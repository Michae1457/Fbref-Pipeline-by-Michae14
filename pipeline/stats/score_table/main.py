import sys
from pathlib import Path
from typing import List, Dict, Any, Optional

# Add the project root to the Python path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from pipeline.utils.database import DatabaseManager
from pipeline.utils.logging import get_logger
from pipeline.utils.scrape import UniversalScraper
from pipeline.stats.score_table.parse import ScoreTableParser

logger = get_logger()

class ScoreTablePipeline:
    """Main pipeline for scraping FBref score table data."""
    
    def __init__(self, db_path: str = "database/fbref_database.db"):
        """
        Initialize the score table pipeline.
        
        Args:
            db_path: Path to the DuckDB database file
        """
        self.db_manager = DatabaseManager(db_path)
        self.scraper = UniversalScraper(pipeline_name="stats")
        self.parser = ScoreTableParser()
        
    def get_competitions_with_seasons(self) -> List[Dict[str, Any]]:
        """
        Get DOMESTIC competitions that have seasons data to scrape score tables for.
        
        Returns:
            List of competition dictionaries with seasons data
        """
        
        try:
            with self.db_manager:
                # Get only club competitions (domestic leagues only)
                club_competitions = self.db_manager.get_competitions('competition_club')
                
                logger.info(f"Found {len(club_competitions)} club competitions in database")
                
                # Filter for domestic leagues only and those with seasons data
                competitions_with_seasons = []
                for competition in club_competitions:
                    competition_id = competition.get('competition_id')
                    competition_type = competition.get('competition_type', 'domestic')
                    
                    # Only process domestic leagues
                    if competition_type == 'domestic' and competition_id:
                        seasons = self.db_manager.get_seasons(competition_id)
                        if seasons:
                            competition['seasons'] = seasons
                            competitions_with_seasons.append(competition)
                
                logger.info(f"Found {len(competitions_with_seasons)} domestic leagues with seasons data")
                return competitions_with_seasons
                
        except Exception as e:
            logger.error(f"Failed to get competitions with seasons: {e}")
            return []
    
    def scrape_score_tables_for_competition(self, competition: Dict[str, Any]) -> bool:
        """
        Scrape score tables for all seasons of a single DOMESTIC LEAGUE competition.
        
        Args:
            competition: Competition dictionary with seasons data
            
        Returns:
            True if successful, False otherwise
        """
        competition_name = competition.get('competition_name', 'Unknown')
        competition_id = competition.get('competition_id')
        seasons = competition.get('seasons', [])
        
        if not seasons:
            logger.warning(f"No seasons found for {competition_name}")
            return False
        
        try:
            score_tables_by_season = {}
            total_teams = 0
            
            # Scrape each season page individually
            for season_data in seasons:
                season = season_data.get('season')
                season_link = season_data.get('season_link')
                
                if not season_link:
                    logger.warning(f"No season link for {competition_name} {season}")
                    continue
                
                # Skip current ongoing season
                if self._is_current_season(season):
                    continue
                
                # Scrape HTML content for this season
                soup = self.scraper.scrape_season_page(
                    season_link, season, competition_name, competition_id
                )
                
                if not soup:
                    logger.warning(f"✗ {season}: Failed to scrape page")
                    continue
                
                # Parse regular score table
                score_table_data = self.parser.parse_season_score_table(
                    soup, season, competition_name, competition_id
                )
                
                if score_table_data:
                    score_tables_by_season[season] = score_table_data
                    total_teams += len(score_table_data)
                    logger.info(f"✓ {season}: {len(score_table_data)} teams scraped")
                else:
                    logger.warning(f"✗ {season}: No data scraped")
            
            if not score_tables_by_season:
                logger.warning(f"No score table data found for {competition_name}")
                return False
            
            # Store in database
            with self.db_manager:
                self.db_manager.insert_score_tables(competition_name, competition_id, score_tables_by_season)
            
            logger.info(f"✅ {competition_name}: {total_teams} total team records across {len(score_tables_by_season)} seasons")
            return True
            
        except Exception as e:
            logger.error(f"Failed to scrape score tables for {competition_name}: {e}")
            return False
    
    def _is_current_season(self, season: str) -> bool:
        """
        Check if a season is the current ongoing season that should be skipped.
        
        Args:
            season: Season string (e.g., "2024-2025", "2024")
            
        Returns:
            True if it's the current season that should be skipped
        """
        # Current ongoing seasons to skip
        current_seasons = ["2025-2026", "2025"]
        return season in current_seasons
    
    def scrape_score_tables(self, competition_id: Optional[int] = None):
        """
        Run the complete score table scraping pipeline.
        
        Args:
            competition_id: Specific competition ID to scrape, or None for all
        """
        logger.info("Starting Score Table Scraping Pipeline")
        
        try:
            # Get competitions with seasons data
            competitions = self.get_competitions_with_seasons()
            
            if not competitions:
                logger.warning("No competitions with seasons found to scrape score tables for")
                return
            
            # Filter by competition_id if specified
            if competition_id is not None:
                competitions = [c for c in competitions if c.get('competition_id') == competition_id]
                if competitions:
                    logger.info(f"Filtered to competition: {competitions[0]['competition_name']}")
                else:
                    logger.warning(f"No competition found with ID {competition_id}")
                    return
            
            logger.info(f"Processing {len(competitions)} competitions...")
            
            # Initialize database tables
            with self.db_manager:
                self.db_manager.create_tables()
            
            # Scrape score tables for each competition
            successful_scrapes = 0
            failed_scrapes = 0
            
            for i, competition in enumerate(competitions, 1):
                competition_name = competition.get('competition_name', 'Unknown')
                logger.info(f"⚽ {competition_name}")
                
                if self.scrape_score_tables_for_competition(competition):
                    successful_scrapes += 1
                else:
                    failed_scrapes += 1
            
            # Simple summary
            logger.info(f"✅ Score Table Scraping Completed: {successful_scrapes} successful, {failed_scrapes} failed")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise

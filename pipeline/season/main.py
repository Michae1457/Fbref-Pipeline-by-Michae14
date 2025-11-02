import sys
from pathlib import Path
from typing import List, Dict, Any, Optional

# Add the project root to the Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from pipeline.utils.database import DatabaseManager
from pipeline.utils.logging import get_logger
from pipeline.season.parse_season import SeasonParser
from pipeline.season.parse_club_tournament import SeasonClubTournamentParser
from pipeline.season.parse_nation_tournament import SeasonNationTournamentParser

logger = get_logger()

class SeasonPipeline:
    """Main pipeline for scraping FBref season data."""
    
    def __init__(self, db_path: str = "database/fbref_database.db"):
        """
        Initialize the season pipeline.
        
        Args:
            db_path: Path to the DuckDB database file
        """
        self.db_manager = DatabaseManager(db_path)
        self.season_parser = SeasonParser()
        self.club_tournament_parser = SeasonClubTournamentParser()
        self.nation_tournament_parser = SeasonNationTournamentParser()
        
    def get_competitions_to_scrape(self, competition_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get competitions to scrape seasons for.
        
        Args:
            competition_id: Specific competition ID to scrape, or None for all
            
        Returns:
            List of competition dictionaries
        """
        competitions = []
        
        try:
            with self.db_manager:
                # Get club competitions
                club_competitions = self.db_manager.get_competitions('competition_club')
                competitions.extend(club_competitions)
                
                # Get national competitions
                national_competitions = self.db_manager.get_competitions('competition_nation')
                competitions.extend(national_competitions)
                
                # Filter by competition_id if specified
                if competition_id is not None:
                    competitions = [c for c in competitions if c.get('competition_id') == competition_id]
                    logger.info(f"Filtered to {len(competitions)} competitions with ID {competition_id} - {competitions[0]['competition_name']}")
                
        except Exception as e:
            logger.error(f"Failed to get competitions: {e}")
            return []
        
        return competitions
    
    def scrape_seasons_for_competition(self, competition: Dict[str, Any]) -> bool:
        """
        Scrape seasons for a single competition and store in the appropriate table.
        
        Args:
            competition: Competition dictionary with name, link, and ID
            
        Returns:
            True if successful, False otherwise
        """
        competition_name = competition.get('competition_name', 'Unknown')
        competition_link = competition.get('competition_link', '')
        competition_id = competition.get('competition_id')
        
        if not competition_link or not competition_id:
            logger.error(f"Missing competition_link or competition_id for {competition_name}")
            return False

        try:
            # Determine which parser to use based on competition type
            with self.db_manager:
                competition_db_type = self.db_manager.get_competition_type(competition_id)
            
            # Choose the appropriate parser
            if competition_db_type == 'national':
                parser = self.nation_tournament_parser
                table_name = 'season_nation_tournament'
            elif competition_db_type == 'international':
                parser = self.club_tournament_parser
                table_name = 'season_club_tournament'
            else:
                parser = self.season_parser
                table_name = 'season'
                
            # Scrape seasons data using the appropriate parser
            seasons_data = parser.scrape_competition_seasons(
                competition_link, competition_name, competition_id
            )
            
            if not seasons_data:
                logger.warning(f"No seasons data found for {competition_name}")
                return False
            
            # Store in the appropriate table
            with self.db_manager:
                self.db_manager.insert_seasons(competition_name, competition_id, seasons_data, table_name)
                logger.info(f"✓ {competition_name}: Stored in {table_name} table")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to scrape seasons for {competition_name}: {e}")
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
        seasons = ["2025-2026", "2025"]

        return False
    
    def scrape_seasons(self, competition_id: Optional[int] = None):
        """
        Run the complete season scraping pipeline.
        
        Args:
            competition_id: Specific competition ID to scrape, or None for all
        """
        logger.info("Starting Season Scraping Pipeline")
        
        try:
            # Get competitions to scrape
            competitions = self.get_competitions_to_scrape(competition_id)
            
            if not competitions:
                logger.warning("No competitions found to scrape seasons for")
                return
            
            logger.info(f"Processing {len(competitions)} competitions...")
            
            # Initialize database tables
            with self.db_manager:
                self.db_manager.create_tables()
            
            # Scrape seasons for each competition
            successful_scrapes = 0
            failed_scrapes = 0
            
            for i, competition in enumerate(competitions, 1):
                competition_name = competition.get('competition_name', 'Unknown')
                
                if self.scrape_seasons_for_competition(competition):
                    successful_scrapes += 1
                else:
                    failed_scrapes += 1
            
            # Simple summary
            logger.info(f"✅ Competition Scraping Completed: {successful_scrapes} successful, {failed_scrapes} failed")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise

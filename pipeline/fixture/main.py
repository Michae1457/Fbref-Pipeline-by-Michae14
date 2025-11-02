import sys
from pathlib import Path
from typing import List, Dict, Any, Optional

# Add the project root to the Python path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from pipeline.utils.database import DatabaseManager
from pipeline.utils.logging import get_logger
from pipeline.utils.scrape import UniversalScraper
from pipeline.fixture.parse import FixtureParser


logger = get_logger()

class FixturePipeline:
    """Main pipeline for scraping FBref fixture data."""
    
    def __init__(self, db_path: str = "database/fbref_database.db"):
        """
        Initialize the fixture pipeline.
        
        Args:
            db_path: Path to the DuckDB database file
        """
        self.db_manager = DatabaseManager(db_path)
        self.scraper = UniversalScraper(pipeline_name="fixture")
        self.parser = FixtureParser()

    def convert_season_link_to_fixture_link(self,season_link: str) -> str:
        """
        Convert a season stats link to a fixture/schedule link.
        
        Examples:
            /en/comps/9/2024-2025/2024-2025-Premier-League-Stats 
            -> /en/comps/9/2024-2025/schedule/2024-2025-Premier-League-Scores-and-Fixtures
            
            /en/comps/9/Premier-League-Stats
            -> /en/comps/9/schedule/Premier-League-Scores-and-Fixtures
        
        Args:
            season_link: Season stats page link
            
        Returns:
            Fixture/schedule page link
        """
        if not season_link:
            return ""
        
        # Simple pattern: replace '-Stats' with '-Scores-and-Fixtures' and add '/schedule/'
        if season_link.endswith('-Stats'):
            # Replace the last occurrence of '-Stats' with '-Scores-and-Fixtures'
            fixture_link = season_link.replace('-Stats', '-Scores-and-Fixtures')
            
            # Insert '/schedule' before the last segment
            parts = fixture_link.split('/')
            if len(parts) >= 5:
                # Insert 'schedule' before the last part
                parts.insert(-1, 'schedule')
                return '/'.join(parts)
        
        # If we can't parse it, return the original link
        logger.warning(f"Could not convert season link to fixture link: {season_link}")
        return season_link

    def _is_season_within_years_back(self, season: str, years_back: int) -> bool:
        """
        Check if a season is within the specified number of years back.
        
        Args:
            season: Season string (e.g., "2024-2025", "2023-2024")
            years_back: Number of years back to include
            
        Returns:
            True if season should be included, False otherwise
        """
        try:
            # Extract the starting year from season string (e.g., "2025-2026" -> 2025)
            if '-' in season:
                start_year = int(season.split('-')[0])
            else:
                # Handle single year format (e.g., "2024")
                start_year = int(season)
            
            # Calculate the cutoff year
            from datetime import datetime
            current_year = 2026 #datetime.now().year
            cutoff_year = current_year - years_back #2016
            
            # Include seasons that start from the cutoff year onwards
            return start_year >= cutoff_year
            
        except (ValueError, IndexError) as e:
            logger.warning(f"Could not parse season '{season}': {e}")
            return False
        
    def get_club_competitions_with_seasons(self) -> List[Dict[str, Any]]:
        """
        Get DOMESTIC competitions that have seasons data to scrape score tables for.
        
        Returns:
            List of competition dictionaries with seasons data
        """
        try:
            with self.db_manager:
                # Get only club competitions (domestic leagues only)
                club_competitions = self.db_manager.get_competitions('competition_club')
                
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

    def get_club_tournament_competitions_with_seasons(self) -> List[Dict[str, Any]]:
        """
        Get CLUB TOURNAMENT competitions that have seasons data to scrape score tables for.
        
        Returns:
            List of competition dictionaries with seasons data
        """
        
        try:
            with self.db_manager:
                # Get only club competitions (international club tournaments only)
                club_competitions = self.db_manager.get_competitions('competition_club')
                
                # Get seasons for each competition
                competitions_with_seasons = []
                for competition in club_competitions:
                    competition_id = competition['competition_id']
                    competition_type = competition.get('competition_type', 'international')

                    # Only process international club tournaments
                    if competition_type == 'international' and competition_id:
                        seasons = self.db_manager.get_club_tournament_seasons(competition_id)
                        if seasons:
                            competition['seasons'] = seasons
                            competitions_with_seasons.append(competition)
                
                logger.info(f"Found {len(competitions_with_seasons)} club tournaments with seasons data")
                return competitions_with_seasons
                
        except Exception as e:
            logger.error(f"Failed to get competitions with seasons: {e}")
            return []

    def get_nation_tournament_competitions_with_seasons(self) -> List[Dict[str, Any]]:
        """
        Get NATION TOURNAMENT competitions that have seasons data to scrape score tables for.
        
        Returns:
            List of competition dictionaries with seasons data
        """
        
        try:
            with self.db_manager:
                # Get only nation competitions (national tournaments only)
                nation_competitions = self.db_manager.get_competitions('competition_nation')
                
                # Get seasons for each competition
                competitions_with_seasons = []
                for competition in nation_competitions:
                    competition_id = competition['competition_id']
                    competition_name = competition['competition_name']
                    competition_type = competition.get('competition_type', 'national')
                    
                    if competition_id:
                        seasons = self.db_manager.get_nation_tournament_seasons(competition_id)
                        if seasons:
                            competition = {
                                'competition_id': competition_id,
                                'competition_name': competition_name,
                                'competition_type': competition_type,
                                'seasons': seasons
                            }
                            competitions_with_seasons.append(competition)
                
                logger.info(f"Found {len(competitions_with_seasons)} nation tournaments with seasons data")
                return competitions_with_seasons
                
        except Exception as e:
            logger.error(f"Failed to get competitions with seasons: {e}")
            return []
    
    def scrape_fixture_for_competition(self, competition: Dict[str, Any], refresh_current_season: bool = False, years_back: int = 10) -> bool:
        """
        Scrape fixture for all seasons of a single competition.
        
        Args:
            competition: Competition data dictionary
            refresh_current_season: Whether to force refresh the current ongoing season
            years_back: Number of recent years to include (e.g., 10 means from 2015-16 onwards)
        """
        competition_name = competition.get('competition_name', 'Unknown')
        competition_id = competition.get('competition_id')
        competition_type = competition.get('competition_type')
        seasons = competition.get('seasons', [])
        
        if not seasons:
            logger.warning(f"No seasons found for {competition_name}")
            return False
        
        try:
            fixtures_by_season = {}
            total_teams = 0
            
            # Scrape each season page individually
            for season_data in seasons:
                season = season_data.get('season')
                season_link = season_data.get('season_link')

                # Filter by years_back parameter
                if not self._is_season_within_years_back(season, years_back):
                    logger.debug(f"Skipping season {season} (outside {years_back} years back)")
                    continue

                # Initialize use_cache for this iteration
                use_cache = True  # Default to using cache for efficiency
                
                if not season_link:
                    logger.warning(f"No season link for {competition_name} {season}")
                    continue

                # Convert season stats link to fixture link
                fixture_link = self.convert_season_link_to_fixture_link(season_link)

                # Handle current ongoing season based on refresh parameter
                if self._is_current_season(season):
                    # Skip current ongoing season for national tournaments
                    if competition_type == 'national':
                        logger.info(f"⏭️  Skipping current or upcoming season for national tournament: {season} {competition_name}")
                        continue

                    if refresh_current_season:
                        logger.info(f"🔄 Parsing current season from cache: {season}")
                        # Skip HTML scraping but still parse cached data
                        use_cache = False
                    else:
                        use_cache = True
                        logger.info(f"⏭️  Skipping refresh the current season: {season} {competition_name} (use --refresh-current to include)")
                        #continue
                
                # Scrape HTML content for this season's fixtures
                # For current season with refresh_current=True, refresh the cached data
                # For other seasons, always use cache for efficiency
                soup = self.scraper.scrape_season_page(
                    fixture_link, season, competition_name, competition_id, use_cache=use_cache
                )
                
                if not soup:
                    logger.warning(f"✗ {season}: Failed to scrape page")
                    continue
                
                if competition_type == 'domestic':
                    # Parse regular fixture table
                    fixture_data = self.parser.parse_fixture(
                        soup, season, competition_name, competition_id, future_games=False
                    )
                else:
                    # Parse tournament fixture table
                    fixture_data = self.parser.parse_tournament_fixture(
                        soup, season, competition_name, competition_id, future_games=False
                    )
                
                if fixture_data:
                    fixtures_by_season[season] = fixture_data
                    total_teams += len(fixture_data)
                    logger.info(f"✓ {season}: {len(fixture_data)} teams scraped")
                else:
                    logger.warning(f"✗ {season}: No data scraped")
            
            if not fixtures_by_season:
                logger.warning(f"No fixture data found for {competition_name}")
                return False
            
            # Store in database
            with self.db_manager:
                self.db_manager.insert_fixtures(competition_name, competition_id, fixtures_by_season)
            
            logger.info(f"✅ {competition_name}: {total_teams} total team records across {len(fixtures_by_season)} seasons")
            return True
            
        except Exception as e:
            logger.error(f"Failed to scrape score tables for {competition_name}: {e}")
            return False
    
    def _is_current_season(self, season: str) -> bool:
        """
        Check if a season is the current ongoing season.
        
        Args:
            season: Season string (e.g., "2024-2025", "2024")
            
        Returns:
            True if it's the current ongoing season
        """
        # Current ongoing seasons that need special handling
        current_seasons = ["2025-2026", "2026"]
        return season in current_seasons
    
    def scrape_fixtures(self, competition_id: Optional[int] = None, refresh_current_season: bool = False, years_back: int = 10):
        """
        Run the complete fixture scraping pipeline.
        
        Args:
            competition_id: Specific competition ID to scrape, or None for all
            refresh_current_season: Whether to force refresh the current ongoing season (2025-2026)
            years_back: Number of recent years to include (e.g., 10 means from 2015-16 onwards)
        """
        logger.info("Starting Fixture Scraping Pipeline")
        logger.info(f"Scraping fixtures from the last {years_back} years")
        
        try:
            # Get competitions with seasons data
            competitions = []
            # Get domestic leagues
            club_competitions = self.get_club_competitions_with_seasons()
            competitions.extend(club_competitions)

            # Get club tournaments
            club_tournament_competitions = self.get_club_tournament_competitions_with_seasons()
            competitions.extend(club_tournament_competitions)

            # Get nation tournaments
            nation_competitions = self.get_nation_tournament_competitions_with_seasons()
            competitions.extend(nation_competitions)
            
            if not competitions:
                logger.warning("No competitions with seasons found to scrape fixtures for")
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
            
            # Scrape fixtures for each competition
            successful_scrapes = 0
            failed_scrapes = 0
            
            for i, competition in enumerate(competitions, 1):
                competition_name = competition.get('competition_name', 'Unknown')
                logger.info(f"⚽ {competition_name}")
                
                if self.scrape_fixture_for_competition(competition, refresh_current_season, years_back):
                    successful_scrapes += 1
                else:
                    failed_scrapes += 1
            
            # Simple summary
            logger.info(f"✅ Score Table Scraping Completed: {successful_scrapes} successful, {failed_scrapes} failed")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise

if __name__ == "__main__":
    pipeline = FixturePipeline()
    pipeline.scrape_fixtures()
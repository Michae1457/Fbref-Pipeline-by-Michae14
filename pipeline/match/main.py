import sys
from pathlib import Path
from typing import List, Dict, Any, Optional

# Add the project root to the Python path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from pipeline.utils.database import DatabaseManager
from pipeline.utils.logging import get_logger
from pipeline.utils.scrape import UniversalScraper
from pipeline.match.parse import MatchParser, PipelineStopError

logger = get_logger()

class MatchPipeline:
    """Main pipeline for scraping FBref match report data."""
    
    def __init__(self, db_path: str = "database/fbref_database.db"):
        """
        Initialize the match pipeline.
        
        Args:
            db_path: Path to the DuckDB database file
        """
        self.db_manager = DatabaseManager(db_path)
        self.scraper = UniversalScraper(pipeline_name="match")
        self.parser = MatchParser()

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

    def process_fixture_data(self, competition_id: Optional[int] = None, years_back: int = 10) -> List[Dict[str, Any]]:
        """
        Process fixture data to extract match report links and prepare for individual match scraping.
        
        Args:
            competition_id: Optional competition ID to filter by
            years_back: Number of recent years to include (e.g., 10 means from 2015-16 onwards)
            
        Returns:
            List of match dictionaries with metadata and match report links ready for scraping
        """
        if not self.db_manager.conn:
            self.db_manager.connect()
        
        try:
            import json
            
            # Get all fixture data
            fixture_data = self.db_manager.get_fixtures()
            processed_matches = []
            
            for competition_row in fixture_data:
                competition_name = competition_row['competition_name']
                comp_id = competition_row['competition_id']
                fixtures_struct = competition_row['fixtures']
                
                # Apply competition filter if specified
                if competition_id and comp_id != competition_id:
                    continue
                
                # Process each fixture entry in the struct
                for fixture_json in fixtures_struct:
                    season = fixture_json['season']
                    
                    # Filter by years_back parameter
                    if not self._is_season_within_years_back(season, years_back):
                        logger.debug(f"Skipping season {season} (outside {years_back} years back)")
                        continue
                    
                    round_name = fixture_json['round']
                    week = fixture_json['week']
                    fixture_data_json = fixture_json['scores_and_fixture']
                    
                    # Parse the JSON to get individual games
                    try:
                        games_data = json.loads(fixture_data_json)
                    except (json.JSONDecodeError, TypeError) as e:
                        logger.warning(f"Failed to parse fixture JSON for {competition_name} {season} {round_name}: {e}")
                        continue
                    
                    # Process each game in the fixture
                    for game_key, game_data in games_data.items():
                        match_report_link = game_data.get('match_report_link')
                        
                        if not match_report_link:
                            logger.debug(f"No match report link for {game_key} in {competition_name} {season}")
                            continue
                        
                        # Create processed match record
                        processed_match = {
                            'competition_name': competition_name,
                            'competition_id': comp_id,
                            'season': season,
                            'round': round_name,
                            'week': week,
                            'game_key': game_key,
                            'match_link': match_report_link,
                            'home_team': game_data.get('home_team'),
                            'away_team': game_data.get('away_team'),
                            'home_team_id': game_data.get('home_team_id'),
                            'away_team_id': game_data.get('away_team_id'),
                            'date': game_data.get('date'),
                            'time': game_data.get('time'),
                            'venue': game_data.get('venue'),
                            'referee': game_data.get('referee'),
                            'home_team_xg': game_data.get('home_team_xg'),
                            'away_team_xg': game_data.get('away_team_xg'),
                            'score': game_data.get('score'),
                            'attendance': game_data.get('attendance'),
                        }
                        
                        processed_matches.append(processed_match)

            logger.info(f"Processed {len(processed_matches)} matches from fixture data")
            return processed_matches
            
        except Exception as e:
            logger.error(f"Failed to process fixture data: {e}")
            return []

    def scrape_match_for_competition(self, competition_id: Optional[int] = None, years_back: int = 10) -> bool:
        """
        Scrape match reports for a specific competition.
        
        Args:
            competition_id: Competition ID to scrape matches for
            years_back: Number of recent years to include (e.g., 10 means from 2015-16 onwards)
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get match data from fixtures
            match_data = self.process_fixture_data(competition_id, years_back)

            if not match_data:
                logger.warning("No match data found to scrape matches for")
                return False

            logger.info(f"Processing {len(match_data)} matches...")
            
            successful_scrapes = 0
            failed_scrapes = 0
            
            for i, match in enumerate(match_data, 1):
                logger.info(f"Processing match {i}/{len(match_data)}: {match['home_team']} vs {match['away_team']}")
                
                try:
                    # Scrape the match report page
                    soup = self.scraper.get_page(match['match_link'])
                    
                    if not soup:
                        logger.warning(f"Failed to scrape match report: {match['match_link']}")
                        failed_scrapes += 1
                        continue
                    
                    # Parse the match data
                    parsed_data = self.parser.parse_match_data(soup, match['match_link'])
                    
                    if parsed_data:
                        # Combine fixture data with parsed match data
                        complete_match = {**match, **parsed_data}
                        
                        # Store in database
                        self.db_manager.insert_match(complete_match)
                        
                        successful_scrapes += 1
                        logger.info(f"✓ Successfully processed match: {match['home_team']} vs {match['away_team']}")
                    else:
                        logger.warning(f"✗ Failed to parse match data: {match['home_team']} vs {match['away_team']}")
                        failed_scrapes += 1
                        
                except PipelineStopError as e:
                    logger.error(f"Pipeline stopping error processing match {match['home_team']} vs {match['away_team']}: {e}")
                     # Re-raise to stop the entire pipeline
                except Exception as e:
                    logger.error(f"Error processing match {match['home_team']} vs {match['away_team']}: {e}")
                    failed_scrapes += 1
                    continue
            
            logger.info(f"Match scraping completed: {successful_scrapes} successful, {failed_scrapes} failed")
            
            return successful_scrapes > 0
            
        except (Exception, PipelineStopError) as e:
            logger.error(f"Failed to scrape matches for competition {competition_id}: {e}")
            return False
    
    def scrape_matches(self, competition_id: Optional[int] = None, years_back: int = 10):
        """
        Run the complete match scraping pipeline.
        
        Args:
            competition_id: Specific competition ID to scrape, or None for all
            years_back: Number of recent years to include (e.g., 10 means from 2015-16 onwards)
        """
        logger.info("Starting Match Scraping Pipeline")
        logger.info(f"Scraping matches from the last {years_back} years")
        
        try:
            # Initialize database tables (without context manager)
            self.db_manager.create_tables()
            
            # Get competitions to process from all competition tables
            competitions = []
            
            # Get club competitions
            club_competitions = self.db_manager.get_competitions('competition_club')
            competitions.extend(club_competitions)
            
            # Get nation competitions  
            nation_competitions = self.db_manager.get_competitions('competition_nation')
            competitions.extend(nation_competitions)
            
            if competition_id:
                # Filter to specific competition
                competitions = [c for c in competitions if c.get('competition_id') == competition_id]
            
            if not competitions:
                logger.warning("No competitions found to process")
                return
            
            logger.info(f"Processing {len(competitions)} competitions...")
            
            # Scrape matches for each competition
            successful_scrapes = 0
            failed_scrapes = 0
            
            for i, competition in enumerate(competitions, 1):
                competition_name = competition.get('competition_name', 'Unknown')
                comp_id = competition.get('competition_id')
                logger.info(f"⚽ {competition_name}")
                
                if self.scrape_match_for_competition(comp_id, years_back):
                    successful_scrapes += 1
                else:
                    failed_scrapes += 1
            
            # Summary
            logger.info(f"✅ Match Scraping Completed: {successful_scrapes} successful, {failed_scrapes} failed")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise

if __name__ == "__main__":
    pipeline = MatchPipeline()
    pipeline.scrape_matches()
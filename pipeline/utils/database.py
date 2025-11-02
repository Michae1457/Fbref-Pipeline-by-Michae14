import duckdb
import json
from pathlib import Path
from typing import List, Dict, Any, Optional
from pipeline.utils.logging import get_logger
from pipeline.utils.query import DatabaseQueries
from pipeline.utils.mapping import LEADER_TABLE_TYPE_MAPPING

logger = get_logger()

class DatabaseManager:
    """Manages DuckDB database operations for FBref competition data."""
    
    def __init__(self, db_path: str = "database/fbref_database.db"):
        """
        Initialize database manager.
        
        Args:
            db_path: Path to the DuckDB database file
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = None
        
    def connect(self):
        """Connect to the database."""
        try:
            self.conn = duckdb.connect(str(self.db_path))
            # Removed verbose connection logging
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise


    ### CREATE TABLES ###

    
    def create_tables(self):
        """Create all competition tables."""
        if not self.conn:
            self.connect()
        
        # Consolidated Club Competitions table (Big 5, Domestic, International Cups)
        self.conn.execute(DatabaseQueries.CREATE_COMPETITION_CLUB_TABLE)
        
        # National Team Competitions table
        self.conn.execute(DatabaseQueries.CREATE_COMPETITION_NATION_TABLE)
        
        # Season table (only for domestic leagues)
        self.conn.execute(DatabaseQueries.CREATE_SEASON_TABLE)
        
        # Tournament Club table (for club international cups like UEFA Champions League)
        self.conn.execute(DatabaseQueries.CREATE_SEASON_CLUB_TOURNAMENT_TABLE)
        
        # Tournament Nation table (for national tournaments like World Cup)
        self.conn.execute(DatabaseQueries.CREATE_SEASON_NATION_TOURNAMENT_TABLE)
        
        # Score table (only for domestic leagues)
        self.conn.execute(DatabaseQueries.CREATE_SCORE_TABLE)

        # Tournament History Club table - club (e.g., UEFA Champions League)
        self.conn.execute(DatabaseQueries.CREATE_SCORE_TABLE_CLUB_TOURNAMENT)

        # Tournament History Nation table - nation (e.g., World Cup)
        self.conn.execute(DatabaseQueries.CREATE_SCORE_TABLE_NATION_TOURNAMENT)

        # Fixture table - stores fixture data
        self.conn.execute(DatabaseQueries.CREATE_FIXTURE_TABLE)
        
        # Match table - stores individual match report data
        self.conn.execute(DatabaseQueries.CREATE_MATCH_TABLE)


    ### INSERT QUERIES ###

        
    def insert_competitions(self, table_name: str, competitions: List[Dict[str, Any]]):
        """
        Insert competition data into specified table.
        Clears existing data before inserting new data to avoid duplicates.
        
        Args:
            table_name: Name of the table to insert into
            competitions: List of competition dictionaries
        """
        if not self.conn:
            self.connect()
        
        if not competitions:
            logger.warning(f"No competitions to insert into {table_name}")
            return
        
        try:
            # Clear existing data to avoid duplicates
            self.conn.execute(DatabaseQueries.DELETE_COMPETITIONS.format(table_name=table_name))
            
            # Convert awards to JSON string for DuckDB STRUCT array
            for comp in competitions:
                if 'awards' in comp and comp['awards']:
                    comp['awards'] = json.dumps(comp['awards'])
                else:
                    comp['awards'] = json.dumps([])
            
            # Insert competitions
            for comp in competitions:
                placeholders = ', '.join(['?' for _ in comp])
                columns = ', '.join(comp.keys())
                values = list(comp.values())
                
                query = DatabaseQueries.INSERT_COMPETITIONS.format(
                    table_name=table_name, 
                    columns=columns, 
                    placeholders=placeholders
                )
                self.conn.execute(query, values)
            
        except Exception as e:
            logger.error(f"Failed to insert competitions into {table_name}: {e}")
            raise
    
    def insert_seasons(self, competition_name: str, competition_id: int, seasons: List[Dict[str, Any]], table_name: str = "season"):
        """
        Insert season data for a competition into the specified table.
        Clears existing data for this competition before inserting new data.
        
        Args:
            competition_name: Name of the competition
            competition_id: ID of the competition
            seasons: List of season data dictionaries
            table_name: Table to insert into ("season", "season_club_tournament", or "season_nation_tournament")
        """
        if not self.conn:
            self.connect()
        
        try:
            # Clear existing data for this competition
            self.conn.execute(
                DatabaseQueries.DELETE_SEASONS.format(table_name=table_name), 
                (competition_id,)
            )
            
            if not seasons:
                logger.info(f"No seasons to insert for {competition_name}")
                return
            
            # Convert seasons to DuckDB STRUCT format based on table type
            seasons_struct = []
            for season in seasons:
                # Convert top_scorer list to string if needed
                top_scorer = season.get('top_scorer')
                if isinstance(top_scorer, list):
                    top_scorer = ', '.join(top_scorer)
                
                if table_name == "season":
                    # Domestic league format
                    seasons_struct.append({
                        'season': season.get('season'),
                        'season_link': season.get('season_link'),
                        'champion': season.get('champion'),
                        'points': season.get('points'),
                        'top_scorer': top_scorer,
                        'top_goals': season.get('top_goals'),
                        'num_squads': season.get('num_squads')
                    })
                elif table_name == "season_club_tournament":
                    # Club tournament format
                    seasons_struct.append({
                        'season': season.get('season'),
                        'season_link': season.get('season_link'),
                        'num_squads': season.get('num_squads'),
                        'champion': season.get('champion'),
                        'runner_up': season.get('runner_up'),
                        'top_scorer': top_scorer,
                        'top_scorer_goals': season.get('top_goals')
                    })
                elif table_name == "season_nation_tournament":
                    # Nation tournament format
                    seasons_struct.append({
                        'season': season.get('season'),
                        'season_link': season.get('season_link'),
                        'host_country': season.get('host_country'),
                        'num_squads': season.get('num_squads'),
                        'champion': season.get('champion'),
                        'runner_up': season.get('runner_up'),
                        'top_scorer': top_scorer,
                        'top_scorer_goals': season.get('top_goals')
                    })
            
            # Determine the correct column name for the STRUCT array
            if table_name == "season":
                struct_column = "seasons"
            elif table_name == "season_club_tournament":
                struct_column = "club_tournaments"
            elif table_name == "season_nation_tournament":
                struct_column = "national_tournaments"
            else:
                struct_column = "seasons"  # fallback
            
            # Insert season data
            query = DatabaseQueries.INSERT_SEASONS.format(
                table_name=table_name, 
                struct_column=struct_column
            )
            self.conn.execute(query, (competition_name, competition_id, seasons_struct))
            
            # Seasons inserted silently
            
        except Exception as e:
            logger.error(f"Failed to insert seasons for {competition_name} into {table_name}: {e}")
            raise
    
    def insert_score_tables(self, competition_name: str, competition_id: int, score_tables_by_season: Dict[str, List[Dict[str, Any]]]):
        """
        Insert score table data for a competition organized by season (DOMESTIC LEAGUES ONLY).
        Clears existing data for this competition before inserting new data.
        
        Args:
            competition_name: Name of the competition
            competition_id: ID of the competition
            score_tables_by_season: Dictionary with season as key and list of team records as value
                                   e.g., {"2024-2025": [{rank: 1, team: "Liverpool", ...}, ...]}
        """
        if not self.conn:
            self.connect()
        
        try:
            # Clear existing data for this competition
            self.conn.execute(DatabaseQueries.DELETE_SCORE_TABLES, (competition_id,))
            
            if not score_tables_by_season:
                logger.info(f"No score table data to insert for {competition_name}")
                return
            
            # Convert score tables to STRUCT array format with JSON
            import json
            score_tables_struct = []
            
            for season, score_table_data in score_tables_by_season.items():
                # Use ensure_ascii=False to preserve non-ASCII characters (e.g., accents)
                score_table_json = json.dumps(score_table_data, ensure_ascii=False) if score_table_data else None
                
                score_tables_struct.append({
                    'season': season,
                    'score_table': score_table_json,
                    
                })
            
            # Insert score table data
            self.conn.execute(DatabaseQueries.INSERT_SCORE_TABLES, (competition_name, competition_id, score_tables_struct))
            
            # Score tables inserted silently
            
        except Exception as e:
            logger.error(f"Failed to insert score tables for {competition_name}: {e}")
            raise

    def insert_tournament_score_tables(self, table_name: str, competition_name: str, competition_id: int, score_tables_by_season: Dict[str, List[Dict[str, Any]]]):
        """
        Insert tournament league table data (club or nation) organized by season into the specified table.
        The table should be either 'tournament_score_table_club' or 'tournament_score_table_nation'.
        """
        if not self.conn:
            self.connect()
        
        try:
            self.conn.execute(
                DatabaseQueries.DELETE_TOURNAMENT_SCORE_TABLES.format(table_name=table_name),
                (competition_id,)
            )
            if not score_tables_by_season:
                logger.info(f"No tournament score table data to insert for {competition_name}")
                return
            import json
            score_tables_struct = []
            for season, score_table_data in score_tables_by_season.items():
                # Preserve unicode characters in JSON
                score_table_json = json.dumps(score_table_data, ensure_ascii=False) if score_table_data else None
                score_tables_struct.append({
                    'season': season,
                    'score_table': score_table_json
                })
            query = DatabaseQueries.INSERT_TOURNAMENT_SCORE_TABLE.format(table_name=table_name)
            self.conn.execute(query, (competition_name, competition_id, score_tables_struct))
        except Exception as e:
            logger.error(f"Failed to insert tournament score tables for {competition_name} into {table_name}: {e}")
            raise

    def insert_fixtures(self, competition_name: str, competition_id: int, fixtures_by_season: Dict[str, List[Dict[str, Any]]]):
        """
        Insert fixture data for a competition organized by season.
        Clears existing data for this competition before inserting new data.
        
        Args:
            competition_name: Name of the competition
            competition_id: ID of the competition
            fixtures_by_season: Dictionary with season as key and list of fixture records as value
                                   e.g., {"2024-2025": [{week: 1, day: "Thursday", date: "2024-08-15", ...}, ...]}
        """
        if not self.conn:
            self.connect()
        
        try:
            # Clear existing data for this competition
            #self.conn.execute(DatabaseQueries.DELETE_FIXTURE_TABLES.format(table_name='fixture'), (competition_id,))

            if not fixtures_by_season:
                logger.info(f"No fixture data to insert for {competition_name}")
                return
            
            import json
            fixtures_struct = []

            for season, fixture_data in fixtures_by_season.items():
                if fixture_data:
                    # Group fixtures by (round, week). Using round ensures knockout stages are separated
                    fixtures_by_group = {}
                    for fixture in fixture_data:
                        round_value = fixture.get('round')
                        week = fixture.get('week')
                        key = (round_value, week)
                        if key not in fixtures_by_group:
                            fixtures_by_group[key] = {}

                        # Create game key (e.g., "game1", "game2", etc.)
                        game_num = len(fixtures_by_group[key]) + 1
                        game_key = f"game{game_num}"
                        fixtures_by_group[key][game_key] = fixture

                    # Create fixtures structure entries for each (round, week) group
                    for (round_value, week), grouped_fixtures in fixtures_by_group.items():
                        fixtures_struct.append({
                            'season': season,
                            'round': round_value if round_value is not None else competition_name,
                            'week': week,
                            'scores_and_fixture': json.dumps(grouped_fixtures, ensure_ascii=False)
                        })
            
            # Insert fixture data
            query = DatabaseQueries.INSERT_FIXTURE_TABLE.format(table_name='fixture')
            self.conn.execute(query, (competition_name, competition_id, fixtures_struct))
            
            logger.info(f"✓ Inserted fixtures for {competition_name}")
            
        except Exception as e:
            logger.error(f"Failed to insert fixtures for {competition_name}: {e}")
            raise

    def insert_match(self, match_data: Dict[str, Any]) -> None:
        """
        Insert match data into the match table.
        
        Args:
            match_data: Dictionary containing match information
        """
        if not self.conn:
            self.connect()
        
        try:
            import json
            
            # Prepare lineup data
            lineup_data = match_data.get('lineup', {})
            lineup_struct = {
                'start': json.dumps(lineup_data.get('start', {}), ensure_ascii=False),
                'bench': json.dumps(lineup_data.get('bench', {}), ensure_ascii=False)
            }
            
            # Prepare match summary data
            match_summary_json = json.dumps(match_data.get('match_summary', {}), ensure_ascii=False)
            
            # Prepare team stats data
            team_stats_data = match_data.get('team_stats', {})
            team_stats_struct = {
                'home_team': json.dumps(team_stats_data.get('home_team', {}), ensure_ascii=False),
                'away_team': json.dumps(team_stats_data.get('away_team', {}), ensure_ascii=False)
            }
            
            # Prepare player stats data
            def prepare_player_stats_struct(stats_key: str) -> Dict[str, str]:
                """Helper function to prepare player stats struct."""
                stats_data = match_data.get(stats_key, {})
                return {
                    'home_team': json.dumps(stats_data.get('home_team', []), ensure_ascii=False),
                    'away_team': json.dumps(stats_data.get('away_team', []), ensure_ascii=False)
                }
            
            player_summary_stats_struct = prepare_player_stats_struct('player_summary_stats')
            player_passing_stats_struct = prepare_player_stats_struct('player_passing_stats')
            player_pass_types_stats_struct = prepare_player_stats_struct('player_pass_types_stats')
            player_defense_stats_struct = prepare_player_stats_struct('player_defense_stats')
            player_possession_stats_struct = prepare_player_stats_struct('player_possession_stats')
            player_miscellaneous_stats_struct = prepare_player_stats_struct('player_miscellaneous_stats')
            player_goalkeeper_stats_struct = prepare_player_stats_struct('player_goalkeeper_stats')
            
            # Helper function to convert empty strings to None for numeric fields
            def safe_numeric(value):
                if value == '' or value is None:
                    return None
                try:
                    return float(value) if isinstance(value, str) else value
                except (ValueError, TypeError):
                    return None
            
            def safe_int(value):
                if value == '' or value is None:
                    return None
                try:
                    return int(value) if isinstance(value, str) else value
                except (ValueError, TypeError):
                    return None
            
            # Insert match data
            self.conn.execute(DatabaseQueries.INSERT_MATCH_TABLE, (
                match_data.get('match_id', ''),
                match_data.get('match_link', ''),
                match_data.get('competition_name', ''),
                match_data.get('competition_id'),
                match_data.get('season', ''),
                match_data.get('week', ''),
                match_data.get('date', ''),
                match_data.get('time', ''),
                safe_int(match_data.get('attendance')),
                match_data.get('venue', ''),
                match_data.get('referee', ''),
                match_data.get('home_team', ''),
                match_data.get('home_team_id', ''),
                match_data.get('away_team', ''),
                match_data.get('away_team_id', ''),
                safe_numeric(match_data.get('home_team_xg')),
                safe_numeric(match_data.get('away_team_xg')),
                match_data.get('score', ''),
                lineup_struct,
                match_summary_json,
                team_stats_struct,
                player_summary_stats_struct,
                player_passing_stats_struct,
                player_pass_types_stats_struct,
                player_defense_stats_struct,
                player_possession_stats_struct,
                player_miscellaneous_stats_struct,
                player_goalkeeper_stats_struct
            ))
            
            logger.debug(f"Inserted match: {match_data.get('home_team')} vs {match_data.get('away_team')}")
            
        except Exception as e:
            logger.error(f"Failed to insert match {match_data.get('match_id')}: {e}")
            raise


    ### GET QUERIES ###

    
    def get_seasons(self, competition_id: int) -> List[Dict[str, Any]]:
        """Get all seasons for a specific competition."""
        if not self.conn:
            self.connect()
        
        try:
            result = self.conn.execute(DatabaseQueries.GET_SEASONS, (competition_id,)).fetchone()
            
            if result and result[0]:
                return result[0]
            return []
        except Exception as e:
            logger.error(f"Failed to get seasons for competition {competition_id}: {e}")
            return []
    
    def get_competitions(self, table_name: str) -> List[Dict[str, Any]]:
        """Get all competitions from a table."""
        if not self.conn:
            self.connect()
        
        try:
            result = self.conn.execute(DatabaseQueries.GET_COMPETITIONS.format(table_name=table_name)).fetchall()
            columns = [desc[0] for desc in self.conn.description]
            return [dict(zip(columns, row)) for row in result]
        except Exception as e:
            logger.error(f"Failed to get competitions from {table_name}: {e}")
            return []

    def get_competition_type(self, competition_id: int) -> str:
        """
        Get the competition type for a given competition ID.
        
        Args:
            competition_id: The competition ID to check
            
        Returns:
            'national' if it's a national competition, 'international' if it's an international club competition, 'domestic' otherwise
        """
        if not self.conn:
            self.connect()
        
        try:
            # Check if it's a national competition
            nation_result = self.conn.execute(DatabaseQueries.GET_COMPETITION_TYPE_NATION, (competition_id,)).fetchone()
            
            if nation_result[0] > 0:
                return 'national'
            
            # Check if it's an international club competition
            club_result = self.conn.execute(DatabaseQueries.GET_COMPETITION_TYPE_CLUB, (competition_id,)).fetchone()
            
            if club_result and club_result[0] == 'international':
                return 'international'
            
            # Default to domestic
            return 'domestic'
            
        except Exception as e:
            logger.error(f"Failed to get competition type for {competition_id}: {e}")
            return 'domestic'

    def get_club_tournament_seasons(self, competition_id: int) -> List[Dict[str, Any]]:
        """Get all seasons for a specific club tournament competition."""
        if not self.conn:
            self.connect()
        try:
            rows = self.conn.execute(DatabaseQueries.GET_CLUB_TOURNAMENT_SEASONS, (competition_id,)).fetchall()
            return [{'season': r[0], 'season_link': r[1]} for r in rows]
        except Exception as e:
            logger.error(f"Failed to get club tournament seasons for {competition_id}: {e}")
            return []

    def get_nation_tournament_seasons(self, competition_id: int) -> List[Dict[str, Any]]:
        """Get all seasons for a specific nation tournament competition."""
        if not self.conn:
            self.connect()
        try:
            rows = self.conn.execute(DatabaseQueries.GET_NATION_TOURNAMENT_SEASONS, (competition_id,)).fetchall()
            return [{'season': r[0], 'season_link': r[1]} for r in rows]
        except Exception as e:
            logger.error(f"Failed to get nation tournament seasons for {competition_id}: {e}")
            return []
    
    def get_score_tables(self) -> List[Dict[str, Any]]:
        """
        Extract raw score table data with competition metadata for Python processing (DOMESTIC LEAGUES ONLY).
        
        Returns:
            List of dictionaries containing competition metadata and raw JSON score table data
        """
        if not self.conn:
            self.connect()
        
        try:
            # Simple query to get raw score table data
            result = self.conn.execute(DatabaseQueries.GET_SCORE_TABLES).fetchall()
            columns = ['competition_name', 'competition_id', 'season', 'team_data_json']
            
            raw_data = []
            for row in result:
                raw_data.append(dict(zip(columns, row)))
            
            return raw_data
            
        except Exception as e:
            logger.error(f"Failed to extract score table data: {e}")
            return []

    def get_fixtures(self) -> List[Dict[str, Any]]:
        """Get all fixture data from the database."""
        if not self.conn:
            self.connect()
        
        try:
            # Query to get fixture data with proper column names
            result = self.conn.execute(DatabaseQueries.GET_FIXTURES).fetchall()
            columns = ['competition_name', 'competition_id', 'fixtures']
            
            fixture_data = []
            for row in result:
                fixture_data.append(dict(zip(columns, row)))
            
            return fixture_data
            
        except Exception as e:
            logger.error(f"Failed to extract fixture data: {e}")
            return []

    def get_table_count(self, table_name: str) -> int:
        """Get the number of records in a table."""
        if not self.conn:
            self.connect()
        
        result = self.conn.execute(DatabaseQueries.GET_TABLE_COUNT.format(table_name=table_name)).fetchone()
        return result[0] if result else 0
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()

    def disconnect(self):
        """Disconnect from the database."""
        if self.conn:
            self.conn.close()
            # Disconnected silently

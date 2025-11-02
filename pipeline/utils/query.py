"""
SQL queries for FBref pipeline database operations.
This file contains all SQL queries used throughout the pipeline to keep them organized and maintainable.
"""

class DatabaseQueries:
    """Collection of SQL queries for database operations."""
    
    # ==================== TABLE CREATION QUERIES ====================
    
    # Consolidated Club Competitions table (Big 5, Domestic, International Cups)
    CREATE_COMPETITION_CLUB_TABLE = """
        CREATE TABLE IF NOT EXISTS competition_club (
            competition_name VARCHAR,
            competition_id BIGINT PRIMARY KEY,
            competition_link VARCHAR,
            gender VARCHAR,
            country VARCHAR,
            governing_body VARCHAR,
            tier VARCHAR,
            first_season VARCHAR,
            last_season VARCHAR,
            awards STRUCT(award_name VARCHAR, award_link VARCHAR)[],
            competition_type VARCHAR
        )
    """
    
    # National Team Competitions table
    CREATE_COMPETITION_NATION_TABLE = """
        CREATE TABLE IF NOT EXISTS competition_nation (
            competition_name VARCHAR,
            competition_id BIGINT PRIMARY KEY,
            competition_link VARCHAR,
            gender VARCHAR,
            governing_body VARCHAR,
            tier VARCHAR,
            first_season VARCHAR,
            last_season VARCHAR,
            awards STRUCT(award_name VARCHAR, award_link VARCHAR)[],
            competition_type VARCHAR
        )
    """
    
    # Season table (only for domestic leagues)
    CREATE_SEASON_TABLE = """
        CREATE TABLE IF NOT EXISTS season (
            competition_name VARCHAR,
            competition_id BIGINT PRIMARY KEY,
            seasons STRUCT(
                season VARCHAR,
                season_link VARCHAR,
                champion VARCHAR,
                points BIGINT,
                top_scorer VARCHAR,
                top_goals BIGINT,
                num_squads BIGINT
            )[]
        )
    """
    
    # Tournament Club table (for club international cups like UEFA Champions League)
    CREATE_SEASON_CLUB_TOURNAMENT_TABLE = """
        CREATE TABLE IF NOT EXISTS season_club_tournament (
            competition_name VARCHAR,
            competition_id BIGINT PRIMARY KEY,
            club_tournaments STRUCT(
                season VARCHAR,
                season_link VARCHAR,
                num_squads INTEGER,
                champion VARCHAR,
                runner_up VARCHAR,
                top_scorer VARCHAR,
                top_scorer_goals INTEGER
            )[]
        )
    """
    
    # Tournament Nation table (for national tournaments like World Cup)
    CREATE_SEASON_NATION_TOURNAMENT_TABLE = """
        CREATE TABLE IF NOT EXISTS season_nation_tournament (
            competition_name VARCHAR,
            competition_id BIGINT PRIMARY KEY,
            national_tournaments STRUCT(
                season VARCHAR,
                season_link VARCHAR,
                host_country VARCHAR,
                num_squads INTEGER,
                champion VARCHAR,
                runner_up VARCHAR,
                top_scorer VARCHAR,
                top_scorer_goals INTEGER
            )[]
        )
    """
    
    # Score table (only for domestic leagues)
    CREATE_SCORE_TABLE = """
        CREATE TABLE IF NOT EXISTS score_table (
            competition_name VARCHAR,
            competition_id BIGINT PRIMARY KEY,
            score_tables STRUCT(
                season VARCHAR,
                score_table JSON
            )[]
        )
    """
    
    # Tournament History Club table - club (e.g., UEFA Champions League)
    CREATE_SCORE_TABLE_CLUB_TOURNAMENT = """
        CREATE TABLE IF NOT EXISTS score_table_club_tournament (
            competition_name VARCHAR,
            competition_id BIGINT PRIMARY KEY,
            score_tables STRUCT(
                season VARCHAR,
                score_table JSON
            )[]
        )
    """
    
    # Tournament History Nation table - nation (e.g., World Cup)
    CREATE_SCORE_TABLE_NATION_TOURNAMENT = """
        CREATE TABLE IF NOT EXISTS score_table_nation_tournament (
            competition_name VARCHAR,
            competition_id BIGINT PRIMARY KEY,
            score_tables STRUCT(
                season VARCHAR,
                score_table JSON
            )[]
        )
    """

    # Fixture table - stores fixture data
    CREATE_FIXTURE_TABLE = """
        CREATE TABLE IF NOT EXISTS fixture (
            competition_name VARCHAR,
            competition_id BIGINT PRIMARY KEY,
            fixtures STRUCT(
                season VARCHAR,
                round VARCHAR,
                week INTEGER,
                scores_and_fixture JSON
            )[]
        )
    """

    # Match table - stores match data
    CREATE_MATCH_TABLE = """
        CREATE TABLE IF NOT EXISTS match (
            match_id VARCHAR PRIMARY KEY,
            match_link VARCHAR,
            competition VARCHAR,
            competition_id INTEGER,
            season VARCHAR,
            week INTEGER,
            date VARCHAR,
            time VARCHAR,
            attendance BIGINT,
            venue VARCHAR,
            referee VARCHAR,
            home_team VARCHAR,
            home_team_id VARCHAR,
            away_team VARCHAR,
            away_team_id VARCHAR,
            home_team_xg FLOAT,
            away_team_xg FLOAT,
            score VARCHAR,
            lineup STRUCT(
                start JSON,
                bench JSON
            ),
            match_summary JSON,
            team_stats STRUCT(
                home_team JSON,
                away_team JSON
            ),
            player_summary_stats STRUCT(
                home_team JSON,
                away_team JSON
            ),
            player_passing_stats STRUCT(
                home_team JSON,
                away_team JSON
            ),
            player_pass_types_stats STRUCT(
                home_team JSON,
                away_team JSON
            ),
            player_defense_stats STRUCT(
                home_team JSON,
                away_team JSON
            ),
            player_possession_stats STRUCT(
                home_team JSON,
                away_team JSON
            ),
            player_miscellaneous_stats STRUCT(
                home_team JSON,
                away_team JSON
            ),
            player_goalkeeper_stats STRUCT(
                home_team JSON,
                away_team JSON
            )
        )
    """
    
    # ==================== INSERT QUERIES ====================
    
    # Insert competitions query template
    INSERT_COMPETITIONS = """
        INSERT INTO {table_name} ({columns}) VALUES ({placeholders})
    """
    
    # Insert seasons query template
    INSERT_SEASONS = """
        INSERT INTO {table_name} (competition_name, competition_id, {struct_column})
        VALUES (?, ?, ?)
    """
    
    # Insert score tables query
    INSERT_SCORE_TABLES = """
        INSERT INTO score_table (competition_name, competition_id, score_tables)
        VALUES (?, ?, ?)
    """
    
    # Insert tournament score tables query template
    INSERT_TOURNAMENT_SCORE_TABLE = """
        INSERT INTO {table_name} (competition_name, competition_id, score_tables)
        VALUES (?, ?, ?)
    """

    # Insert fixture tables query template
    INSERT_FIXTURE_TABLE = """
        INSERT OR REPLACE INTO fixture (competition_name, competition_id, fixtures)
        VALUES (?, ?, ?)
    """

    # Insert match tables query template
    INSERT_MATCH_TABLE = """
        INSERT OR REPLACE INTO match (
            match_id, match_link, competition, competition_id, season, week, date, time,
            attendance, venue, referee, home_team, home_team_id, away_team, away_team_id,
            home_team_xg, away_team_xg, score, lineup, match_summary, team_stats,
            player_summary_stats, player_passing_stats, player_pass_types_stats, player_defense_stats,
            player_possession_stats, player_miscellaneous_stats, player_goalkeeper_stats
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    # ==================== DELETE QUERIES ====================
    
    # Delete competitions query template
    DELETE_COMPETITIONS = """
        DELETE FROM {table_name}
    """
    
    # Delete seasons query template
    DELETE_SEASONS = """
        DELETE FROM {table_name} WHERE competition_id = ?
    """
    
    # Delete score tables query
    DELETE_SCORE_TABLES = """
        DELETE FROM score_table WHERE competition_id = ?
    """
    
    # Delete tournament score tables query template
    DELETE_TOURNAMENT_SCORE_TABLES = """
        DELETE FROM {table_name} WHERE competition_id = ?
    """
    
    # Delete fixture tables query template
    DELETE_FIXTURE_TABLES = """
        DELETE FROM {table_name} WHERE competition_id = ?
    """

    # Delete match tables query template
    DELETE_MATCH_TABLES = """
        DELETE FROM match WHERE match_id = ?
    """
    
    # ==================== SELECT QUERIES ====================
    
    # Get seasons query
    GET_SEASONS = """
        SELECT seasons FROM season WHERE competition_id = ?
    """
    
    # Get competitions query template
    GET_COMPETITIONS = """
        SELECT * FROM {table_name}
    """
    
    # Get matches query
    GET_FIXTURES = """
        SELECT * FROM fixture
    """
    
    DELETE_MATCH_TABLES = """
        DELETE FROM {table_name} WHERE competition_id = ?
    """
    
    # Get club tournament seasons query
    GET_CLUB_TOURNAMENT_SEASONS = """
        SELECT unnest.season, unnest.season_link
        FROM season_club_tournament s,
        UNNEST(s.club_tournaments) as unnest
        WHERE s.competition_id = ?
        ORDER BY unnest.season
    """
    
    # Get nation tournament seasons query
    GET_NATION_TOURNAMENT_SEASONS = """
        SELECT unnest.season, unnest.season_link
        FROM season_nation_tournament s,
        UNNEST(s.national_tournaments) as unnest
        WHERE s.competition_id = ?
        ORDER BY unnest.season
    """
    
    # Get score tables query
    GET_SCORE_TABLES = """
        SELECT 
            st.competition_name,
            st.competition_id,
            unnest.season,
            unnest.score_table as team_data_json
        FROM score_table st,
        UNNEST(st.score_tables) as unnest
        ORDER BY st.competition_name, unnest.season
    """
    
    # Get competition type queries
    GET_COMPETITION_TYPE_NATION = """
        SELECT COUNT(*) FROM competition_nation WHERE competition_id = ?
    """
    
    GET_COMPETITION_TYPE_CLUB = """
        SELECT competition_type FROM competition_club WHERE competition_id = ?
    """

    # Get table count query template
    GET_TABLE_COUNT = """
        SELECT COUNT(*) FROM {table_name}
    """


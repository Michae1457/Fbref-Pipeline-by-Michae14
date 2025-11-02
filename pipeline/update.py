import sys
from pathlib import Path
from typing import Dict, Any
import json

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pipeline.utils.logging import get_logger
from pipeline.utils.database import DatabaseManager
from pipeline.utils.scrape import UniversalScraper
from pipeline.fixture.parse import FixtureParser
from pipeline.fixture.main import FixturePipeline
from pipeline.cli import CompId

logger = get_logger()

def scrape_fixture_for_competition(pipeline, competition: Dict[str, Any], refresh_current_season: bool = False, years_back: int = 1):
    """Scrape fixtures for all seasons of a competition."""
    competition_name = competition.get('competition_name', 'Unknown')
    competition_id = competition.get('competition_id')
    competition_type = competition.get('competition_type')
    seasons = competition.get('seasons', [])
    
    if not seasons:
        return None
    
    fixtures_by_season = {}
    
    for season_data in seasons:
        season = season_data.get('season')
        season_link = season_data.get('season_link')

        if not pipeline._is_season_within_years_back(season, years_back):
            continue
        
        if not season_link:
            continue

        fixture_link = pipeline.convert_season_link_to_fixture_link(season_link)
        use_cache = True

        if pipeline._is_current_season(season):
            if competition_type == 'national':
                continue
            use_cache = not refresh_current_season

        scraper = UniversalScraper(pipeline_name="fixture")
        soup = scraper.scrape_season_page(
            fixture_link, season, competition_name, competition_id, use_cache=use_cache
        )
        
        if not soup:
            continue
        
        parser = FixtureParser()
        
        if competition_type == 'domestic':
            fixture_data = parser.parse_fixture(
                soup, season, competition_name, competition_id, future_games=True
            )
        else:
            fixture_data = parser.parse_tournament_fixture(
                soup, season, competition_name, competition_id, future_games=True
            )
        
        if fixture_data:
            fixtures_by_season[season] = fixture_data
    
    return fixtures_by_season if fixtures_by_season else None

def scrape_fixtures(competition_id: int, refresh_current_season: bool = False, years_back: int = 1):
    """Scrape fixtures and return nested structure."""
    pipeline = FixturePipeline()
    db_manager = DatabaseManager(db_path="database/fbref_database.db")
    
    comps = db_manager.get_competitions('competition_club')
    
    competition = None
    for comp in comps:
        if comp.get('competition_id') == competition_id:
            competition = comp
            comp_type = comp.get('competition_type', 'domestic')
            if comp_type == 'domestic':
                comp['seasons'] = db_manager.get_seasons(competition_id)
            elif comp_type == 'international':
                comp['seasons'] = db_manager.get_club_tournament_seasons(competition_id)
            elif comp_type == 'national':
                comp['seasons'] = db_manager.get_nation_tournament_seasons(competition_id)
            else:
                comp['seasons'] = db_manager.get_seasons(competition_id)
            break
    
    if not competition:
        return {}
    
    fixture_data = scrape_fixture_for_competition(pipeline, competition, refresh_current_season, years_back)
    if not fixture_data:
        return {}
    
    # Group fixtures by week: {season: {week: {game1: {...}}}}
    fixture_structure = {}
    for season, fixtures in fixture_data.items():
        if fixtures:
            fixtures_by_week = {}
            for fixture in fixtures:
                week = f'week {fixture.get("week")}'
                if week not in fixtures_by_week:
                    fixtures_by_week[week] = {}
                game_key = f"game{len(fixtures_by_week[week]) + 1}"
                fixtures_by_week[week][game_key] = fixture
            fixture_structure[season] = fixtures_by_week
    
    return fixture_structure

def get_fixture_time_from_fixture_structure(fixture_structure: Dict[str, Any]) -> Dict[str, Any]:
    """Extract timeline from fixture structure."""
    timeline = {}
    
    for season, fixtures_by_week in fixture_structure.items():
        timeline[season] = {}
        
        for week, fixtures_by_game in fixtures_by_week.items():
            dates = []
            for game_key, fixture_data in fixtures_by_game.items():
                date = fixture_data.get('date')
                time = fixture_data.get('time')
                if date:
                    datetime_str = f'{date}' + (f' {time}' if time else '')
                    dates.append(datetime_str)
            
            if dates:
                timeline[season][week] = sorted(dates)[0]
    
    return timeline


def save_timeline_to_json(timelines: Dict[str, Any], output_dir: str = "timelines"):
    """Save timeline to JSON file."""
    import os
    
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "2025-2026_top10_european_leagues_timeline.json")
    
    with open(output_file, 'w') as f:
        json.dump(timelines, f, indent=2, ensure_ascii=False)
    
    return output_file


def run_pipeline(competition_id: int = None, refresh_current_season: bool = False, years_back: int = 1):
    """Scrape fixtures and save timeline for all domestic leagues."""
    db_manager = DatabaseManager(db_path="database/fbref_database.db")
    comps = db_manager.get_competitions('competition_club')
    
    if competition_id is None:
        all_timelines = {}
        for comp in comps:
            if comp.get('competition_type') == 'domestic':
                comp_id = comp.get('competition_id')
                comp_name = comp.get('competition_name', 'Unknown')
                
                fixture_structure = scrape_fixtures(comp_id, refresh_current_season, years_back)
                if fixture_structure:
                    timeline = get_fixture_time_from_fixture_structure(fixture_structure)
                    all_timelines[comp_name] = timeline
        
        output_file = save_timeline_to_json(all_timelines)
        logger.info(f"✅ Saved: {output_file}")
        return all_timelines
    else:
        fixture_structure = scrape_fixtures(competition_id, refresh_current_season, years_back)
        if not fixture_structure:
            return
        
        fixture_timeline = get_fixture_time_from_fixture_structure(fixture_structure)
        comp_name = next((c.get('competition_name', 'Unknown') for c in comps if c.get('competition_id') == competition_id), f"Competition_{competition_id}")
        
        output_file = save_timeline_to_json({comp_name: fixture_timeline})
        logger.info(f"✅ Saved: {output_file}")
        return fixture_timeline

def create_scraping_timeline(fixture_timeline):
    scraping_timeline = {}


if __name__ == "__main__":
    # Run the pipeline for all domestic leagues
    run_pipeline(refresh_current_season=False, years_back=1)
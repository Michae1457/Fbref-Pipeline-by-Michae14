"""

Setup:

1. Activate virtual environment
    source venv/bin/activate

2. Install requirements
    uv pip install -r requirements.txt

3. download the cache file from google drive and place it in the cache folder
    https://drive.google.com/drive/u/0/folders/1veqacDjEy2tBU_QFx3GDp3u6cM6gCNXi


Commands:

    # Scrape competition data
    uv run -m pipeline.cli competition

    # Scrape season data
    uv run -m pipeline.cli season

    # Scrape score table stats
    uv run -m pipeline.cli score-table

    # Scrape score table stats for club tournaments
    uv run -m pipeline.cli tournament-club

    # Scrape score table stats for nation tournaments
    uv run -m pipeline.cli tournament-nation

    # Scrape fixture data
    uv run -m pipeline.cli fixture
    uv run -m pipeline.cli fixture --competition-id 20 --refresh-current True --years-back 1

    # Scrape match data
    uv run -m pipeline.cli match
    uv run -m pipeline.cli match --competition-id 20 --years-back 1


"""

import sys
from pathlib import Path
import typer

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pipeline.competition.main import CompetitionPipeline
from pipeline.season.main import SeasonPipeline
from pipeline.stats.score_table.main import ScoreTablePipeline
from pipeline.stats.score_table_tournament.club.main import ScoreTableTournamentClubPipeline
from pipeline.stats.score_table_tournament.nation.main import ScoreTableTournamentNationPipeline
from pipeline.fixture.main import FixturePipeline
from pipeline.match.main import MatchPipeline


class CompId:
    """Competition IDs"""
    
    WorldCup = 1
    ChampionsLeague = 8
    PremierLeague = 9
    SerieA = 11
    LaLiga = 12
    Ligue1 = 13
    Bundesliga = 20
    Eredivisie = 23
    PrimeiraLiga = 32
    BelgianProLeague = 37
    SuperLig = 26
    CzechFirstLeague = 66


app = typer.Typer(pretty_exceptions_show_locals=False)


@app.command()
def competition():
    """Scrape competition data from FBref"""
    pipeline = CompetitionPipeline()
    pipeline.scrape_competitions()


@app.command()
def season(competition_id: int | None = None): 
    """Scrape season data from FBref"""
    pipeline = SeasonPipeline()
    pipeline.scrape_seasons(competition_id)


@app.command()
def score_table(competition_id: int | None = None):
    """Scrape score table data from FBref"""
    pipeline = ScoreTablePipeline()
    pipeline.scrape_score_tables(competition_id)


@app.command()
def tournament_club(competition_id: int | None = None):
    """Scrape tournament history club data from FBref"""
    pipeline = ScoreTableTournamentClubPipeline()
    pipeline.scrape_score_tables(competition_id)


@app.command()
def tournament_nation(competition_id: int | None = None):
    """Scrape tournament history nation data from FBref"""
    pipeline = ScoreTableTournamentNationPipeline()
    pipeline.scrape_score_tables(competition_id)


@app.command()
def fixture(competition_id: int | None = None, refresh_current: bool = True, years_back: int = typer.Option(1)):
    """Scrape fixture data from FBref"""
    """refresh_current: Force refresh the current ongoing season (2025-2026) cache"""
    """years_back: Number of recent years to scrape (e.g., 10 means scrape from 2015-16 onwards)"""
    pipeline = FixturePipeline()
    pipeline.scrape_fixtures(competition_id, refresh_current_season=refresh_current, years_back=years_back)

@app.command()
def match(competition_id: int | None = None, years_back: int = typer.Option(1)):
    """Scrape match data from FBref"""
    """years_back: Number of recent years to scrape (e.g., 10 means scrape from 2015-16 onwards)"""
    pipeline = MatchPipeline()
    pipeline.scrape_matches(competition_id, years_back)


if __name__ == "__main__":
    app()

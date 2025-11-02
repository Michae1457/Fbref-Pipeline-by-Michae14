#!/usr/bin/env python3
"""
FBref Competition Scraping Pipeline

This script orchestrates the scraping of all competition data from FBref
and stores it in DuckDB tables.
"""

import sys
from pathlib import Path
from typing import Dict, Any

# Add the project root to the Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from pipeline.utils.database import DatabaseManager
from pipeline.utils.logging import get_logger
from pipeline.competition.scrape.domestic_scraper import DomesticLeaguesScraper
from pipeline.competition.scrape.international_scraper import ClubInternationalCupsScraper
from pipeline.competition.scrape.national_scraper import NationalTeamCompetitionsScraper

logger = get_logger()

class CompetitionPipeline:
    """Main pipeline for scraping and storing FBref competition data."""
    
    def __init__(self, db_path: str = "database/fbref_database.db"):
        """
        Initialize the pipeline.
        
        Args:
            db_path: Path to the DuckDB database file
        """
        self.db_manager = DatabaseManager(db_path)
        self.scrapers = {
            'domestic': DomesticLeaguesScraper(),
            'international': ClubInternationalCupsScraper(),
            'national': NationalTeamCompetitionsScraper()
        }
    
    def scrape_competitions(self):
        """Run the complete competition scraping pipeline."""
        logger.info("Starting Competition Scraping Pipeline")
        
        try:
            # Initialize database
            with self.db_manager:
                self.db_manager.create_tables()
                
                # Scrape and store all club competitions (Domestic, International)
                logger.info("Scraping club competitions...")
                all_club_competitions = []
                
                # Scrape Domestic Leagues - 1st Tier (includes Big 5)
                domestic_data = self.scrapers['domestic'].scrape_domestic_leagues()
                if domestic_data:
                    all_club_competitions.extend(domestic_data)
                    logger.info(f"✅ Domestic Leagues Scraping Completed: {len(domestic_data)} leagues")
                
                # Scrape Club International Cups
                international_data = self.scrapers['international'].scrape_club_international_cups()
                if international_data:
                    all_club_competitions.extend(international_data)
                    logger.info(f"✓ International: {len(international_data)} cups")
                    logger.info(f"✅ Club International Cups Scraping Completed: {len(domestic_data)} cups")
                
                # Store all club competitions in consolidated table
                if all_club_competitions:
                    self.db_manager.insert_competitions('competition_club', all_club_competitions)
                
                # Scrape and store National Team Competitions
                logger.info("Scraping national competitions...")
                national_data = self.scrapers['national'].scrape_national_team_competitions()
                if national_data:
                    self.db_manager.insert_competitions('competition_nation', national_data)
                    logger.info(f"✅ National Team Competitions Scraping Completed: {len(national_data)} national competitions")
                
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
    
    def run_specific_scraper(self, scraper_type: str):
        """
        Run a specific scraper.
        
        Args:
            scraper_type: Type of scraper ('domestic', 'international', 'national')
        """
        if scraper_type not in self.scrapers:
            logger.error(f"Unknown scraper type: {scraper_type}")
            return
        
        logger.info(f"Running {scraper_type} scraper only")
        
        try:
            with self.db_manager:
                self.db_manager.create_tables()
                
                if scraper_type == 'domestic':
                    data = self.scrapers['domestic'].scrape_domestic_leagues()
                    table = 'competition_club'
                elif scraper_type == 'international':
                    data = self.scrapers['international'].scrape_club_international_cups()
                    table = 'competition_club'
                elif scraper_type == 'national':
                    data = self.scrapers['national'].scrape_national_team_competitions()
                    table = 'competition_nation'
                
                if data:
                    self.db_manager.insert_competitions(table, data)
                    logger.info(f"Stored {len(data)} records in {table}")
                
        except Exception as e:
            logger.error(f"Scraper {scraper_type} failed: {e}")
            raise

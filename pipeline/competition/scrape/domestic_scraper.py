from typing import List, Dict, Any, Optional
from pipeline.competition.parse import CompetitionScraper
from pipeline.utils.logging import get_logger
from pipeline.utils.mapping import COUNTRY_MAPPING

logger = get_logger()

class DomesticLeaguesScraper(CompetitionScraper):
    """Scraper for Domestic Leagues - 1st Tier competitions."""
    
    def __init__(self):
        super().__init__()
        self.url = "https://fbref.com/en/comps/"
    
    def parse_competition_row(self, cells) -> Optional[Dict[str, Any]]:
        """
        Parse a Domestic Leagues row.
        Expected columns: Competition Name, Gender, Country, First Season, Last Season, Awards
        """
        try:
            # Competition Name (with link)
            comp_cell = cells[0]
            comp_link_elem = comp_cell.find('a')
            if not comp_link_elem:
                return None
            
            competition_name = comp_link_elem.get_text(strip=True)
            competition_link = comp_link_elem.get('href', '')
            
            # Extract competition ID
            competition_id = self.extract_competition_id(competition_link)
            if not competition_id:
                return None
            
            # Gender
            gender = cells[1].get_text(strip=True)
            
            # Country
            country_cell = cells[2]
            country_tag = country_cell.find_all('a')
            country = country_tag[-1].get_text(strip=True) if country_tag else country_cell.get_text(strip=True)
            country = COUNTRY_MAPPING.get(country, country)

            # First Season
            first_season = cells[3].get_text(strip=True)
            
            # Last Season
            last_season = cells[4].get_text(strip=True)
            
            # Awards
            awards = self.parse_awards(cells[5]) if len(cells) > 5 else []
            
            return {
                'competition_name': competition_name,
                'competition_id': competition_id,
                'competition_link': competition_link,
                'gender': gender,
                'country': country,
                'governing_body': None,  # Domestic leagues don't have governing body
                'tier': '1st',  # All domestic leagues are 1st tier
                'first_season': first_season,
                'last_season': last_season,
                'awards': awards,
                'competition_type': 'domestic'
            }
            
        except Exception as e:
            logger.error(f"Error parsing Domestic Leagues row: {e}")
            return None
    
    def scrape_domestic_leagues(self) -> List[Dict[str, Any]]:
        """Scrape Domestic Leagues - 1st Tier data."""
        
        # Look for the Domestic Leagues - 1st Tier table by ID
        soup = self.get_page(self.url)
        if not soup:
            return []
        
        # Find the Domestic Leagues - 1st Tier table by ID
        table = soup.find('table', id='comps_1_fa_club_league_senior')
        if not table:
            logger.error("Domestic Leagues - 1st Tier table not found")
            return []
        
        competitions = []
        rows = table.find_all('tr')[1:]  # Skip header row
        
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 6:
                competition_data = self.parse_competition_row(cells)
                if competition_data:
                    competitions.append(competition_data)
        
        return competitions

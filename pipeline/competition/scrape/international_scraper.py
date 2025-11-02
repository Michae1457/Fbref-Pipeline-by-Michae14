from typing import List, Dict, Any, Optional
from pipeline.competition.parse import CompetitionScraper
from pipeline.utils.logging import get_logger

logger = get_logger()

class ClubInternationalCupsScraper(CompetitionScraper):
    """Scraper for Club International Cups competitions."""
    
    def __init__(self):
        super().__init__()
        self.url = "https://fbref.com/en/comps/"
    
    def parse_competition_row(self, cells) -> Optional[Dict[str, Any]]:
        """
        Parse a Club International Cups row.
        Expected columns: Competition Name, Gender, Governing Body, First Season, Last Season, Tier, Awards
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
            
            # Governing Body
            governing_body = cells[2].get_text(strip=True)
            
            # First Season
            first_season = cells[3].get_text(strip=True)
            
            # Last Season
            last_season = cells[4].get_text(strip=True)
            
            # Tier
            tier = cells[5].get_text(strip=True) if len(cells) > 5 else ''
            
            # Awards
            awards = self.parse_awards(cells[6]) if len(cells) > 6 else []
            
            return {
                'competition_name': competition_name,
                'competition_id': competition_id,
                'competition_link': competition_link,
                'gender': gender,
                'country': None,  # International cups don't have country
                'governing_body': governing_body,
                'tier': tier,
                'first_season': first_season,
                'last_season': last_season,
                'awards': awards,
                'competition_type': 'international'
            }
            
        except Exception as e:
            logger.error(f"Error parsing Club International Cups row: {e}")
            return None
    
    def scrape_club_international_cups(self) -> List[Dict[str, Any]]:
        """Scrape Club International Cups data."""
        
        # Look for the Club International Cups table by ID
        soup = self.get_page(self.url)
        if not soup:
            return []
        
        # Find the Club International Cups table by ID
        table = soup.find('table', id='comps_intl_club_cup')
        if not table:
            logger.error("Club International Cups table not found")
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

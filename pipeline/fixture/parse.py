import re
import json
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Optional
from datetime import datetime
from pipeline.utils.logging import get_logger

logger = get_logger()

class FixtureParser:
    """Class for parsing FBref fixture HTML content."""
    
    def __init__(self):
        """Initialize the parser."""
        pass
    
    def parse_fixture(self, soup: BeautifulSoup, season: str, competition_name: str, competition_id: int, future_games: bool = False) -> Optional[List[Dict[str, Any]]]:
        """
        Parse fixture data for regular league competitions (domestic leagues).
        
        Args:
            soup: BeautifulSoup object of the fixtures page
            season: Season string (e.g., "2024-2025")
            competition_name: Name of the competition
            competition_id: ID of the competition
            
        Returns:
            List of fixture dictionaries or None if parsing failed
        """
        try:
            # For regular leagues, find the main fixtures table
            fixtures_table = self.find_fixtures_table(season, competition_id, soup)
            if not fixtures_table:
                logger.warning(f"No fixtures table found for {competition_name} {season}")
                return None
            
            # Get column headers and create column mapping
            column_mapping = self._get_column_mapping(fixtures_table)
            if not column_mapping:
                logger.warning(f"Could not determine column structure for {competition_name} {season}")
                return None
            
            # Parse all fixture rows
            fixtures = []
            rows = fixtures_table.find('tbody').find_all('tr') if fixtures_table.find('tbody') else []
            
            for row in rows:
                fixture_data = self.parse_fixture_row_with_mapping(row, competition_id, competition_name, season, column_mapping, future_games=future_games)
                if fixture_data:
                    fixtures.append(fixture_data)
            
            if fixtures:
                logger.info(f"✓ Parsed {len(fixtures)} fixtures for {season} {competition_name}")
                return fixtures
            else:
                logger.warning(f"No fixture data parsed for {season} {competition_name}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to parse fixtures for {season} {competition_name}: {e}")
            return None
    
    def parse_tournament_fixture(self, soup: BeautifulSoup, season: str, competition_name: str, competition_id: int, future_games: bool = False) -> Optional[List[Dict[str, Any]]]:
        """
        Parse fixture data for tournament competitions (World Cup, Champions League, etc.).
        Handles multiple tables with different rounds and complex tournament structures.
        
        Args:
            soup: BeautifulSoup object of the fixtures page
            season: Season string (e.g., "2024-2025")
            competition_name: Name of the competition
            competition_id: ID of the competition
            
        Returns:
            List of fixture dictionaries or None if parsing failed
        """
        try:
            all_fixtures = []
            seen_fixtures = set()  # To avoid duplicates
            
            # Find all tournament tables
            tournament_tables = self.find_tournament_fixture_tables(season, competition_id, soup)
            
            if not tournament_tables:
                logger.warning(f"No tournament tables found for {competition_name} {season}")
                return None
            
            logger.debug(f"Found {len(tournament_tables)} tournament tables")
            
            for table in tournament_tables:
                table_id = table.get('id', 'no-id')
                logger.debug(f"Processing tournament table: {table_id}")
                
                # Get column headers and create column mapping
                column_mapping = self._get_column_mapping(table)
                if not column_mapping:
                    logger.debug(f"Skipping table {table_id} - no valid column mapping")
                    continue
                
                # Parse all fixture rows from this table
                rows = table.find('tbody').find_all('tr') if table.find('tbody') else []

                # Track section headers to propagate round to rows missing it
                current_round = None
                
                for row in rows:
                    ths = row.find_all('th')
                    tds = row.find_all('td')
                    # Header-only row announces the next round
                    if ths and not tds:
                        header_text = row.get_text(strip=True)
                        if header_text:
                            current_round = header_text
                        continue

                    fixture_data = self.parse_fixture_row_with_mapping(row, competition_id, competition_name, season, column_mapping, future_games=future_games)
                    if fixture_data:
                        if not fixture_data.get('round'):
                            fixture_data['round'] = current_round or competition_name
                        
                        # Create a unique key for deduplication
                        fixture_key = f"{fixture_data.get('date')}_{fixture_data.get('home_team')}_{fixture_data.get('away_team')}"
                        
                        if fixture_key not in seen_fixtures:
                            seen_fixtures.add(fixture_key)
                            all_fixtures.append(fixture_data)
                        else:
                            logger.debug(f"Skipping duplicate fixture: {fixture_key}")
            
            if all_fixtures:
                logger.info(f"✓ Parsed {len(all_fixtures)} unique tournament fixtures for {season} {competition_name}")
                return all_fixtures
            else:
                logger.warning(f"No tournament fixture data parsed for {season} {competition_name}")
                return None
                
        except Exception as e:
            logger.error(f"Failed to parse tournament fixtures for {season} {competition_name}: {e}")
            return None
    
    def find_fixtures_table(self, season: str, competition_id: int, soup: BeautifulSoup) -> Optional[BeautifulSoup]:
        """
        Find the fixtures table for regular league competitions.
        Looks for tables with week numbers (typical of domestic leagues).
        
        Args:
            soup: BeautifulSoup object of the page
            
        Returns:
            BeautifulSoup object of the fixtures table or None if not found
        """
        # Look for the fixtures table - try different possible IDs
        table_ids = [
            f'sched_{season}_{competition_id}_1',  # Specific ID
            f'sched_{season}_{competition_id}',    # Alternative ID
        ]
        
        for table_id in table_ids:
            table = soup.find('table', {'id': table_id})
            if table:
                logger.debug(f"Found league fixtures table by ID: {table_id}")
                return table
        
        return None
    
    def find_tournament_fixture_tables(self, season: str, competition_id: int, soup: BeautifulSoup) -> List[BeautifulSoup]:
        # 1) Prefer consolidated 'sched_all'
        sched_all = soup.find('table', {'id': 'sched_all'})
        if sched_all:
            logger.debug("Found consolidated tournament table: sched_all")
            return [sched_all]

        # 2) Collect per-round tables by id pattern
        tables: List[BeautifulSoup] = []
        for s in ['1', '2', '3']:
            table_id = f'sched_{season}_{competition_id}_{s}'
            t = soup.find('table', {'id': table_id})
            if t:
                logger.debug(f"Found tournament table by ID: {table_id}")
                tables.append(t)
        if tables:
            return tables
    
    def _get_column_mapping(self, table: BeautifulSoup) -> Optional[Dict[str, int]]:
        """
        Get column mapping by analyzing table headers.
        
        Args:
            table: BeautifulSoup object of the fixtures table
            
        Returns:
            Dictionary mapping column names to indices, or None if failed
        """
        try:
            # Find the table header
            thead = table.find('thead')
            if not thead:
                return None
            
            # Get header row
            header_row = thead.find('tr')
            if not header_row:
                return None
            
            # Get all header cells
            header_cells = header_row.find_all(['th', 'td'])
            column_mapping = {}
            
            for i, cell in enumerate(header_cells):
                header_text = cell.get_text(strip=True).lower()
                
                # Map headers to our expected column names
                if 'round' in header_text:
                    column_mapping['round'] = i
                elif 'wk' in header_text or 'week' in header_text:
                    column_mapping['week'] = i
                elif 'day' in header_text:
                    column_mapping['day'] = i
                elif 'date' in header_text:
                    column_mapping['date'] = i
                elif 'time' in header_text:
                    column_mapping['time'] = i
                elif 'home' in header_text:
                    column_mapping['home_team'] = i
                elif 'xg' in header_text and 'home' in str(cell).lower():
                    column_mapping['home_xg'] = i
                elif 'score' in header_text:
                    column_mapping['score'] = i
                elif 'xg' in header_text and 'away' in str(cell).lower():
                    column_mapping['away_xg'] = i
                elif 'away' in header_text:
                    column_mapping['away_team'] = i
                elif 'attendance' in header_text:
                    column_mapping['attendance'] = i
                elif 'venue' in header_text:
                    column_mapping['venue'] = i
                elif 'referee' in header_text:
                    column_mapping['referee'] = i
                elif 'match' in header_text and 'report' in header_text:
                    column_mapping['match_report'] = i
                elif 'notes' in header_text:
                    column_mapping['notes'] = i
            
            # Check if we have essential columns
            essential_columns = ['day', 'date', 'home_team', 'away_team']
            if not all(col in column_mapping for col in essential_columns):
                logger.warning(f"Missing essential columns. Found: {list(column_mapping.keys())}")
                return None
            
            # Week is optional for tournament tables (knockout rounds might not have weeks)
            if 'week' not in column_mapping:
                logger.debug("No week column found - this is common for tournament knockout rounds")
            
            logger.debug(f"Column mapping: {column_mapping}")
            return column_mapping
            
        except Exception as e:
            logger.error(f"Failed to get column mapping: {e}")
            return None
    
    def parse_fixture_row_with_mapping(self, row: BeautifulSoup, competition_id: int, competition_name: str, season: str, column_mapping: Dict[str, int], future_games: bool = False) -> Optional[Dict[str, Any]]:
        """
        Parse a single fixture row using column mapping.
        
        Args:
            row: BeautifulSoup object of a table row
            column_mapping: Dictionary mapping column names to indices
            season: Season string
            competition_name: Name of the competition
            competition_id: ID of the competition
            
        Returns:
            Dictionary with fixture data or None if parsing failed
        """
        try:
            cells = row.find_all(['td', 'th'])
            if len(cells) < len(column_mapping):
                return None
            
            fixture_data = {}
            
            # Helper function to get cell content by column name
            def get_col(col_name: str) -> Optional[str]:
                if col_name in column_mapping:
                    cell = cells[column_mapping[col_name]]
                    return cell.get_text(strip=True)
                return None

            # Parse round - use competition name as fallback if no round column
            fixture_data['round'] = get_col('round') if get_col('round') else competition_name
            
            # Parse each field using the column mapping
            fixture_data['week'] = self._parse_int(get_col('week'))  # Week is optional for tournaments
            fixture_data['day'] = get_col('day')
            fixture_data['date'] = get_col('date')
            fixture_data['time'] = get_col('time')
            
            # Parse teams
            home_team_cell = cells[column_mapping['home_team']] if 'home_team' in column_mapping else None
            away_team_cell = cells[column_mapping['away_team']] if 'away_team' in column_mapping else None
            
            if home_team_cell:
                fixture_data['home_team'], fixture_data['home_team_id'] = self._parse_team(home_team_cell)
            if away_team_cell:
                fixture_data['away_team'], fixture_data['away_team_id'] = self._parse_team(away_team_cell)
            
            # Parse xG values (optional fields)
            fixture_data['home_team_xg'] = get_col('home_xg')
            fixture_data['away_team_xg'] = get_col('away_xg')
            
            # Parse score
            fixture_data['score'] = get_col('score')
            
            # Parse attendance
            attendance_text = get_col('attendance')
            fixture_data['attendance'] = self._parse_int(attendance_text) if attendance_text else None
            
            # Parse venue
            fixture_data['venue'] = get_col('venue')
            
            # Parse referee
            fixture_data['referee'] = get_col('referee')
            
            # Parse match report link
            if 'match_report' in column_mapping:
                match_report_cell = cells[column_mapping['match_report']]
                fixture_data['match_report_link'] = self._parse_match_report_link(match_report_cell)
            else:
                fixture_data['match_report_link'] = None
            
            # Parse notes
            fixture_data['notes'] = get_col('notes') or ""
            
            # Skip if essential data is missing (week is optional for tournaments)
            if not all([fixture_data.get('day'), fixture_data.get('date'), 
                       fixture_data.get('home_team'), fixture_data.get('away_team')]):
                return None
            
            # Check if this individual game is in the future
            if future_games is False:
                game_date = fixture_data.get('date')
                if game_date and game_date > datetime.now().strftime('%Y-%m-%d'):
                    return None  # Skip this future game
            
            return fixture_data
            
        except Exception as e:
            logger.error(f"Failed to parse fixture row with mapping: {e}")
            return None
    
    def parse_fixture_row(self, row: BeautifulSoup, season: str, competition_name: str, competition_id: int) -> Optional[Dict[str, Any]]:
        """
        Parse a single fixture row from the fixtures table using keyword-based column detection.
        
        Args:
            row: BeautifulSoup object of a table row
            season: Season string
            competition_name: Name of the competition
            competition_id: ID of the competition
            
        Returns:
            Dictionary with fixture data or None if parsing failed
        """
        try:
            cells = row.find_all(['td', 'th'])
            if len(cells) < 5:  # Need at least basic columns
                return None
            
            # Get column headers from the table header (we'll need to pass this from the parent method)
            # For now, let's use a more flexible approach by checking cell content patterns
            
            fixture_data = {}
            
            # Parse each cell based on content patterns and position hints
            for i, cell in enumerate(cells):
                cell_text = cell.get_text(strip=True)
                
                # Week number (usually first column, numeric)
                if i == 0 and cell_text.isdigit():
                    fixture_data['week'] = int(cell_text)
                
                # Day of week (usually second column, short day names)
                elif i == 1 and cell_text in ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']:
                    fixture_data['day'] = cell_text
                
                # Date (usually third column, YYYY-MM-DD format)
                elif i == 2 and re.match(r'\d{4}-\d{2}-\d{2}', cell_text):
                    fixture_data['date'] = cell_text
                
                # Time (usually fourth column, HH:MM format)
                elif i == 3 and re.match(r'\d{1,2}:\d{2}', cell_text):
                    fixture_data['time'] = cell_text
                
                # Team names (contain links to team pages)
                elif cell.find('a') and '/squads/' in cell.find('a').get('href', ''):
                    team_name, team_id = self._parse_team(cell)
                    if 'home_team' not in fixture_data:
                        fixture_data['home_team'] = team_name
                        fixture_data['home_team_id'] = team_id
                    else:
                        fixture_data['away_team'] = team_name
                        fixture_data['away_team_id'] = team_id
                
                # xG values (decimal numbers)
                elif re.match(r'^\d+\.\d+$', cell_text):
                    if 'home_team_xg' not in fixture_data:
                        fixture_data['home_team_xg'] = cell_text
                    else:
                        fixture_data['away_team_xg'] = cell_text
                
                # Score (format like "1-1", "2-0", etc.)
                elif re.match(r'^\d+–\d+$', cell_text) or re.match(r'^\d+-\d+$', cell_text):
                    fixture_data['score'] = cell_text
                
                # Attendance (large numbers, possibly with commas)
                elif re.match(r'^\d{1,3}(,\d{3})*$', cell_text.replace(',', '')):
                    fixture_data['attendance'] = int(cell_text.replace(',', ''))
                
                # Venue (stadium names, usually longer text)
                elif len(cell_text) > 5 and not cell_text.isdigit() and ':' not in cell_text and not re.match(r'^\d+\.\d+$', cell_text):
                    if 'venue' not in fixture_data:
                        fixture_data['venue'] = cell_text
                
                # Referee (person names, usually in later columns)
                elif len(cell_text) > 3 and not cell_text.isdigit() and ':' not in cell_text and not re.match(r'^\d+\.\d+$', cell_text):
                    if 'referee' not in fixture_data and 'venue' in fixture_data:
                        fixture_data['referee'] = cell_text
                
                # Match report link
                elif cell.find('a') and 'Match Report' in cell_text:
                    fixture_data['match_report_link'] = self._parse_match_report_link(cell)
                
                # Notes (usually last column, often empty)
                elif i == len(cells) - 1:
                    fixture_data['notes'] = cell_text
            
            # Skip if essential data is missing
            if not all([fixture_data.get('week'), fixture_data.get('day'), fixture_data.get('date'), 
                       fixture_data.get('home_team'), fixture_data.get('away_team')]):
                return None
            
            return fixture_data
            
        except Exception as e:
            logger.error(f"Failed to parse fixture row: {e}")
            return None
    
    def _parse_int(self, text: Optional[str]) -> Optional[int]:
        """Parse integer from text."""
        try:
            if not text:
                return None
            # Remove commas and other non-numeric characters except minus sign
            cleaned_text = ''.join(c for c in text if c.isdigit() or c == '-')
            return int(cleaned_text) if cleaned_text else None
        except:
            return None
    
    
    def _parse_team(self, cell) -> tuple[Optional[str], Optional[str]]:
        """Parse team name and ID from cell."""
        try:
            link = cell.find('a')
            if link:
                team_name = link.get_text(strip=True)
                team_id = self._extract_team_id(link.get('href', ''))
                return team_name, team_id
            else:
                team_name = cell.get_text(strip=True)
                return team_name, None
        except:
            return None, None
    
    
    def _parse_match_report_link(self, cell) -> Optional[str]:
        """Parse match report link from cell."""
        try:
            link = cell.find('a')
            if link:
                href = link.get('href', '')
                if href.startswith('/'):
                    return f"https://fbref.com{href}"
                return href
            return None
        except:
            return None
    
    
    def _extract_team_id(self, team_link: str) -> Optional[str]:
        """
        Extract team ID from team link.
        
        Args:
            team_link: Link like '/en/squads/822bd0ba/2024-2025/Liverpool-Stats'
            
        Returns:
            Team ID or None if not found
        """
        try:
            # Pattern: /en/squads/{id}/...
            match = re.search(r'/en/squads/([a-f0-9]+)/', team_link)
            if match:
                return match.group(1)
            return None
        except Exception as e:
            logger.error(f"Failed to extract team ID from {team_link}: {e}")
            return None
    

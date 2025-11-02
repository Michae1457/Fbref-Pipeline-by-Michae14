import re
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Optional
from pipeline.utils.logging import get_logger
from pipeline.utils.mapping import METRIC_MAPPING

logger = get_logger()

class ScoreTableParser:
    """Class for parsing FBref score table data from HTML for domestic leagues."""
    
    def __init__(self):
        """Initialize the parser."""
        pass
    
    def extract_squad_id(self, squad_link: str) -> Optional[str]:
        """
        Extract squad ID from squad link.
        
        Args:
            squad_link: Link like '/en/squads/822bd0ba/2024-2025/Liverpool-Stats'
            
        Returns:
            Squad ID or None if not found
        """
        try:
            # Pattern: /en/squads/{id}/...
            match = re.search(r'/en/squads/([a-f0-9]+)/', squad_link)
            if match:
                return match.group(1)
            return None
        except Exception as e:
            logger.error(f"Failed to extract squad ID from {squad_link}: {e}")
            return None
    
    def _parse_rank(self, text: str):
        """Rank can be integer (1, 2, 3) or non-numeric marker; return int when possible, else string, else None."""
        if not text:
            return None
        t = text.strip()

        rank_mapping = {
            "W": 1,
            "F": 2,
            "SF": "semi-final",
            "QF": "quarter-final",
            "R16": "round of 16",
            "R32": "round of 32",
            "KO": "knockout",
            "GR": "group stage",
            "GR1": "first group stage",
            "GR2": "second group stage",
            "Rd1": "first round",
            "Rd2": "second round",
            "Lg": "league stage"
        }

        if t.isdigit():
            try:
                return int(t)
            except ValueError:
                return None

        if t in rank_mapping:
            return rank_mapping[t]

        return t  # keep textual ranks like 'GR', 'QF', 'SF' if ever present

    def _normalize_header(self, text: str) -> str:
        t = (text or '').strip().lower()
        mapping = {
            'rk': 'rank',
            'squad': 'team',
            'mp': 'matches_played',
            'w': 'wins',
            'd': 'draws',
            'l': 'losses',
            'gf': 'goals_for',
            'ga': 'goals_against',
            'gd': 'goal_difference',
            'pts': 'points',
            'pts/mp': 'points_per_match_played',
            'xg': 'expected_goals',
            'xga': 'expected_goals_allowed',
            'xgd': 'expected_goals_difference',
            'xgd/90': 'expected_goals_difference_per_90_minutes',
            'attendance': 'attendance',
            'top team scorer': 'top_team_scorer',
            'goalkeeper': 'goalkeeper',
            'notes': 'notes'
        }
        return mapping.get(t, t)

    def _extract_headers_map(self, standings_table: BeautifulSoup) -> Dict[str, int]:
        headers_map: Dict[str, int] = {}
        thead = standings_table.find('thead')
        if not thead:
            return headers_map
        header_cells = thead.find_all('th')
        for idx, th in enumerate(header_cells):
            norm = self._normalize_header(th.get_text(strip=True))
            if norm:
                headers_map[norm] = idx
        return headers_map

    def parse_score_table_row(self, cells, season: str, headers_map: Dict[str, int]) -> Optional[Dict[str, Any]]:
        """
        Parse a score table row from the standings table.
        
        Args:
            cells: List of table cells
            season: Season string (e.g., "2024-2025")
            
        Returns:
            Dictionary with score table data or None if parsing failed
        """
        try:
            if len(cells) < 10:  # Ensure we have enough columns
                logger.warning(f"Expected at least 10 columns, got {len(cells)}")
                return None
            
            # Extract rank (may be text; not mandatory integer)
            rank_idx = headers_map.get('rank', 0)
            rank_cell = cells[rank_idx] if rank_idx < len(cells) else cells[0]
            rank_text = rank_cell.get_text(strip=True)
            rank_value = self._parse_rank(rank_text)

            # Extract squad name and link
            team_idx = headers_map.get('team', 1)
            squad_cell = cells[team_idx] if team_idx < len(cells) else cells[1]
            squad_link = squad_cell.find('a')
            squad_name = squad_link.get_text(strip=True) if squad_link else squad_cell.get_text(strip=True)
            squad_link_href = squad_link.get('href', '') if squad_link else ''
            squad_id = self.extract_squad_id(squad_link_href) if squad_link_href else None
            # Skip non-team rows (e.g., group separators) or blank rows
            if not squad_name:
                return None
            
            # Initialize score table data (season is now the dictionary key, not a field)
            score_data = {
                'rank': rank_value,
                'team': squad_name,
                'team_id': squad_id,
                'team_link': squad_link_href
            }
            
            # Helper to safely read a column by normalized header key
            def read_col(key: str) -> Optional[str]:
                idx = headers_map.get(key)
                if idx is None or idx >= len(cells):
                    return None
                return cells[idx].get_text(strip=True)

            score_data['matches_played'] = self._parse_int(read_col('matches_played') or '')
            score_data['wins'] = self._parse_int(read_col('wins') or '')
            score_data['draws'] = self._parse_int(read_col('draws') or '')
            score_data['losses'] = self._parse_int(read_col('losses') or '')
            score_data['goals_for'] = self._parse_int(read_col('goals_for') or '')
            score_data['goals_against'] = self._parse_int(read_col('goals_against') or '')
            score_data['goal_difference'] = self._parse_int(read_col('goal_difference') or '')
            score_data['points'] = self._parse_int(read_col('points') or '')
            score_data['expected_goals'] = self._parse_float(read_col('expected_goals') or '')
            score_data['expected_goals_allowed'] = self._parse_float(read_col('expected_goals_allowed') or '')
            score_data['expected_goals_difference'] = self._parse_float(read_col('expected_goals_difference') or '')
            score_data['expected_goals_difference_per_90_minutes'] = self._parse_float(read_col('expected_goals_difference_per_90_minutes') or '')

            # Optional: attendance (rare in national tournament overall)
            attendance_text = read_col('attendance')
            if attendance_text is not None:
                score_data['avg_home_attendance'] = self._parse_int(attendance_text.replace(',', ''))

            # Top Team Scorer
            scorer_text = read_col('top_team_scorer') or ''
            if scorer_text:
                if '-' in scorer_text:
                    parts = scorer_text.rsplit('-', 1)
                    score_data['top_team_scorer'] = parts[0].strip()
                    score_data['top_team_scorer_goals'] = self._parse_int(parts[1].strip()) if len(parts) == 2 else None
                else:
                    score_data['top_team_scorer'] = scorer_text
                    score_data['top_team_scorer_goals'] = None

            # Goalkeeper and Notes
            keeper_text = read_col('goalkeeper')
            if keeper_text is not None:
                score_data['goalkeeper'] = keeper_text
            notes_text = read_col('notes')
            if notes_text is not None:
                score_data['notes'] = notes_text
            
            return score_data
            
        except Exception as e:
            logger.error(f"Failed to parse score table row: {e}")
            return None
    
    def _parse_int(self, text: str) -> Optional[int]:
        """Parse text to integer, handling common formats."""
        if not text or text == '':
            return None
        if '+' in text or '-' in text:
            return text
        try:
            # Remove commas and other formatting
            cleaned = text.replace(',', '')
            return int(cleaned) if cleaned.isdigit() else None
        except ValueError:
            return None
    
    def _parse_float(self, text: str) -> Optional[float]:
        """Parse text to float, handling common formats."""
        if not text or text == '':
            return None
        if '+' in text or '-' in text:
            return text
        return float(text)
    
    def find_standings_table(self, soup: BeautifulSoup, competition_name: str, season: str) -> Optional[BeautifulSoup]:
        """
        Find the OVERALL League Table (image 2 format) on the page.
        We specifically select the table whose header includes both
        "Top Team Scorer" and "Goalkeeper" columns.
        """
        # 1) Prefer an explicit header match among all tables
        for table in soup.find_all('table'):
            thead = table.find('thead')
            if not thead:
                continue
            header_cells = [th.get_text(strip=True) for th in thead.find_all('th')]
            if not header_cells:
                continue
            # Look for the distinctive overall headers
            has_scorer = any('Top Team Scorer' in h for h in header_cells)
            has_keeper = any('Goalkeeper' in h for h in header_cells)
            if has_scorer and has_keeper:
                table_id = table.get('id', '')
                if table_id:
                    logger.info(f"Found overall league table by headers with ID: {table_id}")
                else:
                    logger.info("Found overall league table by headers (no id)")
                return table

        # 2) Fall back to id heuristics (results...overall) if header match failed
        competition_id_match = re.search(r'/comps/(\d+)/', str(soup))
        comp_id = competition_id_match.group(1) if competition_id_match else None
        if comp_id:
            for suffix in ["", "1", "2", "11", "12", "12A"]:
                table_id = f"results{comp_id}{suffix}_overall" if suffix else f"results{comp_id}_overall"
                table = soup.find('table', {'id': table_id})
                if table:
                    logger.info(f"Found overall league table by ID: {table_id}")
                    return table

        # 3) Last resort: any table containing 'results' and 'overall'
        for table in soup.find_all('table'):
            table_id = table.get('id', '')
            if 'results' in table_id.lower() and 'overall' in table_id.lower():
                logger.info(f"Found overall league table by fuzzy ID: {table_id}")
                return table

        logger.warning(f"Overall League Table not found for {competition_name} {season}")
        return None
    
    def parse_season_score_table(self, soup: BeautifulSoup, season: str, competition_name: str, competition_id: int) -> List[Dict[str, Any]]:
        """
        Parse score table data from HTML soup for a specific season.
        
        Args:
            soup: BeautifulSoup object of the season page
            season: Season string (e.g., "2024-2025")
            competition_name: Name of the competition
            competition_id: ID of the competition
            
        Returns:
            List of score table data dictionaries
        """
        logger.info(f"Parsing {competition_name} {season} score table...")
        
        # Find the standings table
        standings_table = self.find_standings_table(soup, competition_name, season)
        if not standings_table:
            return []
        
        headers_map = self._extract_headers_map(standings_table)
        score_table_data = []
        tbody = standings_table.find('tbody')
        if not tbody:
            logger.warning(f"Standings table body not found for {competition_name} {season}")
            return []
        
        rows = tbody.find_all('tr')
        logger.info(f"Found {len(rows)} teams in {season}")
        
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) >= 3:  # rank + team + at least one stat
                score_data = self.parse_score_table_row(cells, season, headers_map)
                if score_data:
                    score_table_data.append(score_data)
        
        logger.info(f"✓ Parsed {len(score_table_data)} teams")
        return score_table_data

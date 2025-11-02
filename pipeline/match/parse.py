"""
Match Parser for FBref match report pages.

This module parses HTML content from FBref match report pages to extract:
- Lineup data (starting XI and bench players)
- Match summary (events like goals, cards, substitutions)
- Team statistics (possession, passing accuracy, etc.)
"""

import logging
from typing import List, Dict, Any, Optional
from bs4 import BeautifulSoup
import re
from pipeline.utils.mapping import COUNTRY_MAPPING, STATS_MAPPING
logger = logging.getLogger(__name__)


class PipelineStopError(Exception):
    """Exception that should stop the entire pipeline execution."""
    pass

class MatchParser:
    """Parser for FBref match report pages."""

    def extract_match_id(self, match_url: str) -> str:
        """Extract match ID from the match URL."""
        try:
            # Extract ID from URL like: https://fbref.com/en/matches/cc5b4244/Manchester-United-Fulham-August-16-2024-Premier-League
            match = re.search(r'/matches/([^/]+)/', match_url)
            return match.group(1) if match else ""
        except Exception as e:
            logger.error(f"Error extracting match ID from {match_url}: {e}")
            return ""
    
    def parse_match_data(self, soup: BeautifulSoup, match_url: str) -> Dict[str, Any]:
        """
        Parse all match data from the HTML soup.
        
        Args:
            soup: BeautifulSoup object of the match report page
            match_url: URL of the match report page
            
        Returns:
            Dictionary containing all parsed match data
        """
        try:
            match_data = {
                'match_id': self.extract_match_id(match_url),
                'match_link': match_url,
            }
            
            # Parse lineup data
            lineup_data = self.parse_lineup(soup)
            if lineup_data:
                match_data['lineup'] = lineup_data
            
            # Parse match summary (events)
            match_summary = self.parse_match_summary(soup)
            if match_summary:
                match_data['match_summary'] = match_summary
            
            # Parse team statistics
            team_stats = self.parse_team_stats(soup)
            if team_stats:
                match_data['team_stats'] = team_stats

            # Parse player statistics
            player_stats = self.parse_player_stats(soup)
            if player_stats:
                match_data.update(player_stats)

            return match_data
            
        except PipelineStopError:
            # Re-raise PipelineStopError to stop the entire pipeline
            raise
        except Exception as e:
            logger.error(f"Error parsing match data: {e}")
            return {}
    
    def parse_lineup(self, soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
        """
        Parse lineup data from the HTML.
        
        Based on the HTML structure:
        - Two divs with class="lineup" (id="a" and id="b")
        - Each contains team name, formation, starting XI, and bench players
        """
        try:
            lineup_data = {
                'start': {'home_team': [], 'away_team': []},
                'bench': {'home_team': [], 'away_team': []}
            }
            
            # Find lineup divs
            lineup_divs = soup.find_all('div', class_='lineup')
            
            for div in lineup_divs:
                div_id = div.get('id', '')
                is_home = div_id == 'a'  # Assuming 'a' is home team
                team_key = 'home_team' if is_home else 'away_team'
                
                # Find the table within the lineup div
                table = div.find('table')
                if not table:
                    continue
                
                # Extract team name and formation from first th
                header_th = table.find('th')
                if header_th:
                    header_text = header_th.get_text(strip=True)
                    # Extract team name (everything before the formation in parentheses)
                    team_name = re.sub(r'\s*\([^)]*\)$', '', header_text).strip()
                    logger.debug(f"Found team: {team_name}")
                
                # Process table rows
                rows = table.find_all('tr')
                current_section = 'start'  # Start with starting XI
                
                for row in rows:
                    th = row.find('th')
                    if th and 'Bench' in th.get_text():
                        current_section = 'bench'
                        continue
                    
                    # Extract player data
                    cells = row.find_all('td')
                    if len(cells) >= 2:
                        player_number = cells[0].get_text(strip=True)
                        player_cell = cells[1]
                        
                        player_link = player_cell.find('a')
                        if player_link:
                            player_name = player_link.get_text(strip=True)
                            player_url = player_link.get('href', '')
                            
                            # Extract player ID from URL
                            player_id = ""
                            if player_url:
                                player_id_match = re.search(r'/players/([^/]+)/', player_url)
                                if player_id_match:
                                    player_id = player_id_match.group(1)
                            
                            player_data = {
                                'name': player_name,
                                'player_id': player_id,
                                'number': player_number
                            }
                            
                            lineup_data[current_section][team_key].append(player_data)
            
            logger.debug(f"Parsed lineup: {len(lineup_data['start']['home_team'])} home starters, {len(lineup_data['start']['away_team'])} away starters")
            return lineup_data
            
        except Exception as e:
            logger.error(f"Error parsing lineup: {e}")
            return None
    
    def parse_match_summary(self, soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
        """
        Parse match summary (events) from the HTML.
        
        Events include goals, cards, substitutions, etc.
        """
        try:
            events = {}
            event_counter = 1
            
            # Find events_wrap div first
            events_wrap = soup.find('div', {'id': 'events_wrap'})
            if not events_wrap:
                logger.debug("No events_wrap div found")
                return None
            
            # Events are in div elements with class 'event'
            # Look for divs with class 'event'
            event_divs = events_wrap.find_all('div', class_='event')
            
            for div in event_divs:
                # Parse event using HTML structure instead of text parsing
                minute = None
                score = None
                player = None
                substitute_for = None
                assist_player = None
                
                # Extract minute and score from the first div
                first_div = div.find('div')
                if first_div:
                    minute_text = first_div.get_text(strip=True)
                    # Extract minute (handles 90+1, 87', etc.)
                    minute_match = re.search(r'(\d+(?:\+\d*)?[\']?)', minute_text)
                    if minute_match:
                        minute = minute_match.group(1)
                    
                    # Extract score from small span
                    score_span = first_div.find('span', style='color:#666')
                    if score_span:
                        score = score_span.get_text(strip=True)
                
                # Detect event type from event_icon class
                event_icon = div.find('div', class_='event_icon')
                event_type = 'Unknown'
                
                if event_icon:
                    icon_classes = event_icon.get('class', [])
                    if 'goal' in icon_classes:
                        event_type = 'Goal'
                    elif 'yellow_card' in icon_classes:
                        event_type = 'Yellow Card'
                    elif 'red_card' in icon_classes:
                        event_type = 'Red Card'
                    elif 'substitute_in' in icon_classes:
                        event_type = 'Substitute'
                    elif 'substitute_out' in icon_classes:
                        event_type = 'Substitute'
                
                # Extract player information based on event type
                if event_type == 'Substitute':
                    # For substitutions, look for the player links
                    player_parts= div.find_all('a')
                    if len(player_parts) >= 2:
                        # First link is the player coming in
                        player = player_parts[0].get_text(strip=True)
                        # Second link is the player going out
                        substitute_for = player_parts[1].get_text(strip=True)
                    elif len(player_parts) == 1:
                        player = player_parts[0].get_text(strip=True)
                
                elif event_type == 'Goal':
                    # For goals, look for player and assist
                    player_parts = div.find_all('a')
                    if player_parts:
                        player = player_parts[0].get_text(strip=True)
                        # Look for assist in small text - check all small elements
                        small_elements = div.find_all('small')
                        for small in small_elements:
                            assist_text = small.get_text(strip=True)
                            if 'Assist:' in assist_text:
                                # Extract assist player name
                                assist_match = re.search(r'Assist:\s*(.+)', assist_text)
                                if assist_match:
                                    assist_player = assist_match.group(1).strip()
                                break
                        
                else:
                    # For cards and other events, just get the first player link
                    player_link = div.find('a')
                    if player_link:
                        player = player_link.get_text(strip=True)
                
                # Skip if we don't have essential data
                if not minute or not event_type or event_type == 'Unknown':
                    continue 

                # Create event
                event_data = {
                    'minute': minute,
                    'score': score,
                    'event': event_type,
                    'player': player,
                    'assist_player': assist_player,
                    'substitute_for': substitute_for
                }
                
                events[f'event_{event_counter}'] = event_data
                event_counter += 1
            
            logger.debug(f"Parsed {len(events)} match events")
            return events
            
        except Exception as e:
            logger.error(f"Error parsing match summary: {e}")
            return None
    
    def parse_team_stats(self, soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
        """
        Parse team statistics from the HTML.
        
        Based on the team stats table structure showing possession, passing accuracy, etc.
        """
        try:
            team_stats = {
                'home_team': {},
                'away_team': {}
            }
            
            # Find team_stats div first
            team_stats_div = soup.find('div', {'id': 'team_stats'})
            if not team_stats_div:
                logger.debug("No team_stats div found")
                return None
            
            # Look for table within team_stats div
            stats_table = team_stats_div.find('table')
            if not stats_table:
                logger.debug("No team stats table found in team_stats div")
                return None
            
            # Process main stats table
            self.process_team_stats_table(stats_table, team_stats)
            
            # Also look for team_stats_extra div for additional stats
            team_stats_extra_div = soup.find('div', {'id': 'team_stats_extra'})
            if team_stats_extra_div:
                logger.debug("Found team_stats_extra div")
                self.process_team_stats_extra(team_stats_extra_div, team_stats)
            
            logger.debug(f"Parsed team stats: {len(team_stats['home_team'])} stats for each team")
            return team_stats
            
        except Exception as e:
            logger.error(f"Error parsing team stats: {e}")
            return None

    def parse_player_stats(self, soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
        """Parse player stats from the HTML."""

        player_stats = {}

        team_ids = []
        
        filter_divs = soup.find_all('div', class_='filter switcher')

        if filter_divs:
            for i in range(len(filter_divs)):
                data_controls_tag = filter_divs[i].get('data-controls')
                if data_controls_tag and data_controls_tag.startswith('#switcher_player_stats'):
                    team_ids.append(data_controls_tag.split('_')[3])

            # Process each team's player stats
            for i, team_id in enumerate(team_ids):
                team_key = 'home_team' if i == 0 else 'away_team'
                
                # Find the player stats div for this team
                player_stats_div = soup.find('div', {'id': f'switcher_player_stats_{team_id}'})
                if player_stats_div:
                    # Process summary stats
                    summary_stats_div = player_stats_div.find('div', {'id': f'div_stats_{team_id}_summary'})
                    if summary_stats_div:
                        if 'player_summary_stats' not in player_stats:
                            player_stats['player_summary_stats'] = {'home_team': [], 'away_team': []}
                        team_stats_data = self.parse_player_stats_table(summary_stats_div)
                        player_stats['player_summary_stats'][team_key] = team_stats_data

                    # Process passing stats
                    passing_stats_div = player_stats_div.find('div', {'id': f'div_stats_{team_id}_passing'})
                    if passing_stats_div:
                        if 'player_passing_stats' not in player_stats:
                            player_stats['player_passing_stats'] = {'home_team': [], 'away_team': []}
                        team_stats_data = self.parse_player_stats_table(passing_stats_div)
                        player_stats['player_passing_stats'][team_key] = team_stats_data

                    # Process pass types stats
                    pass_types_stats_div = player_stats_div.find('div', {'id': f'div_stats_{team_id}_passing_types'})
                    if pass_types_stats_div:
                        if 'player_pass_types_stats' not in player_stats:
                            player_stats['player_pass_types_stats'] = {'home_team': [], 'away_team': []}
                        team_stats_data = self.parse_player_stats_table(pass_types_stats_div)
                        player_stats['player_pass_types_stats'][team_key] = team_stats_data

                    # Process defensive stats
                    defense_stats_div = player_stats_div.find('div', {'id': f'div_stats_{team_id}_defense'})
                    if defense_stats_div:
                        if 'player_defense_stats' not in player_stats:
                            player_stats['player_defense_stats'] = {'home_team': [], 'away_team': []}
                        team_stats_data = self.parse_player_stats_table(defense_stats_div)
                        player_stats['player_defense_stats'][team_key] = team_stats_data

                    # Process possession stats
                    possession_stats_div = player_stats_div.find('div', {'id': f'div_stats_{team_id}_possession'})
                    if possession_stats_div:
                        if 'player_possession_stats' not in player_stats:
                            player_stats['player_possession_stats'] = {'home_team': [], 'away_team': []}
                        team_stats_data = self.parse_player_stats_table(possession_stats_div)
                        player_stats['player_possession_stats'][team_key] = team_stats_data

                    # Process miscellaneous stats
                    misc_stats_div = player_stats_div.find('div', {'id': f'div_stats_{team_id}_misc'})
                    if misc_stats_div:
                        if 'player_miscellaneous_stats' not in player_stats:
                            player_stats['player_miscellaneous_stats'] = {'home_team': [], 'away_team': []}
                        team_stats_data = self.parse_player_stats_table(misc_stats_div)
                        player_stats['player_miscellaneous_stats'][team_key] = team_stats_data

                    # Process goalkeeper stats
                    goalkeeper_stats_div = soup.find('div', {'id': f'div_keeper_stats_{team_id}'})
                    if goalkeeper_stats_div:
                        if 'player_goalkeeper_stats' not in player_stats:
                            player_stats['player_goalkeeper_stats'] = {'home_team': [], 'away_team': []}
                        team_stats_data = self.parse_goalkeeper_stats_table(goalkeeper_stats_div)
                        player_stats['player_goalkeeper_stats'][team_key] = team_stats_data

        else:
            summary_stat_divs = soup.find_all('div', class_='table_container tabbed current')
            if summary_stat_divs:
                if 'player_summary_stats' not in player_stats:
                    player_stats['player_summary_stats'] = {'home_team': [], 'away_team': []}
                for i in range(len(summary_stat_divs)):
                    team_key = 'home_team' if i == 0 else 'away_team'
                    team_stats_data = self.parse_player_stats_table(summary_stat_divs[i])
                    player_stats['player_summary_stats'][team_key] = team_stats_data

            goalkeeper_stat_divs = soup.find_all('div', class_='table_container')
            if goalkeeper_stat_divs:
                if 'player_goalkeeper_stats' not in player_stats:
                    player_stats['player_goalkeeper_stats'] = {'home_team': [], 'away_team': []}
                for i in range(len(goalkeeper_stat_divs)):
                    if i == 0 or i == 2:
                        continue
                    else:
                        team_key = 'home_team' if i == 1 else 'away_team'
                        team_stats_data = self.parse_goalkeeper_stats_table(goalkeeper_stat_divs[i])
                        player_stats['player_goalkeeper_stats'][team_key] = team_stats_data

        return player_stats

    def parse_player_stats_table(self, stats_div: BeautifulSoup) -> List[Dict[str, Any]]:
        """Parse player stats from a stats table div."""
        player_stats = []
        
        # Find the stats table
        stats_table = stats_div.find('table', class_='stats_table')
        if not stats_table:
            return player_stats
        
        # Get table headers to understand the structure
        header_row = stats_table.find('thead')
        if not header_row:
            return player_stats
            
        # Extract column headers from the second row (skip the over_header row)
        headers = []
        header_rows = header_row.find_all('tr')
        
        # Use the second row which contains the actual column headers
        if len(header_rows) >= 2:
            header_cells = header_rows[1].find_all('th')
            for cell in header_cells:
                data_stat = cell.get('data-stat')
                if data_stat:
                    headers.append(data_stat)
        
        # Process each player row
        tbody = stats_table.find('tbody')
        if tbody:
            player_rows = tbody.find_all('tr')
            
            for row in player_rows:
                player_data = {}
                cells = row.find_all(['td', 'th'])
                
                # Extract player name and basic info
                player_cell = row.find('th', {'data-stat': 'player'})
                if player_cell:
                    player_link = player_cell.find('a')
                    if player_link:
                        player_name = player_link.get_text(strip=True)
                        player_url = player_link.get('href', '')
                        player_data['player_name'] = player_name
                        
                        # Extract player ID from URL if available
                        if '/players/' in player_url:
                            player_id = player_url.split('/players/')[1].split('/')[0]
                            player_data['player_id'] = player_id
                
                # Extract other stats
                for i, cell in enumerate(cells):
                    if i < len(headers):
                        stat_name = headers[i]
                        stat_value = cell.get_text(strip=True)
                        
                        # Skip empty values
                        if not stat_value or stat_value == '':
                            continue
                            
                        # Skip the duplicate 'player' field since we already have 'player_name'
                        if stat_name == 'player':
                            continue

                        if stat_name == 'nationality':
                            country_tag = cell.find('a')
                            country_span = country_tag.find('span')
                            country = country_span.find(text=True, recursive=False).strip() if country_span else None
                            if country in COUNTRY_MAPPING:
                                stat_value = COUNTRY_MAPPING[country]
                            else:
                                stat_value = country
                                # Raise exception to stop the entire pipeline
                                raise PipelineStopError(f"Country: {country} not found in COUNTRY_MAPPING")
                        
                        # Use mapped name if available, otherwise use original
                        field_name = STATS_MAPPING.get(stat_name, stat_name)
                        
                        # Parse numeric values
                        if stat_name in ['minutes', 'goals', 'assists', 'shots', 'shots_on_target', 
                                       'cards_yellow', 'cards_red', 'touches', 'tackles', 'interceptions', 
                                       'blocks', 'xg', 'npxg', 'xg_assist', 'sca', 'gca', 'passes_completed', 
                                       'passes', 'passes_pct', 'progressive_passes', 'carries', 
                                       'progressive_carries', 'take_ons', 'take_ons_won', 'shirtnumber']:
                            try:
                                # Handle percentage values
                                if stat_name == 'passes_pct' and '%' in stat_value:
                                    stat_value = float(stat_value.replace('%', ''))
                                else:
                                    stat_value = float(stat_value) if '.' in stat_value else int(stat_value)
                            except (ValueError, TypeError):
                                pass  # Keep as string if conversion fails
                        
                        player_data[field_name] = stat_value
                
                # Only add player if we have a name
                if 'player_name' in player_data:
                    player_stats.append(player_data)
        
        return player_stats

    def parse_goalkeeper_stats_table(self, stats_div: BeautifulSoup) -> List[Dict[str, Any]]:
        """Parse goalkeeper stats from a stats table div."""
        goalkeeper_stats = []
        
        # Find the stats table
        stats_table = stats_div.find('table', class_='stats_table')
        if not stats_table:
            return goalkeeper_stats
        
        # Get table headers to understand the structure
        header_row = stats_table.find('thead')
        if not header_row:
            return goalkeeper_stats
            
        # Extract column headers from the second row (skip the over_header row)
        headers = []
        header_rows = header_row.find_all('tr')
        
        # Use the second row which contains the actual column headers
        if len(header_rows) >= 2:
            header_cells = header_rows[1].find_all('th')
            for cell in header_cells:
                data_stat = cell.get('data-stat')
                if data_stat:
                    headers.append(data_stat)
        
        # Process each goalkeeper row
        tbody = stats_table.find('tbody')
        if tbody:
            goalkeeper_rows = tbody.find_all('tr')
            
            for row in goalkeeper_rows:
                goalkeeper_data = {}
                cells = row.find_all(['td', 'th'])
                
                # Extract goalkeeper name and basic info
                player_cell = row.find('th', {'data-stat': 'player'})
                if player_cell:
                    player_link = player_cell.find('a')
                    if player_link:
                        goalkeeper_name = player_link.get_text(strip=True)
                        player_url = player_link.get('href', '')
                        goalkeeper_data['player_name'] = goalkeeper_name
                        
                        # Extract player ID from URL if available
                        if '/players/' in player_url:
                            player_id = player_url.split('/players/')[1].split('/')[0]
                            goalkeeper_data['player_id'] = player_id
                
                # Extract other stats
                for i, cell in enumerate(cells):
                    if i < len(headers):
                        stat_name = headers[i]
                        stat_value = cell.get_text(strip=True)
                        
                        # Skip empty values
                        if not stat_value or stat_value == '':
                            continue
                            
                        # Skip the duplicate 'player' field since we already have 'player_name'
                        if stat_name == 'player':
                            continue

                        # Handle nationality mapping
                        if stat_name == 'nationality':
                            country_tag = cell.find('a')
                            if country_tag:
                                country_span = country_tag.find('span')
                                if country_span:
                                    # Extract the country code from the span text
                                    span_text = country_span.get_text(strip=True)
                                    # Look for 3-letter country code pattern
                                    country_match = re.search(r'([A-Z]{3})', span_text)
                                    if country_match:
                                        country = country_match.group(1)
                                        if country in COUNTRY_MAPPING:
                                            stat_value = COUNTRY_MAPPING[country]
                                        else:
                                            stat_value = country
                                            # Raise exception to stop the entire pipeline
                                            raise PipelineStopError(f"Country: {country} not found in COUNTRY_MAPPING")
                                    else:
                                        stat_value = span_text
                                else:
                                    stat_value = country_tag.get_text(strip=True)
                            else:
                                stat_value = cell.get_text(strip=True)
                        
                        # Map goalkeeper-specific stats
                        field_name = STATS_MAPPING.get(stat_name, stat_name)
                        
                        # Parse numeric values for goalkeeper stats
                        if stat_name in ['minutes', 'gk_shots_on_target_against', 'gk_goals_against', 
                                       'gk_saves', 'gk_save_pct', 'gk_psxg', 'gk_passes_completed_launched',
                                       'gk_passes_launched', 'gk_passes_pct_launched', 'gk_passes',
                                       'gk_passes_throws', 'gk_pct_passes_launched', 'gk_passes_length_avg',
                                       'gk_goal_kicks', 'gk_pct_goal_kicks_launched', 'gk_goal_kick_length_avg',
                                       'gk_crosses', 'gk_crosses_stopped', 'gk_crosses_stopped_pct',
                                       'gk_def_actions_outside_pen_area', 'gk_avg_distance_def_actions']:
                            try:
                                # Handle percentage values
                                if '%' in stat_value:
                                    stat_value = float(stat_value.replace('%', ''))
                                else:
                                    stat_value = float(stat_value) if '.' in stat_value else int(stat_value)
                            except (ValueError, TypeError):
                                pass  # Keep as string if conversion fails
                        
                        goalkeeper_data[field_name] = stat_value
                
                # Only add if we have at least a player name
                if goalkeeper_data.get('player_name'):
                    goalkeeper_stats.append(goalkeeper_data)
        
        return goalkeeper_stats

    def process_team_stats_table(self, stats_table: BeautifulSoup, team_stats: Dict[str, Any]) -> None:
        """Process a stats table and add results to team_stats dictionary."""
        rows = stats_table.find_all('tr')
        current_stat_name = None
        
        for i, row in enumerate(rows):
            cells = row.find_all(['td', 'th'])
            
            # Handle stat name rows (th with colspan=2)
            if cells[0].name == 'th' and cells[0].get('colspan') == '2':
                current_stat_name = cells[0].get_text(strip=True).lower()
                continue
            
            # Extract content from cells
            cell_texts = [cell.get_text(strip=True) for cell in cells]
            
            # Skip team name row (first row)
            if i == 0:
                continue
            
            # Check if this is a stat name row (has only one cell with stat name)
            if len(cell_texts) == 1 and cell_texts[0] and len(cell_texts[0]) < 30:
                current_stat_name = cell_texts[0].lower()
                continue
            
            # Check if this is a values row (has two cells with values)
            if len(cell_texts) == 2 and current_stat_name and cell_texts[0] and cell_texts[1]:
                # Map stat names to expected format first
                mapped_name = self.map_stat_name(current_stat_name)
                if mapped_name:
                    home_value = self.parse_stat_value(cell_texts[0], mapped_name)
                    away_value = self.parse_stat_value(cell_texts[1], mapped_name)
                    
                    team_stats['home_team'][mapped_name] = home_value
                    team_stats['away_team'][mapped_name] = away_value
                
                current_stat_name = None  # Reset for next stat
    
    def process_team_stats_extra(self, team_stats_extra_div: BeautifulSoup, team_stats: Dict[str, Any]) -> None:
        """Process the team_stats_extra div which contains additional statistics in div format."""
        # Find all stat groups (each group is in its own div)
        stat_groups = team_stats_extra_div.find_all('div')
        
        # Process each stat group
        for group in stat_groups:
            # Skip empty groups
            if not group.get_text(strip=True):
                continue
            
            # Find all divs within this group
            stat_divs = group.find_all('div')
            if len(stat_divs) < 3:  # Need at least home value, stat name, away value
                continue
            
            # Extract stat data
            # The structure is: [home_value, stat_name, away_value, ...]
            stat_data = [div.get_text(strip=True) for div in stat_divs]
            
            # Skip empty stat names
            stat_data = [data for data in stat_data if data.strip()]
            
            # Process stats in groups of 3: home_value, stat_name, away_value
            # Skip the first 2 elements if they contain team names
            start_index = 0
            if len(stat_data) >= 2:# and ('Manchester Utd' in stat_data[:2] or 'Fulham' in stat_data[:2]):
                start_index = 2
            
            for i in range(start_index, len(stat_data) - 2, 3):
                if i + 2 < len(stat_data):
                    home_value = stat_data[i]
                    stat_name = stat_data[i + 1]
                    away_value = stat_data[i + 2]
                    
                    # Skip if any of these are empty or look like team names
                    if not home_value or not stat_name or not away_value:
                        continue
                    if len(stat_name) > 20:  # Skip if it looks like a team name
                        continue
                    
                    # Skip if stat_name is a number (wrong parsing)
                    if stat_name.isdigit():
                        continue
                    
                    # Map stat name first
                    mapped_name = self.map_stat_name(stat_name.lower())
                    if mapped_name:
                        # Parse values with stat name context
                        home_parsed = self.parse_stat_value(home_value, mapped_name)
                        away_parsed = self.parse_stat_value(away_value, mapped_name)
                        
                        team_stats['home_team'][mapped_name] = home_parsed
                        team_stats['away_team'][mapped_name] = away_parsed
    
    def parse_stat_value(self, value_str: str, stat_name: str = None) -> Any:
        """Parse a statistic value, handling percentages and numbers."""
        try:
            # For specific percentage fields, extract only the percentage part
            if stat_name and stat_name.endswith('%'):
                # Extract percentage from strings like "290 of 381 —76%" or "76%— 290 of 381"
                if '—' in value_str:
                    # Split by em dash and look for percentage
                    parts = value_str.split('—')
                    for part in parts:
                        part = part.strip()
                        if '%' in part:
                            # Extract just the number before %
                            percent_match = re.search(r'(\d+(?:\.\d+)?)%', part)
                            if percent_match:
                                return float(percent_match.group(1))
                elif '%' in value_str:
                    # Direct percentage extraction
                    percent_match = re.search(r'(\d+(?:\.\d+)?)%', value_str)
                    if percent_match:
                        return float(percent_match.group(1))
                return None
            
            # For other fields, use original logic
            if '%' in value_str:
                return float(value_str.replace('%', ''))
            
            # Try to convert to int first, then float
            try:
                return int(value_str)
            except ValueError:
                return float(value_str)
        except (ValueError, TypeError):
            return value_str
    
    def map_stat_name(self, stat_name: str) -> Optional[str]:
        """Map HTML stat names to expected database field names."""
        mapping = {
            'possession': 'possession%',
            'passing accuracy': 'passing_accuracy%',
            'shots on target': 'shots_on_target%',
            'saves': 'saves%',
            'cards': 'cards',
            'fouls': 'fouls',
            'corners': 'corners',
            'crosses': 'crosses',
            'touches': 'touches',
            'tackles': 'tackles',
            'interceptions': 'interceptions',
            'aerials won': 'aerials_won',
            'clearances': 'clearances',
            'offsides': 'offsides',
            'goal kicks': 'goal_kicks',
            'throw ins': 'throw_ins',
            'long balls': 'long_balls'
        }
        
        return mapping.get(stat_name.lower())

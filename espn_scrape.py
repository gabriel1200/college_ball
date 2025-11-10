import requests
import json
from urllib.parse import urlencode
import os
import time # Import time for potential delays/backoff
import pandas as pd # Import pandas for DataFrame handling
import re # Import regex for parsing UID

# --- Configuration ---
GAME_SUMMARY_API_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary?event={game_id}"
ESPN_BASE_URL = "https://www.espn.com"
API_DELAY = 1
# Define base path using the structure from examples
# We'll derive the year from the input date
BASE_DATA_PATH_TEMPLATE = "../data/raw/usa_ncaam/{year}"

# --- Helper Functions ---

def fetch_json(url, params=None, headers=None, retries=3, backoff_factor=2):
    """
    General function to fetch JSON data from a URL with retry logic.
    (Handles retries, errors, and JSON decoding)
    """
    if headers is None:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    delay = API_DELAY

    for attempt in range(retries + 1):
        try:
            response = requests.get(url, params=params, headers=headers, timeout=15)
            response.raise_for_status()

            if 'application/json' in response.headers.get('Content-Type', ''):
                return response.json()
            else:
                print(f"Warning: Non-JSON response received from {url}. Content-Type: {response.headers.get('Content-Type')}")
                # print("Response text (first 200 chars):", response.text[:200] + "...")
                return None

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred on attempt {attempt + 1}/{retries + 1}: {http_err} for URL: {url}")
            if response.status_code == 404:
                print(f"Game data not found (404) for {url}. Skipping.")
                return None
            # Retry on server errors (5xx) or potential rate limiting (429) only
            if response.status_code >= 500 or response.status_code == 429:
                 if attempt < retries:
                    print(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                    delay *= backoff_factor
                 else:
                    print("Max retries reached.")
            else:
                # Don't retry for other client errors (like 403 Forbidden)
                break
        except requests.exceptions.ConnectionError as conn_err:
            print(f"Connection error occurred on attempt {attempt + 1}/{retries + 1}: {conn_err} for URL: {url}")
            if attempt < retries:
                 print(f"Retrying in {delay} seconds...")
                 time.sleep(delay)
                 delay *= backoff_factor
            else:
                print("Max retries reached.")
        except requests.exceptions.Timeout as timeout_err:
            print(f"Timeout error occurred on attempt {attempt + 1}/{retries + 1}: {timeout_err} for URL: {url}")
            if attempt < retries:
                 print(f"Retrying in {delay} seconds...")
                 time.sleep(delay)
                 delay *= backoff_factor
            else:
                print("Max retries reached.")
        except requests.exceptions.RequestException as req_err:
            print(f"An unexpected error occurred on attempt {attempt + 1}/{retries + 1}: {req_err} for URL: {url}")
            break
        except json.JSONDecodeError:
            print(f"Error: Failed to decode JSON from response on attempt {attempt + 1}/{retries + 1} for URL: {url}")
            # print("Response text (first 200 chars):", response.text[:200] + "...")
            if attempt < retries:
                 print(f"Retrying in {delay} seconds...")
                 time.sleep(delay)
                 delay *= backoff_factor
            else:
                print("Max retries reached.")

    return None

def get_cbb_schedule_by_date(date_str):
    """Fetches the schedule JSON for a specific date."""
    base_url = f"https://www.espn.com/mens-college-basketball/schedule/_/date/{date_str}"
    params = {
        '_xhr': 'pageContent', 'refetchShell': 'false', 'offset': '-05:00',
        'original': f'date={date_str}', 'date': date_str
    }
    print(f"Fetching schedule URL: {base_url} with params: {params}")
    return fetch_json(base_url, params=params)

def extract_schedule_data_for_master(schedule_data, date_str):
    """Extracts game and team info from the schedule JSON for master files."""
    games_list = []
    teams_dict = {} # Use dict to store unique teams by ID

    if not schedule_data:
        print("  [Debug] extract_schedule_data_for_master received None for schedule_data.")
        return games_list, teams_dict

    schedule_events = schedule_data.get('events', {}).get(date_str, [])
    print(f"  [Debug] Found {len(schedule_events)} events in schedule data for {date_str}.")
    if not schedule_events:
         print(f"  [Debug] No schedule events list found for date {date_str} under 'events' key.")
         return games_list, teams_dict

    for i, game in enumerate(schedule_events):
        game_id = game.get('id')
        if not game_id:
            print(f"    [Debug] Skipping game {i+1} due to missing game_id.")
            continue

        game_entry = {
            'game_id': game_id,
            'date_time_utc': game.get('date'),
            'status_detail': game.get('status', {}).get('detail'),
            'game_link': ESPN_BASE_URL + game.get('link', '') if game.get('link') else None,
            'venue': game.get('venue', {}).get('fullName'),
            'venue_city': game.get('venue', {}).get('address', {}).get('city'),
            'venue_state': game.get('venue', {}).get('address', {}).get('state'),
            'season_year': game.get('season', {}).get('year'),
            'season_type': game.get('season', {}).get('slug'),
        }

        # Extract Competitor Info
        competitors = game.get('competitors', [])
        if len(competitors) >= 2:
            home_comp = next((c for c in competitors if c.get('isHome')), competitors[0])
            away_comp = next((c for c in competitors if not c.get('isHome')), competitors[1])

            # --- CORRECTED EXTRACTION ---
            # Extract info directly from competitor objects
            # Parse team_id from uid
            home_team_id_match = re.search(r't:(\d+)', home_comp.get('uid', ''))
            home_team_id = home_team_id_match.group(1) if home_team_id_match else home_comp.get('id') # Fallback to competitor id if uid parse fails

            away_team_id_match = re.search(r't:(\d+)', away_comp.get('uid', ''))
            away_team_id = away_team_id_match.group(1) if away_team_id_match else away_comp.get('id')

            game_entry['home_team_id'] = home_team_id
            game_entry['home_team_name'] = home_comp.get('displayName')
            game_entry['home_team_abbrev'] = home_comp.get('abbreviation', home_comp.get('abbrev')) # Use 'abbrev' as fallback
            game_entry['home_score'] = home_comp.get('score')
            game_entry['away_team_id'] = away_team_id
            game_entry['away_team_name'] = away_comp.get('displayName')
            game_entry['away_team_abbrev'] = away_comp.get('abbreviation', away_comp.get('abbrev'))
            game_entry['away_score'] = away_comp.get('score')

            # Add teams to the unique teams dictionary
            for comp_info, team_id in [(home_comp, home_team_id), (away_comp, away_team_id)]:
                if team_id and team_id not in teams_dict:
                    print(f"        [Debug] Adding team ID {team_id} ({comp_info.get('displayName')}) to teams_dict.")
                    teams_dict[team_id] = {
                        'team_id': team_id,
                        'uid': comp_info.get('uid'),
                        'location': comp_info.get('location'),
                        'name': comp_info.get('name'), # Mascot / Nickname
                        'abbreviation': comp_info.get('abbreviation', comp_info.get('abbrev')),
                        'displayName': comp_info.get('displayName'),
                        'shortDisplayName': comp_info.get('shortDisplayName'),
                        'logo': comp_info.get('logo')
                        # Removed color keys as they are not in schedule data
                    }
                elif not team_id:
                     print(f"        [Debug] team_id is missing or None/empty for competitor: {comp_info.get('displayName')}")
                elif team_id in teams_dict:
                    pass # Don't need to print every time it exists
                    # print(f"        [Debug] Team ID {team_id} already exists in teams_dict.")
        else:
            print(f"    [Debug] Skipping game {game_id}: Did not find at least 2 competitors.")


        games_list.append(game_entry)

    print(f"  [Debug] Returning {len(games_list)} games and {len(teams_dict)} unique teams from schedule extraction.")
    return games_list, teams_dict


def get_game_details(game_id):
    """Fetches detailed game data from the ESPN summary API."""
    url = GAME_SUMMARY_API_URL.format(game_id=game_id)
    print(f"Fetching game details for ID {game_id} from {url}...")
    return fetch_json(url)

def extract_and_save_detailed_game_data(game_data, game_id, dir_paths, all_players_dict):
    """
    Extracts data from the game summary, saves individual CSVs,
    and updates the master player dictionary.
    """
    if not game_data:
        print(f"  -> No game data provided for extraction (ID: {game_id}).")
        return

    boxscore = game_data.get('boxscore')
    plays_data = game_data.get('plays')

    # --- Extract and Save Play-by-Play ---
    if plays_data:
        try:
            pbp_df = pd.DataFrame(plays_data)
            # Safely extract nested data using .apply() and lambda with .get()
            pbp_df['type_text'] = pbp_df['type'].apply(lambda x: x.get('text') if isinstance(x, dict) else None)
            pbp_df['period_display'] = pbp_df['period'].apply(lambda x: x.get('displayValue') if isinstance(x, dict) else None)
            pbp_df['clock_display'] = pbp_df['clock'].apply(lambda x: x.get('displayValue') if isinstance(x, dict) else None)
            pbp_df['team_id'] = pbp_df['team'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)

            # Select and rename columns, handling potential missing ones gracefully
            desired_pbp_cols = {
                'id': 'play_id', 'sequenceNumber': 'sequence_number', 'type_text': 'type',
                'text': 'description', 'awayScore': 'away_score', 'homeScore': 'home_score',
                'period_display': 'period', 'clock_display': 'clock',
                'scoringPlay': 'scoring_play', 'team_id': 'team_id', 'wallclock': 'timestamp_utc'
            }
            pbp_df_final = pd.DataFrame()
            for original, new in desired_pbp_cols.items():
                if original in pbp_df:
                    pbp_df_final[new] = pbp_df[original]
                else:
                    pbp_df_final[new] = None # Add column with None if missing

            pbp_df_final.to_csv(os.path.join(dir_paths["play_by_play"], f"{game_id}.csv"), index=False, encoding='utf-8-sig')
            print(f"  -> Saved Play-by-Play CSV for {game_id}")
        except Exception as e:
            print(f"  -> Error processing or saving play-by-play for {game_id}: {e}")
            # Consider logging the error in more detail if needed
            # print(pbp_df.head()) # Print head to see structure on error
    else:
        print(f"  -> No play-by-play data found for game {game_id}.")

    if not boxscore:
        print(f"  -> No boxscore data found for game {game_id}. Skipping stats.")
        return

    # --- Extract and Save Team Stats ---
    if boxscore.get('teams'):
        try:
            team_stats_list = []
            for team_data in boxscore['teams']:
                team_id = team_data.get('team', {}).get('id')
                home_away = team_data.get('homeAway') # Get home/away status
                if team_id:
                    team_entry = {'game_id': game_id, 'team_id': team_id, 'home_away': home_away}
                    for stat in team_data.get('statistics', []):
                        # Use 'name' or 'abbreviation' as key, 'displayValue' as value
                        key_name = stat.get('name', stat.get('abbreviation')) # Prefer name, fallback to abbreviation
                        if key_name: # Ensure we have a valid key
                           team_entry[key_name] = stat.get('displayValue')
                    team_stats_list.append(team_entry)

            if team_stats_list:
                team_stats_df = pd.DataFrame(team_stats_list)
                team_stats_df.to_csv(os.path.join(dir_paths["team_stats"], f"{game_id}.csv"), index=False, encoding='utf-8-sig')
                print(f"  -> Saved Team Stats CSV for {game_id}")
        except Exception as e:
            print(f"  -> Error processing or saving team stats for {game_id}: {e}")

    # --- Extract and Save Player Stats & Update Master Player List ---
    if boxscore.get('players'):
        try:
            player_stats_list = []
            for team_player_data in boxscore['players']:
                team_id = team_player_data.get('team', {}).get('id')
                stats_def = team_player_data.get('statistics', [{}])[0]
                stat_keys = stats_def.get('keys', []) # These are the internal ESPN keys

                if team_id and stat_keys:
                    stat_labels = stats_def.get('labels', stat_keys) # Use labels for column names if available

                    for player_entry in stats_def.get('athletes', []):
                        athlete_info = player_entry.get('athlete', {})
                        player_id = athlete_info.get('id')
                        if not player_id: continue

                        # Add/Update player in master dictionary
                        if player_id not in all_players_dict:
                            all_players_dict[player_id] = {
                                'player_id': player_id,
                                'uid': athlete_info.get('uid'),
                                'guid': athlete_info.get('guid'),
                                'displayName': athlete_info.get('displayName'),
                                'shortName': athlete_info.get('shortName'),
                                'position': athlete_info.get('position', {}).get('abbreviation'),
                                'jersey': athlete_info.get('jersey'),
                                'headshot': athlete_info.get('headshot', {}).get('href'),
                                'first_seen_team_id': team_id # Add first known team association
                            }

                        # Prepare player stats row
                        player_stats_row = {'game_id': game_id, 'team_id': team_id, 'player_id': player_id}
                        # Add some basic info directly for easier lookup in the CSV
                        player_stats_row['displayName'] = athlete_info.get('displayName')
                        player_stats_row['starter'] = player_entry.get('starter', False)
                        player_stats_row['didNotPlay'] = player_entry.get('didNotPlay', False)

                        stats_values = player_entry.get('stats', [])
                        # Map labels (or keys as fallback) to values
                        player_stats_row.update({stat_labels[i]: stats_values[i] for i in range(min(len(stat_labels), len(stats_values)))})
                        player_stats_list.append(player_stats_row)

            if player_stats_list:
                player_stats_df = pd.DataFrame(player_stats_list)
                player_stats_df.to_csv(os.path.join(dir_paths["player_stats"], f"{game_id}.csv"), index=False, encoding='utf-8-sig')
                print(f"  -> Saved Player Stats CSV for {game_id}")
        except Exception as e:
            print(f"  -> Error processing or saving player stats for {game_id}: {e}")


def save_master_csv(data_list_or_dict, filename):
    """Saves aggregated data (list of dicts or dict values) to a CSV file."""
    if not data_list_or_dict:
        print(f"No data to save for {filename}.")
        return
    try:
        if isinstance(data_list_or_dict, dict):
            # If it's a dictionary, assume the values are the records
            df = pd.DataFrame(list(data_list_or_dict.values()))
        else: # Assumes list of dicts
            df = pd.DataFrame(data_list_or_dict)

        if df.empty: # Double check if DataFrame is empty after conversion
             print(f"DataFrame is empty, skipping save for {filename}.")
             return

        os.makedirs(os.path.dirname(filename), exist_ok=True)
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"Successfully saved master file: {filename} with {len(df)} rows.")
    except Exception as e:
        print(f"Error saving master file {filename}: {e}")


# --- Main Execution ---
if __name__ == "__main__":
    default_date = "20241231" # Date from the sample game data
    user_date = input(f"Enter date (YYYYMMDD) [default: {default_date}]: ") or default_date

    # Derive year for directory structure (use ENDING year of season)
    year = user_date[:4]
    try:
        month = int(user_date[4:6])
        if month >= 10: # Season starts around Oct/Nov
             year = str(int(year) + 1)
    except ValueError:
        print("Invalid date format used for year calculation. Using start year.")

    base_path = BASE_DATA_PATH_TEMPLATE.format(year=year)

    dir_paths = {
        "play_by_play": os.path.join(base_path, "play_by_play"),
        "team_stats": os.path.join(base_path, "team_stats"),
        "player_stats": os.path.join(base_path, "player_stats"),
        "players": os.path.join(base_path, "players"),
        "teams": os.path.join(base_path, "teams"),
        "games": os.path.join(base_path, "games")
    }

    # Create directories
    for path in dir_paths.values():
        os.makedirs(path, exist_ok=True)
    print(f"Data will be saved in: {os.path.abspath(base_path)}")
    print("-" * 50)


    if len(user_date) == 8 and user_date.isdigit():
        print(f"--- Step 1: Fetching schedule for {user_date} ---")
        raw_schedule_data = get_cbb_schedule_by_date(user_date)

        if raw_schedule_data:
            print("[Debug] Schedule data fetched successfully.")
            daily_games_list, daily_teams_dict = extract_schedule_data_for_master(raw_schedule_data, user_date)
            game_ids = [game['game_id'] for game in daily_games_list]
            print(f"[Debug] Extracted {len(daily_games_list)} games and {len(daily_teams_dict)} unique teams from schedule.") # This should now show > 0 teams

            if game_ids:
                print(f"\n--- Step 2: Fetching and extracting details for {len(game_ids)} games ---")

                master_players_dict = {}

                for game_id in game_ids:
                    print(f"Processing game ID: {game_id}...")
                    # print(f"Waiting {API_DELAY} second(s) before next request...") # Keep delay reasonable
                    time.sleep(API_DELAY)

                    game_details_json = get_game_details(game_id)

                    if game_details_json:
                        # print(f"Extracting data for game {game_id}...") # Redundant with function print
                        extract_and_save_detailed_game_data(game_details_json, game_id, dir_paths, master_players_dict)
                    else:
                        print(f"Failed to fetch details for game {game_id}. Skipping.")

                print("\n--- Step 3: Saving Master Files ---")
                save_master_csv(daily_games_list, os.path.join(dir_paths["games"], "games.csv"))
                # Add Debug print before saving teams
                print(f"[Debug] Attempting to save teams_dict (size: {len(daily_teams_dict)}) to {os.path.join(dir_paths['teams'], 'teams.csv')}")
                save_master_csv(daily_teams_dict, os.path.join(dir_paths["teams"], "teams.csv"))
                save_master_csv(master_players_dict, os.path.join(dir_paths["players"], "players.csv"))

                print("\n--- Processing complete ---")

            else:
                print("No game IDs found in the schedule data.")
        else:
            print("[Debug] Failed to fetch schedule data from ESPN. Cannot proceed.") # More explicit message
    else:
        print("Invalid date format. Please enter in YYYYMMDD format.")



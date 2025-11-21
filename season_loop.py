import requests
import json
from urllib.parse import urlencode
import os
import time # Import time for potential delays/backoff
import pandas as pd # Import pandas for DataFrame handling
import re # Import regex for parsing UID
from datetime import date, timedelta # Added for date iteration

# --- Configuration ---
GAME_SUMMARY_API_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary?event={game_id}"
ESPN_BASE_URL = "https://www.espn.com"
API_DELAY = 1.5 # Slightly increased delay for season-long scrape
# Define base path using the structure from examples
# We'll determine the year within the main loop
BASE_DATA_PATH_TEMPLATE = "data/raw/usa_ncaam/{year}"

# --- Season Date Range ---
# Adjust these dates for the specific season you want to scrape
START_DATE = date(2025, 11, 4)
END_DATE = date.today() - timedelta(days=1)


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
            response = requests.get(url, params=params, headers=headers, timeout=20) # Increased timeout
            response.raise_for_status()

            if 'application/json' in response.headers.get('Content-Type', ''):
                # Handle potential empty responses which are valid JSON ('{}')
                content = response.text
                if not content or content.strip() == '{}':
                    print(f"Warning: Received empty JSON response from {url}")
                    return {} # Return empty dict instead of None
                return response.json()
            else:
                print(f"Warning: Non-JSON response received from {url}. Content-Type: {response.headers.get('Content-Type')}")
                return None # Keep returning None for non-JSON

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred on attempt {attempt + 1}/{retries + 1}: {http_err} for URL: {url}")
            if response.status_code == 404:
                print(f"Data not found (404) for {url}. Skipping.")
                return None
            if response.status_code >= 500 or response.status_code == 429:
                 if attempt < retries:
                    print(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                    delay *= backoff_factor
                 else:
                    print("Max retries reached.")
            else:
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
        '_xhr': 'pageContent', 'refetchShell': 'false', 'offset': '-05:00', # Assuming EST offset, might need adjustment
        'original': f'date={date_str}', 'date': date_str
    }
    # print(f"Fetching schedule URL: {base_url} with params: {params}") # Less verbose for season scrape
    return fetch_json(base_url, params=params)

def extract_schedule_data_for_master(schedule_data, date_str):
    """Extracts game and team info from the schedule JSON for master files."""
    games_list = []
    teams_dict = {} # Use dict to store unique teams by ID

    if not schedule_data or not isinstance(schedule_data, dict): # Check if it's a dict
        print(f"  [Debug] Invalid or empty schedule_data received for {date_str}.")
        return games_list, teams_dict

    # Check structure robustness
    events_for_date = schedule_data.get('events', {}).get(date_str)
    if not events_for_date or not isinstance(events_for_date, list):
         print(f"  [Debug] No schedule events list found for date {date_str} under 'events' -> '{date_str}' keys.")
         return games_list, teams_dict

    print(f"  [Debug] Found {len(events_for_date)} events in schedule data for {date_str}.")

    for i, game in enumerate(events_for_date):
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

        competitors = game.get('competitors', [])
        if len(competitors) >= 2:
            home_comp = next((c for c in competitors if c.get('isHome')), competitors[0])
            away_comp = next((c for c in competitors if not c.get('isHome')), competitors[1])

            home_team_id_match = re.search(r't:(\d+)', home_comp.get('uid', ''))
            home_team_id = home_team_id_match.group(1) if home_team_id_match else home_comp.get('id')

            away_team_id_match = re.search(r't:(\d+)', away_comp.get('uid', ''))
            away_team_id = away_team_id_match.group(1) if away_team_id_match else away_comp.get('id')

            game_entry['home_team_id'] = home_team_id
            game_entry['home_team_name'] = home_comp.get('displayName')
            game_entry['home_team_abbrev'] = home_comp.get('abbreviation', home_comp.get('abbrev'))
            game_entry['home_score'] = home_comp.get('score')
            game_entry['away_team_id'] = away_team_id
            game_entry['away_team_name'] = away_comp.get('displayName')
            game_entry['away_team_abbrev'] = away_comp.get('abbreviation', away_comp.get('abbrev'))
            game_entry['away_score'] = away_comp.get('score')

            for comp_info, team_id in [(home_comp, home_team_id), (away_comp, away_team_id)]:
                if team_id and team_id not in teams_dict:
                    # print(f"        [Debug] Adding team ID {team_id} ({comp_info.get('displayName')}) to teams_dict.") # Less verbose
                    teams_dict[team_id] = {
                        'team_id': team_id,
                        'uid': comp_info.get('uid'),
                        'location': comp_info.get('location'),
                        'name': comp_info.get('name'), # Mascot / Nickname
                        'abbreviation': comp_info.get('abbreviation', comp_info.get('abbrev')),
                        'displayName': comp_info.get('displayName'),
                        'shortDisplayName': comp_info.get('shortDisplayName'),
                        'logo': comp_info.get('logo')
                    }
                # elif not team_id:
                #      print(f"        [Debug] team_id is missing or None/empty for competitor: {comp_info.get('displayName')}")
        else:
            print(f"    [Debug] Skipping game {game_id}: Did not find at least 2 competitors.")

        games_list.append(game_entry)

    # print(f"  [Debug] Returning {len(games_list)} games and {len(teams_dict)} unique teams from schedule extraction.") # Less verbose
    return games_list, teams_dict


def get_game_details(game_id):
    """Fetches detailed game data from the ESPN summary API."""
    url = GAME_SUMMARY_API_URL.format(game_id=game_id)
    # print(f"Fetching game details for ID {game_id} from {url}...") # Less verbose
    return fetch_json(url)

def extract_and_save_detailed_game_data(game_data, game_id, dir_paths, all_players_dict):
    """
    Extracts data from the game summary, saves individual CSVs,
    and updates the master player dictionary.
    Returns True if data was successfully processed and saved, False otherwise.
    """
    if not game_data or not isinstance(game_data, dict): # Check if it's a dict
        print(f"  -> Invalid or empty game data provided for extraction (ID: {game_id}).")
        return False

    boxscore = game_data.get('boxscore')
    plays_data = game_data.get('plays')
    success = False # Track if any file was saved

    # --- Extract and Save Play-by-Play ---
    if plays_data and isinstance(plays_data, list): # Check if it's a list
        try:
            pbp_df = pd.DataFrame(plays_data)
            if not pbp_df.empty:
                # Safely extract nested data
                pbp_df['type_text'] = pbp_df['type'].apply(lambda x: x.get('text') if isinstance(x, dict) else None)
                pbp_df['period_display'] = pbp_df['period'].apply(lambda x: x.get('displayValue') if isinstance(x, dict) else None)
                pbp_df['clock_display'] = pbp_df['clock'].apply(lambda x: x.get('displayValue') if isinstance(x, dict) else None)
                pbp_df['team_id'] = pbp_df['team'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)

                desired_pbp_cols = {
                    'id': 'play_id', 'sequenceNumber': 'sequence_number', 'type_text': 'type',
                    'text': 'description', 'awayScore': 'away_score', 'homeScore': 'home_score',
                    'period_display': 'period', 'clock_display': 'clock',
                    'scoringPlay': 'scoring_play', 'team_id': 'team_id', 'wallclock': 'timestamp_utc'
                }
                pbp_df_final = pd.DataFrame()
                for original, new in desired_pbp_cols.items():
                    pbp_df_final[new] = pbp_df.get(original) # Use .get for safety

                pbp_df_final.to_csv(os.path.join(dir_paths["play_by_play"], f"{game_id}.csv"), index=False, encoding='utf-8-sig')
                print(f"  -> Saved Play-by-Play CSV for {game_id}")
                success = True
            else:
                 print(f"  -> Play-by-play data for {game_id} resulted in an empty DataFrame.")
        except Exception as e:
            print(f"  -> Error processing or saving play-by-play for {game_id}: {e}")
    else:
        print(f"  -> No valid play-by-play data found for game {game_id}.")

    if not boxscore or not isinstance(boxscore, dict): # Check if it's a dict
        print(f"  -> No valid boxscore data found for game {game_id}. Skipping stats.")
        return success # Return current success status

    # --- Extract and Save Team Stats ---
    team_stats_data = boxscore.get('teams')
    if team_stats_data and isinstance(team_stats_data, list): # Check if list
        try:
            team_stats_list = []
            for team_data in team_stats_data:
                 if not isinstance(team_data, dict): continue # Skip if not a dict
                 team_id = team_data.get('team', {}).get('id')
                 home_away = team_data.get('homeAway')
                 if team_id:
                    team_entry = {'game_id': game_id, 'team_id': team_id, 'home_away': home_away}
                    stats_list = team_data.get('statistics')
                    if isinstance(stats_list, list): # Check stats is a list
                        for stat in stats_list:
                            if not isinstance(stat, dict): continue # Skip if not a dict
                            key_name = stat.get('name', stat.get('abbreviation'))
                            if key_name:
                               team_entry[key_name] = stat.get('displayValue')
                    team_stats_list.append(team_entry)

            if team_stats_list:
                team_stats_df = pd.DataFrame(team_stats_list)
                team_stats_df.to_csv(os.path.join(dir_paths["team_stats"], f"{game_id}.csv"), index=False, encoding='utf-8-sig')
                print(f"  -> Saved Team Stats CSV for {game_id}")
                success = True
        except Exception as e:
            print(f"  -> Error processing or saving team stats for {game_id}: {e}")
    else:
         print(f"  -> No valid team stats data found in boxscore for {game_id}.")

    # --- Extract and Save Player Stats & Update Master Player List ---
    player_box_data = boxscore.get('players')
    if player_box_data and isinstance(player_box_data, list): # Check if list
        try:
            player_stats_list = []
            for team_player_data in player_box_data:
                if not isinstance(team_player_data, dict): continue # Skip if not a dict
                team_id = team_player_data.get('team', {}).get('id')
                stats_section = team_player_data.get('statistics')
                # Ensure stats_section is a list and has at least one element which is a dict
                if not isinstance(stats_section, list) or not stats_section or not isinstance(stats_section[0], dict):
                    continue
                stats_def = stats_section[0]

                stat_keys = stats_def.get('keys')
                if not team_id or not isinstance(stat_keys, list): # Check if keys is a list
                    continue

                stat_labels = stats_def.get('labels', stat_keys)
                athletes = stats_def.get('athletes')

                if isinstance(athletes, list): # Check athletes is a list
                    for player_entry in athletes:
                        if not isinstance(player_entry, dict): continue # Skip if not a dict
                        athlete_info = player_entry.get('athlete', {})
                        if not isinstance(athlete_info, dict): continue # Skip if athlete info is malformed
                        player_id = athlete_info.get('id')
                        if not player_id: continue

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
                                'first_seen_team_id': team_id
                            }

                        player_stats_row = {'game_id': game_id, 'team_id': team_id, 'player_id': player_id}
                        player_stats_row['displayName'] = athlete_info.get('displayName')
                        player_stats_row['starter'] = player_entry.get('starter', False)
                        player_stats_row['didNotPlay'] = player_entry.get('didNotPlay', False)

                        stats_values = player_entry.get('stats')
                        if isinstance(stats_values, list): # Check stats values is a list
                            player_stats_row.update({stat_labels[i]: stats_values[i] for i in range(min(len(stat_labels), len(stats_values)))})
                        player_stats_list.append(player_stats_row)

            if player_stats_list:
                player_stats_df = pd.DataFrame(player_stats_list)
                player_stats_df.to_csv(os.path.join(dir_paths["player_stats"], f"{game_id}.csv"), index=False, encoding='utf-8-sig')
                print(f"  -> Saved Player Stats CSV for {game_id}")
                success = True
        except Exception as e:
            print(f"  -> Error processing or saving player stats for {game_id}: {e}")
    else:
        print(f"  -> No valid player boxscore data found for {game_id}.")

    return success


def load_master_df(filename):
    """Loads a master CSV file if it exists, otherwise returns an empty DataFrame."""
    if os.path.exists(filename):
        try:
            print(f"Loading existing master file: {filename}")
            return pd.read_csv(filename)
        except pd.errors.EmptyDataError:
            print(f"Warning: Existing master file {filename} is empty. Starting fresh.")
            return pd.DataFrame()
        except Exception as e:
            print(f"Error loading master file {filename}: {e}. Starting fresh.")
            return pd.DataFrame()
    else:
        print(f"Master file not found: {filename}. Starting fresh.")
        return pd.DataFrame()

def save_master_df(df, filename, unique_key):
    """Saves the master DataFrame, ensuring the directory exists."""
    if df is None or df.empty:
        print(f"No data to save for {filename}.")
        return
    try:
        # Ensure only unique entries based on the key
        if unique_key and unique_key in df.columns:
            df.drop_duplicates(subset=[unique_key], keep='last', inplace=True)

        os.makedirs(os.path.dirname(filename), exist_ok=True)
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"Successfully saved master file: {filename} with {len(df)} rows.")
    except Exception as e:
        print(f"Error saving master file {filename}: {e}")

def get_processed_game_ids(dir_paths):
    """Gets a set of game IDs for which output files already exist."""
    processed_ids = set()
    # Check for existence of play_by_play file as indicator
    pbp_dir = dir_paths["team_stats"]
    if os.path.exists(pbp_dir):
        for filename in os.listdir(pbp_dir):
            if filename.endswith(".csv"):
                game_id = filename.split('.')[0]
                # Basic check to see if it looks like a game ID (numeric)
                if game_id.isdigit():
                    processed_ids.add(game_id)
    print(f"Found {len(processed_ids)} previously processed game IDs.")
    return processed_ids

# --- Main Execution ---
if __name__ == "__main__":

    # --- Determine Year and Paths ---
    current_year = date.today().year
    # Heuristic: If current month is before August, assume season ends this year, else next year
    season_end_year = current_year if date.today().month < 8 else current_year + 1
    # Use START_DATE's year + 1 if START_DATE is Oct-Dec, else use START_DATE's year
    # This might need refinement based on exact season definitions
    start_year_for_folder = START_DATE.year + 1 if START_DATE.month >= 10 else START_DATE.year

    base_path = BASE_DATA_PATH_TEMPLATE.format(year=start_year_for_folder) # Use calculated year

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

    # --- Load Existing Master Data ---
    master_games_df = load_master_df(os.path.join(dir_paths["games"], "games.csv"))
    master_teams_df = load_master_df(os.path.join(dir_paths["teams"], "teams.csv"))
    master_players_df = load_master_df(os.path.join(dir_paths["players"], "players.csv"))

    # Convert existing DFs to dicts for efficient lookup/update
    # Use dict comprehension for faster lookup
    all_teams_dict = {str(row['team_id']): row.to_dict() for index, row in master_teams_df.iterrows()} if not master_teams_df.empty else {}
    all_players_dict = {str(row['player_id']): row.to_dict() for index, row in master_players_df.iterrows()} if not master_players_df.empty else {}
    print(f"Loaded {len(master_games_df)} games, {len(all_teams_dict)} teams, {len(all_players_dict)} players from existing files.")

    # --- Get Already Processed Game IDs ---
    processed_game_ids = get_processed_game_ids(dir_paths)

    # --- Iterate Through Dates ---
    current_date = START_DATE
    newly_added_games = [] # Store games added in this run

    while current_date <= END_DATE:
        date_str = current_date.strftime("%Y%m%d")
        print(f"\n--- Processing Date: {date_str} ---")

        raw_schedule_data = get_cbb_schedule_by_date(date_str)

        if raw_schedule_data:
            daily_games_list, daily_teams_dict = extract_schedule_data_for_master(raw_schedule_data, date_str)

            # Update master teams dict
            new_teams_found_today = 0
            for team_id, team_data in daily_teams_dict.items():
                if str(team_id) not in all_teams_dict: # Ensure using string keys
                    all_teams_dict[str(team_id)] = team_data
                    new_teams_found_today += 1
            if new_teams_found_today > 0:
                print(f"  -> Added {new_teams_found_today} new teams to master list.")

            # Process games for the day
            games_processed_today = 0
            for game_info in daily_games_list:
                game_id = game_info.get('game_id')
                if not game_id:
                    continue

                # Add game to list for master games.csv (do this regardless of skip status)
                # Check if game_id already exists in master_games_df before appending
                if master_games_df.empty or str(game_id) not in master_games_df['game_id'].astype(str).values:
                     newly_added_games.append(game_info)
                     print(f"  -> Queued game {game_id} for addition to games.csv.")
                # else:
                #      print(f"  -> Game {game_id} already exists in master games.csv.") # Optional verbose log


                # Check if game details need processing
                if str(game_id) in processed_game_ids:
                    #print(f"  Skipping game ID: {game_id} (already processed).")
                    continue

                print(f"  Processing game ID: {game_id}...")
                time.sleep(API_DELAY) # Delay before fetching details
                game_details_json = get_game_details(game_id)

                if game_details_json:
                    if extract_and_save_detailed_game_data(game_details_json, game_id, dir_paths, all_players_dict):
                        games_processed_today += 1
                else:
                    print(f"  -> Failed to fetch details for game {game_id}.")
            print(f"--- Finished processing {games_processed_today} new games for {date_str} ---")

        else:
            print(f"No schedule data found or fetch failed for {date_str}.")

        # Move to the next day
        current_date += timedelta(days=1)
        # Optional: Add a longer delay between dates if needed
        # time.sleep(5)

    print("\n--- Step 3: Saving Updated Master Files ---")
    # Append newly added games to the master DataFrame
    if newly_added_games:
        new_games_df = pd.DataFrame(newly_added_games)
        master_games_df = pd.concat([master_games_df, new_games_df], ignore_index=True) if not master_games_df.empty else new_games_df


    save_master_df(master_games_df, os.path.join(dir_paths["games"], "games.csv"), unique_key='game_id')
    # Convert dicts back to DataFrames for saving
    save_master_df(pd.DataFrame(list(all_teams_dict.values())), os.path.join(dir_paths["teams"], "teams.csv"), unique_key='team_id')
    save_master_df(pd.DataFrame(list(all_players_dict.values())), os.path.join(dir_paths["players"], "players.csv"), unique_key='player_id')

    print("\n--- SCRAPING COMPLETE ---")


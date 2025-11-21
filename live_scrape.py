import requests
import json
from urllib.parse import urlencode
import os
import time
import pandas as pd
import re
from datetime import date, datetime
import sys
# --- Configuration ---
GAME_SUMMARY_API_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary?event={game_id}"
ESPN_BASE_URL = "https://www.espn.com"
API_DELAY = 1.0  # Delay between API calls

# Determine year for folder structure
current_year = date.today().year
# If before August, season ends this year; otherwise next year
season_year = current_year if date.today().month < 8 else current_year + 1
BASE_DATA_PATH = f"data/raw/usa_ncaam/{season_year}"

# --- Helper Functions ---

def fetch_json(url, params=None, headers=None, retries=3, backoff_factor=2):
    """
    General function to fetch JSON data from a URL with retry logic.
    """
    if headers is None:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    delay = API_DELAY

    for attempt in range(retries + 1):
        try:
            response = requests.get(url, params=params, headers=headers, timeout=20)
            response.raise_for_status()

            if 'application/json' in response.headers.get('Content-Type', ''):
                content = response.text
                if not content or content.strip() == '{}':
                    print(f"Warning: Received empty JSON response from {url}")
                    return {}
                return response.json()
            else:
                print(f"Warning: Non-JSON response received from {url}.")
                return None

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred on attempt {attempt + 1}/{retries + 1}: {http_err}")
            if response.status_code == 404:
                print(f"Data not found (404). Skipping.")
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
        except requests.exceptions.RequestException as req_err:
            print(f"Request error on attempt {attempt + 1}/{retries + 1}: {req_err}")
            if attempt < retries:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= backoff_factor
            else:
                print("Max retries reached.")
        except json.JSONDecodeError:
            print(f"Error: Failed to decode JSON on attempt {attempt + 1}/{retries + 1}")
            if attempt < retries:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= backoff_factor
            else:
                print("Max retries reached.")

    return None


def get_cbb_schedule_today():
    """Fetches today's schedule JSON."""
    today_str = date.today().strftime("%Y%m%d")
    base_url = f"https://www.espn.com/mens-college-basketball/schedule/_/date/{today_str}"
    print(base_url)
  
    params = {
        '_xhr': 'pageContent',
        'refetchShell': 'false',
        'offset': '-05:00',
        'original': f'date={today_str}',
        'date': today_str
    }
    print(f"Fetching today's schedule: {today_str}")
    return fetch_json(base_url, params=params), today_str


def extract_schedule_data(schedule_data, date_str):
    """Extracts game and team info from schedule JSON."""
    games_list = []
    teams_dict = {}

    if not schedule_data or not isinstance(schedule_data, dict):
        print(f"Invalid or empty schedule_data received.")
        return games_list, teams_dict

    events_for_date = schedule_data.get('events', {}).get(date_str)
    if not events_for_date or not isinstance(events_for_date, list):
        print(f"No schedule events found for today.")
        return games_list, teams_dict

    print(f"Found {len(events_for_date)} games scheduled for today.")

    for i, game in enumerate(events_for_date):
        game_id = game.get('id')
        if not game_id:
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
                    teams_dict[team_id] = {
                        'team_id': team_id,
                        'uid': comp_info.get('uid'),
                        'location': comp_info.get('location'),
                        'name': comp_info.get('name'),
                        'abbreviation': comp_info.get('abbreviation', comp_info.get('abbrev')),
                        'displayName': comp_info.get('displayName'),
                        'shortDisplayName': comp_info.get('shortDisplayName'),
                        'logo': comp_info.get('logo')
                    }

        games_list.append(game_entry)

    return games_list, teams_dict


def get_game_details(game_id):
    """Fetches detailed game data from the ESPN summary API."""
    url = GAME_SUMMARY_API_URL.format(game_id=game_id)
    return fetch_json(url)


def extract_and_save_detailed_game_data(game_data, game_id, dir_paths, all_players_dict):
    """
    Extracts data from game summary, saves individual CSVs,
    and updates the master player dictionary.
    """
    if not game_data or not isinstance(game_data, dict):
        print(f"  -> Invalid or empty game data for ID: {game_id}")
        return False

    boxscore = game_data.get('boxscore')
    plays_data = game_data.get('plays')
    success = False

    # --- Extract and Save Play-by-Play ---
    if plays_data and isinstance(plays_data, list):
        try:
            pbp_df = pd.DataFrame(plays_data)
            if not pbp_df.empty:
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
                    pbp_df_final[new] = pbp_df.get(original)

                pbp_df_final.to_csv(os.path.join(dir_paths["play_by_play"], f"{game_id}.csv"), index=False, encoding='utf-8-sig')
                print(f"  -> Saved Play-by-Play CSV for {game_id}")
                success = True
        except Exception as e:
            print(f"  -> Error processing play-by-play for {game_id}: {e}")

    if not boxscore or not isinstance(boxscore, dict):
        return success

    # --- Extract and Save Team Stats ---
    team_stats_data = boxscore.get('teams')
    if team_stats_data and isinstance(team_stats_data, list):
        try:
            team_stats_list = []
            for team_data in team_stats_data:
                if not isinstance(team_data, dict):
                    continue
                team_id = team_data.get('team', {}).get('id')
                home_away = team_data.get('homeAway')
                if team_id:
                    team_entry = {'game_id': game_id, 'team_id': team_id, 'home_away': home_away}
                    stats_list = team_data.get('statistics')
                    if isinstance(stats_list, list):
                        for stat in stats_list:
                            if not isinstance(stat, dict):
                                continue
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
            print(f"  -> Error processing team stats for {game_id}: {e}")

    # --- Extract and Save Player Stats ---
    player_box_data = boxscore.get('players')
    if player_box_data and isinstance(player_box_data, list):
        try:
            player_stats_list = []
            for team_player_data in player_box_data:
                if not isinstance(team_player_data, dict):
                    continue
                team_id = team_player_data.get('team', {}).get('id')
                stats_section = team_player_data.get('statistics')
                if not isinstance(stats_section, list) or not stats_section or not isinstance(stats_section[0], dict):
                    continue
                stats_def = stats_section[0]

                stat_keys = stats_def.get('keys')
                if not team_id or not isinstance(stat_keys, list):
                    continue

                stat_labels = stats_def.get('labels', stat_keys)
                athletes = stats_def.get('athletes')

                if isinstance(athletes, list):
                    for player_entry in athletes:
                        if not isinstance(player_entry, dict):
                            continue
                        athlete_info = player_entry.get('athlete', {})
                        if not isinstance(athlete_info, dict):
                            continue
                        player_id = athlete_info.get('id')
                        if not player_id:
                            continue

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
                        if isinstance(stats_values, list):
                            player_stats_row.update({stat_labels[i]: stats_values[i] for i in range(min(len(stat_labels), len(stats_values)))})
                        player_stats_list.append(player_stats_row)

            if player_stats_list:
                player_stats_df = pd.DataFrame(player_stats_list)
                player_stats_df.to_csv(os.path.join(dir_paths["player_stats"], f"{game_id}.csv"), index=False, encoding='utf-8-sig')
                print(f"  -> Saved Player Stats CSV for {game_id}")
                success = True
        except Exception as e:
            print(f"  -> Error processing player stats for {game_id}: {e}")

    return success


def load_master_df(filename):
    """Loads a master CSV file if it exists, otherwise returns an empty DataFrame."""
    if os.path.exists(filename):
        try:
            return pd.read_csv(filename)
        except Exception as e:
            print(f"Error loading {filename}: {e}. Starting fresh.")
            return pd.DataFrame()
    return pd.DataFrame()


def save_master_df(df, filename, unique_key):
    """Saves the master DataFrame."""
    if df is None or df.empty:
        print(f"No data to save for {filename}.")
        return
    try:
        if unique_key and unique_key in df.columns:
            df.drop_duplicates(subset=[unique_key], keep='last', inplace=True)
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"Saved: {filename} ({len(df)} rows)")
    except Exception as e:
        print(f"Error saving {filename}: {e}")


# --- Main Execution ---
if __name__ == "__main__":
    print(f"=== NCAA Men's Basketball - Today's Games Scraper ===")
    print(f"Run timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Season year: {season_year}")
    print("-" * 60)

    # Setup directories
    dir_paths = {
        "play_by_play": os.path.join(BASE_DATA_PATH, "play_by_play"),
        "team_stats": os.path.join(BASE_DATA_PATH, "team_stats"),
        "player_stats": os.path.join(BASE_DATA_PATH, "player_stats"),
        "players": os.path.join(BASE_DATA_PATH, "players"),
        "teams": os.path.join(BASE_DATA_PATH, "teams"),
        "games": os.path.join(BASE_DATA_PATH, "games")
    }

    for path in dir_paths.values():
        os.makedirs(path, exist_ok=True)

    # Load existing master data
    master_games_df = load_master_df(os.path.join(dir_paths["games"], "games.csv"))
    master_teams_df = load_master_df(os.path.join(dir_paths["teams"], "teams.csv"))
    master_players_df = load_master_df(os.path.join(dir_paths["players"], "players.csv"))

    all_teams_dict = {str(row['team_id']): row.to_dict() for index, row in master_teams_df.iterrows()} if not master_teams_df.empty else {}
    all_players_dict = {str(row['player_id']): row.to_dict() for index, row in master_players_df.iterrows()} if not master_players_df.empty else {}

    # Fetch today's schedule
    raw_schedule_data, today_str = get_cbb_schedule_today()
   
    json_path = f"deschedule_raw_{today_str}.json"
    with open(json_path, "w") as f:
        json.dump(raw_schedule_data, f, indent=2)

 

    if not raw_schedule_data:
        print("Failed to fetch today's schedule. Exiting.")
        exit(1)

    # Extract game and team data
    daily_games_list, daily_teams_dict = extract_schedule_data(raw_schedule_data, today_str)

    if not daily_games_list:
        print("No games found for today.")
        exit(0)

    # Update teams
    new_teams_count = 0
    for team_id, team_data in daily_teams_dict.items():
        if str(team_id) not in all_teams_dict:
            all_teams_dict[str(team_id)] = team_data
            new_teams_count += 1
    if new_teams_count > 0:
        print(f"Added {new_teams_count} new teams to master list.")

    # Process each game
    print(f"\nProcessing {len(daily_games_list)} games...")
    print("-" * 60)
    
    newly_added_games = []
    games_processed = 0

    for game_info in daily_games_list:
        game_id = game_info.get('game_id')
        if not game_id:
            continue

        # Add to games list if new
        if master_games_df.empty or str(game_id) not in master_games_df['game_id'].astype(str).values:
            newly_added_games.append(game_info)

        print(f"\nProcessing game {game_id}: {game_info.get('away_team_name')} @ {game_info.get('home_team_name')}")
        print(f"  Status: {game_info.get('status_detail')}")
        
        time.sleep(API_DELAY)
        game_details_json = get_game_details(game_id)

        if game_details_json:
            if extract_and_save_detailed_game_data(game_details_json, game_id, dir_paths, all_players_dict):
                games_processed += 1
        else:
            print(f"  -> Failed to fetch details")

    # Save master files
    print("\n" + "=" * 60)
    print("Saving master files...")
    
    if newly_added_games:
        new_games_df = pd.DataFrame(newly_added_games)
        master_games_df = pd.concat([master_games_df, new_games_df], ignore_index=True) if not master_games_df.empty else new_games_df

    save_master_df(master_games_df, os.path.join(dir_paths["games"], "games.csv"), unique_key='game_id')
    save_master_df(pd.DataFrame(list(all_teams_dict.values())), os.path.join(dir_paths["teams"], "teams.csv"), unique_key='team_id')
    save_master_df(pd.DataFrame(list(all_players_dict.values())), os.path.join(dir_paths["players"], "players.csv"), unique_key='player_id')

    print("\n" + "=" * 60)
    print(f"SCRAPING COMPLETE")
    print(f"Games processed: {games_processed}/{len(daily_games_list)}")
    print(f"Data saved to: {os.path.abspath(BASE_DATA_PATH)}")
    print("=" * 60)
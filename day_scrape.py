import requests
import json
from urllib.parse import urlencode
import os
import time # Import time for potential delays/backoff

# --- Configuration ---
# Assume the base URL for the detailed game summary API
GAME_SUMMARY_API_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary?event={game_id}"
# Base URL for constructing full links
ESPN_BASE_URL = "https://www.espn.com"
# Delay between API calls to avoid rate limiting (in seconds)
API_DELAY = 1

# --- Helper Functions ---

def fetch_json(url, params=None, headers=None, retries=3, backoff_factor=2):
    """
    General function to fetch JSON data from a URL with retry logic.

    Args:
        url (str): The URL to fetch data from.
        params (dict, optional): URL parameters. Defaults to None.
        headers (dict, optional): Request headers. Defaults to None.
        retries (int): Number of retry attempts.
        backoff_factor (float): Factor to increase delay between retries.

    Returns:
        dict: Parsed JSON data, or None if fetching fails after retries.
    """
    if headers is None:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
    
    delay = API_DELAY # Initial delay

    for attempt in range(retries + 1):
        try:
            response = requests.get(url, params=params, headers=headers, timeout=15) # Added timeout
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            
            # Check if content type is JSON before attempting to decode
            if 'application/json' in response.headers.get('Content-Type', ''):
                return response.json()
            else:
                print(f"Error: Non-JSON response received from {url}. Content-Type: {response.headers.get('Content-Type')}")
                print("Response text (first 200 chars):", response.text[:200] + "...")
                return None # Return None if not JSON

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred on attempt {attempt + 1}/{retries + 1}: {http_err} for URL: {url}")
            # Specific handling for 404
            if response.status_code == 404:
                print(f"Game data not found (404) for {url}. Skipping.")
                return None
            # Retry on server errors (5xx) or potential rate limiting (429)
            if response.status_code >= 500 or response.status_code == 429:
                 if attempt < retries:
                    print(f"Retrying in {delay} seconds...")
                    time.sleep(delay)
                    delay *= backoff_factor # Exponential backoff
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
            break # Don't retry on unexpected errors
        except json.JSONDecodeError:
            # This case might be less likely now with the content-type check, but kept for robustness
            print(f"Error: Failed to decode JSON from response on attempt {attempt + 1}/{retries + 1} for URL: {url}")
            print("Response text (first 200 chars):", response.text[:200] + "...")
            # Potentially retry if it seems like a transient issue, or break if persistent
            if attempt < retries:
                 print(f"Retrying in {delay} seconds...")
                 time.sleep(delay)
                 delay *= backoff_factor
            else:
                print("Max retries reached.")

    return None


def get_cbb_schedule_by_date(date_str):
    """
    Fetches the men's college basketball schedule from ESPN for a specific date.

    Args:
        date_str (str): The date in 'YYYYMMDD' format (e.g., "20251104").

    Returns:
        dict: A dictionary containing the JSON response from the schedule API,
              or None if an error occurred.
    """
    base_url = f"https://www.espn.com/mens-college-basketball/schedule/_/date/{date_str}"
    params = {
        '_xhr': 'pageContent',
        'refetchShell': 'false',
        'offset': '-05:00', # Assuming EST/EDT offset
        'original': f'date={date_str}',
        'date': date_str
    }
    print(f"Fetching schedule URL: {base_url} with params: {params}")
    return fetch_json(base_url, params=params)


def extract_game_ids_from_schedule(schedule_data, date_str):
    """
    Extracts game IDs from the raw schedule JSON data.

    Args:
        schedule_data (dict): The raw JSON data fetched from the schedule endpoint.
        date_str (str): The date string (YYYYMMDD).

    Returns:
        list: A list of game IDs (str) for the given date.
    """
    game_ids = []
    games = schedule_data.get('events', {}).get(date_str, [])

    if not games:
        print(f"No schedule events found for date {date_str} under 'events' key.")
        return game_ids

    for game in games:
        game_id = game.get('id')
        if game_id:
            game_ids.append(game_id)

    print(f"Found {len(game_ids)} game IDs for {date_str}.")
    return game_ids


def get_game_details(game_id):
    """
    Fetches detailed game data from the ESPN summary API using the game ID.

    Args:
        game_id (str): The unique ID for the game.

    Returns:
        dict: Parsed JSON data for the game summary, or None if fetching fails.
    """
    url = GAME_SUMMARY_API_URL.format(game_id=game_id)
    print(f"Fetching game details for ID {game_id} from {url}...")
    return fetch_json(url)


def extract_detailed_game_data(game_data, game_id):
    """
    Extracts pregame, postgame, and live data from the detailed game summary JSON.

    Args:
        game_data (dict): The JSON response from the game summary API.
        game_id (str): The game ID (used for logging).

    Returns:
        dict: A structured dictionary containing the extracted data,
              or None if essential data is missing.
    """
    if not game_data:
        print(f"No game data provided for extraction (ID: {game_id}).")
        return None

    extracted = {
        "gameId": game_id,
        "pregame": {},
        "postgame": {},
        "live": {}
    }

    # --- Pregame Data ---
    header = game_data.get('header')
    boxscore = game_data.get('boxscore')
    game_info = game_data.get('gameInfo')

    if header and header.get('competitions'):
        comp = header['competitions'][0]
        extracted["pregame"]["date"] = comp.get('date')
        extracted["pregame"]["neutralSite"] = comp.get('neutralSite')
        extracted["pregame"]["conferenceCompetition"] = comp.get('conferenceCompetition')
        extracted["pregame"]["status"] = comp.get('status', {}).get('type', {}).get('detail')
        
        teams_pregame = []
        for competitor in comp.get('competitors', []):
            team_info = competitor.get('team', {})
            teams_pregame.append({
                "id": team_info.get('id'),
                "uid": team_info.get('uid'),
                "location": team_info.get('location'),
                "name": team_info.get('name'),
                "abbreviation": team_info.get('abbreviation'),
                "displayName": team_info.get('displayName'),
                "color": team_info.get('color'),
                "logo": team_info.get('logo'),
                "homeAway": competitor.get('homeAway'),
                "rank": competitor.get('rank', 99)
            })
        extracted["pregame"]["teams"] = teams_pregame
    
    if game_info:
         extracted["pregame"]["venue"] = game_info.get('venue', {}).get('fullName')
         extracted["pregame"]["attendance"] = game_info.get('attendance')
         extracted["pregame"]["officials"] = [o.get('displayName') for o in game_info.get('officials', [])]

    if boxscore and boxscore.get('players'):
         rosters = {}
         player_stats_headers = {}
         for team_player_data in boxscore['players']:
             team_id = team_player_data.get('team', {}).get('id')
             if team_id:
                 stats_def = team_player_data.get('statistics', [{}])[0]
                 player_stats_headers[team_id] = stats_def.get('keys', []) # Store headers per team
                 
                 roster = []
                 for player_entry in stats_def.get('athletes', []):
                     athlete_info = player_entry.get('athlete', {})
                     roster.append({
                         "id": athlete_info.get('id'),
                         "uid": athlete_info.get('uid'),
                         "displayName": athlete_info.get('displayName'),
                         "shortName": athlete_info.get('shortName'),
                         "headshot": athlete_info.get('headshot', {}).get('href'),
                         "jersey": athlete_info.get('jersey'),
                         "position": athlete_info.get('position', {}).get('abbreviation'),
                         "starter": player_entry.get('starter', False),
                         "didNotPlay": player_entry.get('didNotPlay', False),
                         "ejected": player_entry.get('ejected', False)
                     })
                 rosters[team_id] = roster
         extracted["pregame"]["rosters"] = rosters
         extracted["postgame"]["player_stat_keys"] = player_stats_headers # Needed to interpret player stats
         
    # --- Postgame Data ---
    if boxscore and boxscore.get('teams'):
        team_box_scores = {}
        for team_stat_data in boxscore['teams']:
            team_id = team_stat_data.get('team', {}).get('id')
            if team_id:
                stats = {stat.get('name'): stat.get('displayValue') for stat in team_stat_data.get('statistics', [])}
                team_box_scores[team_id] = stats
        extracted["postgame"]["team_stats"] = team_box_scores

    # Extract player box scores using the headers stored earlier
    if boxscore and boxscore.get('players') and extracted["postgame"].get("player_stat_keys"):
        player_box_scores = {}
        for team_player_data in boxscore['players']:
            team_id = team_player_data.get('team', {}).get('id')
            if team_id:
                headers = extracted["postgame"]["player_stat_keys"].get(team_id, [])
                player_scores = []
                stats_def = team_player_data.get('statistics', [{}])[0]
                for player_entry in stats_def.get('athletes', []):
                    athlete_id = player_entry.get('athlete', {}).get('id')
                    stats_list = player_entry.get('stats', [])
                    # Create a dictionary mapping header keys to stat values
                    stats_dict = {headers[i]: stats_list[i] for i in range(min(len(headers), len(stats_list)))}
                    player_scores.append({
                        "id": athlete_id,
                        "stats": stats_dict
                    })
                player_box_scores[team_id] = player_scores
        extracted["postgame"]["player_stats"] = player_box_scores

    # --- Live Data (Play-by-Play) ---
    plays = game_data.get('plays')
    if plays:
        processed_plays = []
        for play in plays:
            processed_play = {
                "id": play.get('id'),
                "sequenceNumber": play.get('sequenceNumber'),
                "type": play.get('type', {}).get('text'),
                "text": play.get('text'),
                "awayScore": play.get('awayScore'),
                "homeScore": play.get('homeScore'),
                "period": play.get('period', {}).get('displayValue'),
                "clock": play.get('clock', {}).get('displayValue'),
                "scoringPlay": play.get('scoringPlay'),
                "teamId": play.get('team', {}).get('id'),
                "wallclock": play.get('wallclock'),
                "participants": [p.get('athlete', {}).get('id') for p in play.get('participants', []) if p.get('athlete')] # Get participant IDs
            }
            processed_plays.append(processed_play)
        extracted["live"]["play_by_play"] = processed_plays
    else:
         extracted["live"]["play_by_play"] = []
         print(f"No play-by-play data found for game {game_id}.")

    return extracted


def save_to_json(data, filename):
    """Saves data to a JSON file."""
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Successfully saved data to {filename}")
    except IOError as e:
        print(f"Error saving data to {filename}: {e}")
    except TypeError as e:
        print(f"Error serializing data to JSON for {filename}: {e}")
    except Exception as e:
         print(f"An unexpected error occurred while saving {filename}: {e}")


# --- Main Execution ---
if __name__ == "__main__":
    default_date = "20241231" # Date from the sample game data
    user_date = input(f"Enter date (YYYYMMDD) [default: {default_date}]: ") or default_date
    output_dir = f"game_data_{user_date}" # Directory to save individual game files

    if len(user_date) == 8 and user_date.isdigit():
        print(f"--- Step 1: Fetching schedule for {user_date} ---")
        raw_schedule_data = get_cbb_schedule_by_date(user_date)

        if raw_schedule_data:
            game_ids = extract_game_ids_from_schedule(raw_schedule_data, user_date)

            if game_ids:
                print(f"\n--- Step 2: Fetching and extracting details for {len(game_ids)} games ---")
                all_games_details = [] # Optional: List to hold all extracted data

                for game_id in game_ids:
                    # Introduce delay
                    print(f"Waiting {API_DELAY} second(s) before next request...")
                    time.sleep(API_DELAY)

                    game_details_json = get_game_details(game_id)

                    if game_details_json:
                        print(f"Extracting data for game {game_id}...")
                        extracted_game_data = extract_detailed_game_data(game_details_json, game_id)

                        if extracted_game_data:
                            # Save data for this specific game
                            game_filename = os.path.join(output_dir, f"game_details_{game_id}.json")
                            save_to_json(extracted_game_data, game_filename)
                            # Optionally append to a master list if you want one big file later
                            # all_games_details.append(extracted_game_data)
                        else:
                            print(f"Could not extract details for game {game_id}.")
                    else:
                        print(f"Failed to fetch details for game {game_id}. Skipping.")

                print("\n--- Processing complete ---")
                # Optional: Save all extracted data into one large file
                # if all_games_details:
                #     master_filename = f"all_game_details_{user_date}.json"
                #     save_to_json(all_games_details, master_filename)

            else:
                print("No game IDs found in the schedule data.")
        else:
            print("Failed to fetch schedule data from ESPN.")
    else:
        print("Invalid date format. Please enter in YYYYMMDD format.")

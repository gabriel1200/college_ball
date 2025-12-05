import requests
import json
from urllib.parse import urlencode
import os
import time 
import pandas as pd 
import re 
from datetime import date, timedelta, datetime

# ==========================================
# 1. Configuration
# ==========================================

GAME_SUMMARY_API_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary?event={game_id}"
ESPN_BASE_URL = "https://www.espn.com"
API_DELAY = 1.5 

# --- Path Templates ---
BASE_DATA_PATH_TEMPLATE = "data/raw/usa_ncaam/{year}"
LOG_FILE_PATH = "data/raw/usa_ncaam/completed_dates_log.csv" # <--- NEW LOG FILE

# --- Scrape Behavior ---
SKIP_COMPLETED_DATES = True  # Set to False to force re-check of all dates
START_DATE = date(2025, 11, 4)
END_DATE = date.today() - timedelta(days=1)

# ==========================================
# 2. Date Log Helpers (New)
# ==========================================

def get_completed_dates_set():
    """Reads the log file and returns a set of date strings (YYYYMMDD) that are fully done."""
    if not os.path.exists(LOG_FILE_PATH):
        return set()
    try:
        df = pd.read_csv(LOG_FILE_PATH)
        return set(df['date'].astype(str).tolist())
    except Exception as e:
        print(f"Warning: Could not read date log: {e}")
        return set()

def mark_date_as_complete(date_str):
    """Appends a date to the log file."""
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(LOG_FILE_PATH), exist_ok=True)
        
        # Check if file exists to determine if we need a header
        file_exists = os.path.exists(LOG_FILE_PATH)
        
        with open(LOG_FILE_PATH, 'a') as f:
            if not file_exists:
                f.write("date,timestamp_updated\n")
            f.write(f"{date_str},{datetime.now().isoformat()}\n")
            
        print(f"  [Log] Date {date_str} marked as fully complete.")
    except Exception as e:
        print(f"  [Log Error] Could not save completion status: {e}")

# ==========================================
# 3. Standard Helpers
# ==========================================

def fetch_json(url, params=None, headers=None, retries=3, backoff_factor=2):
    """General function to fetch JSON data from a URL with retry logic."""
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
                    return {} 
                return response.json()
            else:
                return None 

        except requests.exceptions.RequestException as e:
            if attempt < retries:
                time.sleep(delay)
                delay *= backoff_factor
            else:
                print(f"Error fetching {url}: {e}")
                return None
        except json.JSONDecodeError:
            return None

    return None

def get_cbb_schedule_by_date(date_str):
    base_url = f"https://www.espn.com/mens-college-basketball/schedule/_/date/{date_str}"
    params = {'_xhr': 'pageContent', 'offset': '-05:00', 'date': date_str}
    print(base_url)
    return fetch_json(base_url, params=params)


def extract_schedule_data_for_master(schedule_data, date_str):
    games_list = []
    teams_dict = {} 

    if not schedule_data or not isinstance(schedule_data, dict):
        return games_list, teams_dict

    events_for_date = schedule_data.get('events', {}).get(date_str)
    if not events_for_date:
         return games_list, teams_dict

    print(f"  [Debug] Found {len(events_for_date)} events in schedule.")

    for game in events_for_date:
        game_id = game.get('id')
        if not game_id: continue

        status_obj = game.get('status', {})
        
        # --- CRITICAL: Capture Status State ---
        game_entry = {
            'game_id': str(game_id),
            'date_time_utc': game.get('date'),
            'status_detail': status_obj.get('detail'),
            'status_state': status_obj.get('state'), # 'pre', 'in', 'post'
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

            # Extract IDs robustly
            def get_id(comp):
                match = re.search(r't:(\d+)', comp.get('uid', ''))
                return match.group(1) if match else comp.get('id')

            home_id = get_id(home_comp)
            away_id = get_id(away_comp)

            game_entry['home_team_id'] = str(home_id) if home_id else None
            game_entry['home_team_name'] = home_comp.get('displayName')
            game_entry['home_team_abbrev'] = home_comp.get('abbreviation')
            game_entry['home_score'] = home_comp.get('score')
            
            game_entry['away_team_id'] = str(away_id) if away_id else None
            game_entry['away_team_name'] = away_comp.get('displayName')
            game_entry['away_team_abbrev'] = away_comp.get('abbreviation')
            game_entry['away_score'] = away_comp.get('score')

            for comp_info, team_id in [(home_comp, home_id), (away_comp, away_id)]:
                if team_id and str(team_id) not in teams_dict:
                    teams_dict[str(team_id)] = {
                        'team_id': str(team_id),
                        'uid': comp_info.get('uid'),
                        'location': comp_info.get('location'),
                        'name': comp_info.get('name'), 
                        'abbreviation': comp_info.get('abbreviation'),
                        'displayName': comp_info.get('displayName'),
                        'shortDisplayName': comp_info.get('shortDisplayName'),
                        'logo': comp_info.get('logo')
                    }
        games_list.append(game_entry)

    return games_list, teams_dict

def get_game_details(game_id):
    url = GAME_SUMMARY_API_URL.format(game_id=game_id)
    return fetch_json(url)

def extract_and_save_detailed_game_data(game_data, game_id, dir_paths, all_players_dict):
    """Extracts PBP, Team Stats, Player Stats."""
    if not game_data or not isinstance(game_data, dict): return False

    boxscore = game_data.get('boxscore', {})
    plays_data = game_data.get('plays', [])
    success = False 

    # 1. PBP
    if plays_data:
        try:
            pbp_df = pd.DataFrame(plays_data)
            if not pbp_df.empty:
                # Flattener helpers
                pbp_df['type_text'] = pbp_df['type'].apply(lambda x: x.get('text') if isinstance(x, dict) else None)
                pbp_df['period_display'] = pbp_df['period'].apply(lambda x: x.get('displayValue') if isinstance(x, dict) else None)
                pbp_df['clock_display'] = pbp_df['clock'].apply(lambda x: x.get('displayValue') if isinstance(x, dict) else None)
                pbp_df['team_id'] = pbp_df['team'].apply(lambda x: x.get('id') if isinstance(x, dict) else None)

                # Select/Rename
                cols = {
                    'id': 'play_id', 'sequenceNumber': 'sequence_number', 'type_text': 'type',
                    'text': 'description', 'awayScore': 'away_score', 'homeScore': 'home_score',
                    'period_display': 'period', 'clock_display': 'clock',
                    'scoringPlay': 'scoring_play', 'team_id': 'team_id', 'wallclock': 'timestamp_utc'
                }
                final_df = pd.DataFrame()
                for old, new in cols.items():
                    if old in pbp_df.columns: final_df[new] = pbp_df[old]
                
                final_df.to_csv(os.path.join(dir_paths["play_by_play"], f"{game_id}.csv"), index=False, encoding='utf-8-sig')
                success = True
        except Exception as e:
            print(f"  [PBP Error] {game_id}: {e}")

    if not boxscore: return success

    # 2. Team Stats
    team_stats = boxscore.get('teams', [])
    if team_stats:
        try:
            rows = []
            for t in team_stats:
                tid = t.get('team', {}).get('id')
                if tid:
                    row = {'game_id': game_id, 'team_id': str(tid), 'home_away': t.get('homeAway')}
                    for stat in t.get('statistics', []):
                        row[stat.get('name', stat.get('abbreviation'))] = stat.get('displayValue')
                    rows.append(row)
            if rows:
                pd.DataFrame(rows).to_csv(os.path.join(dir_paths["team_stats"], f"{game_id}.csv"), index=False, encoding='utf-8-sig')
                success = True
        except Exception as e:
            print(f"  [Team Stats Error] {game_id}: {e}")

    # 3. Player Stats
    players = boxscore.get('players', [])
    if players:
        try:
            rows = []
            for t_sec in players:
                tid = t_sec.get('team', {}).get('id')
                if not tid: continue
                
                # Stats Metadata
                stats_head = t_sec.get('statistics', [{}])[0]
                labels = stats_head.get('labels', stats_head.get('keys', []))
                
                for athlete_entry in stats_head.get('athletes', []):
                    ath = athlete_entry.get('athlete', {})
                    pid = ath.get('id')
                    if not pid: continue
                    
                    # Update Master Dict
                    if str(pid) not in all_players_dict:
                        all_players_dict[str(pid)] = {
                            'player_id': str(pid),
                            'displayName': ath.get('displayName'),
                            'shortName': ath.get('shortName'),
                            'position': ath.get('position', {}).get('abbreviation'),
                            'jersey': ath.get('jersey'),
                            'first_seen_team_id': str(tid)
                        }
                    
                    # Create Row
                    row = {'game_id': game_id, 'team_id': str(tid), 'player_id': str(pid)}
                    row['starter'] = athlete_entry.get('starter', False)
                    row['didNotPlay'] = athlete_entry.get('didNotPlay', False)
                    row['displayName'] = ath.get('displayName')
                    
                    stats_vals = athlete_entry.get('stats', [])
                    for i, val in enumerate(stats_vals):
                        if i < len(labels): row[labels[i]] = val
                    rows.append(row)

            if rows:
                pd.DataFrame(rows).to_csv(os.path.join(dir_paths["player_stats"], f"{game_id}.csv"), index=False, encoding='utf-8-sig')
                success = True
        except Exception as e:
            print(f"  [Player Stats Error] {game_id}: {e}")

    return success

def load_master_df(filename):
    if os.path.exists(filename):
        try:
            return pd.read_csv(filename)
        except:
            return pd.DataFrame()
    return pd.DataFrame()

def save_master_df(df, filename, unique_key):
    if df is None or df.empty: return
    try:
        if unique_key in df.columns:
            df.drop_duplicates(subset=[unique_key], keep='last', inplace=True)
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"Saved master file: {filename}")
    except Exception as e:
        print(f"Error saving {filename}: {e}")

# ==========================================
# 4. Main Execution
# ==========================================

if __name__ == "__main__":
    
    # 1. Setup Folders
    # Use START_DATE year if > Oct, else previous year. Simple logic for now.
    year_val = START_DATE.year + 1 if START_DATE.month >= 10 else START_DATE.year
    base_path = BASE_DATA_PATH_TEMPLATE.format(year=year_val) 

    dir_paths = {
        "play_by_play": os.path.join(base_path, "play_by_play"),
        "team_stats": os.path.join(base_path, "team_stats"),
        "player_stats": os.path.join(base_path, "player_stats"),
        "players": os.path.join(base_path, "players"),
        "teams": os.path.join(base_path, "teams"),
        "games": os.path.join(base_path, "games")
    }
    for p in dir_paths.values(): os.makedirs(p, exist_ok=True)

    print(f"--- Starting Season Loop ---")
    print(f"Range: {START_DATE} to {END_DATE}")
    print(f"Skip Completed Dates: {SKIP_COMPLETED_DATES}")
    
    # 2. Load Metadata
    master_games_df = load_master_df(os.path.join(dir_paths["games"], "games.csv"))
    master_teams_df = load_master_df(os.path.join(dir_paths["teams"], "teams.csv"))
    master_players_df = load_master_df(os.path.join(dir_paths["players"], "players.csv"))

    # To Dicts
    all_teams_dict = {str(r['team_id']): r.to_dict() for _, r in master_teams_df.iterrows()}
    all_players_dict = {str(r['player_id']): r.to_dict() for _, r in master_players_df.iterrows()}
    
    # Processed Game IDs (Files exist)
    processed_game_ids = set()
    if os.path.exists(dir_paths["team_stats"]):
        processed_game_ids = {f.split('.')[0] for f in os.listdir(dir_paths["team_stats"]) if f.endswith('.csv')}

    # Processed Dates (Log file)
    processed_dates_set = get_completed_dates_set() if SKIP_COMPLETED_DATES else set()
    print(f"Skipping {len(processed_dates_set)} previously completed dates.")

    # 3. Loop
    current_date = START_DATE
    newly_added_games = []

    while current_date <= END_DATE:
        date_str = current_date.strftime("%Y%m%d")
        
        # --- Check Skip Logic ---
        if SKIP_COMPLETED_DATES and date_str in processed_dates_set:
            print(f"Skipping {date_str} (Marked Complete in Log)")
            current_date += timedelta(days=1)
            continue

        print(f"\nProcessing {date_str}...")
        
        schedule_data = get_cbb_schedule_by_date(date_str)
        if not schedule_data:
            print(f"  No data for {date_str}")
            current_date += timedelta(days=1)
            continue

        games, teams = extract_schedule_data_for_master(schedule_data, date_str)
        
        # Update Teams
        for tid, tdata in teams.items():
            if tid not in all_teams_dict: all_teams_dict[tid] = tdata

        # Track Status for Date Completion
        all_games_final_today = True
        games_found = len(games) > 0

        for g in games:
            gid = g['game_id']
            status_state = g.get('status_state') # 'pre', 'in', 'post'

            # If ANY game is not 'post' (Final), we cannot mark this date as done.
            if status_state != 'post':
                all_games_final_today = False

            # Add to master list if new
            is_known = False
            if not master_games_df.empty:
                if gid in master_games_df['game_id'].astype(str).values: is_known = True
            
            if not is_known:
                newly_added_games.append(g)

            # Process Details
            if gid in processed_game_ids:
                continue

            print(f"  Scraping {gid} ({g['away_team_name']} @ {g['home_team_name']})")
            time.sleep(API_DELAY)
            details = get_game_details(gid)
            if extract_and_save_detailed_game_data(details, gid, dir_paths, all_players_dict):
                processed_game_ids.add(gid)

        # --- Update Log Logic ---
        # If we found games, and ALL of them were 'post' (Final), mark date as done.
        # If we found NO games (e.g. Christmas), also mark as done.
        if (games_found and all_games_final_today) or (not games_found):
            mark_date_as_complete(date_str)
        else:
            print(f"  [Info] Date {date_str} not marked complete (Pending/Live games found).")

        current_date += timedelta(days=1)

    # 4. Save Final Master Files
    print("\n--- Saving Master Files ---")
    
    # Update Games
    if newly_added_games:
        new_df = pd.DataFrame(newly_added_games)
        new_df['game_id'] = new_df['game_id'].astype(str)
        if not master_games_df.empty:
            master_games_df['game_id'] = master_games_df['game_id'].astype(str)
            existing_ids = set(master_games_df['game_id'])
            new_df = new_df[~new_df['game_id'].isin(existing_ids)]
            master_games_df = pd.concat([master_games_df, new_df], ignore_index=True)
        else:
            master_games_df = new_df

    save_master_df(master_games_df, os.path.join(dir_paths["games"], "games.csv"), 'game_id')
    save_master_df(pd.DataFrame(list(all_teams_dict.values())), os.path.join(dir_paths["teams"], "teams.csv"), 'team_id')
    save_master_df(pd.DataFrame(list(all_players_dict.values())), os.path.join(dir_paths["players"], "players.csv"), 'player_id')

    print("Done.")
import requests
import json
import os
import time
import pandas as pd
import re
from datetime import date, datetime
from urllib.parse import urlencode

# --- Configuration ---
API_DELAY = 0.5
SCHEDULE_FILE = 'backup_schedule.csv'

# Determine year for folder structure
current_year = date.today().year
season_year = current_year if date.today().month < 8 else current_year + 1

# UPDATED PATH: data/raw/usa_ncaam_backup
BASE_DATA_PATH = f"data/raw/usa_ncaam_backup/{season_year}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# --- URL Builder Functions (NCAA Specific) ---

def build_ncaa_pbp_url(contest_id):
    base_url = "https://sdataprod.ncaa.com/"
    meta = "NCAA_GetGamecenterPbpBasketballById_web"
    extensions = {"persistedQuery": {"version": 1, "sha256Hash": "6b1232714a3598954c5bacabc0f81570e16d6ee017c9a6b93b601a3d40dafb98"}}
    variables = {"contestId": str(contest_id), "staticTestEnv": None}
    params = {"meta": meta, "extensions": json.dumps(extensions, separators=(',', ':')), "variables": json.dumps(variables, separators=(',', ':'))}
    return base_url + "?" + urlencode(params)

def build_ncaa_boxscore_url(contest_id):
    base_url = "https://sdataprod.ncaa.com/"
    meta = "NCAA_GetGamecenterBoxscoreBasketballById_web"
    extensions = {"persistedQuery": {"version": 1, "sha256Hash": "4a7fa26398db33de3ff51402a90eb5f25acef001cca28d239fe5361315d1419a"}}
    variables = {"contestId": str(contest_id), "staticTestEnv": None}
    params = {"meta": meta, "extensions": json.dumps(extensions, separators=(',', ':')), "variables": json.dumps(variables, separators=(',', ':'))}
    return base_url + "?" + urlencode(params)

def build_ncaa_team_stats_url(contest_id):
    base_url = "https://sdataprod.ncaa.com/"
    meta = "NCAA_GetGamecenterTeamStatsBasketballById_web"
    extensions = {"persistedQuery": {"version": 1, "sha256Hash": "5fcf84602d59c003f37ddd1185da542578080e04fe854e935cbcaee590a0e8a2"}}
    variables = {"contestId": str(contest_id), "staticTestEnv": None}
    params = {"meta": meta, "extensions": json.dumps(extensions, separators=(',', ':')), "variables": json.dumps(variables, separators=(',', ':'))}
    return base_url + "?" + urlencode(params)

# --- Helper Functions ---

def fetch_json(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"Error fetching {url}: {e}")
    return None

def load_master_df(filename):
    if os.path.exists(filename):
        try:
            return pd.read_csv(filename)
        except Exception as e:
            print(f"Error loading {filename}: {e}. Starting fresh.")
            return pd.DataFrame()
    return pd.DataFrame()

def save_master_df(df, filename, unique_key):
    if df is None or df.empty:
        return
    try:
        if unique_key and unique_key in df.columns:
            df.drop_duplicates(subset=[unique_key], keep='last', inplace=True)
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"Saved: {filename} ({len(df)} rows)")
    except Exception as e:
        print(f"Error saving {filename}: {e}")

# --- NCAA Data Extractors ---

def extract_game_status(box_json):
    try:
        meta = box_json.get('data', {}).get('boxscore', {})
        raw_status = meta.get('status', 'Unknown') 
        period = meta.get('period', '')
        
        mins = meta.get('minutes')
        secs = meta.get('seconds')
        clock = ""
        if mins is not None and secs is not None:
            clock = f"{mins}:{secs}"
        
        status_state = 'in'
        detail = 'Live'

        if raw_status == 'F' or str(period).upper() == 'FINAL':
            status_state = 'post'
            detail = 'Final'
        elif raw_status == 'P':
            status_state = 'pre'
            detail = 'Scheduled'
        elif raw_status == 'd':
            status_state = 'pre'
            detail = 'Delayed'

        return status_state, detail, period, clock
    except Exception:
        return "pre", "Unknown", "", ""

def process_pbp(json_data, contest_id, output_path):
    try:
        playbyplay_root = json_data.get('data', {}).get('playbyplay', {})
        periods = playbyplay_root.get('periods', [])
        plays_list = []
        sequence = 0
        
        for period in periods:
            period_num = period.get('periodNumber')
            play_stats = period.get('playbyplayStats', [])
            for play in play_stats:
                sequence += 1
                visitor_text = play.get('visitorText', '')
                home_text = play.get('homeText', '')
                desc = visitor_text if visitor_text else home_text
                
                scoring = False
                desc_lower = desc.lower()
                if 'made' in desc_lower or 'dunk' in desc_lower or 'good' in desc_lower:
                    scoring = True

                plays_list.append({
                    'play_id': f"{contest_id}_{sequence}", 
                    'sequence_number': sequence,
                    'type': play.get('eventDescription', 'Play'),
                    'description': desc,
                    'away_score': play.get('visitorScore'),
                    'home_score': play.get('homeScore'),
                    'period': period_num,
                    'clock': play.get('clock'),
                    'scoring_play': scoring,
                    'team_id': play.get('teamId'),
                    'timestamp_utc': datetime.utcnow().isoformat()
                })

        if plays_list:
            df = pd.DataFrame(plays_list)
            df.to_csv(os.path.join(output_path, f"{contest_id}.csv"), index=False, encoding='utf-8-sig')
            return True
    except Exception as e:
        print(f"  -> PBP Error: {e}")
    return False

def process_box_and_players(json_data, contest_id, player_stats_path, all_players_dict):
    try:
        boxscore_data = json_data.get('data', {}).get('boxscore', {})
        if not boxscore_data: return False

        team_boxscores = boxscore_data.get('teamBoxscore', [])
        all_player_stats = []

        for team_box in team_boxscores:
            team_id = team_box.get('teamId')
            players = team_box.get('playerStats', [])
            for player in players:
                p_id = player.get('id')
                if not p_id: continue

                if str(p_id) not in all_players_dict:
                    all_players_dict[str(p_id)] = {
                        'player_id': p_id,
                        'displayName': f"{player.get('firstName')} {player.get('lastName')}",
                        'first_seen_team_id': team_id,
                        'position': player.get('position'),
                        'jersey': player.get('uniformNumber')
                    }

                row = {
                    'game_id': contest_id,
                    'team_id': team_id,
                    'player_id': p_id,
                    'displayName': f"{player.get('firstName')} {player.get('lastName')}",
                    'starter': player.get('starter', False),
                    'minutes': player.get('minutesPlayed'),
                    'points': player.get('points'),
                    'rebounds': player.get('totalRebounds'),
                    'assists': player.get('assists'),
                    'fieldGoalsMade': player.get('fieldGoalsMade'),
                    'threePointsMade': player.get('threePointsMade'),
                    'freeThrowsMade': player.get('freeThrowsMade'),
                }
                all_player_stats.append(row)

        if all_player_stats:
            df = pd.DataFrame(all_player_stats)
            df.to_csv(os.path.join(player_stats_path, f"{contest_id}.csv"), index=False, encoding='utf-8-sig')
            return True
    except Exception as e:
        print(f"  -> Player Stats Error: {e}")
    return False

def process_team_stats(json_data, contest_id, team_stats_path):
    try:
        boxscore_data = json_data.get('data', {}).get('boxscore', {})
        if not boxscore_data: return False

        teams_info = {team['teamId']: team for team in boxscore_data.get('teams', [])}
        team_boxscores = boxscore_data.get('teamBoxscore', [])
        
        game_team_stats = []

        for team_box in team_boxscores:
            team_id = team_box.get('teamId')
            meta = teams_info.get(str(team_id), {})
            stats = team_box.get('teamStats', {})
            
            home_away = 'home' if meta.get('isHome') else 'away'

            row = {
                'game_id': contest_id,
                'team_id': team_id,
                'home_away': home_away,
                'fieldGoalPercentage': stats.get('fieldGoalPercentage'),
                'threePointPercentage': stats.get('threePointPercentage'),
                'totalRebounds': stats.get('totalRebounds'),
                'assists': stats.get('assists'),
                'points': stats.get('points')
            }
            game_team_stats.append(row)

        if game_team_stats:
            df = pd.DataFrame(game_team_stats)
            df.to_csv(os.path.join(team_stats_path, f"{contest_id}.csv"), index=False, encoding='utf-8-sig')
            return True
    except Exception as e:
        print(f"  -> Team Stats Error: {e}")
    return False

# --- Main Execution ---

if __name__ == "__main__":
    print(f"=== NCAA Men's Basketball - BACKUP SCRAPER (Source: NCAA) ===")
    print(f"Run timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Season year: {season_year}")
    print("-" * 60)

    # 1. Setup directories
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

    # 2. Load existing master data
    master_games_df = load_master_df(os.path.join(dir_paths["games"], "games.csv"))
    master_teams_df = load_master_df(os.path.join(dir_paths["teams"], "teams.csv"))
    master_players_df = load_master_df(os.path.join(dir_paths["players"], "players.csv"))

    all_teams_dict = {str(row['team_id']): row.to_dict() for index, row in master_teams_df.iterrows()} if not master_teams_df.empty else {}
    all_players_dict = {str(row['player_id']): row.to_dict() for index, row in master_players_df.iterrows()} if not master_players_df.empty else {}

    # 3. Identify completed games to skip
    completed_game_ids = set()
    if not master_games_df.empty and 'status_state' in master_games_df.columns:
        completed_games = master_games_df[master_games_df['status_state'] == 'post']
        completed_game_ids = set(completed_games['game_id'].astype(str).tolist())
        print(f"  -> Found {len(completed_game_ids)} previously completed games in master record.")

    # 4. Load Schedule
    if not os.path.exists(SCHEDULE_FILE):
        print(f"Error: {SCHEDULE_FILE} not found.")
        exit(1)

    print(f"Reading {SCHEDULE_FILE}...")
    schedule_df = pd.read_csv(SCHEDULE_FILE)
    
    # Filter for TODAY only
    schedule_df['dt'] = pd.to_datetime(schedule_df['startDate'], format='%m/%d/%Y', errors='coerce')
    today = pd.Timestamp.now().normalize()
    live_schedule_df = schedule_df[schedule_df['dt'] == today].copy()

    if live_schedule_df.empty:
        print(f"No games found for today ({today.strftime('%m/%d/%Y')}). Exiting.")
        exit(0)

    if 'url' in live_schedule_df.columns:
        live_schedule_df['extracted_id'] = live_schedule_df['url'].astype(str).apply(lambda x: x.split('/')[-1])
    else:
        print("Error: 'url' column missing in schedule.")
        exit(1)

    print(f"Processing {len(live_schedule_df)} games...")
    print("-" * 60)

    games_to_save = []
    games_processed_count = 0

    # 5. Loop through games
    for i, row in live_schedule_df.iterrows():
        game_id = str(row['extracted_id'])
        
        # Fetch Boxscore for status
        box_json = fetch_json(build_ncaa_boxscore_url(game_id))
        if not box_json:
            print(f"Game {game_id}: No data found.")
            continue

        status_state, status_detail, period, clock = extract_game_status(box_json)

        print(f"\nGame {game_id}: {row.get('team2_name')} @ {row.get('team1_name')}")
        print(f"  Current Status: {status_detail} (State: {status_state})")

        game_entry = {
            'game_id': game_id,
            'date_time_utc': row.get('startDate'), 
            'home_team_name': row.get('team1_name') if row.get('team1_isHome') else row.get('team2_name'),
            'away_team_name': row.get('team2_name') if row.get('team1_isHome') else row.get('team1_name'),
            'status_state': status_state,
            'status_detail': status_detail,
            'season_year': season_year,
            'period': period,
            'clock': clock
        }
        games_to_save.append(game_entry)

        if status_state == 'pre':
            print(f"  -> Game has not started. Skipping details.")
            continue
        
        pbp_path = os.path.join(dir_paths["play_by_play"], f"{game_id}.csv")
        if status_state == 'post' and game_id in completed_game_ids and os.path.exists(pbp_path):
            print(f"  -> Game previously scraped as Final. Skipping.")
            continue

        print(f"  -> Fetching fresh data...")
        time.sleep(API_DELAY)
        
        if process_box_and_players(box_json, game_id, dir_paths["player_stats"], all_players_dict):
            pbp_json = fetch_json(build_ncaa_pbp_url(game_id))
            if pbp_json:
                process_pbp(pbp_json, game_id, dir_paths["play_by_play"])
            
            stats_json = fetch_json(build_ncaa_team_stats_url(game_id))
            if stats_json:
                process_team_stats(stats_json, game_id, dir_paths["team_stats"])
            
            games_processed_count += 1

    # 6. Save Master Files
    print("\n" + "=" * 60)
    print("Saving master files...")

    if games_to_save:
        daily_df = pd.DataFrame(games_to_save)
        if not master_games_df.empty:
            master_games_df['game_id'] = master_games_df['game_id'].astype(str)
            daily_df['game_id'] = daily_df['game_id'].astype(str)
            ids_to_update = daily_df['game_id'].tolist()
            master_games_df = master_games_df[~master_games_df['game_id'].isin(ids_to_update)]
            master_games_df = pd.concat([master_games_df, daily_df], ignore_index=True)
        else:
            master_games_df = daily_df

    save_master_df(master_games_df, os.path.join(dir_paths["games"], "games.csv"), unique_key='game_id')
    save_master_df(pd.DataFrame(list(all_players_dict.values())), os.path.join(dir_paths["players"], "players.csv"), unique_key='player_id')

    print("\n" + "=" * 60)
    print(f"BACKUP SCRAPING COMPLETE")
    print(f"Games details processed: {games_processed_count}")
    print(f"Data saved to: {os.path.abspath(BASE_DATA_PATH)}")
    print("=" * 60)
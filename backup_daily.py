import json
import pandas as pd
import requests
import time
import os
from urllib.parse import urlencode
from datetime import datetime

# ==========================================
# 1. Configuration & Paths
# ==========================================

# Path for live data (separated to avoid messing up historical backups)
BASE_DATA_PATH_TEMPLATE = "data/live/{year}"
SCHEDULE_FILE = 'backup_schedule.csv'

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# ==========================================
# 2. URL Builder Functions
# ==========================================

def build_ncaa_pbp_url(contest_id):
    base_url = "https://sdataprod.ncaa.com/"
    meta = "NCAA_GetGamecenterPbpBasketballById_web"
    extensions = {"persistedQuery": {"version": 1, "sha256Hash": "6b1232714a3598954c5bacabc0f81570e16d6ee017c9a6b93b601a3d40dafb98"}}
    variables = {"contestId": str(contest_id), "staticTestEnv": None}
    params = {
        "meta": meta,
        "extensions": json.dumps(extensions, separators=(',', ':')),
        "variables": json.dumps(variables, separators=(',', ':'))
    }
    return base_url + "?" + urlencode(params)

def build_ncaa_boxscore_url(contest_id):
    base_url = "https://sdataprod.ncaa.com/"
    meta = "NCAA_GetGamecenterBoxscoreBasketballById_web"
    extensions = {"persistedQuery": {"version": 1, "sha256Hash": "4a7fa26398db33de3ff51402a90eb5f25acef001cca28d239fe5361315d1419a"}}
    variables = {"contestId": str(contest_id), "staticTestEnv": None}
    params = {
        "meta": meta,
        "extensions": json.dumps(extensions, separators=(',', ':')),
        "variables": json.dumps(variables, separators=(',', ':'))
    }
    return base_url + "?" + urlencode(params)

def build_ncaa_team_stats_url(contest_id):
    base_url = "https://sdataprod.ncaa.com/"
    meta = "NCAA_GetGamecenterTeamStatsBasketballById_web"
    extensions = {"persistedQuery": {"version": 1, "sha256Hash": "5fcf84602d59c003f37ddd1185da542578080e04fe854e935cbcaee590a0e8a2"}}
    variables = {"contestId": str(contest_id), "staticTestEnv": None}
    params = {
        "meta": meta,
        "extensions": json.dumps(extensions, separators=(',', ':')),
        "variables": json.dumps(variables, separators=(',', ':'))
    }
    return base_url + "?" + urlencode(params)

# ==========================================
# 3. Helpers
# ==========================================

def fetch_json(url):
    try:
        response = requests.get(url, headers=HEADERS, timeout=5)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"Error fetching {url}: {e}")
    return None

def save_master_df(df, filename, unique_key):
    if df is None or df.empty: return
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        # For live scraping, we read existing, append new, and drop dupes to keep latest status
        if os.path.exists(filename):
            existing_df = pd.read_csv(filename)
            combined = pd.concat([existing_df, df])
            combined.drop_duplicates(subset=[unique_key], keep='last', inplace=True)
            combined.to_csv(filename, index=False, encoding='utf-8-sig')
        else:
            df.to_csv(filename, index=False, encoding='utf-8-sig')
    except Exception as e:
        print(f"Error saving master file {filename}: {e}")

# ==========================================
# 4. Status Extraction Logic
# ==========================================

def extract_game_status(box_json):
    """Parses the JSON to find the live status."""
    try:
        # Based on backup_boxscore.json structure
        meta = box_json.get('data', {}).get('boxscore', {})
        
        raw_status = meta.get('status', 'Unknown') # 'F', 'I', 'P'
        period = meta.get('period', '')
        
        # Build clock string
        mins = meta.get('minutes')
        secs = meta.get('seconds')
        clock = ""
        if mins is not None and secs is not None:
            clock = f"{mins}:{secs}"
        
        # Map to readable status
        status_map = {
            'F': 'Final',
            'I': 'Live', 
            'P': 'Scheduled',
            'd': 'Delayed'
        }
        
        readable_status = status_map.get(raw_status, raw_status)
        
        # Override if period explicitly says FINAL
        if str(period).upper() == 'FINAL':
            readable_status = 'Final'

        return readable_status, period, clock
        
    except Exception:
        return "Unknown", "", ""

# ==========================================
# 5. Parsing Logic (Standard)
# ==========================================

def process_pbp(json_data, contest_id, output_path):
    try:
        # Based on playbyplay_backup.json structure
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
        print(f"  PBP Error: {e}")
    return False

def process_box_and_players(json_data, contest_id, player_stats_path):
    try:
        # Based on backup_boxscore.json structure
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
        print(f"  Player Stats Error: {e}")
    return False

def process_team_stats(json_data, contest_id, team_stats_path):
    try:
        # Based on backup_teamstats.json structure
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
        print(f"  Team Stats Error: {e}")
    return False

# ==========================================
# 6. Main Execution
# ==========================================

def main():
    if not os.path.exists(SCHEDULE_FILE):
        print(f"Error: {SCHEDULE_FILE} not found.")
        return

    print(f"Reading {SCHEDULE_FILE}...")
    schedule_df = pd.read_csv(SCHEDULE_FILE)
    
    # ---------------------------------------------------------
    # STRICT DATE FILTERING
    # ---------------------------------------------------------
    # Convert schedule dates to datetime objects
    schedule_df['dt'] = pd.to_datetime(schedule_df['startDate'], format='%m/%d/%Y', errors='coerce')
    
    # Get today's date (normalized to midnight for comparison)
    today = pd.Timestamp.now().normalize()
    
    # Filter: ONLY keep rows where the date is TODAY
    live_schedule_df = schedule_df[schedule_df['dt'] == today].copy()

    if live_schedule_df.empty:
        print(f"No games found for today ({today.strftime('%m/%d/%Y')}). Exiting.")
        return

    print(f"Found {len(live_schedule_df)} games scheduled for today ({today.strftime('%m/%d/%Y')}).")

    # Extract IDs and Seasons
    if 'url' in live_schedule_df.columns:
        live_schedule_df['extracted_id'] = live_schedule_df['url'].astype(str).apply(lambda x: x.split('/')[-1])
    else:
        print("Error: 'url' column missing in schedule.")
        return
        
    # Season logic based on your backup_loop.py
    live_schedule_df['season'] = live_schedule_df['dt'].apply(lambda x: x.year + 1 if x.month > 8 else x.year)

    dir_cache = {} 
    updated_games_list = []

    for i, row in live_schedule_df.iterrows():
        contest_id = row['extracted_id']
        year = int(row['season'])
        
        # --- Setup Directories ---
        if year not in dir_cache:
            base_path = BASE_DATA_PATH_TEMPLATE.format(year=year)
            paths = {
                "base": base_path,
                "play_by_play": os.path.join(base_path, "play_by_play"),
                "team_stats": os.path.join(base_path, "team_stats"),
                "player_stats": os.path.join(base_path, "player_stats"),
                "games": os.path.join(base_path, "games")
            }
            for p in paths.values():
                os.makedirs(p, exist_ok=True)
            dir_cache[year] = paths
            
        paths = dir_cache[year]

        print(f"[{i+1}/{len(live_schedule_df)}] checking {contest_id}...", end=" ", flush=True)

        # --- 1. Fetch Boxscore (to check status) ---
        box_json = fetch_json(build_ncaa_boxscore_url(contest_id))
        
        if not box_json:
            print("No Data")
            continue

        # --- 2. Extract Status ---
        status, period, clock = extract_game_status(box_json)
        print(f"Status: {status} | {period} {clock}")

        # --- 3. Build/Update Game Entry ---
        game_entry = {
            'game_id': contest_id,
            'date_time_utc': row.get('startDate'),
            'home_team': row.get('team1_name') if row.get('team1_isHome') else row.get('team2_name'),
            'away_team': row.get('team2_name') if row.get('team1_isHome') else row.get('team1_name'),
            'season_year': year,
            'status': status,          # <--- Live/Final/Scheduled
            'current_period': period,  # <--- 1st Half, 2nd Half, etc.
            'current_clock': clock,    # <--- 12:00
            'last_updated': datetime.now().isoformat()
        }
        updated_games_list.append(game_entry)

        # --- 4. Process Details IF Active or Final ---
        # We process 'Live' to get updates. 
        # We process 'Final' in case the game just finished and we need the final stats.
        if status in ['Live', 'Final', 'In Progress']:
            
            # Save Player Stats (we already have box_json)
            process_box_and_players(box_json, contest_id, paths["player_stats"])
            
            # Fetch & Save PBP
            pbp_json = fetch_json(build_ncaa_pbp_url(contest_id))
            if pbp_json:
                process_pbp(pbp_json, contest_id, paths["play_by_play"])

            # Fetch & Save Team Stats
            stats_json = fetch_json(build_ncaa_team_stats_url(contest_id))
            if stats_json:
                process_team_stats(stats_json, contest_id, paths["team_stats"])

        time.sleep(0.5) 

    # --- Update Master Games File ---
    print("\n--- Updating Master Games List ---")
    if updated_games_list:
        df_all = pd.DataFrame(updated_games_list)
        # Iterate years in case the schedule spans New Year's Eve/Day
        for year in df_all['season_year'].unique():
            year_df = df_all[df_all['season_year'] == year]
            paths = dir_cache[year]
            save_master_df(year_df, os.path.join(paths['games'], "games.csv"), 'game_id')

    print("Live Scrape Cycle Complete.")

if __name__ == "__main__":
    main()
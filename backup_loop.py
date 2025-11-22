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

# CHANGED: Added '_backup' to the path to prevent overwriting your main data
BASE_DATA_PATH_TEMPLATE = "data/raw/usa_ncaam_backup/{year}"
SCHEDULE_FILE = 'backup_schedule.csv'

# Headers for scraping
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
# 3. Helpers & Master Data Logic
# ==========================================

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
        except:
            return pd.DataFrame()
    return pd.DataFrame()

def save_master_df(df, filename, unique_key):
    if df is None or df.empty: return
    try:
        if unique_key and unique_key in df.columns:
            df.drop_duplicates(subset=[unique_key], keep='last', inplace=True)
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"Updated master file: {filename}")
    except Exception as e:
        print(f"Error saving master file {filename}: {e}")

# ==========================================
# 4. Parsing & Formatting Logic (ESPN Format)
# ==========================================

def process_pbp(json_data, contest_id, output_path):
    """Extracts PBP and maps to ESPN format."""
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
                
                # Scoring play logic: naive check
                scoring = False
                if 'made' in desc.lower() or 'dunk' in desc.lower() or 'good' in desc.lower():
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
                    'timestamp_utc': datetime.utcnow().isoformat() # Placeholder
                })

        if plays_list:
            df = pd.DataFrame(plays_list)
            df.to_csv(os.path.join(output_path, f"{contest_id}.csv"), index=False, encoding='utf-8-sig')
            return True
    except Exception as e:
        print(f"  PBP Error: {e}")
    return False

def process_box_and_players(json_data, contest_id, player_stats_path, master_players_dict):
    """Extracts Player Stats & Updates Master Player List."""
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

                # 1. Update Master Dictionary
                if str(p_id) not in master_players_dict:
                    master_players_dict[str(p_id)] = {
                        'player_id': p_id,
                        'displayName': f"{player.get('firstName')} {player.get('lastName')}",
                        'shortName': f"{player.get('firstName')[0]}. {player.get('lastName')}" if player.get('firstName') else player.get('lastName'),
                        'position': player.get('position'),
                        'jersey': player.get('number'),
                        'first_seen_team_id': team_id
                    }

                # 2. Create Game Stats Entry 
                row = {
                    'game_id': contest_id,
                    'team_id': team_id,
                    'player_id': p_id,
                    'displayName': f"{player.get('firstName')} {player.get('lastName')}",
                    'starter': player.get('starter', False),
                    'didNotPlay': False,
                    'minutes': player.get('minutesPlayed'),
                    'points': player.get('points'),
                    'rebounds': player.get('totalRebounds'),
                    'assists': player.get('assists'),
                    'fieldGoalsMade': player.get('fieldGoalsMade'),
                    'fieldGoalsAttempted': player.get('fieldGoalsAttempted'),
                    'threePointsMade': player.get('threePointsMade'),
                    'threePointsAttempted': player.get('threePointsAttempted'),
                    'freeThrowsMade': player.get('freeThrowsMade'),
                    'freeThrowsAttempted': player.get('freeThrowsAttempted'),
                    'steals': player.get('steals'),
                    'blocks': player.get('blockedShots'),
                    'turnovers': player.get('turnovers'),
                    'fouls': player.get('personalFouls')
                }
                all_player_stats.append(row)

        if all_player_stats:
            df = pd.DataFrame(all_player_stats)
            df.to_csv(os.path.join(player_stats_path, f"{contest_id}.csv"), index=False, encoding='utf-8-sig')
            return True

    except Exception as e:
        print(f"  Player Stats Error: {e}")
    return False

def process_team_stats(json_data, contest_id, team_stats_path, master_teams_dict):
    """Extracts Team Stats & Updates Master Team List."""
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

            # 1. Update Master Team Dictionary
            if str(team_id) not in master_teams_dict:
                master_teams_dict[str(team_id)] = {
                    'team_id': team_id,
                    'name': meta.get('nameShort'),
                    'abbreviation': meta.get('nameShort'),
                    'displayName': meta.get('nameFull'),
                    'logo': meta.get('logoUrl')
                }

            # 2. Build Stats Row
            row = {
                'game_id': contest_id,
                'team_id': team_id,
                'home_away': home_away,
                'fieldGoalsMade': stats.get('fieldGoalsMade'),
                'fieldGoalsAttempted': stats.get('fieldGoalsAttempted'),
                'fieldGoalPercentage': stats.get('fieldGoalPercentage'),
                'threePointsMade': stats.get('threePointsMade'),
                'threePointsAttempted': stats.get('threePointsAttempted'),
                'freeThrowsMade': stats.get('freeThrowsMade'),
                'freeThrowsAttempted': stats.get('freeThrowsAttempted'),
                'totalRebounds': stats.get('totalRebounds'),
                'offensiveRebounds': stats.get('offensiveRebounds'),
                'assists': stats.get('assists'),
                'steals': stats.get('steals'),
                'blocks': stats.get('blockedShots'),
                'turnovers': stats.get('turnovers'),
                'fouls': stats.get('personalFouls'),
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
# 5. Main Execution
# ==========================================

def main():
    if not os.path.exists(SCHEDULE_FILE):
        print(f"Error: {SCHEDULE_FILE} not found.")
        return

    print(f"Reading {SCHEDULE_FILE}...")
    schedule_df = pd.read_csv(SCHEDULE_FILE)
    
    # Extract Game ID and determine Season Year
    if 'url' in schedule_df.columns and 'startDate' in schedule_df.columns:
        schedule_df['extracted_id'] = schedule_df['url'].astype(str).apply(lambda x: x.split('/')[-1])
        # Simple year logic: If month > 8 (August), season is year + 1. Else season is current year.
        schedule_df['dt'] = pd.to_datetime(schedule_df['startDate'], format='%m/%d/%Y', errors='coerce')
        schedule_df['season'] = schedule_df['dt'].apply(lambda x: x.year + 1 if x.month > 8 else x.year)
    else:
        print("Error: Required columns 'url' or 'startDate' missing in schedule.")
        return

    # Dictionary to cache processed years to avoid checking folders every loop
    dir_cache = {} 
    
    # Master Data Containers (Split by year)
    master_data_storage = {} 

    for i, row in schedule_df.iterrows():
        contest_id = row['extracted_id']
        year = int(row['season'])
        date_str = row['startDate']
        
        # --- Setup Directories for this Year ---
        if year not in dir_cache:
            base_path = BASE_DATA_PATH_TEMPLATE.format(year=year)
            paths = {
                "base": base_path,
                "play_by_play": os.path.join(base_path, "play_by_play"),
                "team_stats": os.path.join(base_path, "team_stats"),
                "player_stats": os.path.join(base_path, "player_stats"),
                "players": os.path.join(base_path, "players"),
                "teams": os.path.join(base_path, "teams"),
                "games": os.path.join(base_path, "games")
            }
            for p in paths.values():
                os.makedirs(p, exist_ok=True)
            dir_cache[year] = paths
            
            # Initialize Master Load for this year
            master_data_storage[year] = {
                'teams': {},
                'players': {},
                'games': []
            }
            # Try loading existing masters from BACKUP folder
            t_df = load_master_df(os.path.join(paths['teams'], "teams.csv"))
            if not t_df.empty: master_data_storage[year]['teams'] = {str(r['team_id']): r.to_dict() for _, r in t_df.iterrows()}
            
            p_df = load_master_df(os.path.join(paths['players'], "players.csv"))
            if not p_df.empty: master_data_storage[year]['players'] = {str(r['player_id']): r.to_dict() for _, r in p_df.iterrows()}
            
            g_df = load_master_df(os.path.join(paths['games'], "games.csv"))
            if not g_df.empty: master_data_storage[year]['games'] = g_df.to_dict('records')

        paths = dir_cache[year]
        
        # --- Check if processed ---
        if os.path.exists(os.path.join(paths["play_by_play"], f"{contest_id}.csv")):
            print(f"Skipping {contest_id} (Already Processed)")
            continue

        print(f"[{i+1}/{len(schedule_df)}] Processing {contest_id} ({date_str})...")

        # --- 1. Update Master Games List ---
        game_entry = {
            'game_id': contest_id,
            'date_time_utc': row.get('startDate'),
            'status_detail': row.get('gameState'),
            'home_team_name': row.get('team1_name') if row.get('team1_isHome') else row.get('team2_name'),
            'home_score': row.get('team1_score') if row.get('team1_isHome') else row.get('team2_score'),
            'away_team_name': row.get('team2_name') if row.get('team1_isHome') else row.get('team1_name'),
            'away_score': row.get('team2_score') if row.get('team1_isHome') else row.get('team1_score'),
            'season_year': year
        }
        master_data_storage[year]['games'].append(game_entry)

        # --- 2. Fetch Data ---
        # Fetch PBP
        pbp_json = fetch_json(build_ncaa_pbp_url(contest_id))
        if pbp_json:
            process_pbp(pbp_json, contest_id, paths["play_by_play"])

        # Fetch Boxscore (For Player Stats)
        box_json = fetch_json(build_ncaa_boxscore_url(contest_id))
        if box_json:
            process_box_and_players(box_json, contest_id, paths["player_stats"], master_data_storage[year]['players'])

        # Fetch Team Stats
        stats_json = fetch_json(build_ncaa_team_stats_url(contest_id))
        if stats_json:
            process_team_stats(stats_json, contest_id, paths["team_stats"], master_data_storage[year]['teams'])

        time.sleep(0.5) # Be polite to API

    # --- Final Save of Master Files ---
    print("\n--- Saving Master Files ---")
    for year, data in master_data_storage.items():
        paths = dir_cache[year]
        
        if data['games']:
            save_master_df(pd.DataFrame(data['games']), os.path.join(paths['games'], "games.csv"), 'game_id')
        
        if data['teams']:
            save_master_df(pd.DataFrame(list(data['teams'].values())), os.path.join(paths['teams'], "teams.csv"), 'team_id')
            
        if data['players']:
            save_master_df(pd.DataFrame(list(data['players'].values())), os.path.join(paths['players'], "players.csv"), 'player_id')

    print("Backup Scrape Complete.")

if __name__ == "__main__":
    main()
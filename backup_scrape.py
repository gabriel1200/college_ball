import json
import pandas as pd
import requests
import time
from urllib.parse import urlencode
import os

# ==========================================
# 1. URL Builder Functions
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
# 2. Data Extraction & Flattening Logic
# ==========================================

def fetch_json(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        print(f"Error fetching {url}: {e}")
    return None

def parse_pbp_data(json_data, contest_id):
    """
    Parses PBP JSON based on actual API structure.
    Structure: data -> playbyplay -> periods (list) -> playbyplayStats (list)
    """
    plays_list = []
    try:
        # Correct root path
        playbyplay_root = json_data.get('data', {}).get('playbyplay', {})
        
        if not playbyplay_root:
            return pd.DataFrame()

        periods = playbyplay_root.get('periods', [])
        
        for period in periods:
            period_num = period.get('periodNumber')
            period_display = period.get('periodDisplay')
            play_stats = period.get('playbyplayStats', [])  # Changed from 'playStats'
            
            for play in play_stats:
                # Combine visitorText and homeText for description
                visitor_text = play.get('visitorText', '')
                home_text = play.get('homeText', '')
                description = visitor_text if visitor_text else home_text
                
                plays_list.append({
                    'contest_id': contest_id,
                    'period': period_num,
                    'period_display': period_display,
                    'clock': play.get('clock'),  # Changed from 'time'
                    'score': play.get('score'),
                    'home_score': play.get('homeScore'),
                    'visitor_score': play.get('visitorScore'),  # Changed from 'vis_score'
                    'description': description,
                    'event_description': play.get('eventDescription'),
                    'team_id': play.get('teamId'),
                    'is_home': play.get('isHome'),
                    'first_name': play.get('firstName'),
                    'last_name': play.get('lastName'),
                    'event_id': play.get('eventId')
                })
    except Exception as e:
        print(f"Error parsing PBP for {contest_id}: {e}")

    return pd.DataFrame(plays_list)

def parse_boxscore_data(json_data, contest_id):
    """
    Parses Boxscore JSON based on actual API structure.
    Structure: data -> boxscore -> teamBoxscore (list) -> playerStats (list)
    """
    player_stats_list = []
    try:
        boxscore_data = json_data.get('data', {}).get('boxscore', {})
        if not boxscore_data: 
            return pd.DataFrame()

        # Get teams info for mapping
        teams_info = {team['teamId']: team for team in boxscore_data.get('teams', [])}
        
        # Get team boxscore data
        team_boxscores = boxscore_data.get('teamBoxscore', [])
        
        for team_box in team_boxscores:
            team_id = team_box.get('teamId')
            
            # Get team info from teams array
            team_info = teams_info.get(str(team_id), {})
            team_side = 'home' if team_info.get('isHome') else 'away'
            team_name = team_info.get('nameShort')
            
            # Get player stats
            players = team_box.get('playerStats', [])
            
            for player in players:
                player_stats_list.append({
                    'contest_id': contest_id,
                    'team_side': team_side,
                    'team_id': team_id,
                    'team_name': team_name,
                    'player_id': player.get('id'),
                    'first_name': player.get('firstName'),
                    'last_name': player.get('lastName'),
                    'jersey': player.get('number'),
                    'position': player.get('position'),
                    'is_starter': player.get('starter'),
                    'year': player.get('year'),
                    'elig': player.get('elig'),
                    
                    # Stats
                    'minutes': player.get('minutesPlayed'),
                    'points': player.get('points'),
                    'fg_made': player.get('fieldGoalsMade'),
                    'fg_att': player.get('fieldGoalsAttempted'),
                    '3pt_made': player.get('threePointsMade'),
                    '3pt_att': player.get('threePointsAttempted'),
                    'ft_made': player.get('freeThrowsMade'),
                    'ft_att': player.get('freeThrowsAttempted'),
                    'rebounds': player.get('totalRebounds'),
                    'rebounds_off': player.get('offensiveRebounds'),
                    'assists': player.get('assists'),
                    'turnovers': player.get('turnovers'),
                    'steals': player.get('steals'),
                    'blocks': player.get('blockedShots'),
                    'fouls': player.get('personalFouls')
                })
    except Exception as e:
        print(f"Error parsing Boxscore for {contest_id}: {e}")
    return pd.DataFrame(player_stats_list)

def parse_team_stats(json_data, contest_id):
    """
    Parses Team Stats based on actual API structure.
    Structure: data -> boxscore -> teamBoxscore (list) -> teamStats
    """
    team_stats_list = []
    try:
        boxscore_data = json_data.get('data', {}).get('boxscore', {})
        if not boxscore_data: 
            return pd.DataFrame()

        # Get teams info for mapping
        teams_info = {team['teamId']: team for team in boxscore_data.get('teams', [])}
        
        # Get team boxscore data
        team_boxscores = boxscore_data.get('teamBoxscore', [])

        for team_box in team_boxscores:
            team_id = team_box.get('teamId')
            stats = team_box.get('teamStats', {})
            
            # Get team info from teams array
            team_info = teams_info.get(str(team_id), {})
            team_side = 'home' if team_info.get('isHome') else 'away'
            
            team_stats_list.append({
                'contest_id': contest_id,
                'team_side': team_side,
                'team_id': team_id,
                'team_name': team_info.get('nameShort'),
                'team_name_full': team_info.get('nameFull'),
                
                # Extracting stats
                'fg_made': stats.get('fieldGoalsMade'),
                'fg_att': stats.get('fieldGoalsAttempted'),
                'fg_pct': stats.get('fieldGoalPercentage'),
                '3pt_made': stats.get('threePointsMade'),
                '3pt_att': stats.get('threePointsAttempted'),
                '3pt_pct': stats.get('threePointPercentage'),
                'ft_made': stats.get('freeThrowsMade'),
                'ft_att': stats.get('freeThrowsAttempted'),
                'ft_pct': stats.get('freeThrowPercentage'),
                'rebounds_tot': stats.get('totalRebounds'),
                'rebounds_off': stats.get('offensiveRebounds'),
                'rebounds_def': stats.get('defensiveRebounds'),  # May not be in response
                'assists': stats.get('assists'),
                'steals': stats.get('steals'),
                'blocks': stats.get('blockedShots'),
                'turnovers': stats.get('turnovers'),
                'fouls': stats.get('personalFouls'),
                'points': stats.get('points'),  # May not be in response
                'fast_break_pts': stats.get('fastBreakPoints'),  # May not be in response
                'paint_pts': stats.get('pointsInPaint'),  # May not be in response
                'turnover_pts': stats.get('pointsOffTurnovers'),  # May not be in response
                'bench_pts': stats.get('benchPoints'),  # May not be in response
                'largest_lead': stats.get('largestLead')  # May not be in response
            })
    except Exception as e:
        print(f"Error parsing Team Stats for {contest_id}: {e}")
    return pd.DataFrame(team_stats_list)

# ==========================================
# 3. Main Execution Logic
# ==========================================

def main():
    schedule_file = 'backup_schedule.csv'
    if not os.path.exists(schedule_file):
        print(f"Error: {schedule_file} not found.")
        return

    print(f"Reading {schedule_file}...")
    schedule_df = pd.read_csv(schedule_file)
    schedule_df=schedule_df.head(5)
    # Extract ID
    if 'url' in schedule_df.columns:
        schedule_df['extracted_id'] = schedule_df['url'].astype(str).apply(lambda x: x.split('/')[-1])
    else:
        print("Error: 'url' column missing.")
        return

    all_pbp, all_box, all_stats = [], [], []
    
    # Using all games
    games_to_scrape = schedule_df
    print(f"Found {len(games_to_scrape)} games to scrape.")
    
    for i, row in games_to_scrape.iterrows():
        contest_id = row['extracted_id']
        matchup = f"{row.get('team1_name', 'T1')} vs {row.get('team2_name', 'T2')}"
        
        print(f"[{i+1}/{len(games_to_scrape)}] Scraping {contest_id}: {matchup}")

        # 1. Play-by-Play
        pbp_df = parse_pbp_data(fetch_json(build_ncaa_pbp_url(contest_id)), contest_id)
        if not pbp_df.empty: all_pbp.append(pbp_df)

        # 2. Boxscore
        box_df = parse_boxscore_data(fetch_json(build_ncaa_boxscore_url(contest_id)), contest_id)
        if not box_df.empty: all_box.append(box_df)
        
        # 3. Team Stats
        stats_df = parse_team_stats(fetch_json(build_ncaa_team_stats_url(contest_id)), contest_id)
        if not stats_df.empty: all_stats.append(stats_df)

        time.sleep(0.5)

    # Save Output
    if all_pbp:
        pd.concat(all_pbp, ignore_index=True).to_csv("ncaa_pbp_data.csv", index=False)
        print(f"Saved {len(pd.concat(all_pbp))} PBP rows.")
    
    if all_box:
        pd.concat(all_box, ignore_index=True).to_csv("ncaa_boxscore_data.csv", index=False)
        print(f"Saved {len(pd.concat(all_box))} Boxscore rows.")

    if all_stats:
        pd.concat(all_stats, ignore_index=True).to_csv("ncaa_team_stats.csv", index=False)
        print(f"Saved {len(pd.concat(all_stats))} Team Stats rows.")

if __name__ == "__main__":
    main()
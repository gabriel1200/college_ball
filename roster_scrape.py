import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
import re
import time

# ==========================================
# 1. Configuration
# ==========================================
SEASON_YEAR = 2026
INPUT_FILE = "roster_links.csv"  # Assumes this is in your current folder
SAVE_FREQUENCY = 10              # Save every 10 teams
API_DELAY = 1.0                  # Pause between requests

# Construct the output path
OUTPUT_DIR = f"data/raw/usa_ncaam/{SEASON_YEAR}/rosters"
MASTER_FILE = os.path.join(OUTPUT_DIR, "rosters.csv")

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
}

# ==========================================
# 2. Helper Functions
# ==========================================

def extract_id_from_url(url, pattern=r'/id/(\d+)'):
    """Extracts numeric ID from a URL using regex."""
    if not isinstance(url, str): return None
    match = re.search(pattern, url)
    return match.group(1) if match else None

def load_processed_team_ids(filename):
    """Reads the master file to find which Team IDs are already done."""
    if not os.path.exists(filename):
        return set()
    try:
        # We only need the team_id column to check for duplicates
        df = pd.read_csv(filename, usecols=['team_id'], dtype=str)
        return set(df['team_id'].unique())
    except Exception:
        return set()

def parse_roster_table(soup, team_name, team_id):
    """Parses the HTML table to extract player rows."""
    players = []
    
    # Target the rows in the table body.
    rows = soup.select('tbody.Table__TBODY tr.Table__TR')

    for row in rows:
        try:
            cols = row.find_all('td')
            # We expect at least 6 columns: Headshot, Name, Pos, Ht, Wt, Class
            if len(cols) < 6: 
                continue

            # --- 1. Player Info (Column 1) ---
            # Column 0 is Headshot. Column 1 is Name/Jersey.
            name_col = cols[1]
            
            # Extract Name (inside the AnchorLink)
            link_tag = name_col.select_one('a.AnchorLink')
            if not link_tag: continue
            
            player_name = link_tag.get_text(strip=True)
            if player_name == "NAME": continue  # Skip header rows

            player_url = link_tag.get('href', '')
            
            # Extract Player ID from the href
            player_id = extract_id_from_url(player_url)

            # Extract Jersey Number (inside span.pl2)
            jersey_tag = name_col.select_one('span.pl2') 
            jersey = jersey_tag.get_text(strip=True) if jersey_tag else ""
            
            # --- 2. Stats (Columns 2-5) ---
            pos = cols[2].get_text(strip=True)
            ht = cols[3].get_text(strip=True)
            wt = cols[4].get_text(strip=True)
            cls = cols[5].get_text(strip=True)

            players.append({
                'season_year': SEASON_YEAR, # Added season year to the record
                'team_id': team_id,
                'team_name': team_name,
                'player_id': player_id,
                'player_name': player_name,
                'jersey': jersey,
                'position': pos,
                'height': ht,
                'weight': wt,
                'class': cls,
                'player_url': player_url
            })
            
        except Exception as e:
            print(f"    Error parsing row for {team_name}: {e}")
            continue
            
    return players

# ==========================================
# 3. Main Loop
# ==========================================

def main():
    # 1. Setup Directories
    if not os.path.exists(OUTPUT_DIR):
        print(f"Creating directory: {OUTPUT_DIR}")
        os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 2. Load Inputs
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found. Please ensure it is in the current directory.")
        return

    roster_links_df = pd.read_csv(INPUT_FILE)
    
    # 3. Check Existing Progress
    processed_teams = load_processed_team_ids(MASTER_FILE)
    print(f"Target File: {MASTER_FILE}")
    print(f"Found {len(processed_teams)} teams already scraped in target file.")
    
    new_data_buffer = []
    teams_scraped_count = 0

    print(f"Starting scrape for {len(roster_links_df)} teams...")

    for index, row in roster_links_df.iterrows():
        team_name = row['Team Name']
        roster_url = row['Roster URL']
        
        team_id = extract_id_from_url(roster_url)
        
        if not team_id: continue

        # Skip if already done
        if team_id in processed_teams:
            continue

        print(f"Scraping: {team_name} (ID: {team_id})...")
        
        try:
            resp = requests.get(roster_url, headers=HEADERS, timeout=10)
            resp.raise_for_status()
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            team_players = parse_roster_table(soup, team_name, team_id)
            
            if team_players:
                new_data_buffer.extend(team_players)
                teams_scraped_count += 1
                print(f"  -> Found {len(team_players)} players.")
            else:
                print(f"  -> Warning: No players found.")

            processed_teams.add(team_id)

        except Exception as e:
            print(f"  -> Error scraping {team_name}: {e}")

        time.sleep(API_DELAY)

        # --- SAVE CHECKPOINT ---
        if teams_scraped_count > 0 and teams_scraped_count % SAVE_FREQUENCY == 0:
            print(f"\n--- Checkpoint: Saving {len(new_data_buffer)} players to {MASTER_FILE} ---")
            df = pd.DataFrame(new_data_buffer)
            header_mode = not os.path.exists(MASTER_FILE)
            df.to_csv(MASTER_FILE, mode='a', header=header_mode, index=False, encoding='utf-8')
            new_data_buffer = []
            print("--- Save Complete ---\n")

    # --- FINAL SAVE ---
    if new_data_buffer:
        print(f"\n--- Final Save: Writing remaining {len(new_data_buffer)} players ---")
        df = pd.DataFrame(new_data_buffer)
        header_mode = not os.path.exists(MASTER_FILE)
        df.to_csv(MASTER_FILE, mode='a', header=header_mode, index=False, encoding='utf-8')

    print("Done!")

if __name__ == "__main__":
    main()
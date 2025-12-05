import requests
from bs4 import BeautifulSoup
import re
import csv
import os # Import os for path handling

# ==========================================
# 1. Configuration
# ==========================================
BASE_URL = "https://www.espn.com"
TEAMS_PAGE_URL = f"{BASE_URL}/mens-college-basketball/teams"
OUTPUT_FILENAME = "roster_links.csv"

# A basic User-Agent is recommended
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
}

# ==========================================
# 2. Main Scraper Function
# ==========================================

def scrape_cbb_roster_links():
    """Fetches the main team page and extracts the team name and Roster link for every team."""
    print(f"Fetching team list from: {TEAMS_PAGE_URL}")

    try:
        response = requests.get(TEAMS_PAGE_URL, headers=HEADERS)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching the teams page: {e}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    roster_links_data = []

    # Target the container for each individual team and its associated links
    team_list_items = soup.select('div[role="listitem"]')

    for item in team_list_items:
        # --- 1. Extract Team Name ---
        team_link_tag = item.select_one('a.AnchorLink[href*="/team/_/id/"]')
        team_name = "Unknown Team"
        
        if team_link_tag:
            # Get the text content of the link
            team_name = team_link_tag.get_text(strip=True)
            if not team_name:
                # Fallback to image attributes if link text is empty
                img_tag = team_link_tag.find('img')
                if img_tag and img_tag.get('title'):
                    team_name = img_tag['title']
                elif img_tag and img_tag.get('alt'):
                    team_name = img_tag['alt']

        # --- 2. Extract Roster Link ---
        # Search the item for an AnchorLink whose 'href' attribute contains '/roster/'
        roster_link_tag = item.find("a", class_="AnchorLink", href=re.compile(r'/roster/_/id/\d+'))

        if roster_link_tag and team_name != "Unknown Team":
            href = roster_link_tag.get("href")

            # Construct the full URL
            if href:
                full_url = BASE_URL + href
                roster_links_data.append((team_name, full_url))
                
    # Remove duplicates
    unique_links = list(dict.fromkeys(roster_links_data))
    return unique_links

# ==========================================
# 3. CSV Writer Function (New)
# ==========================================

def save_to_csv(data, filename):
    """Saves a list of (Team Name, Roster URL) tuples to a CSV file."""
    if not data:
        print("No data to save.")
        return

    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            # Write header
            writer.writerow(["Team Name", "Roster URL"])
            # Write data rows
            writer.writerows(data)
        
        print(f"✅ Successfully saved {len(data)} records to {filename}")

    except Exception as e:
        print(f"Error saving file {filename}: {e}")


# ==========================================
# 4. Main Execution
# ==========================================

if __name__ == "__main__":

    teams_with_rosters = scrape_cbb_roster_links()

    if teams_with_rosters:
        # Save the data
        save_to_csv(teams_with_rosters, OUTPUT_FILENAME)
        
        # Optionally print the data (for quick verification)
        print("\n--- Verification Sample ---")
        for name, link in teams_with_rosters[:5]: # Print first 5 items
            print(f"{name} => {link}")
        if len(teams_with_rosters) > 5:
            print(f"...")
    else:
        print("Scraping failed: No roster links were found.")
import requests
from datetime import date, timedelta
import pandas as pd

def build_ncaa_urls(start_date, end_date, sport_code="MBB", division=1, season_year=2025):
    urls = []
    current_date = start_date
    while current_date <= end_date:
        contest_date_str = current_date.strftime("%m/%d/%Y")
        url = (
            "https://sdataprod.ncaa.com/?meta=GetContests_web"
            "&extensions={%22persistedQuery%22:{%22version%22:1,%22sha256Hash%22:%227287cda610a9326931931080cb3a604828febe6fe3c9016a7e4a36db99efdb7c%22}}"
            "&queryName=GetContests_web"
            f"&variables=%7B%22sportCode%22%3A%22{sport_code}%22%2C%22division%22%3A{division}%2C%22seasonYear%22%3A{season_year}%2C%22month%22%3A{current_date.month}%2C%22contestDate%22%3A%22{contest_date_str}%22%2C%22week%22%3Anull%7D"
        )
        urls.append(url)
        current_date += timedelta(days=1)
    return urls

def fetch_ncaa_data(urls):
    all_games = []
    for url in urls:
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            contests = data.get("data", {}).get("contests", [])
            
            for contest in contests:
                game_info = {
                    "contestId": contest.get("contestId"),
                    "url": contest.get("url"),
                    "gameState": contest.get("gameState"),
                    "startDate": contest.get("startDate"),
                    "startTime": contest.get("startTime"),
                }
                
                # Flatten team info
                teams = contest.get("teams", [])
                for i, team in enumerate(teams):
                    prefix = f"team{i+1}"
                    game_info.update({
                        f"{prefix}_name": team.get("nameShort"),
                        f"{prefix}_score": team.get("score"),
                        f"{prefix}_isHome": team.get("isHome"),
                        f"{prefix}_isWinner": team.get("isWinner"),
                        f"{prefix}_conference": team.get("conferenceSeo"),
                    })
                
                all_games.append(game_info)
                
        except Exception as e:
            print(f"Error fetching {url}: {e}")
    
    return pd.DataFrame(all_games)

# Example usage
# Adjust dates as needed
START_DATE = date(2025, 11, 4)
END_DATE = date.today() + timedelta(days=60)

urls = build_ncaa_urls(START_DATE, END_DATE)
ncaa_df = fetch_ncaa_data(urls)
ncaa_df.to_csv('backup_schedule.csv', index=False)

print(f"Schedule saved with {len(ncaa_df)} games.")
print(ncaa_df.head())
import requests
import json
from urllib.parse import urlencode
import os

def get_cbb_schedule_by_date(date_str):
    """
    Fetches the men's college basketball schedule from ESPN for a specific date.

    Args:
        date_str (str): The date in 'YYYYMMDD' format (e.g., "20251104").

    Returns:
        dict: A dictionary containing the JSON response from the API,
              or None if an error occurred.
    """
    base_url = f"https://www.espn.com/mens-college-basketball/schedule/_/date/{date_str}"
    params = {
        '_xhr': 'pageContent',
        'refetchShell': 'false',
        'offset': '-05:00', # Assuming EST/EDT offset, adjust if needed
        'original': f'date={date_str}',
        'date': date_str
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        response = requests.get(base_url, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error occurred: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"An unexpected error occurred: {req_err}")
    except json.JSONDecodeError:
        print("Error: Failed to decode JSON from response.")
        print("Response text (first 200 chars):", response.text[:200] + "...")
    return None

def extract_schedule_info(data, date_str):
    """
    Extracts relevant schedule information from the raw ESPN JSON data.

    Args:
        data (dict): The raw JSON data fetched from ESPN.
        date_str (str): The date string (YYYYMMDD) used to fetch the data.

    Returns:
        list: A list of dictionaries, each containing details for one game.
              Returns an empty list if no game data is found.
    """
    extracted_games = []
    # Access games using the date string as the key within 'events'
    games = data.get('events', {}).get(date_str, [])

    if not games:
        print(f"No games found for date {date_str} under the 'events' key.")
        return extracted_games

    for game in games:
        game_info = {
            'game_id': game.get('id'),
            'date_time_utc': game.get('date'),
            'status_detail': game.get('status', {}).get('detail'),
            'game_link': "https://www.espn.com" + game.get('link', '') if game.get('link') else None,
            'teams': [],
            'venue': game.get('venue', {}).get('fullName'),
            'venue_city': game.get('venue', {}).get('address', {}).get('city'),
            'venue_state': game.get('venue', {}).get('address', {}).get('state'),
            'broadcasts': [b.get('name') for b in game.get('broadcasts', [])],
            'tickets_summary': game.get('tickets', {}).get('summary'),
            'tickets_link': game.get('tickets', {}).get('link')
        }

        # Extract team details
        for competitor in game.get('competitors', []):
            team_details = {
                'team_id': competitor.get('id'),
                'name': competitor.get('displayName'),
                'abbreviation': competitor.get('abbrev'),
                'short_name': competitor.get('shortName'),
                'logo': competitor.get('logo'),
                'team_link': "https://www.espn.com" + competitor.get('links', '') if competitor.get('links') else None,
                'is_home': competitor.get('isHome'),
                'conference_id': competitor.get('conferenceId'),
                'rank': competitor.get('rank', 99) # Default rank if not present
            }
            game_info['teams'].append(team_details)

        extracted_games.append(game_info)

    return extracted_games

def save_to_json(data, filename="extracted_schedule.json"):
    """Saves the extracted data to a JSON file."""
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        print(f"Successfully saved schedule data to {filename}")
    except IOError as e:
        print(f"Error saving data to {filename}: {e}")
    except TypeError as e:
        print(f"Error serializing data to JSON: {e}")


if __name__ == "__main__":
    # Get input from the user, with a default for easy testing
    default_date = "20251104" # Use the date from the sample provided
    user_date = input(f"Enter date (YYYYMMDD) [default: {default_date}]: ") or default_date

    if len(user_date) == 8 and user_date.isdigit():
        print(f"Fetching schedule for {user_date}...")
        raw_schedule_data = get_cbb_schedule_by_date(user_date)

        if raw_schedule_data:
            print("Successfully fetched raw data!")
            extracted_data = extract_schedule_info(raw_schedule_data, user_date)

            if extracted_data:
                print(f"Extracted details for {len(extracted_data)} games.")
                # Define filename based on date
                output_filename = f"espn_cbb_schedule_{user_date}.json"
                save_to_json(extracted_data, output_filename)
            else:
                print("Could not extract game details from the fetched data.")
        else:
            print("Failed to fetch data from ESPN.")

    else:
        print("Invalid date format. Please enter in YYYYMMDD format.")

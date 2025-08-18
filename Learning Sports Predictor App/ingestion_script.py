#!/usr/bin/env python3
import os
import requests
import psycopg2
from datetime import date, timedelta, datetime

# --- CONFIGURATION ---
DB_URL = os.environ.get('DATABASE_URL')
ODDS_API_KEY = os.environ.get('ODDS_API_KEY')

# --- NEW: Define the sports we want to ingest data for ---
SPORTS_TO_INGEST = [
    {
        "sport_key": "baseball_mlb",
        "table_name": "games" # Your existing table for MLB
    },
    {
        "sport_key": "americanfootball_nfl",
        "table_name": "nfl_games" # The new table you just created
    }
]

# --- DATABASE FUNCTIONS ---
def get_db_connection():
    """Establishes and returns a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(DB_URL)
        print("Successfully connected to the database.")
        return conn
    except Exception as e:
        print(f"Error: Could not connect to the database. {e}")
        return None

def insert_games_data(conn, games, table_name):
    """
    Inserts completed game data into a specified table.
    Uses 'ON CONFLICT (game_id) DO NOTHING' to prevent duplicate entries.
    """
    # Note: We use an f-string for the table_name because it's a controlled value from our script,
    # but we use %s for all user-facing data to prevent SQL injection.
    query = f"""
    INSERT INTO {table_name} (game_id, sport_key, commence_time, home_team, away_team, home_score, away_score)
    VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (game_id) DO NOTHING;
    """
    inserted_count = 0
    with conn.cursor() as cur:
        for game in games:
            home_score = next((s['score'] for s in game['scores'] if s['name'] == game['home_team']), None)
            away_score = next((s['score'] for s in game['scores'] if s['name'] == game['away_team']), None)
            
            if home_score is not None and away_score is not None:
                try:
                    cur.execute(query, (
                        game['id'], game['sport_key'], game['commence_time'],
                        game['home_team'], game['away_team'],
                        int(home_score), int(away_score)
                    ))
                    inserted_count += cur.rowcount
                except (ValueError, TypeError) as e:
                    print(f"Warning: Could not process game {game['id']} due to invalid score. Error: {e}")

    conn.commit()
    if inserted_count > 0:
        print(f"Successfully inserted data for {inserted_count} new games into '{table_name}'.")
    else:
        print(f"No new games were added to '{table_name}' (already up-to-date).")


# --- DATA FETCHING ---
def get_completed_games_from_api(sport_key, target_date_str):
    """Fetches completed games for a specific sport and date from The Odds API."""
    print(f"Fetching completed {sport_key} games for date: {target_date_str}")
    try:
        target_date = datetime.strptime(target_date_str, '%Y-%m-%d').date()
        days_from = (date.today() - target_date).days
    except ValueError:
        print(f"Error: Invalid date format for '{target_date_str}'. Please use YYYY-MM-DD.")
        return []

    if not 1 <= days_from <= 3:
        print(f"Warning: 'daysFrom' ({days_from}) is out of the acceptable range (1-3). No data will be fetched.")
        return []

    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/scores/?apiKey={ODDS_API_KEY}&daysFrom={days_from}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        all_games = response.json()
        
        completed_games = [g for g in all_games if g.get('completed', False) and g.get('scores')]
        print(f"Found {len(completed_games)} completed games from the API for {sport_key}.")
        return completed_games
    except requests.exceptions.RequestException as e:
        print(f"Error: Failed to fetch data from The Odds API for {sport_key}. {e}")
        return []

# --- MAIN EXECUTION ---
def main():
    """Main function to run the daily data ingestion pipeline for all configured sports."""
    print("--- Starting Daily Data Ingestion Cron Job ---")
    yesterday_str = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    if not DB_URL or not ODDS_API_KEY:
        print("Error: DATABASE_URL or ODDS_API_KEY environment variables are not set.")
        return

    conn = get_db_connection()
    if not conn:
        print("Halting execution due to database connection failure.")
        return
        
    try:
        # --- NEW: Loop through each sport and ingest its data ---
        for sport in SPORTS_TO_INGEST:
            print(f"\n--- Processing {sport['sport_key']} ---")
            completed_games = get_completed_games_from_api(sport['sport_key'], yesterday_str)
            if completed_games:
                insert_games_data(conn, completed_games, sport['table_name'])
            else:
                print(f"No completed games found to insert for {sport['sport_key']}.")
    finally:
        if conn:
            conn.close()
            print("\nDatabase connection closed.")
            
    print("--- Ingestion Pipeline Finished ---")

if __name__ == "__main__":
    main()

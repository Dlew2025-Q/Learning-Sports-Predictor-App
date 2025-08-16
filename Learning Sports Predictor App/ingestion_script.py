#!/usr/bin/env python3
import os
import requests
import psycopg2
import pandas as pd
from datetime import date, timedelta, datetime

# --- CONFIGURATION ---
# Securely load environment variables for database URL and API key.
DB_URL = os.environ.get('DATABASE_URL')
ODDS_API_KEY = os.environ.get('ODDS_API_KEY')

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

def insert_games_data(conn, games):
    """
    Inserts completed game data into the 'games' table.
    Uses 'ON CONFLICT (game_id) DO NOTHING' to prevent duplicate entries.
    """
    query = """
    INSERT INTO games (game_id, sport_key, commence_time, home_team, away_team, home_score, away_score)
    VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (game_id) DO NOTHING;
    """
    inserted_count = 0
    with conn.cursor() as cur:
        for game in games:
            # Safely extract scores, defaulting to None if not found.
            home_score = next((s['score'] for s in game['scores'] if s['name'] == game['home_team']), None)
            away_score = next((s['score'] for s in game['scores'] if s['name'] == game['away_team']), None)
            
            # Only insert if both scores are present and valid integers.
            if home_score is not None and away_score is not None:
                try:
                    cur.execute(query, (
                        game['id'], game['sport_key'], game['commence_time'],
                        game['home_team'], game['away_team'],
                        int(home_score), int(away_score)
                    ))
                    inserted_count += cur.rowcount # Add 1 if a row was inserted, 0 otherwise.
                except (ValueError, TypeError) as e:
                    print(f"Warning: Could not process game {game['id']} due to invalid score. Error: {e}")

    conn.commit()
    if inserted_count > 0:
        print(f"Successfully inserted data for {inserted_count} new games.")
    else:
        print("No new games were added to the database (already up-to-date).")


# --- DATA FETCHING ---
def get_completed_games_from_api(target_date_str):
    """
    Fetches completed MLB games for a specific date from The Odds API.
    The API allows fetching for the last 3 days.
    """
    print(f"Fetching completed games for date: {target_date_str}")
    # The 'daysFrom' parameter tells the API how many days ago to look for scores.
    # We calculate this based on the target date.
    try:
        target_date = datetime.strptime(target_date_str, '%Y-%m-%d').date()
        days_from = (date.today() - target_date).days
    except ValueError:
        print(f"Error: Invalid date format for '{target_date_str}'. Please use YYYY-MM-DD.")
        return []

    # The Odds API only supports fetching scores for the past 3 days.
    if not 1 <= days_from <= 3:
        print(f"Warning: 'daysFrom' ({days_from}) is out of the acceptable range (1-3). No data will be fetched.")
        return []

    url = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/scores/?apiKey={ODDS_API_KEY}&daysFrom={days_from}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
        all_games = response.json()
        
        # Filter to ensure we only get games that are marked 'completed' and have scores.
        completed_games = [
            g for g in all_games 
            if g.get('completed', False) and g.get('scores')
        ]
        print(f"Found {len(completed_games)} completed games from the API.")
        return completed_games
    except requests.exceptions.RequestException as e:
        print(f"Error: Failed to fetch data from The Odds API. {e}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred during API fetch: {e}")
        return []

# --- MAIN EXECUTION ---
def main():
    """Main function to run the daily data ingestion pipeline."""
    print("--- Starting Daily Data Ingestion Cron Job ---")
    
    # We fetch data for the previous day.
    yesterday_str = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    if not DB_URL or not ODDS_API_KEY:
        print("Error: DATABASE_URL or ODDS_API_KEY environment variables are not set.")
        return

    conn = get_db_connection()
    if not conn:
        print("Halting execution due to database connection failure.")
        return
        
    try:
        completed_games = get_completed_games_from_api(yesterday_str)
        if completed_games:
            insert_games_data(conn, completed_games)
        else:
            print("No completed games found to insert.")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")
            
    print("--- Ingestion Pipeline Finished ---")

if __name__ == "__main__":
    main()

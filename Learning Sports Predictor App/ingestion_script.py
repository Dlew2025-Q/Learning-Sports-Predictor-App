import os
import requests
import psycopg2
import pandas as pd
from datetime import date, timedelta, datetime
from pybaseball import statcast_pitcher, statcast_batter, schedule_and_record

# --- CONFIGURATION ---
# Load secrets from environment variables (set in Render)
DB_URL = os.environ.get('DATABASE_URL')
ODDS_API_KEY = os.environ.get('ODDS_API_KEY')

# --- DATABASE FUNCTIONS ---

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(DB_URL)
        return conn
    except psycopg2.OperationalError as e:
        print(f"Error: Could not connect to the database. {e}")
        return None

def create_tables_if_not_exist(conn):
    """
    Creates the necessary database tables if they don't already exist.
    Note: It's better to run these manually, but this is a fallback.
    """
    create_table_queries = [
        """
        CREATE TABLE IF NOT EXISTS games (
            game_id VARCHAR(255) PRIMARY KEY,
            sport_key VARCHAR(50),
            commence_time TIMESTAMP WITH TIME ZONE,
            home_team VARCHAR(100),
            away_team VARCHAR(100),
            home_score INT,
            away_score INT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS pitcher_stats (
            stat_id SERIAL PRIMARY KEY,
            game_id VARCHAR(255) REFERENCES games(game_id),
            player_name VARCHAR(100),
            team VARCHAR(100),
            strikeouts INT,
            innings_pitched REAL,
            hits_allowed INT,
            earned_runs INT,
            walks INT,
            home_runs_allowed INT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS batter_stats (
            stat_id SERIAL PRIMARY KEY,
            game_id VARCHAR(255) REFERENCES games(game_id),
            player_name VARCHAR(100),
            team VARCHAR(100),
            at_bats INT,
            hits INT,
            runs INT,
            rbi INT,
            walks INT,
            strikeouts INT,
            home_runs INT,
            total_bases INT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        """
    ]
    with conn.cursor() as cur:
        for query in create_table_queries:
            cur.execute(query)
    conn.commit()
    print("Database tables verified.")

# --- DATA FETCHING FUNCTIONS ---

def get_completed_games_from_api(target_date):
    """
    Fetches completed MLB games for a specific date from The Odds API.
    Args:
        target_date (str): Date in 'YYYY-MM-DD' format.
    Returns:
        list: A list of completed game objects from the API.
    """
    print(f"Fetching completed games for {target_date}...")
    # The 'scores' endpoint can fetch historical scores up to 3 days ago.
    # We calculate daysFrom based on the target_date.
    days_from = (date.today() - datetime.strptime(target_date, '%Y-%m-%d').date()).days
    if not 1 <= days_from <= 3:
        print(f"Warning: Cannot fetch scores for {target_date}. It is {days_from} days ago. The API only supports 1-3 days in the past.")
        return []

    url = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/scores/?apiKey={ODDS_API_KEY}&daysFrom={days_from}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        all_games = response.json()
        
        # Filter for games that completed on the target date and have scores
        completed_games = [
            g for g in all_games 
            if g.get('completed', False) and g.get('scores') and g['commence_time'].startswith(target_date)
        ]
        print(f"Found {len(completed_games)} completed games.")
        return completed_games
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from The Odds API: {e}")
        return []

def get_player_stats_for_date(target_date):
    """
    Uses pybaseball to get pitcher and batter stats for a given date.
    Args:
        target_date (str): Date in 'YYYY-MM-DD' format.
    Returns:
        tuple: (DataFrame of pitcher stats, DataFrame of batter stats)
    """
    print(f"Fetching player stats for {target_date} using pybaseball...")
    try:
        # Statcast can be slow, so we fetch for a single day.
        pitcher_df = statcast_pitcher(target_date, target_date)
        batter_df = statcast_batter(target_date, target_date)
        print(f"Found stats for {len(pitcher_df)} pitchers and {len(batter_df)} batters.")
        return pitcher_df, batter_df
    except Exception as e:
        print(f"Error fetching data from pybaseball/statcast: {e}")
        return pd.DataFrame(), pd.DataFrame()


# --- DATA PROCESSING & INSERTION ---

def insert_games_data(conn, games):
    """Inserts game data into the 'games' table, ignoring duplicates."""
    insert_query = """
    INSERT INTO games (game_id, sport_key, commence_time, home_team, away_team, home_score, away_score)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (game_id) DO NOTHING;
    """
    with conn.cursor() as cur:
        for game in games:
            home_score = next((s['score'] for s in game['scores'] if s['name'] == game['home_team']), None)
            away_score = next((s['score'] for s in game['scores'] if s['name'] == game['away_team']), None)
            
            if home_score is not None and away_score is not None:
                cur.execute(insert_query, (
                    game['id'], game['sport_key'], game['commence_time'],
                    game['home_team'], game['away_team'],
                    int(home_score), int(away_score)
                ))
    conn.commit()
    print(f"Inserted/updated {len(games)} games.")


# --- MAIN EXECUTION LOGIC ---

def main():
    """Main function to run the data ingestion pipeline."""
    print("Starting MLB data ingestion pipeline...")
    
    # We will fetch data for yesterday, as all games will be completed.
    yesterday = date.today() - timedelta(days=1)
    target_date_str = yesterday.strftime('%Y-%m-%d')

    conn = get_db_connection()
    if not conn:
        return # Exit if DB connection fails

    try:
        # Ensure tables exist
        create_tables_if_not_exist(conn)

        # 1. Fetch completed games from Odds API and insert them
        completed_games = get_completed_games_from_api(target_date_str)
        if completed_games:
            insert_games_data(conn, completed_games)
        else:
            print("No completed games found to insert.")

        # Steps to add player stats would go here.
        # This would involve matching pybaseball data with game data.
        # For simplicity, this initial script focuses on the game data pipeline.
        # Future steps:
        # 2. Fetch player stats from pybaseball for the target_date
        # 3. Process and clean the player stats DataFrames
        # 4. Match player stats to the game_id from the 'games' table
        # 5. Insert into 'pitcher_stats' and 'batter_stats' tables

    finally:
        if conn:
            conn.close()
            print("Database connection closed.")
    
    print("Pipeline finished.")


if __name__ == "__main__":
    # This check ensures the main function runs only when the script is executed directly
    main()

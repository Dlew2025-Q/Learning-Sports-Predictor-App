#!/usr/bin/env python3
import os
import pickle
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import numpy as np

load_dotenv()
DB_URL = os.environ.get('DATABASE_URL')

# Mapping for MLB team names (from Odds API to your feature file format)
MLB_TEAM_NAME_MAP = {
    "ARI": "ARI", "ATL": "ATL", "BAL": "BAL", "BOS": "BOS", "CHC": "CHC", "CHW": "CHW", "CIN": "CIN", "CLE": "CLE",
    "COL": "COL", "DET": "DET", "HOU": "HOU", "KCR": "KC", "KC": "KC", "LAA": "LAA", "LAD": "LAD", "MIA": "MIA",
    "MIL": "MIL", "MIN": "MIN", "NYM": "NYM", "NYY": "NYY", "OAK": "OAK", "PHI": "PHI", "PIT": "PIT", "SDP": "SD",
    "SD": "SD", "SFG": "SF", "SF": "SF", "SEA": "SEA", "STL": "STL", "TBR": "TB", "TB": "TB", "TEX": "TEX",
    "TOR": "TOR", "WSN": "WSH", "WAS": "WSH", 
    "Arizona Diamondbacks": "ARI", "Atlanta Braves": "ATL", "Baltimore Orioles": "BAL", "Boston Red Sox": "BOS",
    "Chicago Cubs": "CHC", "Chicago White Sox": "CHW", "Cincinnati Reds": "CIN", "Cleveland Guardians": "CLE",
    "Colorado Rockies": "COL", "Detroit Tigers": "DET", "Houston Astros": "HOU", "Kansas City Royals": "KC",
    "Los Angeles Angels": "LAA", "Los Angeles Dodgers": "LAD", "Miami Marlins": "MIA", "Milwaukee Brewers": "MIL",
    "Minnesota Twins": "MIN", "New York Mets": "NYM", "New York Yankees": "NYY", "Oakland Athletics": "OAK",
    "Philadelphia Phillies": "PHI", "Pittsburgh Pirates": "PIT", "San Diego Padres": "SD", "San Francisco Giants": "SF",
    "Seattle Mariners": "SEA", "St. Louis Cardinals": "STL", "Tampa Bay Rays": "TB", "Texas Rangers": "TEX",
    "Toronto Blue Jays": "TOR", "Washington Nationals": "WSH", 
    "Diamondbacks": "ARI", "D-backs": "ARI", "Braves": "ATL", "Orioles": "BAL", "Red Sox": "BOS", "Cubs": "CHC",
    "White Sox": "CHW", "Reds": "CIN", "Guardians": "CLE", "Indians": "CLE", "Rockies": "COL", "Angels": "LAA",
    "Dodgers": "LAD", "Marlins": "MIA", "Brewers": "MIL", "Twins": "MIN", "Mets": "NYM", "Yankees": "NYY",
    "Athletics": "OAK", "Phillies": "PHI", "Pirates": "PIT", "Padres": "SD", "Giants": "SF", "Mariners": "SEA",
    "Cardinals": "STL", "Rays": "TB", "Rangers": "TEX", "Blue Jays": "TOR", "Nationals": "WSH", "ARZ": "ARI",
    "AZ": "ARI", "CWS": "CHW", "NY Mets": "NYM", "WSH Nationals": "WSH", "METS": "NYM", "YANKEES": "NYY", "ATH": "OAK"
}

def precompute_mlb_features(engine):
    """
    Connects to the database, computes the latest MLB features for all teams,
    and saves them to a pickle file.
    """
    print("--- Starting MLB Feature Pre-computation ---")
    try:
        print("Connecting to database and loading data...")
        games_df = pd.read_sql("SELECT * FROM games", engine)
        batter_stats_df = pd.read_sql("SELECT * FROM batter_stats", engine)
        pitcher_stats_df = pd.read_sql("SELECT * FROM pitcher_stats", engine)
        print("MLB data loaded successfully.")

        # --- Data Cleaning ---
        games_df['home_team'] = games_df['home_team'].str.strip().map(MLB_TEAM_NAME_MAP)
        games_df['away_team'] = games_df['away_team'].str.strip().map(MLB_TEAM_NAME_MAP)
        batter_stats_df['team'] = batter_stats_df['team'].str.strip().map(MLB_TEAM_NAME_MAP)
        pitcher_stats_df['team'] = pitcher_stats_df['team'].str.strip().map(MLB_TEAM_NAME_MAP)
        batter_stats_df.fillna(0, inplace=True)
        pitcher_stats_df.fillna(0, inplace=True)
        games_df['commence_time'] = pd.to_datetime(games_df['commence_time'])
        games_df.sort_values('commence_time', inplace=True)
        
        # --- Team Strength Rankings ---
        team_pitching_agg = pitcher_stats_df.groupby('team').agg(total_er=('earned_runs', 'sum'), total_ip=('innings_pitched', 'sum')).reset_index()
        team_pitching_agg['season_era'] = (team_pitching_agg['total_er'] * 9) / (team_pitching_agg['total_ip'] + 1e-6)
        team_pitching_agg['pitching_rank'] = team_pitching_agg['season_era'].rank(ascending=True, method='first')

        team_hitting_agg = batter_stats_df.groupby('team').agg(total_hits=('hits', 'sum'), total_homers=('home_runs', 'sum'), total_at_bats=('at_bats', 'sum')).reset_index()
        team_hitting_agg['production_score'] = (team_hitting_agg['total_hits'] + (2 * team_hitting_agg['total_homers'])) / (team_hitting_agg['total_at_bats'] + 1e-6)
        team_hitting_agg['hitting_rank'] = team_hitting_agg['production_score'].rank(ascending=False, method='first')

        # --- Opponent-Adjusted Stats ---
        opponent_map = pd.concat([
            games_df[['game_id', 'home_team', 'away_team']].rename(columns={'home_team': 'team', 'away_team': 'opponent'}),
            games_df[['game_id', 'away_team', 'home_team']].rename(columns={'away_team': 'team', 'home_team': 'opponent'})
        ])
        batter_stats_df = pd.merge(batter_stats_df, opponent_map, on=['game_id', 'team'], how='left')
        batter_stats_df = pd.merge(batter_stats_df, team_pitching_agg[['team', 'pitching_rank']], left_on='opponent', right_on='team', how='left', suffixes=('', '_opponent'))
        batter_stats_df['adj_hits'] = batter_stats_df['hits'] * (1 + (15.5 - batter_stats_df['pitching_rank']) / 15.5)
        batter_stats_df['adj_home_runs'] = batter_stats_df['home_runs'] * (1 + (15.5 - batter_stats_df['pitching_rank']) / 15.5)
        batter_stats_df['adj_walks'] = batter_stats_df['walks'] * (1 + (15.5 - batter_stats_df['pitching_rank']) / 15.5)
        batter_stats_df['adj_strikeouts'] = batter_stats_df['strikeouts'] * (1 + (15.5 - batter_stats_df['pitching_rank']) / 15.5)
        
        # --- Feature Engineering ---
        batter_stats_df = pd.merge(batter_stats_df, games_df[['game_id', 'home_team', 'away_team', 'commence_time']], on='game_id', how='left').sort_values('commence_time')
        batter_agg = batter_stats_df.groupby(['game_id', 'team', 'home_team', 'away_team']).agg(
            total_adj_hits=('adj_hits', 'sum'), 
            total_adj_homers=('adj_home_runs', 'sum'),
            total_adj_walks=('adj_walks', 'sum'),
            total_adj_strikeouts=('adj_strikeouts', 'sum')
        ).reset_index()
        batter_agg = pd.merge(batter_agg, games_df[['game_id', 'commence_time']], on='game_id', how='left').sort_values('commence_time')
        
        batter_agg['location'] = np.where(batter_agg['team'] == batter_agg['home_team'], 'Home', 'Away')
        home_batter_agg = batter_agg[batter_agg['location'] == 'Home'].copy()
        away_batter_agg = batter_agg[batter_agg['location'] == 'Away'].copy()

        home_batter_agg['rolling_avg_adj_hits_home_perf'] = home_batter_agg.groupby('team')['total_adj_hits'].transform(lambda x: x.shift(1).rolling(10, min_periods=1).mean())
        home_batter_agg['rolling_avg_adj_homers_home_perf'] = home_batter_agg.groupby('team')['total_adj_homers'].transform(lambda x: x.shift(1).rolling(10, min_periods=1).mean())
        home_batter_agg['rolling_avg_adj_walks_home_perf'] = home_batter_agg.groupby('team')['total_adj_walks'].transform(lambda x: x.shift(1).rolling(10, min_periods=1).mean())
        home_batter_agg['rolling_avg_adj_strikeouts_home_perf'] = home_batter_agg.groupby('team')['total_adj_strikeouts'].transform(lambda x: x.shift(1).rolling(10, min_periods=1).mean())
        
        away_batter_agg['rolling_avg_adj_hits_away_perf'] = away_batter_agg.groupby('team')['total_adj_hits'].transform(lambda x: x.shift(1).rolling(10, min_periods=1).mean())
        away_batter_agg['rolling_avg_adj_homers_away_perf'] = away_batter_agg.groupby('team')['total_adj_homers'].transform(lambda x: x.shift(1).rolling(10, min_periods=1).mean())
        away_batter_agg['rolling_avg_adj_walks_away_perf'] = away_batter_agg.groupby('team')['total_adj_walks'].transform(lambda x: x.shift(1).rolling(10, min_periods=1).mean())
        away_batter_agg['rolling_avg_adj_strikeouts_away_perf'] = away_batter_agg.groupby('team')['total_adj_strikeouts'].transform(lambda x: x.shift(1).rolling(10, min_periods=1).mean())

        # --- Assemble Latest Features ---
        latest_home_perf = home_batter_agg.groupby('team').last().reset_index()
        latest_away_perf = away_batter_agg.groupby('team').last().reset_index()
        
        latest_features = pd.merge(latest_home_perf, latest_away_perf, on='team', how='outer')

        # --- Save Features to File ---
        output_filename = 'latest_features.pkl'
        with open(output_filename, 'wb') as file:
            pickle.dump(latest_features, file)
        
        print(f"\nSuccessfully pre-computed and saved MLB features to '{output_filename}'")
        print("Feature columns saved:")
        print(latest_features.columns.tolist())

    except Exception as e:
        print(f"An error occurred during MLB feature pre-computation: {e}")

def precompute_nfl_features(engine):
    """
    Connects to the database, computes the latest NFL features for all teams,
    and saves them to a pickle file.
    """
    print("\n--- Starting NFL Feature Pre-computation ---")
    try:
        print("Connecting to database and loading NFL data...")
        nfl_games_df = pd.read_sql("SELECT * FROM nfl_games", engine)
        print("NFL data loaded successfully.")

        # --- 1. Data Cleaning & Initial Prep ---
        print("Cleaning and preparing data...")
        nfl_games_df.dropna(subset=['home_score', 'away_score'], inplace=True)
        nfl_games_df['commence_time'] = pd.to_datetime(nfl_games_df['commence_time'])
        nfl_games_df.sort_values('commence_time', inplace=True)
        
        # NFL team mapping to handle abbreviations and standardize to full names
        # This mapping ensures consistency with the Odds API and feature computation
        nfl_team_name_map = {
            "ARI": "Arizona Cardinals", "ATL": "Atlanta Falcons", "BAL": "Baltimore Ravens", 
            "BUF": "Buffalo Bills", "CAR": "Carolina Panthers", "CHI": "Chicago Bears",
            "CIN": "Cincinnati Bengals", "CLE": "Cleveland Browns", "DAL": "Dallas Cowboys", 
            "DEN": "Denver Broncos", "DET": "Detroit Lions", "GB": "Green Bay Packers", 
            "HOU": "Houston Texans", "IND": "Indianapolis Colts", "JAX": "Jacksonville Jaguars", 
            "KC": "Kansas City Chiefs", "LV": "Las Vegas Raiders", "LAC": "Los Angeles Chargers",
            "LA": "Los Angeles Rams", "MIA": "Miami Dolphins", "MIN": "Minnesota Vikings", 
            "NE": "New England Patriots", "NO": "New Orleans Saints", "NYG": "New York Giants",
            "NYJ": "New York Jets", "OAK": "Las Vegas Raiders", "PHI": "Philadelphia Eagles", 
            "PIT": "Pittsburgh Steelers", "SF": "San Francisco 49ers", "SEA": "Seattle Seahawks", 
            "TB": "Tampa Bay Buccaneers", "TEN": "Tennessee Titans", "WAS": "Washington Commanders",
        }
        nfl_games_df['home_team'] = nfl_games_df['home_team'].str.strip().map(nfl_team_name_map).fillna(nfl_games_df['home_team'])
        nfl_games_df['away_team'] = nfl_games_df['away_team'].str.strip().map(nfl_team_name_map).fillna(nfl_games_df['away_team'])

        home_games = nfl_games_df[['game_id', 'commence_time', 'home_team', 'away_team', 'home_score', 'away_score']].rename(columns={'home_team': 'team', 'away_team': 'opponent', 'home_score': 'points_scored', 'away_score': 'points_allowed'})
        away_games = nfl_games_df[['game_id', 'commence_time', 'away_team', 'home_team', 'away_score', 'home_score']].rename(columns={'away_team': 'team', 'home_team': 'opponent', 'away_score': 'points_scored', 'home_score': 'points_allowed'})
        team_game_stats = pd.concat([home_games, away_games]).sort_values('commence_time')

        # --- 2. Calculate Team Strength Rankings ---
        print("Calculating team strength rankings...")
        offensive_rank_df = team_game_stats.groupby('team')['points_scored'].mean().rank(ascending=False, method='first').reset_index(name='offensive_rank')
        defensive_rank_df = team_game_stats.groupby('team')['points_allowed'].mean().rank(ascending=True, method='first').reset_index(name='defensive_rank')

        # --- 3. Map Opponent Strength to Each Game ---
        opponent_ranks_df = pd.merge(offensive_rank_df, defensive_rank_df, on='team')
        opponent_ranks_df.rename(columns={'team': 'opponent'}, inplace=True)
        team_game_stats = pd.merge(team_game_stats, opponent_ranks_df, on='opponent', how='left')

        # --- 4. Calculate Opponent-Adjusted Stats ---
        team_game_stats['adj_points_scored'] = team_game_stats['points_scored'] * (1 + (16.5 - team_game_stats['defensive_rank']) / 16.5)
        team_game_stats['adj_points_allowed'] = team_game_stats['points_allowed'] * (1 + (16.5 - team_game_stats['offensive_rank']) / 16.5)

        # --- 5. Feature Engineering (Rolling Averages) ---
        print("Engineering rolling average features...")
        # Create a new column to indicate home vs away for filtering
        team_game_stats['is_home_game'] = team_game_stats['team'] == nfl_games_df.set_index('game_id').loc[team_game_stats['game_id']]['home_team'].reset_index(drop=True)
        
        home_games_stats = team_game_stats[team_game_stats['is_home_game']].copy()
        away_games_stats = team_game_stats[~team_game_stats['is_home_game']].copy()

        home_games_stats['rolling_avg_adj_pts_scored_home'] = home_games_stats.groupby('team')['adj_points_scored'].transform(lambda x: x.shift(1).rolling(4, min_periods=1).mean())
        home_games_stats['rolling_avg_adj_pts_allowed_home'] = home_games_stats.groupby('team')['adj_points_allowed'].transform(lambda x: x.shift(1).rolling(4, min_periods=1).mean())

        away_games_stats['rolling_avg_adj_pts_scored_away'] = away_games_stats.groupby('team')['adj_points_scored'].transform(lambda x: x.shift(1).rolling(4, min_periods=1).mean())
        away_games_stats['rolling_avg_adj_pts_allowed_away'] = away_games_stats.groupby('team')['adj_points_allowed'].transform(lambda x: x.shift(1).rolling(4, min_periods=1).mean())

        # --- 6. Assemble Latest Features ---
        print("Assembling the latest features for each team...")
        latest_home_features = home_games_stats.groupby('team').last().reset_index()
        latest_away_features = away_games_stats.groupby('team').last().reset_index()

        latest_nfl_features = pd.merge(latest_home_features, latest_away_features, on='team', how='outer')
        
        # --- 7. Save Features to File ---
        output_filename = 'latest_nfl_features.pkl'
        with open(output_filename, 'wb') as file:
            pickle.dump(latest_nfl_features, file)
        
        print(f"\nSuccessfully pre-computed and saved NFL features to '{output_filename}'")
        print("Feature columns saved:")
        print(latest_nfl_features.columns.tolist())

    except Exception as e:
        print(f"An error occurred during NFL feature pre-computation: {e}")

def main():
    if not DB_URL:
        print("Error: DATABASE_URL environment variable not found.")
        return

    try:
        engine = create_engine(DB_URL)
        precompute_mlb_features(engine)
        precompute_nfl_features(engine)
    except Exception as e:
        print(f"A critical error occurred: {e}")

if __name__ == '__main__':
    main()

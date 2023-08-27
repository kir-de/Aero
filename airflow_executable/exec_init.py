import psycopg2
import hashlib
conn = psycopg2.connect("host=postgresMain dbname=postgres user=postgres password=changeme port=5432")

cur = conn.cursor()

# Create table for statsSingleSeason
cur.execute("""
    CREATE TABLE IF NOT EXISTS stats_single_season (
        id int4 GENERATED ALWAYS AS IDENTITY,
        team_id INT,
        team_name VARCHAR(50),
        games_played INT,
        wins INT,
        losses INT,
        ot INT,
        pts INT,
        pt_pctg DECIMAL(5,2),
        goals_per_game DECIMAL(7,4),
        goals_against_per_game DECIMAL(9,6),
        ev_gga_ratio DECIMAL(3,1),
        power_play_percentage DECIMAL(5,2),
        power_play_goals DECIMAL(5,1),
        power_play_goals_against DECIMAL(5,1),
        power_play_opportunities DECIMAL(5,1),
        penalty_kill_percentage DECIMAL(5,2),
        shots_per_game DECIMAL(7,4),
        shots_allowed DECIMAL(7,4),
        win_score_first DECIMAL(5,3),
        win_opp_score_first DECIMAL(5,3),
        win_lead_first_per DECIMAL(5,3),
        win_lead_second_per DECIMAL(5,3),
        win_outshoot_opp DECIMAL(5,3),
        win_outshot_by_opp DECIMAL(5,3),
        face_offs_taken DECIMAL(6,1),
        face_offs_won DECIMAL(6,1),
        face_offs_lost DECIMAL(6,1),
        face_off_win_percentage DECIMAL(5,2),
        shooting_pctg DECIMAL(4,1),
        save_pctg DECIMAL(5,3),
        hashdiff VARCHAR(32)
    )
""")

# Create table for regularSeasonStatRankings
cur.execute("""
    CREATE TABLE IF NOT EXISTS regular_season_stat_rankings (
        id int4 GENERATED ALWAYS AS IDENTITY,
        team_id int4,
        team_name VARCHAR(50),
        wins VARCHAR(10),
        losses VARCHAR(10),
        ot VARCHAR(10),
        pts VARCHAR(10),
        pt_pctg VARCHAR(10),
        goals_per_game VARCHAR(10),
        goals_against_per_game VARCHAR(10),
        ev_gga_ratio VARCHAR(10),
        power_play_percentage VARCHAR(10),
        power_play_goals VARCHAR(10),
        power_play_goals_against VARCHAR(10),
        power_play_opportunities VARCHAR(10),
        penalty_kill_opportunities VARCHAR(10),
        penalty_kill_percentage VARCHAR(10),
        shots_per_game VARCHAR(10),
        shots_allowed VARCHAR(10),
        win_score_first VARCHAR(10),
        win_opp_score_first VARCHAR(10),
        win_lead_first_per VARCHAR(10),
        win_lead_second_per VARCHAR(10),
        win_outshoot_opp VARCHAR(10),
        win_outshot_by_opp VARCHAR(10),
        face_offs_taken VARCHAR(10),
        face_offs_won VARCHAR(10),
        face_offs_lost VARCHAR(10),
        face_off_win_percentage VARCHAR(10),
        save_pct_rank VARCHAR(10),
        shooting_pct_rank VARCHAR(10),
        hashdiff VARCHAR(32)
    )
""")

conn.commit()
cur.close()
conn.close()

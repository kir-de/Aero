import json
import requests
import psycopg2
import hashlib


def create_hash(row):
    return hashlib.md5(json.dumps(row, sort_keys=True).encode()).hexdigest()

def insert_stats_single_season(cur, team_id, team_name, stat, hashdiff):
    cur.execute("""
        INSERT INTO stats_single_season (
            team_id, team_name, games_played, wins, losses, ot, pts, pt_pctg,
            goals_per_game, goals_against_per_game, ev_gga_ratio, power_play_percentage,
            power_play_goals, power_play_goals_against, power_play_opportunities, penalty_kill_percentage,
            shots_per_game, shots_allowed, win_score_first, win_opp_score_first, win_lead_first_per,
            win_lead_second_per, win_outshoot_opp, win_outshot_by_opp, face_offs_taken, face_offs_won,
            face_offs_lost, face_off_win_percentage, shooting_pctg, save_pctg, hashdiff
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        team_id, team_name, stat['gamesPlayed'], stat['wins'], stat['losses'], stat['ot'], stat['pts'],
        stat['ptPctg'], stat['goalsPerGame'], stat['goalsAgainstPerGame'], stat['evGGARatio'], stat['powerPlayPercentage'],
        stat['powerPlayGoals'], stat['powerPlayGoalsAgainst'], stat['powerPlayOpportunities'], stat['penaltyKillPercentage'],
        stat['shotsPerGame'], stat['shotsAllowed'], stat['winScoreFirst'], stat['winOppScoreFirst'], stat['winLeadFirstPer'],
        stat['winLeadSecondPer'], stat['winOutshootOpp'], stat['winOutshotByOpp'], stat['faceOffsTaken'], stat['faceOffsWon'],
        stat['faceOffsLost'], stat['faceOffWinPercentage'], stat['shootingPctg'], stat['savePctg'], hashdiff
    ))


def insert_regular_season_stat_rankings(cur, team_id, team_name, stat, hashdiff):
    cur.execute("""
        INSERT INTO regular_season_stat_rankings (
            team_id, team_name, wins, losses, ot, pts, pt_pctg, goals_per_game, goals_against_per_game,
            ev_gga_ratio, power_play_percentage, power_play_goals, power_play_goals_against, power_play_opportunities,
            penalty_kill_opportunities, penalty_kill_percentage, shots_per_game, shots_allowed, win_score_first,
            win_opp_score_first, win_lead_first_per, win_lead_second_per, win_outshoot_opp, win_outshot_by_opp,
            face_offs_taken, face_offs_won, face_offs_lost, face_off_win_percentage, save_pct_rank, shooting_pct_rank,
            hashdiff
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        team_id, team_name, stat['wins'], stat['losses'], stat['ot'], stat['pts'], stat['ptPctg'],
        stat['goalsPerGame'], stat['goalsAgainstPerGame'], stat['evGGARatio'], stat['powerPlayPercentage'],
        stat['powerPlayGoals'], stat['powerPlayGoalsAgainst'], stat['powerPlayOpportunities'], stat['penaltyKillOpportunities'],
        stat['penaltyKillPercentage'], stat['shotsPerGame'], stat['shotsAllowed'], stat['winScoreFirst'], stat['winOppScoreFirst'],
        stat['winLeadFirstPer'], stat['winLeadSecondPer'], stat['winOutshootOpp'], stat['winOutshotByOpp'], stat['faceOffsTaken'],
        stat['faceOffsWon'], stat['faceOffsLost'], stat['faceOffWinPercentage'], stat['savePctRank'], stat['shootingPctRank'], hashdiff
    ))


def run_parser():
    url = 'https://statsapi.web.nhl.com/api/v1/teams/21/stats'
    response = requests.get(url)
    data = response.json()

    with psycopg2.connect("host=postgresMain dbname=postgres user=postgres password=changeme port=5432") as conn:
        with conn.cursor() as cur:
            for stats in data['stats']:
                type = stats['type']['displayName']
                for splits in stats['splits']:
                    team_id = splits['team']['id']
                    team_name = splits['team']['name']
                    stat = splits['stat']
                    
                    hashdiff = create_hash(stat)
                    
                    if type == 'statsSingleSeason':
                        cur.execute("SELECT * FROM stats_single_season WHERE hashdiff = %s", (hashdiff,))
                        if not cur.fetchone():
                            insert_stats_single_season(cur, team_id, team_name, stat, hashdiff)
                    elif type == 'regularSeasonStatRankings':
                        cur.execute("SELECT * FROM regular_season_stat_rankings WHERE hashdiff = %s", (hashdiff,))
                        if not cur.fetchone():
                            insert_regular_season_stat_rankings(cur, team_id, team_name, stat, hashdiff)

    conn.commit()
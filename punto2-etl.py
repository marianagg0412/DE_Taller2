"""
MongoDB (staging collections) -> PostgreSQL (star schema)
Supports: Soccer (fact_match), Basketball (fact_game_basketball), Formula-1 (fact_race_results)

Environment variables (use .env or export):
 - MONGO_URI
 - PG_HOST
 - PG_PORT
 - PG_DB
 - PG_USER
 - PG_PASS
"""

import os
import sys
import logging
from datetime import datetime, date
from decimal import Decimal

import pymongo
import psycopg2
from psycopg2.extras import DictCursor, execute_values
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------- Connections ----------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
PG_CONN = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": os.getenv("PG_PORT", "5432"),
    "dbname": os.getenv("PG_DB", "sports_dw"),
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASS", "postgres"),
}

mongo = pymongo.MongoClient(MONGO_URI)
db = mongo["sports_db"]

def pg_connect():
    return psycopg2.connect(cursor_factory=DictCursor, **PG_CONN)

# ---------- Utility helpers ----------
def safe_get(d, *keys, default=None):
    cur = d
    for k in keys:
        if cur is None:
            return default
        if isinstance(cur, dict):
            cur = cur.get(k, None)
        else:
            return default
    return cur if cur is not None else default

def to_date(datestr):
    if not datestr:
        return None
    # try common ISO forms
    try:
        if isinstance(datestr, (date, datetime)):
            return datestr.date() if isinstance(datestr, datetime) else datestr
        # remove timezone Z
        if datestr.endswith("Z"):
            datestr = datestr[:-1]
        return datetime.fromisoformat(datestr).date()
    except Exception:
        # try yyyy-mm-dd
        try:
            return datetime.strptime(datestr[:10], "%Y-%m-%d").date()
        except Exception:
            return None

# ---------- Ensure unique indexes for ON CONFLICT ----------
def ensure_unique_indexes(conn):
    # create unique indexes for api keys (idempotent)
    queries = [
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_team_api_team_id ON dim_team (api_team_id);",
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_driver_api_driver_id ON dim_driver (api_driver_id);",
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_team_f1_api_team_id ON dim_team_f1 (api_team_id);",
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_race_api_race_id ON dim_race (api_race_id);",
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_circuit_api_circuit_id ON dim_circuit (api_circuit_id);",
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_league_api_league_id ON dim_league (api_league_id);",
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_team_basket_api_team_id ON dim_team_basketball (api_team_id);",
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_player_basket_api_player_id ON dim_player_basketball (api_player_id);",
    ]
    cur = conn.cursor()
    for q in queries:
        cur.execute(q)
    conn.commit()
    cur.close()

# ---------- Upsert helpers (return surrogate key) ----------
def upsert_dim_time(conn, dt: date):
    if dt is None:
        return None
    cur = conn.cursor()
    # use date_date as unique natural key
    cur.execute("""
        INSERT INTO dim_time (date_date, year, month, day, weekday, is_weekend, hour)
        VALUES (%s, %s, %s, %s, %s, %s, NULL)
        ON CONFLICT (date_date) DO UPDATE SET
            year = EXCLUDED.year
        RETURNING time_key;
    """, (dt, dt.year, dt.month, dt.day, dt.strftime("%A"), dt.weekday() >= 5))
    row = cur.fetchone()
    conn.commit()
    cur.close()
    return row["time_key"]

def upsert_dim_team(conn, api_team_id, name=None, country=None, founded=None, stadium_name=None, city=None, short_code=None):
    if api_team_id is None:
        return None
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dim_team (api_team_id, name, country, founded, stadium_name, city, short_code)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (api_team_id) DO UPDATE SET
          name = COALESCE(EXCLUDED.name, dim_team.name),
          country = COALESCE(EXCLUDED.country, dim_team.country),
          founded = COALESCE(EXCLUDED.founded, dim_team.founded),
          stadium_name = COALESCE(EXCLUDED.stadium_name, dim_team.stadium_name),
          city = COALESCE(EXCLUDED.city, dim_team.city),
          short_code = COALESCE(EXCLUDED.short_code, dim_team.short_code)
        RETURNING team_key;
    """, (api_team_id, name, country, founded, stadium_name, city, short_code))
    row = cur.fetchone()
    conn.commit()
    cur.close()
    return row["team_key"]

def upsert_dim_league(conn, api_league_id, name=None, country=None, season=None):
    if api_league_id is None:
        return None
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dim_league (api_league_id, name, country, season)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (api_league_id) DO UPDATE SET
          name = COALESCE(EXCLUDED.name, dim_league.name),
          country = COALESCE(EXCLUDED.country, dim_league.country),
          season = COALESCE(EXCLUDED.season, dim_league.season)
        RETURNING league_key;
    """, (api_league_id, name, country, season))
    row = cur.fetchone()
    conn.commit()
    cur.close()
    return row["league_key"]

def upsert_dim_venue(conn, api_venue_id, name=None, city=None, capacity=None):
    if api_venue_id is None:
        return None
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dim_venue (api_venue_id, name, city, capacity)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (api_venue_id) DO UPDATE SET
          name = COALESCE(EXCLUDED.name, dim_venue.name),
          city = COALESCE(EXCLUDED.city, dim_venue.city),
          capacity = COALESCE(EXCLUDED.capacity, dim_venue.capacity)
        RETURNING venue_key;
    """, (api_venue_id, name, city, capacity))
    row = cur.fetchone()
    conn.commit()
    cur.close()
    return row["venue_key"]

def upsert_dim_referee(conn, api_referee_id, name=None, nationality=None):
    if api_referee_id is None:
        return None
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dim_referee (api_referee_id, name, nationality)
        VALUES (%s,%s,%s)
        ON CONFLICT (api_referee_id) DO UPDATE SET
          name = COALESCE(EXCLUDED.name, dim_referee.name),
          nationality = COALESCE(EXCLUDED.nationality, dim_referee.nationality)
        RETURNING referee_key;
    """, (api_referee_id, name, nationality))
    row = cur.fetchone()
    conn.commit()
    cur.close()
    return row["referee_key"]

# Soccer fact upsert
def upsert_fact_match(conn, doc):
    # extract api ids and measures from doc (supports common API-Football shape)
    api_match_id = safe_get(doc, "fixture", "id") or doc.get("id") or safe_get(doc, "id")
    league_obj = safe_get(doc, "league") or doc.get("league", {})
    league_api_id = safe_get(league_obj, "id")
    league_name = safe_get(league_obj, "name")
    season = safe_get(league_obj, "season") or doc.get("season")
    league_key = upsert_dim_league(conn, league_api_id, league_name, season)

    # time
    date_str = safe_get(doc, "fixture", "date") or safe_get(doc, "date")
    date_dt = to_date(date_str)
    time_key = upsert_dim_time(conn, date_dt)

    # venue
    venue = safe_get(doc, "fixture", "venue") or {}
    venue_key = None
    api_venue_id = safe_get(venue, "id")
    if api_venue_id:
        venue_key = upsert_dim_venue(conn, api_venue_id, safe_get(venue, "name"), safe_get(venue, "city"), safe_get(venue, "capacity"))

    # referee
    referee = safe_get(doc, "fixture", "referee") or {}
    referee_key = None
    # some responses provide referee object, sometimes just name; here we attempt to use id if exists
    api_referee_id = safe_get(referee, "id")
    referee_name = safe_get(referee, "name") or referee
    if api_referee_id or referee_name:
        referee_key = upsert_dim_referee(conn, api_referee_id or None, referee_name if isinstance(referee_name, str) else None, None)

    # teams
    teams = safe_get(doc, "teams") or {}
    home = safe_get(teams, "home") or {}
    away = safe_get(teams, "away") or {}
    home_api_id = safe_get(home, "id")
    away_api_id = safe_get(away, "id")
    home_key = upsert_dim_team(conn, home_api_id, safe_get(home, "name"))
    away_key = upsert_dim_team(conn, away_api_id, safe_get(away, "name"))

    # stats/goals
    goals = safe_get(doc, "goals") or {}
    home_goals = goals.get("home")
    away_goals = goals.get("away")

    # possible possession/stats under 'statistics' or 'stats'
    possession_home = None
    possession_away = None
    stats = safe_get(doc, "statistics") or safe_get(doc, "stats")
    if stats and isinstance(stats, list):
        # try to find possession entries if structure is list of dicts
        for item in stats:
            if safe_get(item, "type") == "Ball Possession" or safe_get(item, "type") == "Possession":
                # each has 'home'/'away' maybe under 'value' field
                home_val = safe_get(item, "home", "value") or safe_get(item, "home")
                away_val = safe_get(item, "away", "value") or safe_get(item, "away")
                try:
                    possession_home = Decimal(str(home_val).replace("%","")) if home_val is not None else None
                    possession_away = Decimal(str(away_val).replace("%","")) if away_val is not None else None
                except Exception:
                    pass

    # insert/update fact_match using api_match_id as natural key
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO fact_match (
            api_match_id, league_key, season, time_key, venue_key, referee_key,
            home_team_key, away_team_key, home_goals, away_goals,
            attendance, possession_home, possession_away
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (api_match_id) DO UPDATE SET
            home_goals = EXCLUDED.home_goals,
            away_goals = EXCLUDED.away_goals,
            attendance = COALESCE(EXCLUDED.attendance, fact_match.attendance),
            possession_home = COALESCE(EXCLUDED.possession_home, fact_match.possession_home),
            possession_away = COALESCE(EXCLUDED.possession_away, fact_match.possession_away)
        RETURNING match_key;
    """, (
        api_match_id, league_key, season, time_key, venue_key, referee_key,
        home_key, away_key, home_goals, away_goals,
        safe_get(doc, "fixture", "attendance") or None,
        possession_home, possession_away
    ))

    row = cur.fetchone()
    conn.commit()
    cur.close()
    return row["match_key"] if row else None

# ---------- Basketball dimension upserts ----------
def upsert_dim_team_basket(conn, api_team_id, name=None, city=None):
    if api_team_id is None:
        return None
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dim_team_basketball (api_team_id, name, city)
        VALUES (%s,%s,%s)
        ON CONFLICT (api_team_id) DO UPDATE SET
          name = COALESCE(EXCLUDED.name, dim_team_basketball.name),
          city = COALESCE(EXCLUDED.city, dim_team_basketball.city)
        RETURNING id;
    """, (api_team_id, name, city))
    res = cur.fetchone()
    conn.commit()
    cur.close()
    return res["id"]

def upsert_dim_player_basket(conn, api_player_id, full_name=None, position=None, nationality=None, birthdate=None):
    if api_player_id is None:
        return None
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dim_player_basketball (api_player_id, full_name, position, nationality, birthdate)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (api_player_id) DO UPDATE SET
          full_name = COALESCE(EXCLUDED.full_name, dim_player_basketball.full_name),
          position = COALESCE(EXCLUDED.position, dim_player_basketball.position),
          nationality = COALESCE(EXCLUDED.nationality, dim_player_basketball.nationality),
          birthdate = COALESCE(EXCLUDED.birthdate, dim_player_basketball.birthdate)
        RETURNING id;
    """, (api_player_id, full_name, position, nationality, birthdate))
    res = cur.fetchone()
    conn.commit()
    cur.close()
    return res["id"]

def upsert_dim_league_basket(conn, api_league_id, name=None, country=None):
    if api_league_id is None:
        return None
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dim_league_basketball (api_league_id, name, country)
        VALUES (%s,%s,%s)
        ON CONFLICT (api_league_id) DO UPDATE SET
          name = COALESCE(EXCLUDED.name, dim_league_basketball.name),
          country = COALESCE(EXCLUDED.country, dim_league_basketball.country)
        RETURNING id;
    """, (api_league_id, name, country))
    res = cur.fetchone()
    conn.commit()
    cur.close()
    return res["id"]

def upsert_dim_date(conn, dt: date):
    if dt is None:
        return None
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dim_date (date, year, month, day, day_of_week)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (date) DO NOTHING
        RETURNING id;
    """, (dt, dt.year, dt.month, dt.day, dt.strftime("%A")))
    row = cur.fetchone()
    if row:
        res = row["id"]
    else:
        cur.execute("SELECT id FROM dim_date WHERE date = %s", (dt,))
        res = cur.fetchone()["id"]
    conn.commit()
    cur.close()
    return res

def insert_fact_game_basketball(conn, doc):
    # doc expected from basketball API shape
    game_id = doc.get("id") or safe_get(doc, "game", "id")
    league = doc.get("league") or {}
    league_key = upsert_dim_league_basket(conn, league.get("id"), league.get("name"), league.get("country"))

    # date
    date_str = safe_get(doc, "date") or safe_get(doc, "fixture", "date")
    dt = to_date(date_str)
    date_id = upsert_dim_date(conn, dt)

    # teams
    home = safe_get(doc, "teams", "home") or {}
    away = safe_get(doc, "teams", "away") or {}
    home_id = upsert_dim_team_basket(conn, safe_get(home, "id"), safe_get(home, "name"))
    away_id = upsert_dim_team_basket(conn, safe_get(away, "id"), safe_get(away, "name"))

    # optional player-level stats (choose first player if present)
    player_id = None
    # if there's an array of players stats, choose the first player's api id
    players_stats = safe_get(doc, "players") or []
    if players_stats and isinstance(players_stats, list):
        first = players_stats[0]
        player_api = safe_get(first, "player", "id") or safe_get(first, "player", "player_id")
        player_id = upsert_dim_player_basket(conn, player_api, safe_get(first, "player", "name"))

    # measures: points/rebounds/etc may be in 'statistics' or 'scores'
    stats = safe_get(doc, "statistics") or {}
    points = safe_get(doc, "points") or safe_get(doc, "score") or None
    rebounds = safe_get(doc, "rebounds") or None
    assists = safe_get(doc, "assists") or None
    steals = safe_get(doc, "steals") or None
    blocks = safe_get(doc, "blocks") or None

    cur = conn.cursor()
    cur.execute("""
        INSERT INTO fact_game_basketball (date_id, team_home_id, team_away_id, player_id, league_id, points, rebounds, assists, steals, blocks)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING;
    """, (date_id, home_id, away_id, player_id, league_key, points, rebounds, assists, steals, blocks))
    conn.commit()
    cur.close()

# ---------- Formula 1 helpers ----------
def upsert_dim_driver(conn, api_driver_id, name=None, birthdate=None, nationality=None, number=None):
    if api_driver_id is None:
        return None
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dim_driver (api_driver_id, driver_name, birthdate, nationality, number)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (api_driver_id) DO UPDATE SET
          driver_name = COALESCE(EXCLUDED.driver_name, dim_driver.driver_name),
          birthdate = COALESCE(EXCLUDED.birthdate, dim_driver.birthdate),
          nationality = COALESCE(EXCLUDED.nationality, dim_driver.nationality),
          number = COALESCE(EXCLUDED.number, dim_driver.number)
        RETURNING driver_id;
    """, (api_driver_id, name, birthdate, nationality, number))
    res = cur.fetchone()
    conn.commit()
    cur.close()
    return res["driver_id"]

def upsert_dim_team_f1(conn, api_team_id, name=None, base=None, principal=None):
    if api_team_id is None:
        return None
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dim_team_f1 (api_team_id, team_name, base, principal)
        VALUES (%s,%s,%s,%s)
        ON CONFLICT (api_team_id) DO UPDATE SET
          team_name = COALESCE(EXCLUDED.team_name, dim_team_f1.team_name),
          base = COALESCE(EXCLUDED.base, dim_team_f1.base),
          principal = COALESCE(EXCLUDED.principal, dim_team_f1.principal)
        RETURNING team_id;
    """, (api_team_id, name, base, principal))
    res = cur.fetchone()
    conn.commit()
    cur.close()
    return res["team_id"]

def upsert_dim_race(conn, api_race_id, season=None, roundn=None, name=None, dateval=None):
    if api_race_id is None:
        return None
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dim_race (api_race_id, season, round, race_name, date)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (api_race_id) DO UPDATE SET
          season = COALESCE(EXCLUDED.season, dim_race.season),
          round = COALESCE(EXCLUDED.round, dim_race.round),
          race_name = COALESCE(EXCLUDED.race_name, dim_race.race_name),
          date = COALESCE(EXCLUDED.date, dim_race.date)
        RETURNING race_id;
    """, (api_race_id, season, roundn, name, dateval))
    res = cur.fetchone()
    conn.commit()
    cur.close()
    return res["race_id"]

def upsert_dim_circuit(conn, api_circuit_id, name=None, location=None, country=None, length_km=None):
    if api_circuit_id is None:
        return None
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dim_circuit (api_circuit_id, circuit_name, location, country, length_km)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (api_circuit_id) DO UPDATE SET
          circuit_name = COALESCE(EXCLUDED.circuit_name, dim_circuit.circuit_name),
          location = COALESCE(EXCLUDED.location, dim_circuit.location),
          country = COALESCE(EXCLUDED.country, dim_circuit.country),
          length_km = COALESCE(EXCLUDED.length_km, dim_circuit.length_km)
        RETURNING circuit_id;
    """, (api_circuit_id, name, location, country, length_km))
    res = cur.fetchone()
    conn.commit()
    cur.close()
    return res["circuit_id"]

def insert_fact_race_result(conn, doc):
    # doc assumed shape from API: contains driver id, team id, race id/circuit etc.
    api_race_id = safe_get(doc, "race", "id") or doc.get("raceId") or doc.get("race_id")
    api_driver_id = safe_get(doc, "driver", "id") or safe_get(doc, "driverId") or safe_get(doc, "driver", "driverId") or safe_get(doc, "driver", "api_id")
    api_team_id = safe_get(doc, "team", "id") or safe_get(doc, "constructorId") or safe_get(doc, "teamId")
    api_circuit_id = safe_get(doc, "circuit", "id") or safe_get(doc, "circuitId")
    season = safe_get(doc, "season") or safe_get(doc, "year")
    race_name = safe_get(doc, "race", "name") or safe_get(doc, "raceName")
    race_date = to_date(safe_get(doc, "race", "date") or safe_get(doc, "date"))

    driver_key = upsert_dim_driver(conn, api_driver_id, safe_get(doc, "driver", "name"), None, safe_get(doc, "driver", "nationality"), safe_get(doc, "driver", "number"))
    team_key = upsert_dim_team_f1(conn, api_team_id, safe_get(doc, "team", "name"))
    circuit_key = upsert_dim_circuit(conn, api_circuit_id, safe_get(doc, "circuit", "name"), safe_get(doc, "circuit", "location"), safe_get(doc, "circuit", "country"), safe_get(doc, "circuit", "length"))
    race_key = upsert_dim_race(conn, api_race_id, season, safe_get(doc, "race", "round") or safe_get(doc, "round"), race_name, race_date)

    position = safe_get(doc, "position") or safe_get(doc, "result", "position") or None
    points = safe_get(doc, "points") or safe_get(doc, "result", "points") or None
    laps = safe_get(doc, "laps") or safe_get(doc, "result", "laps") or None
    time = safe_get(doc, "time") or safe_get(doc, "result", "time")
    status = safe_get(doc, "status") or safe_get(doc, "result", "status")

    cur = conn.cursor()
    cur.execute("""
        INSERT INTO fact_race_results (driver_id, team_id, race_id, circuit_id, position, points, laps, time, status)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        RETURNING race_result_id;
    """, (driver_key, team_key, race_key, circuit_key, position, points, laps, time, status))
    rid = cur.fetchone()["race_result_id"]
    conn.commit()
    cur.close()
    return rid

# ---------- Main ETL functions ----------
def etl_soccer(conn):
    logging.info("Starting ETL Soccer")
    coll_names = [c for c in db.list_collection_names() if c.startswith("soccer")]
    # prefer soccer_fixtures or soccer_fixtures if present
    candidates = [n for n in coll_names if "fixtures" in n or "matches" in n or n == "soccer_fixtures" or n == "soccer_matches" or n == "soccer_fixtures"]
    if not candidates and "soccer" in coll_names:
        candidates = ["soccer"]
    total = 0
    for coll in candidates:
        cursor = db[coll].find()
        for doc in cursor:
            try:
                upsert_fact_match(conn, doc)
                total += 1
            except Exception as e:
                logging.exception("Error processing soccer doc: %s", e)
                conn.rollback()
    logging.info("Soccer ETL completed: processed %d records", total)

def etl_basketball(conn):
    logging.info("Starting ETL Basketball")
    coll_names = [c for c in db.list_collection_names() if c.startswith("basketball")]
    candidates = [n for n in coll_names if "games" in n or "games" in n or n == "basketball_games" or n == "basketball"]
    if not candidates and "basketball" in coll_names:
        candidates = ["basketball"]
    total = 0
    for coll in candidates:
        cursor = db[coll].find()
        for doc in cursor:
            try:
                insert_fact_game_basketball(conn, doc)
                total += 1
            except Exception as e:
                logging.exception("Error processing basketball doc: %s", e)
                conn.rollback()
    logging.info("Basketball ETL completed: processed %d records", total)

def etl_f1(conn):
    logging.info("Starting ETL Formula 1")
    coll_names = [c for c in db.list_collection_names() if c.startswith("f1") or c.startswith("formula")]
    # candidate collections: f1_races, f1_results, f1_drivers, f1_teams
    candidates = [n for n in coll_names if "results" in n or "races" in n or n == "f1" or n == "f1_results"]
    if not candidates and "f1" in coll_names:
        candidates = ["f1"]
    total = 0
    for coll in candidates:
        cursor = db[coll].find()
        for doc in cursor:
            try:
                insert_fact_race_result(conn, doc)
                total += 1
            except Exception as e:
                logging.exception("Error processing f1 doc: %s", e)
                conn.rollback()
    logging.info("F1 ETL completed: processed %d records", total)

# ---------- Runner ----------
def main():
    try:
        conn = pg_connect()
    except Exception as e:
        logging.error("Could not connect to Postgres: %s", e)
        sys.exit(1)

    # ensure unique indexes used for ON CONFLICT
    ensure_unique_indexes(conn)

    try:
        # run ETLs
        etl_soccer(conn)
        etl_basketball(conn)
    finally:
        conn.close()
        mongo.close()
    logging.info("ETL finished for all sports.")

if __name__ == "__main__":
    main()

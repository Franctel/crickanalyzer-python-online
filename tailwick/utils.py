

import pyodbc
import pandas as pd
import matplotlib
import dash_table, html
matplotlib.use('Agg')  # <-- Add this line

import os, sys
from pathlib import Path

import mysql.connector
import pandas as pd
import os, sys
from pathlib import Path

BATTER_DATA = {}
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

# Base directory (works in dev and PyInstaller)
def resource_path(relative_path):
    try:
        base_path = sys._MEIPASS  # PyInstaller temp folder
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from urllib.parse import quote_plus
import threading, json, os
import time   # ✅ Make sure time is imported

# ✅ PASTE THIS JUST BELOW IMPORTS
def _ttl_cache(seconds=300):
    def deco(fn):
        cache = {}
        def wrapper(*a, **k):
            key = (a, tuple(sorted(k.items())))
            now = time.time()
            if key in cache:
                val, ts = cache[key]
                if now - ts < seconds:
                    return val
            val = fn(*a, **k)
            cache[key] = (val, now)
            return val
        return wrapper
    return deco

# ----------------- Config + Connection Helper -----------------
# utils.py
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from urllib.parse import quote_plus
import threading, json, os

_ENGINE = None
_ENGINE_LOCK = threading.Lock()

def load_config():
    with open(os.path.join(os.path.dirname(__file__), "config.json"), "r") as f:
        return json.load(f)

def _get_engine():
    global _ENGINE
    if _ENGINE is None:
        with _ENGINE_LOCK:
            if _ENGINE is None:
                cfg = load_config()
                user = cfg.get("username")
                pwd = cfg.get("password")
                host = cfg.get("host", "localhost")
                port = cfg.get("port", 3306)
                db   = cfg.get("database")

                uri = f"mysql+pymysql://{user}:{quote_plus(pwd)}@{host}:{port}/{db}?charset=utf8mb4"

                _ENGINE = create_engine(
                    uri,
                    poolclass=QueuePool,
                    pool_size=3,          # small pool for low-RAM VPS
                    max_overflow=0,       # do NOT allow extra connections
                    pool_recycle=1800,    # refresh stale DB connections
                    pool_pre_ping=True,   # detect & drop dead connections
                )
    return _ENGINE

def get_connection():
    # always close() after using
    return _get_engine().connect()

def read_bbb_in_chunks(sql, params, chunk=20000):
    import pandas as pd
    conn = get_connection()
    try:
        it = pd.read_sql(sql, conn, params=params, chunksize=chunk)
        for part in it:
            yield part
    finally:
        conn.close()







#97.74.87.222 https://crickanalyzer.com/
#  $db['default'] = array(
#	'dsn'	=> '',
#	'hostname' => 'localhost',
#   'username' => 'cricketanalyzer_dbLite',
#	'password' => 'dbLiteuser',
#	'database' => 'cricketanalyzer_dbLite',
#	'dbdriver' => 'mysqli',
#	'dbprefix' => '',
#	'pconnect' => FALSE,
#	'db_debug' => (ENVIRONMENT !== 'production'),
#	'cache_on' => FALSE,
#	'cachedir' => '',
#	'char_set' => 'utf8',
#	'dbcollat' => 'utf8_general_ci',
#	'swap_pre' => '',
#	'encrypt' => FALSE,
#	'compress' => FALSE,
#	'stricton' => FALSE,
#	'failover' => array(),
#	'save_queries' => TRUE
#);

#============================ MySql Cloud DB Functions =============================

def get_all_teams():
    """
    Fetch all unique team names from tblscoremaster (across all tournaments).
    Used mainly for dashboards or admin views.
    """
    import pandas as pd
    try:
        conn = get_connection()

        query = """
            SELECT DISTINCT scrM_tmMIdBatting, scrM_tmMIdBattingName
            FROM tblscoremaster
            WHERE scrM_tmMIdBatting IS NOT NULL
              AND scrM_tmMIdBattingName IS NOT NULL
              AND scrM_tmMIdBattingName <> ''
        """
        df = pd.read_sql(query, conn)
        conn.close()

        teams = [
            {"id": int(row['scrM_tmMIdBatting']), "name": row['scrM_tmMIdBattingName']}
            for _, row in df.iterrows() if pd.notnull(row['scrM_tmMIdBatting'])
        ]
        teams = sorted(teams, key=lambda x: x['name'])
        print(f"✅ Loaded {len(teams)} total teams.")
        return teams

    except Exception as e:
        print("❌ Failed to fetch teams:", e)
        return []


def get_fiftyover_deliveries(match_name, team_id=None):
    import pandas as pd
    try:
        conn = get_connection()

        if team_id:
            query = """
                SELECT
                    scrM_OverNo,
                    scrM_DelNo,
                    scrM_PlayMIdStrikerName AS Batter,
                    scrM_PlayMIdBowlerName AS Bowler,
                    scrM_DelRuns,
                    scrM_IsWicket,
                    scrM_WideRuns,
                    scrM_NoBallRuns,
                    scrM_DeliveryType_zName,
                    scrM_FFRunsTarget,
                    scrM_tmMIdBattingName,
                    scrM_tmMIdBowlingName
                FROM tblscoremaster
                WHERE scrM_MatchName = %s
                  AND scrM_IsFFOver = 1
                  AND scrM_tmMIdBatting = %s
                ORDER BY scrM_OverNo, scrM_DelNo
            """
            params = (match_name, team_id)

        else:
            query = """
                SELECT
                    scrM_OverNo,
                    scrM_DelNo,
                    scrM_PlayMIdStrikerName AS Batter,
                    scrM_PlayMIdBowlerName AS Bowler,
                    scrM_DelRuns,
                    scrM_IsWicket,
                    scrM_WideRuns,
                    scrM_NoBallRuns,
                    scrM_DeliveryType_zName,
                    scrM_FFRunsTarget
                FROM tblscoremaster
                WHERE scrM_MatchName = %s
                  AND scrM_IsFFOver = 1
                ORDER BY scrM_OverNo, scrM_DelNo
            """
            params = (match_name,)

        df = pd.read_sql(query, conn, params=params)
        conn.close()

        if df.empty:
            return [], {'Runs': 0, 'Wkts': 0, 'Balls': 0, 'FFTarget': None}, None

        deliveries = []
        for _, r in df.iterrows():
            deliveries.append({
                'Over': int(r['scrM_OverNo']),
                'Ball': int(r['scrM_DelNo']),
                'Batter': r['Batter'],
                'Bowler': r['Bowler'],
                'Runs': int(r['scrM_DelRuns']),
                'IsWicket': int(r['scrM_IsWicket']),
                'Wide': int(r['scrM_WideRuns']),
                'NoBall': int(r['scrM_NoBallRuns']),
                'DeliveryType': r['scrM_DeliveryType_zName'],
                'FFTarget': int(r['scrM_FFRunsTarget']) if pd.notnull(r['scrM_FFRunsTarget']) else None
            })

        summary = {
            'Runs': int(df['scrM_DelRuns'].sum()),
            'Wkts': int(df['scrM_IsWicket'].sum()),
            'Balls': len(df),
            'FFTarget': int(df['scrM_FFRunsTarget'].dropna().iloc[0])
                        if not df['scrM_FFRunsTarget'].dropna().empty else None
        }

        return deliveries, summary, None

    except Exception as e:
        err = str(e)
        print(f"❌ Error in get_fiftyover_deliveries: {err}")
        return [], {'Runs': 0, 'Wkts': 0, 'Balls': 0, 'FFTarget': None}, err



 #@_ttl_cache(600)   # cache for 10 minutes
def get_all_tournaments(association_id=None):
    """
    Returns tournaments in {value,label} format.
    If association_id is provided, only tournaments for that association
    will be returned. Otherwise, returns all tournaments.
    """
    import pandas as pd
    try:
        conn = get_connection()

        if association_id:
            query = """
                SELECT t.trnM_Id, t.trnM_TournamentName
                FROM tbltournaments t
                WHERE t.trnM_TournamentName IS NOT NULL
                  AND t.trnM_AssociationId = %s
                ORDER BY t.trnM_TournamentName
            """
            df = pd.read_sql(query, conn, params=(association_id,))
        else:
            query = """
                SELECT t.trnM_Id, t.trnM_TournamentName
                FROM tbltournaments t
                WHERE t.trnM_TournamentName IS NOT NULL
                ORDER BY t.trnM_TournamentName
            """
            df = pd.read_sql(query, conn)

        conn.close()

        tournaments = [
            {"id": int(row['trnM_Id']), "name": row['trnM_TournamentName']}
            for _, row in df.iterrows()
        ]
        print(f"✅ Loaded tournaments for association_id={association_id}: {len(tournaments)} found")
        return tournaments

    except Exception as e:
        print("❌ Failed to fetch tournaments:", e)
        return []


def get_data_from_db(team1, team2):
    """
    Fetch data for two teams (batting + bowling) from MySQL.
    Ensures only valid balls are returned.
    """
    import pandas as pd
    try:
        conn = get_connection()
        print("✅ DB connected!")

        query = """
            SELECT *
            FROM tblscoremaster
            WHERE scrM_tmMIdBattingName IN (%s, %s)
              AND scrM_tmMIdBowlingName IN (%s, %s)
              AND scrM_IsValidBall = 1
        """
        df = pd.read_sql(query, conn, params=(team1, team2, team1, team2))
        conn.close()

        print(f"✅ Loaded {len(df)} rows from DB for {team1} vs {team2}.")
        return df

    except Exception as e:
        print("❌ DB error:", e)
        return pd.DataFrame()


def get_match_format_by_tournament(tournament_id):
    """
    Fetch the match format (e.g., T20, ODI, Test) for a given tournament.
    """
    import pandas as pd
    try:
        conn = get_connection()

        result = pd.read_sql(
            """
            SELECT z.z_Name AS format
            FROM tbltournaments t
            JOIN tblz z ON t.trnM_MatchFormat_z = z.z_Id
            WHERE t.trnM_Id = %s
            """,
            conn,
            params=(tournament_id,)
        )
        conn.close()

        return result.iloc[0]["format"] if not result.empty else None

    except Exception as e:
        print("❌ Match format fetch error:", e)
        return None


def get_teams_by_tournament(tournament_id):
    """
    Returns distinct teams for a given tournament id,
    ensuring they belong only to matches from that tournament.
    This prevents overlap when same team short name (e.g., CSK) exists in multiple tournaments.
    """
    import pandas as pd
    if not tournament_id:
        return []

    try:
        conn = get_connection()
        query = """
            SELECT DISTINCT s.scrM_tmMIdBatting, s.scrM_tmMIdBattingName
            FROM tblscoremaster s
            INNER JOIN tblmatchmaster m ON s.scrM_MchMId = m.mchM_Id
            INNER JOIN tbltournaments t ON m.mchM_TrnMId = t.trnM_Id
            WHERE t.trnM_Id = %s
              AND s.scrM_tmMIdBatting IS NOT NULL
              AND s.scrM_tmMIdBattingName IS NOT NULL
              AND s.scrM_tmMIdBattingName <> ''
            ORDER BY s.scrM_tmMIdBattingName
        """
        df = pd.read_sql(query, conn, params=(tournament_id,))
        conn.close()

        if df.empty:
            print(f"⚠️ No teams found for tournament {tournament_id}")
            return []

        teams = [
            {"id": int(row['scrM_tmMIdBatting']), "name": row['scrM_tmMIdBattingName']}
            for _, row in df.iterrows() if pd.notnull(row['scrM_tmMIdBatting'])
        ]
        teams = sorted(teams, key=lambda x: x['name'])
        print(f"✅ Filtered {len(teams)} teams for tournament {tournament_id}: {teams}")
        return teams

    except Exception as e:
        print("❌ Teams by tournament error:", e)
        return []



def get_matches_by_team(team, tournament_id=None):
    """
    Returns distinct matches for a given team,
    restricted to the given tournament if provided.
    Uses tblmatchmaster to ensure correct tournament-year filtering.
    (Fixes MySQL 8 DISTINCT + ORDER BY compatibility)
    """
    import pandas as pd
    if not team:
        return []

    try:
        conn = get_connection()

        if tournament_id:
            query = """
                SELECT DISTINCT m.mchM_Id, m.mchM_MatchName, m.mchM_StartDateTime
                FROM tblmatchmaster m
                INNER JOIN tblscoremaster s ON s.scrM_MchMId = m.mchM_Id
                WHERE (s.scrM_tmMIdBatting = %s OR s.scrM_tmMIdBowling = %s)
                  AND m.mchM_TrnMId = %s
                  AND m.mchM_MatchName IS NOT NULL
                ORDER BY m.mchM_StartDateTime DESC
            """
            df = pd.read_sql(query, conn, params=(team, team, tournament_id))
        else:
            query = """
                SELECT DISTINCT m.mchM_Id, m.mchM_MatchName, m.mchM_StartDateTime
                FROM tblmatchmaster m
                INNER JOIN tblscoremaster s ON s.scrM_MchMId = m.mchM_Id
                WHERE (s.scrM_tmMIdBatting = %s OR s.scrM_tmMIdBowling = %s)
                  AND m.mchM_MatchName IS NOT NULL
                ORDER BY m.mchM_StartDateTime DESC
            """
            df = pd.read_sql(query, conn, params=(team, team))

        conn.close()

        if df.empty:
            print(f"⚠️ No matches found for team {team} (tournament={tournament_id})")
            return []

        matches = [
            {"id": int(row["mchM_Id"]), "label": row["mchM_MatchName"]}
            for _, row in df.iterrows() if pd.notnull(row["mchM_Id"])
        ]
        print(f"✅ Found {len(matches)} matches for team={team}, tournament={tournament_id}")
        return matches

    except Exception as e:
        print("❌ Matches by team error:", e)
        return []




 
def get_days_innings_sessions_by_matches(match_ids):
    """
    Fetch distinct days, innings, and sessions for the given match IDs.
    Works with MySQL (pymysql).
    """
    import pandas as pd

    try:
        if not match_ids:
            return [], [], []

        # ✅ Ensure match_ids clean
        match_ids = [str(x) for x in match_ids if x]

        placeholders = ",".join(["%s"] * len(match_ids))
        query = f"""
            SELECT DISTINCT scrM_DayNo, scrM_InningNo, scrM_SessionNo
            FROM tblscoremaster
            WHERE scrM_MchMId IN ({placeholders})
        """

        conn = get_connection()
        df = pd.read_sql(query, conn, params=tuple(match_ids))
        conn.close()

        days = sorted(df["scrM_DayNo"].dropna().unique(), key=lambda x: int(x))
        innings = sorted(df["scrM_InningNo"].dropna().unique(), key=lambda x: int(x))
        sessions = sorted(df["scrM_SessionNo"].dropna().unique(), key=lambda x: int(x))

        print(f"✅ Found {len(days)} days, {len(innings)} innings, {len(sessions)} sessions")
        return days, innings, sessions

    except Exception as e:
        print("❌ Error getting days/innings/sessions:", e)
        return [], [], []






def get_players_by_match(match_ids, day=None, inning=None, session=None):
    """
    ✅ Returns batter + bowler list as dicts:
    [
      {"id": 123, "name": "Rohit Sharma (RHB)"},
      ...
    ]
    So UI shows name, backend queries by id.
    """

    import pandas as pd

    if not match_ids:
        return [], []

    match_ids = [str(x).strip() for x in match_ids if str(x).strip()]

    conn = get_connection()
    try:
        placeholders = ",".join(["%s"] * len(match_ids))

        query = f"""
            SELECT
                s.scrM_PlayMIdStriker AS batter_id,
                s.scrM_PlayMIdStrikerName AS batter_name,
                MAX(NULLIF(TRIM(s.scrM_StrikerBatterSkill), '')) AS batter_skill,

                s.scrM_PlayMIdBowler AS bowler_id,
                s.scrM_PlayMIdBowlerName AS bowler_name,
                MAX(NULLIF(TRIM(s.scrM_BowlerSkill), '')) AS bowler_skill

            FROM tblscoremaster s
            WHERE s.scrM_MchMId IN ({placeholders})
              AND s.scrM_IsValidBall = 1
        """

        params = list(match_ids)

        if day:
            query += " AND s.scrM_DayNo = %s"
            params.append(int(day))

        if inning:
            query += " AND s.scrM_InningNo = %s"
            params.append(int(inning))

        if session:
            query += " AND s.scrM_SessionNo = %s"
            params.append(int(session))

        query += """
            GROUP BY
                s.scrM_PlayMIdStriker, s.scrM_PlayMIdStrikerName,
                s.scrM_PlayMIdBowler, s.scrM_PlayMIdBowlerName
        """

        df = pd.read_sql(query, conn, params=tuple(params))

        if df.empty:
            return [], []

        # ✅ Normalize skills like (RHB)/(LHB)
        def normalize_skill(x):
            if pd.isna(x):
                return ""
            x = str(x).strip().upper()
            # extract short code inside brackets if exists
            import re
            m = re.search(r"\(([A-Z]+)\)", x)
            if m:
                return m.group(1)
            return x

        df["batter_skill"] = df["batter_skill"].apply(normalize_skill)
        df["bowler_skill"] = df["bowler_skill"].apply(normalize_skill)

        # ✅ Build batter list (unique)
        bat_df = (
            df[["batter_id", "batter_name", "batter_skill"]]
            .dropna(subset=["batter_id"])
            .drop_duplicates(subset=["batter_id"])
        )
        batters = []
        for _, r in bat_df.iterrows():
            pid = str(r["batter_id"]).strip()
            nm = str(r["batter_name"]).strip()
            sk = str(r["batter_skill"]).strip()
            label = f"{nm} ({sk})" if sk else nm
            batters.append({"id": pid, "name": label})

        # ✅ Build bowler list (unique)
        bowl_df = (
            df[["bowler_id", "bowler_name", "bowler_skill"]]
            .dropna(subset=["bowler_id"])
            .drop_duplicates(subset=["bowler_id"])
        )
        bowlers = []
        for _, r in bowl_df.iterrows():
            pid = str(r["bowler_id"]).strip()
            nm = str(r["bowler_name"]).strip()
            sk = str(r["bowler_skill"]).strip()
            label = f"{nm} ({sk})" if sk else nm
            bowlers.append({"id": pid, "name": label})

        return batters, bowlers

    except Exception as e:
        print("❌ get_players_by_match error:", e)
        return [], []
    finally:
        conn.close()







def get_filtered_score_data(
    conn,
    match_ids,
    batters=None,
    bowlers=None,
    inning=None,
    session=None,
    day=None,
    phase=None,
    from_over=None,
    to_over=None,
    type=None,
    ball_phase=None
):
    import pandas as pd

    # ✅ match_ids must be list like ["880","881"]
    if not match_ids:
        return pd.DataFrame()

    # ✅ Ensure match IDs clean (ONLY numeric IDs)
    match_ids = [str(x).strip() for x in match_ids if str(x).strip().isdigit()]
    if not match_ids:
        return pd.DataFrame()

    id_placeholders = ",".join(["%s"] * len(match_ids))

    # ✅ Base query
    query = f"""
        SELECT *
        FROM tblscoremaster
        WHERE scrM_MchMId IN ({id_placeholders})
          AND scrM_IsValidBall = 1
    """
    params = list(match_ids)

    # ✅ Batter filter (ID based) -------------- FIXED
    if batters:
        batters = [str(x).strip() for x in batters if str(x).strip().isdigit()]
        if batters:
            bat_placeholders = ",".join(["%s"] * len(batters))
            query += f" AND scrM_PlayMIdStriker IN ({bat_placeholders})"
            params.extend(batters)

    # ✅ Bowler filter (ID based) -------------- FIXED
    if bowlers:
        bowlers = [str(x).strip() for x in bowlers if str(x).strip().isdigit()]
        if bowlers:
            bowl_placeholders = ",".join(["%s"] * len(bowlers))
            query += f" AND scrM_PlayMIdBowler IN ({bowl_placeholders})"
            params.extend(bowlers)

    # ✅ inning/session/day -------------------- FIXED (safe int)
    if inning:
        query += " AND scrM_InningNo = %s"
        params.append(int(inning))

    if session:
        query += " AND scrM_SessionNo = %s"
        params.append(int(session))

    if day:
        query += " AND scrM_DayNo = %s"
        params.append(int(day))

    # ✅ Over range ---------------------------- FIXED (safe int + allow float overs)
    if from_over or to_over:
        try:
            from_over_val = int(float(from_over)) if from_over not in (None, "", "None") else None
        except:
            from_over_val = None

        try:
            to_over_val = int(float(to_over)) if to_over not in (None, "", "None") else None
        except:
            to_over_val = None

        if from_over_val is not None and to_over_val is not None:
            query += " AND scrM_OverNo BETWEEN %s AND %s"
            params.extend([from_over_val, to_over_val])
        elif from_over_val is not None:
            query += " AND scrM_OverNo >= %s"
            params.append(from_over_val)
        elif to_over_val is not None:
            query += " AND scrM_OverNo <= %s"
            params.append(to_over_val)

    # ✅ Sorting
    query += " ORDER BY scrM_InningNo, scrM_OverNo, scrM_DelNo"

    # ✅ Execute
    df = pd.read_sql(query, conn, params=tuple(params))
    print(f"✅ get_filtered_score_data loaded {len(df)} rows")

    if df is None or df.empty:
        return pd.DataFrame()

    # ✅ Convert numeric for safe filtering ---------------- FIXED (more columns)
    for col in ["scrM_OverNo", "scrM_DelNo", "scrM_InningNo", "scrM_IsValidBall"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    # ✅ PHASE FILTER (optional) --------------------------- FIXED (skip if column missing)
    if phase and "scrM_OverNo" in df.columns:
        sel_phase = str(phase).strip().lower()

        try:
            # Default T20
            pp_end = 6
            middle_end = 15

            if sel_phase not in ("all", "", "none"):
                if "power" in sel_phase:
                    df = df[df["scrM_OverNo"] <= pp_end]
                elif "middle" in sel_phase:
                    df = df[(df["scrM_OverNo"] > pp_end) & (df["scrM_OverNo"] <= middle_end)]
                elif "death" in sel_phase or "slog" in sel_phase:
                    df = df[df["scrM_OverNo"] > middle_end]

        except Exception as e:
            print("⚠️ phase filtering failed:", e)

    # ✅ BALL PHASE FILTER (optional) ---------------------- FIXED (safe group col)
    if ball_phase and "scrM_OverNo" in df.columns and "scrM_DelNo" in df.columns:
        sel_bp = str(ball_phase).strip().lower()

        try:
            df["ball_index"] = (df["scrM_OverNo"] - 1) * 6 + df["scrM_DelNo"]

            key_col = "scrM_PlayMIdStriker" if (str(type).lower() == "batter") else "scrM_PlayMIdBowler"

            if key_col in df.columns:
                phase_dfs = []
                for _, sub in df.groupby(key_col, group_keys=False):
                    sub = sub.sort_values(["scrM_OverNo", "scrM_DelNo"]).reset_index(drop=True)
                    total = len(sub)

                    if total <= 10:
                        phase_dfs.append(sub)
                        continue

                    if "first" in sel_bp:
                        phase_dfs.append(sub.head(10))
                    elif "last" in sel_bp:
                        phase_dfs.append(sub.tail(10))
                    elif "middle" in sel_bp:
                        if total > 20:
                            phase_dfs.append(sub.iloc[10:-10])
                        else:
                            mid_start = max(0, total // 2 - 2)
                            mid_end = min(total, total // 2 + 2)
                            phase_dfs.append(sub.iloc[mid_start:mid_end])

                if phase_dfs:
                    df = pd.concat(phase_dfs, ignore_index=True)

        except Exception as e:
            print("⚠️ ball_phase filtering failed:", e)

    # ✅ Attach match name safely -------------------------- FIXED (conn reuse safe)
    try:
        match_df = pd.read_sql(
            f"""
            SELECT mchM_Id, mchM_MatchName
            FROM tblmatchmaster
            WHERE mchM_Id IN ({id_placeholders})
            """,
            conn,
            params=tuple(match_ids)
        )

        if not match_df.empty:
            id_to_name = dict(zip(match_df["mchM_Id"].astype(str), match_df["mchM_MatchName"]))
            if "scrM_MchMId" in df.columns:
                df["MatchName"] = df["scrM_MchMId"].astype(str).map(id_to_name)

    except Exception as e:
        print("⚠️ Could not map match ids to names:", e)

    return df




import pandas as pd
import pyodbc

def get_ball_by_ball_details(
    match_ids,
    batters=None,
    bowlers=None,
    inning=None,
    session=None,
    day=None,
    from_over=None,
    to_over=None,
    player_id=None,
    view_type="batter"
):
    """
    Fetch ball-by-ball details from MySQL with optional filters.
    ✅ Uses Match IDs (scrM_MchMId)
    ✅ Batter/Bowler filters are ID based
    ✅ Returns Team IDs also (scrM_tmMIdBatting, scrM_tmMIdBowling)
    """

    import pandas as pd

    if not match_ids:
        return pd.DataFrame()

    # ✅ Ensure match_ids clean
    match_ids = [str(x).strip() for x in match_ids if str(x).strip()]

    conn = None
    try:
        conn = get_connection()

        match_placeholders = ",".join(["%s"] * len(match_ids))

        query = f"""
            SELECT
                s.scrM_DelId,
                s.scrM_MchMId,
                s.scrM_MatchName,

                s.scrM_DayNo,
                s.scrM_SessionNo,
                s.scrM_InningNo,

                s.scrM_OverNo,
                s.scrM_DelNo,

                s.scrM_PitchXPos,
                s.scrM_BatPitchXPos,

                s.scrM_IsBoundry,
                s.scrM_IsSixer,

                s.scrM_WideRuns,
                s.scrM_NoBallRuns,
                s.scrM_ByeRuns,
                s.scrM_LegByeRuns,
                s.scrM_PenaltyRuns,

                s.scrM_DeliveryType_zName,

                -- ✅ TEAM IDS + NAMES (IMPORTANT)
                s.scrM_tmMIdBatting,
                s.scrM_tmMIdBattingName,
                s.scrM_tmMIdBowling,
                s.scrM_tmMIdBowlingName,

                -- ✅ PLAYER IDS + NAMES
                s.scrM_PlayMIdStriker,
                s.scrM_PlayMIdStrikerName,
                s.scrM_PlayMIdNonStrikerName,

                s.scrM_PlayMIdBowler,
                s.scrM_PlayMIdBowlerName,

                s.scrM_IsValidBall,
                s.scrM_BatsmanRuns,
                s.scrM_DelRuns,

                s.scrM_IsWicket,
                s.scrM_IsNoBall,
                s.scrM_IsWideBall,

                s.scrM_playMIdCaughtName,
                s.scrM_playMIdRunOutName,
                s.scrM_playMIdStumpingName,

                s.scrM_PitchArea_zName,
                s.scrM_BatPitchArea_zName,

                s.scrM_ShotType_zName,
                s.scrM_BowlerSkill,

                s.scrM_Wagon_x,
                s.scrM_Wagon_y,
                s.scrM_WagonArea_z,
                s.scrM_WagonArea_zName,

                s.scrM_PitchX,
                s.scrM_PitchY,

                s.scrM_StrikerBatterSkill,
                s.scrM_DecisionFinal_zName,

                s.scrM_PitchXPos AS scrM_Line,
                s.scrM_PitchYPos AS scrM_Length,

                s.scrM_Video1FileName,
                s.scrM_Video2FileName,
                s.scrM_Video3FileName,
                s.scrM_Video4FileName,
                s.scrM_Video5FileName,
                s.scrM_Video6FileName,

                s.scrM_Video1URL,
                s.scrM_Video2URL,
                s.scrM_Video3URL,
                s.scrM_Video4URL,
                s.scrM_Video5URL,
                s.scrM_Video6URL,

                s.scrM_IsBeaten,
                s.scrM_IsUncomfort

            FROM tblscoremaster s
            WHERE s.scrM_MchMId IN ({match_placeholders})
        """

        params = list(match_ids)

        # ---------------- ✅ BATTER FILTER (ID based) ----------------
        if batters:
            batters = [str(x).strip() for x in batters if str(x).strip().isdigit()]
            if batters:
                query += " AND s.scrM_PlayMIdStriker IN ({})".format(
                    ",".join(["%s"] * len(batters))
                )
                params.extend(batters)

        # ---------------- ✅ BOWLER FILTER (ID based) ----------------
        if bowlers:
            bowlers = [str(x).strip() for x in bowlers if str(x).strip().isdigit()]
            if bowlers:
                query += " AND s.scrM_PlayMIdBowler IN ({})".format(
                    ",".join(["%s"] * len(bowlers))
                )
                params.extend(bowlers)

        # ---------------- ✅ PLAYER DROPDOWN FILTER ----------------
        if player_id and str(player_id).strip().isdigit():
            pid = int(str(player_id).strip())
            if str(view_type).lower().strip() == "bowler":
                query += " AND s.scrM_PlayMIdBowler = %s"
            else:
                query += " AND s.scrM_PlayMIdStriker = %s"
            params.append(pid)

        # ---------------- ✅ OTHER FILTERS ----------------
        if inning:
            query += " AND s.scrM_InningNo = %s"
            params.append(int(inning))

        if session:
            query += " AND s.scrM_SessionNo = %s"
            params.append(int(session))

        if day:
            query += " AND s.scrM_DayNo = %s"
            params.append(int(day))

        if from_over or to_over:
            if from_over and to_over:
                query += " AND s.scrM_OverNo BETWEEN %s AND %s"
                params.extend([int(from_over), int(to_over)])
            elif from_over:
                query += " AND s.scrM_OverNo >= %s"
                params.append(int(from_over))
            elif to_over:
                query += " AND s.scrM_OverNo <= %s"
                params.append(int(to_over))

        query += " ORDER BY s.scrM_InningNo, s.scrM_OverNo, s.scrM_DelNo"

        df = pd.read_sql(query, conn, params=tuple(params))

        if not df.empty:
            df["commentary"] = df.apply(generate_commentary, axis=1)

        print(f"✅ get_ball_by_ball_details loaded {len(df)} rows | player_id={player_id} | view_type={view_type}")
        return df

    except Exception as e:
        print(f"❌ Error in get_ball_by_ball_details: {e}")
        return pd.DataFrame()

    finally:
        if conn:
            conn.close()










#========================== MSSQL Functions =========================
# def get_all_teamsMSSQL():
#     connection_string = """
#         DRIVER={ODBC Driver 17 for SQL Server};
#         SERVER=DESKTOP-CE11INB\SQLEXPRESS;
#         DATABASE=dbCrick;
#         Trusted_Connection=yes;
#     """
#     try:
#         conn = get_connection()

#         query = """
#             SELECT DISTINCT scrM_tmMIdBattingName
#             FROM tblscoremaster
#             WHERE scrM_tmMIdBattingName IS NOT NULL
#         """
#         df = pd.read_sql(query, conn)
#         conn.close()

#         teams = sorted(df['scrM_tmMIdBattingName'].dropna().unique())
#         return teams
#     except Exception as e:
#         print("Failed to fetch teams:", e)
#         return []
    
# # === GET DATA FOR TWO TEAMS ===
# def get_data_from_dbMSSQL(team1, team2):
#     connection_string = """
#         DRIVER={ODBC Driver 17 for SQL Server};
#         SERVER=DESKTOP-CE11INB\SQLEXPRESS;
#         DATABASE=dbCrick;
#         Trusted_Connection=yes;
#     """
#     try:
#         conn = get_connection()

#         print("DB connected!")

#         query = """
#             SELECT *
#             FROM tblscoremaster
#             WHERE scrM_tmMIdBattingName IN (?, ?)
#               AND scrM_tmMIdBowlingName IN (?, ?)
#               AND scrM_IsValidBall = 1
#         """

#         df = pd.read_sql(query, conn, params=[team1, team2, team1, team2])
#         conn.close()

#         print(f"Loaded {len(df)} rows from DB.")
#         return df
#     except Exception as e:
#         print("DB error:", e)
#         return pd.DataFrame()
    
# def get_all_tournamentsMSSQL():
#     connection_string = """
#         DRIVER={ODBC Driver 17 for SQL Server};
#         SERVER=DESKTOP-CE11INB\SQLEXPRESS;
#         DATABASE=dbCrick;
#         Trusted_Connection=yes;
#     """
#     try:
#         conn = get_connection()

#         query = """
#             SELECT 
#                 t.trnM_Id, 
#                 t.trnM_TournamentName, 
#                 z.z_Name AS match_format
#             FROM tbltournaments t
#             LEFT JOIN tblz z ON t.trnM_MatchFormat_z = z.z_Id
#             WHERE t.trnM_TournamentName IS NOT NULL
#             ORDER BY t.trnM_TournamentName
#         """
#         df = pd.read_sql(query, conn)
#         conn.close()

#         options = []
#         for _, row in df.iterrows():
#             format_name = row['match_format'] or "Unknown"
#             label = f"{row['trnM_TournamentName']} ({format_name})"
#             options.append({
#                 "label": label,
#                 "value": row['trnM_Id'],
#                 "format": format_name.lower()  # e.g., "t20", "multiday"
#             })

#         print(f"✅ Fetched {len(options)} tournaments")
#         return options

#     except Exception as e:
#         print("❌ Failed to fetch tournaments:", e)
#         return []
    
# def get_match_format_by_tournamentMSSQL(tournament_id):
#     connection_string = """
#         DRIVER={ODBC Driver 17 for SQL Server};
#         SERVER=DESKTOP-CE11INB\SQLEXPRESS;
#         DATABASE=dbCrick;
#         Trusted_Connection=yes;
#     """
#     try:
#         conn = get_connection()

#         result = pd.read_sql("""
#             SELECT z.z_Name AS format
#             FROM tbltournaments t
#             JOIN tblz z ON t.trnM_MatchFormat_z = z.z_Id
#             WHERE t.trnM_Id = ?
#         """, conn, params=[tournament_id])
#         conn.close()
#         return result.iloc[0]['format'] if not result.empty else None
#     except Exception as e:
#         print("⚠️ Match format fetch error:", e)
#         return None

# # Get teams based on selected tournament
# def get_teams_by_tournamentMSSQL(tournament_name):
#     connection_string = """
#         DRIVER={ODBC Driver 17 for SQL Server};
#         SERVER=DESKTOP-CE11INB\SQLEXPRESS;
#         DATABASE=dbCrick;
#         Trusted_Connection=yes;
#     """
#     try:
#         conn = get_connection()

#         query = """
#             SELECT DISTINCT scrM_tmMIdBattingName 
#             FROM tblscoremaster 
#             WHERE scrM_TrnMId = ? AND scrM_tmMIdBattingName IS NOT NULL
#         """
#         df = pd.read_sql(query, conn, params=[tournament_name])
#         conn.close()
#         return sorted(df['scrM_tmMIdBattingName'].dropna().unique())
#     except Exception as e:
#         print("❌ Teams by tournament error:", e)
#         return []


# # Get matches based on selected team (batting or bowling)
# def get_matches_by_teamMSSQL(team):
#     connection_string = """
#         DRIVER={ODBC Driver 17 for SQL Server};
#         SERVER=DESKTOP-CE11INB\SQLEXPRESS;
#         DATABASE=dbCrick;
#         Trusted_Connection=yes;
#     """
#     try:
#         conn = get_connection()

#         query = """
#             SELECT DISTINCT scrM_MatchName 
#             FROM tblscoremaster 
#             WHERE (scrM_tmMIdBattingName = ? OR scrM_tmMIdBowlingName = ?) AND scrM_MatchName IS NOT NULL
#         """
#         df = pd.read_sql(query, conn, params=[team, team])
#         conn.close()
#         return sorted(df['scrM_MatchName'].dropna().unique())
#     except Exception as e:
#         print("❌ Matches by team error:", e)
#         return []
    
# def get_days_innings_sessions_by_matchesMSSQL(matches):
#     connection_string = """
#         DRIVER={ODBC Driver 17 for SQL Server};
#         SERVER=DESKTOP-CE11INB\SQLEXPRESS;
#         DATABASE=dbCrick;
#         Trusted_Connection=yes;
#     """
#     try:
#         if not matches:
#             return [], [], []

#         placeholders = ",".join("?" for _ in matches)
#         query = f"""
#             SELECT DISTINCT scrM_DayNo, scrM_InningNo, scrM_SessionNo
#             FROM tblscoremaster
#             WHERE scrM_MatchName IN ({placeholders})
#         """
#         conn = get_connection()

#         df = pd.read_sql(query, conn, params=matches)
#         conn.close()

#         days = sorted(df['scrM_DayNo'].dropna().unique(), key=lambda x: int(x))
#         innings = sorted(df['scrM_InningNo'].dropna().unique(), key=lambda x: int(x))
#         sessions = sorted(df['scrM_SessionNo'].dropna().unique(), key=lambda x: int(x))
#         return days, innings, sessions

#     except Exception as e:
#         print("❌ Error getting days/innings/sessions:", e)
#         return [], [], []

# def get_players_by_matchMSSQL(matches, day=None, inning=None, session=None):
#     if not matches:
#         return [], []

#     connection_string = """
#         DRIVER={ODBC Driver 17 for SQL Server};
#         SERVER=DESKTOP-CE11INB\SQLEXPRESS;
#         DATABASE=dbCrick;
#         Trusted_Connection=yes;
#     """
#     try:
#         conn = get_connection()

#         placeholders = ','.join(['?'] * len(matches))
#         query = f"""
#             SELECT DISTINCT scrM_PlayMIdStrikerName AS Batter,
#                             scrM_PlayMIdBowlerName AS Bowler
#             FROM tblscoremaster
#             WHERE scrM_MatchName IN ({placeholders})
#               AND scrM_PlayMIdStrikerName IS NOT NULL
#               AND scrM_PlayMIdBowlerName IS NOT NULL
#         """
#         params = list(matches)

#         if day:
#             query += " AND scrM_DayNo = ?"
#             params.append(int(day))
#         if inning:
#             query += " AND scrM_InningNo = ?"
#             params.append(int(inning))
#         if session:
#             query += " AND scrM_SessionNo = ?"
#             params.append(int(session))

#         df = pd.read_sql(query, conn, params=params)
#         conn.close()

#         batters = sorted(df["Batter"].dropna().unique().tolist())
#         bowlers = sorted(df["Bowler"].dropna().unique().tolist())
#         return batters, bowlers

#     except Exception as e:
#         print("❌ get_players_by_match Error:", e)
#         return [], []


# def get_filtered_score_dataMSSQL(
#     conn, match_names, batters=None, bowlers=None, inning=None,
#     session=None, day=None, phase=None, from_over=None, to_over=None, type=None, ball_phase=None
# ):
#     import pandas as pd

#     # 🔄 Convert match names to IDs
#     match_df = pd.read_sql(
#         "SELECT mchM_Id, mchM_MatchName FROM tblmatchmaster WHERE mchM_MatchName IN ({})".format(
#             ','.join(['?'] * len(match_names))
#         ),
#         conn, params=match_names
#     )
#     match_id_map = dict(zip(match_df['mchM_MatchName'], match_df['mchM_Id']))
#     match_ids = list(match_id_map.values())

#     # 🔍 Base query
#     query = """
#     SELECT * FROM tblscoremaster
#     WHERE scrM_MchMId IN ({match_ids})
#     AND scrM_IsValidBall = 1
#     """.format(match_ids=','.join(['?'] * len(match_ids)))

#     params = match_ids

#     # ⛳ Filters
#     if batters:
#         query += " AND scrM_PlayMIdStrikerName IN ({})".format(','.join(['?'] * len(batters)))
#         params += batters

#     if bowlers:
#         query += " AND scrM_PlayMIdBowlerName IN ({})".format(','.join(['?'] * len(bowlers)))
#         params += bowlers

#     if inning:
#         query += " AND scrM_InningNo = ?"
#         params.append(inning)

#     if session:
#         query += " AND scrM_SessionNo = ?"
#         params.append(session)

#     if from_over and to_over:
#         query += " AND scrM_OverNo BETWEEN ? AND ?"
#         params.extend([int(from_over), int(to_over)])

#     df = pd.read_sql(query, conn, params=params)
#     df['MatchName'] = df['scrM_MchMId'].map({v: k for k, v in match_id_map.items()})
#     return df

#========================================================================
#===========Non query functions
    
def create_team_order_summary(df, inn):
    if df[df["scrM_InningNo"] == inn].empty:
        return None, None

    bat_team = df[df["scrM_InningNo"] == inn]["scrM_tmMIdBattingName"].dropna().unique()
    if len(bat_team) == 0:
        return None, None

    bat_team = bat_team[0]
    df_inn = df[(df["scrM_IsValidBall"] == 1) & (df["scrM_InningNo"] == inn)].copy()
    if df_inn.empty:
        return bat_team, None

    df_inn["BallNumber"] = df_inn.groupby("scrM_InningNo")["scrM_PlayMIdStrikerName"].transform(lambda x: pd.factorize(x)[0])
    df_inn["Order No"] = pd.cut(df_inn["BallNumber"], bins=[-1, 2, 5, float("inf")], labels=["Top Order", "Middle Order", "Lower Order"])

    def count_runs(d, val): return (d["scrM_DelRuns"] == val).sum()

    summary_rows = []
    for order in ["Top Order", "Middle Order", "Lower Order"]:
        group = df_inn[df_inn["Order No"] == order]
        if not group.empty:
            balls = group.shape[0]
            summary_rows.append({
                "Innings": f"{bat_team} Inn-{inn}", "Order": order,
                "Runs": group["scrM_DelRuns"].sum(),
                "S/R": round(group["scrM_DelRuns"].sum() / balls * 100, 2) if balls else 0,
                "Dots%": round(count_runs(group, 0) / balls * 100, 1) if balls else 0,
                "1s%": round(count_runs(group, 1) / balls * 100, 1),
                "2s%": round(count_runs(group, 2) / balls * 100, 1),
                "3s%": round(count_runs(group, 3) / balls * 100, 1),
                "Fours%": round(group["scrM_IsBoundry"].sum() / balls * 100, 1),
                "Sixers%": round(group["scrM_IsSixer"].sum() / balls * 100, 1),
            })

    # Team total
    if not df_inn.empty:
        balls = df_inn.shape[0]
        summary_rows.append({
            "Innings": f"{bat_team} Inn-{inn}", "Order": "Team Total",
            "Runs": df_inn["scrM_DelRuns"].sum(),
            "S/R": round(df_inn["scrM_DelRuns"].sum() / balls * 100, 2),
            "Dots%": round(count_runs(df_inn, 0) / balls * 100, 1),
            "1s%": round(count_runs(df_inn, 1) / balls * 100, 1),
            "2s%": round(count_runs(df_inn, 2) / balls * 100, 1),
            "3s%": round(count_runs(df_inn, 3) / balls * 100, 1),
            "Fours%": round(df_inn["scrM_IsBoundry"].sum() / balls * 100, 1),
            "Sixers%": round(df_inn["scrM_IsSixer"].sum() / balls * 100, 1),
        })

    return bat_team, pd.DataFrame(summary_rows)

def render_summary_table(team_name, df):
    if df.empty:
        return f"""
            <div class="text-red-500 text-center">No summary available for {team_name}.</div>
        """

    headers = df.columns
    rows = df.to_dict(orient='records')

    html = """
    <div class="overflow-x-auto max-w-full xl:max-w-[95%] mx-auto">
        <table class="w-auto min-w-[500px] text-xs bg-custom-50 dark:bg-custom-500/10">
            <thead class="ltr:text-left rtl:text-right bg-custom-100 dark:bg-custom-500/10">
                <tr>
    """

    for col in headers:
        html += f"""<th class="px-2.5 py-2 font-semibold border-b border-custom-200 dark:border-custom-900">{col}</th>"""

    html += "</tr></thead><tbody>"

    for row in rows:
        html += "<tr>"
        for val in row.values():
            html += f"""<td class="px-2.5 py-2 border-y border-custom-200 dark:border-custom-900">{val}</td>"""
        html += "</tr>"

    html += "</tbody></table></div>"

    return html

def make_summary_table(team_name, data, orders):
    if data.empty:
        return f"<div class='text-red-500 text-center'>No summary available for {team_name}.</div>"

    data = data[data['Order'].isin(orders)]
    summary_rows = []

    for order in orders:
        group = data[data['Order'] == order]
        if not group.empty:
            summary_rows.append({
                "No of Innings": len(group),
                "Order": order,
                "Runs": group["Runs"].sum(),
                "S/R": round(group["S/R"].mean(), 2),
                "Average": round(group["Runs"].sum() / len(group), 2),
                "Dots%": round(group["Dots%"].mean(), 1),
                "1s%": round(group["1s%"].mean(), 1),
                "2s%": round(group["2s%"].mean(), 1),
                "3s%": round(group["3s%"].mean(), 1),
                "Fours%": round(group["Fours%"].mean(), 1),
                "Sixers%": round(group["Sixers%"].mean(), 1),
            })

    if not summary_rows:
        return f"<div class='text-red-500 text-center'>No summary available for {team_name}.</div>"

    headers = summary_rows[0].keys()
    html = """
    <div class="overflow-x-auto max-w-full xl:max-w-[95%] mx-auto">
        <table class="w-auto min-w-[500px] text-xs bg-custom-50 dark:bg-custom-500/10">
            <thead class="ltr:text-left rtl:text-right bg-custom-100 dark:bg-custom-500/10">
                <tr>
    """

    for header in headers:
        html += f"""<th class="px-2.5 py-2 font-semibold border-b border-custom-200 dark:border-custom-900">{header}</th>"""
    html += "</tr></thead><tbody>"

    for row in summary_rows:
        html += "<tr>"
        for val in row.values():
            html += f"""<td class="px-2.5 py-2 border-y border-custom-200 dark:border-custom-900">{val}</td>"""
        html += "</tr>"

    html += "</tbody></table></div>"
    return html


import os
from urllib.parse import quote

def generate_commentary(ball):
    """Generates a descriptive commentary string for a given ball.
       Uses scrM_Video{i}URL for video references (not displayed directly)."""

    bowler = ball.get('scrM_PlayMIdBowlerName', 'Bowler')
    batter = ball.get('scrM_PlayMIdStrikerName', 'Batter')
    delivery_type = ball.get('scrM_DeliveryType_zName')
    shot_type = ball.get('scrM_ShotType_zName')
    runs = ball.get('scrM_BatsmanRuns', 0)
    is_wicket = ball.get('scrM_IsWicket', 0)
    wicket_type = ball.get('scrM_DecisionFinal_zName')

    # Combine all possible fielder names
    fielder = (
        ball.get('scrM_playMIdCaughtName')
        or ball.get('scrM_playMIdRunOutName')
        or ball.get('scrM_playMIdStumpingName')
    )

    # Collect available video URLs (not displayed, but stored for JS hooks)
    urls = []
    for i in range(1, 7):
        url_field = f"scrM_Video{i}URL"
        if ball.get(url_field):
            urls.append(str(ball[url_field]))

    # Start the commentary
    commentary = f"{bowler} to {batter}"

    # Add delivery type
    if delivery_type and str(delivery_type).lower().strip() not in ['nan', 'none', '']:
        commentary += f", {delivery_type}"

    # Add shot type
    if shot_type and str(shot_type).lower().strip() not in ['nan', 'none', '']:
        commentary += f", {shot_type}"

    # Add runs or wicket info
    if is_wicket == 1 and wicket_type and str(wicket_type).lower().strip() not in ['nan', 'none', '']:
        wicket_details = f"OUT ({wicket_type}"
        if fielder and str(fielder).lower().strip() not in ['nan', 'none', '']:
            wicket_details += f" by {fielder}"
        wicket_details += ")"
        commentary += f', <span class="text-red-500 font-semibold">{wicket_details}</span>'
    else:
        if runs == 1:
            commentary += ", 1 run"
        elif runs > 1:
            commentary += f", {runs} runs"
        else:
            commentary += ", no run"

    # Attach video URLs for JS to pick up later (if needed)
    if urls:
        commentary = f'<span class="commentary-line" data-videos="{",".join(urls)}">{commentary}</span>'
    else:
        commentary = f'<span class="commentary-line">{commentary}</span>'

    return commentary

def safe_int(x):
    try:
        return int(float(x))
    except:
        return None


def get_dismissal_text(row):
    """
    Matches CodeIgniter dismissal text logic exactly.
    Pulls correct fielder names from DB fields.
    """

    wicket_type = row.get("scrM_DecisionFinal_zName", "")
    bowler = row.get("scrM_PlayMIdBowlerName", "")

    caught = row.get("scrM_PlayMIdFielderName", "")
    runout = row.get("scrM_playMIdRunOutName", "")
    stumped = row.get("scrM_PlayMIdFielderName", "")
    wicket_fielder = row.get("scrM_PlayMIdFielderName", "")

    if not wicket_type:
        return "not out"

    wicket_type = wicket_type.strip()

    # ----------------------
    # Caught
    # ----------------------
    if wicket_type == "Caught":

        # If fielder missing → fallback
        if not caught:
            caught = wicket_fielder

        # 👉 NEW LOGIC: Caught & Bowled
        if caught and bowler and caught.strip().lower() == bowler.strip().lower():
            return f"c & b {bowler}"

        return f"c {caught} b {bowler}"

    # ----------------------
    # Bowled
    # ----------------------
    if wicket_type == "Bowled":
        return f"b {bowler}"

    # ----------------------
    # LBW
    # ----------------------
    if wicket_type == "LBW":
        return f"lbw {bowler}"

    # ----------------------
    # Run Out
    # ----------------------
    if wicket_type == "Run Out":
        fielder = runout if runout else wicket_fielder
        return f"run out ({fielder})"

    # ----------------------
    # Stumped
    # ----------------------
    if wicket_type == "Stumped":
        if not stumped:
            stumped = wicket_fielder
        return f"st {stumped} b {bowler}"

    # ----------------------
    # Hit Wicket
    # ----------------------
    if wicket_type == "Hit Wicket":
        return f"hit wicket b {bowler}"

    # ----------------------
    # Default
    # ----------------------
    return f"{wicket_type} b {bowler}"



def generate_line_length_report(df, selected_metric=None, is_single_match=False, selected_type="batter"):
    """
    Enhanced Line & Length analysis — supports metric-based display (Average, SR, Dot Ball %, Wicket %, etc.)
    and returns complete structure (heatmap_data, totals_by_length, totals_by_line, pitch_points)
    for both Batter/Bowler filters and Metric dropdown filters.
    
    Args:
        df: DataFrame with ball-by-ball data
        selected_metric: Metric to display (Strike Rate, Dot Ball %, etc.)
        is_single_match: Boolean indicating if single match is selected (True = show counts, False = show percentages)
    """

    import pandas as pd
    from collections import defaultdict

    if df is None or df.empty or 'scrM_PitchX' not in df.columns or 'scrM_PitchY' not in df.columns:
        return {
            'heatmap_data': {},
            'totals': {'balls': 0, 'runs': 0, 'boundaries': 0, 'wickets': 0},
            'table_data': [],
            'pitch_points': [],
            'totals_by_length': {},
            'totals_by_line': {}
        }

    df_ll = df.copy()

    # Convert to numeric and drop NaNs
    df_ll['scrM_PitchX'] = pd.to_numeric(df_ll['scrM_PitchX'], errors='coerce')
    df_ll['scrM_PitchY'] = pd.to_numeric(df_ll['scrM_PitchY'], errors='coerce')
    df_ll = df_ll.dropna(subset=['scrM_PitchX', 'scrM_PitchY'])
    # Remove rows where either coordinate is 0 (garbage values)
    df_ll = df_ll[(df_ll['scrM_PitchX'] != 0) & (df_ll['scrM_PitchY'] != 0)]

    if df_ll.empty:
        return {
            'heatmap_data': {},
            'totals': {'balls': 0, 'runs': 0, 'boundaries': 0, 'wickets': 0},
            'table_data': [],
            'pitch_points': [],
            'totals_by_length': {},
            'totals_by_line': {}
        }
    
    # Define bins
    line_bins = [-float('inf'), 50, 70, 80, 84, 88, 95, float('inf')]
    line_labels = ['Way Outside Off', 'Outside Off', 'Just Outside Off', 'Off Stump',
                   'Middle Stump', 'Leg Stump', 'Outside Leg']
    
    length_bins = [-float('inf'), 93, 107.5, 128, 150.5, 177, 205, float('inf')]
    length_labels = ['Fulltoss', 'Yorker', 'Full Length', 'Overpitch', 'Good Length', 'Short of Good', 'Short Pitch']
    
    df_ll['LineZone'] = pd.cut(df_ll['scrM_PitchX'], bins=line_bins, labels=line_labels, right=False)
    df_ll['LengthZone'] = pd.cut(df_ll['scrM_PitchY'], bins=length_bins, labels=length_labels, right=False)
    
    # Create combined zone
    df_ll['Zone'] = df_ll['LengthZone'].astype(str) + '-' + df_ll['LineZone'].astype(str)

    # ✅ Boundaries & wickets
    df_ll['fours'] = (df_ll.get('scrM_IsBoundry', 0) == 1).astype(int)
    df_ll['sixes'] = (df_ll.get('scrM_IsSixer', 0) == 1).astype(int)
    df_ll['boundaries'] = df_ll['fours'] + df_ll['sixes']

    # Safe wicket flag
    if 'scrM_IsWicket' in df_ll.columns:
        df_ll['is_wicket'] = df_ll['scrM_IsWicket'].fillna(0).astype(int)
    elif 'scrM_IsWicketBall' in df_ll.columns:
        df_ll['is_wicket'] = df_ll['scrM_IsWicketBall'].fillna(0).astype(int)
    elif 'scrM_WicketType' in df_ll.columns:
        df_ll['is_wicket'] = df_ll['scrM_WicketType'].apply(
            lambda x: 0 if pd.isna(x) or str(x).strip() in ("", "0", "None", "nan") else 1
        ).astype(int)
    else:
        df_ll['is_wicket'] = 0

    total_balls = len(df_ll)
    total_runs = int(df_ll.get('scrM_BatsmanRuns', pd.Series(dtype=int)).sum() if 'scrM_BatsmanRuns' in df_ll.columns else 0)
    total_boundaries = int(df_ll['boundaries'].sum())
    total_wickets = int(df_ll['is_wicket'].sum())

    # ✅ Aggregate zone-wise
    zone_summary = df_ll.groupby('Zone').agg(
        balls=('Zone', 'count'),
        runs=('scrM_BatsmanRuns', 'sum'),
        boundaries=('boundaries', 'sum'),
        wickets=('is_wicket', 'sum')
    ).reset_index()

    zone_summary['balls_percentage'] = (zone_summary['balls'] / total_balls * 100) if total_balls > 0 else 0

    # ✅ Heatmap data skeleton - use predefined zones
    heatmap_data = {}
    all_zones = [f"{length}-{line}" for length in length_labels for line in line_labels]
    for zone in all_zones:
        heatmap_data[zone] = {
            'balls': 0, 'runs': 0, 'boundaries': 0, 'wickets': 0,
            'balls_percentage': 0.0, 'display_text': '', 'metric_value': 0.0
        }

    # ✅ Fill heatmap data
    for _, row in zone_summary.iterrows():
        zone = row['Zone']
        balls = int(row['balls'])
        runs = int(row['runs'])
        wickets = int(row['wickets'])
        boundaries = int(row['boundaries'])
        dots = df_ll[(df_ll['Zone'] == zone) & (df_ll['scrM_BatsmanRuns'] == 0)].shape[0]

        metric_value = 0.0
        display_text = f"{balls} b<br>{runs} r<br>{boundaries} B, {wickets} W"

        if selected_metric:
            try:
                if selected_metric == "Strike Rate":

                    if selected_type == "bowler":
                        # ✅ Bowling Strike Rate = Balls per wicket (NO ×100)
                        metric_value = round((balls / wickets), 2) if wickets > 0 else 0

                    else:
                        # ✅ Batting Strike Rate = Runs per 100 balls
                        metric_value = round((runs / balls) * 100, 2) if balls > 0 else 0

                    label = "BSR" if selected_type == "bowler" else "SR"
                    display_text = f"{label}<br>{metric_value}"


                elif selected_metric == "Dot Ball %":
                    if is_single_match:
                        metric_value = dots
                        display_text = f"Dots<br>{int(dots)}"
                    else:
                        metric_value = round((dots / balls) * 100, 2) if balls > 0 else 0
                        display_text = f"Dot%<br>{metric_value}"
                elif selected_metric == "Boundary %":
                    if is_single_match:
                        metric_value = boundaries
                        display_text = f"Bound<br>{int(boundaries)}"
                    else:
                        metric_value = round((boundaries / balls) * 100, 2) if balls > 0 else 0
                        display_text = f"Bound%<br>{metric_value}"
                elif selected_metric == "Average":
                    metric_value = round((runs / wickets), 2) if wickets > 0 else 0
                    display_text = f"Avg<br>{metric_value}"
                elif selected_metric == "Balls Per Dismissal":
                    metric_value = round((balls / wickets), 2) if wickets > 0 else 0
                    display_text = f"BPD<br>{metric_value}"
                elif selected_metric == "Ball %":
                    if is_single_match:
                        metric_value = balls
                        display_text = f"Balls<br>{int(balls)}"
                    else:
                        metric_value = round((balls / total_balls) * 100, 2) if total_balls > 0 else 0
                        display_text = f"Ball%<br>{metric_value}"
                elif selected_metric == "Wicket":
                    metric_value = wickets
                    display_text = f"<b>{wickets}</b><br>Wkt" if wickets > 0 else "0"
            except Exception as e:
                print(f"⚠️ Metric calc failed for {zone}: {e}")

        if zone in heatmap_data:
            heatmap_data[zone] = {
                'balls': balls,
                'runs': runs,
                'boundaries': boundaries,
                'wickets': wickets,
                'balls_percentage': round(row['balls_percentage'], 1),
                'display_text': display_text,
                'metric_value': metric_value
            }

    # ✅ Totals by length and line
    length_agg = defaultdict(lambda: {"balls": 0, "runs": 0, "boundaries": 0, "wickets": 0, "dots": 0})
    line_agg = defaultdict(lambda: {"balls": 0, "runs": 0, "boundaries": 0, "wickets": 0, "dots": 0})

    for _, r in df_ll.iterrows():
        length = str(r.get('LengthZone', '')).strip()
        line = str(r.get('LineZone', '')).strip()

        runs = int(r.get('scrM_BatsmanRuns') or 0)
        boundaries = int(r.get('boundaries') or 0)
        is_wicket = int(r.get('is_wicket') or 0)
        dot = 1 if runs == 0 else 0

        length_agg[length]['balls'] += 1
        length_agg[length]['runs'] += runs
        length_agg[length]['boundaries'] += boundaries
        length_agg[length]['wickets'] += is_wicket
        length_agg[length]['dots'] += dot

        line_agg[line]['balls'] += 1
        line_agg[line]['runs'] += runs
        line_agg[line]['boundaries'] += boundaries
        line_agg[line]['wickets'] += is_wicket
        line_agg[line]['dots'] += dot

    def compute_metric(agg, total_balls_all):
        b, r, bd, w, dots = agg['balls'], agg['runs'], agg['boundaries'], agg['wickets'], agg['dots']
        mv = 0.0
        if selected_metric:
            if selected_metric == "Strike Rate":
                if selected_type == "bowler":
                    mv = round((b / w), 2) if w > 0 else 0
                else:
                    mv = round((r / b) * 100, 2) if b > 0 else 0

            elif selected_metric == "Dot Ball %":
                if is_single_match:
                    mv = int(dots)
                else:
                    mv = round((dots / b) * 100, 2) if b > 0 else 0
            elif selected_metric == "Boundary %":
                if is_single_match:
                    mv = int(bd)
                else:
                    mv = round((bd / b) * 100, 2) if b > 0 else 0
            elif selected_metric == "Average":
                mv = round((r / w), 2) if w > 0 else 0
            elif selected_metric == "Balls Per Dismissal":
                mv = round((b / w), 2) if w > 0 else 0
            elif selected_metric == "Ball %":
                if is_single_match:
                    mv = int(b)
                else:
                    mv = round((b / total_balls_all) * 100, 2) if total_balls_all > 0 else 0
            elif selected_metric == "Wicket":
                mv = w
            else:
                mv = r
        else:
            mv = r
        return mv

    totals_by_length = {}
    for length_label, agg in length_agg.items():
        mv = compute_metric(agg, total_balls)
        # For SR Bowler, show SR value in label if wickets > 0, else just the length label
        if selected_metric == "SR Bowler":
            if agg['wickets'] > 0:
                label = f"{length_label} (SR: {round(agg['balls']/agg['wickets'], 2)})"
            else:
                label = f"{length_label}"
        else:
            label = f"{length_label} ({agg['balls']})"
        totals_by_length[length_label] = {
            "balls": int(agg['balls']),
            "runs": int(agg['runs']),
            "boundaries": int(agg['boundaries']),
            "wickets": int(agg['wickets']),
            "dots": int(agg['dots']),
            "metric_value": mv,
            "display_label": label
        }

    totals_by_line = {}
    for line_label, agg in line_agg.items():
        mv = compute_metric(agg, total_balls)
        totals_by_line[line_label] = {
            "balls": int(agg['balls']),
            "runs": int(agg['runs']),
            "boundaries": int(agg['boundaries']),
            "wickets": int(agg['wickets']),
            "dots": int(agg['dots']),
            "metric_value": mv
        }

    # ✅ Totals footer
    totals = {
        'balls': int(total_balls),
        'runs': int(total_runs),
        'boundaries': int(total_boundaries),
        'wickets': int(total_wickets)
    }

    # ✅ Collect pitch points for playback (keep scrM_PitchX/Y for pitch pad visualization)
    pitch_cols = [
         'scrM_DelId',
        'scrM_PitchX', 'scrM_PitchY', 'scrM_BatsmanRuns', 'is_wicket',
        'scrM_IsBoundry', 'scrM_IsSixer',
        'scrM_StrikerBatterSkill', 'scrM_BowlerSkill',
        'scrM_Video1URL', 'scrM_Video2URL', 'scrM_Video3URL',
        'scrM_Video4URL', 'scrM_Video5URL', 'scrM_Video6URL',
        'LineZone', 'LengthZone'  # Needed for Zone
    ]
    pitch_cols = [c for c in pitch_cols if c in df_ll.columns]

    pitch_points = df_ll[pitch_cols].to_dict(orient='records')
    # Add Zone field to each pitch point for frontend compatibility
    # for p in pitch_points:
    #     line = str(p.get('LineZone', ''))
    #     length = str(p.get('LengthZone', ''))
    #     p['Zone'] = f"{length}-{line}"

    for p in pitch_points:
        line = str(p.get('LineZone', '')).strip()
        length = str(p.get('LengthZone', '')).strip()
        p['zone_key'] = f"{length}-{line}"
        p['del_id'] = p.get('scrM_DelId')

    # ✅ Final return
    return {
        'heatmap_data': heatmap_data,
        'totals': totals,
        'table_data': zone_summary.to_dict(orient='records'),
        'pitch_points': pitch_points,
        'totals_by_length': totals_by_length,
        'totals_by_line': totals_by_line
    }



def generate_areawise_report(df):
    if df.empty or 'scrM_WagonArea_zName' not in df.columns:
        return None

    df_area = df.copy()
    df_area = df_area.dropna(subset=['scrM_WagonArea_zName'])
    df_area = df_area[df_area['scrM_WagonArea_zName'].str.strip() != '']

    if df_area.empty:
        return None

    # Calculate scoring shots
    df_area['ones'] = (df_area['scrM_BatsmanRuns'] == 1).astype(int)
    df_area['twos'] = (df_area['scrM_BatsmanRuns'] == 2).astype(int)
    df_area['fours'] = (df_area['scrM_IsBoundry'] == 1).astype(int)
    df_area['sixes'] = (df_area['scrM_IsSixer'] == 1).astype(int)

    # Area summary for charts
    area_summary = df_area.groupby('scrM_WagonArea_zName').agg(
        total_runs=('scrM_BatsmanRuns', 'sum'),
        ones=('ones', 'sum'),
        twos=('twos', 'sum'),
        fours=('fours', 'sum'),
        sixes=('sixes', 'sum')
    ).reset_index()

    chart_data = {
        'labels': area_summary['scrM_WagonArea_zName'].tolist(),
        'series': [
            {'name': 'Ones', 'data': area_summary['ones'].tolist()},
            {'name': 'Twos', 'data': area_summary['twos'].tolist()},
            {'name': 'Fours', 'data': area_summary['fours'].tolist()},
            {'name': 'Sixes', 'data': area_summary['sixes'].tolist()}
        ]
    }

    # ✅ Use direct online video URLs instead of offline FileNames
    video_cols = [f'scrM_Video{i}URL' for i in range(1, 7)]
    df_area['video_urls'] = df_area[video_cols].apply(
        lambda row: [
            str(x)
            for x in row if pd.notna(x) and str(x).strip() != ""
        ],
        axis=1
    )

    # Aggregation logic
    agg_logic = {
        'runs': ('scrM_BatsmanRuns', 'sum'),
        'balls': ('scrM_WagonArea_zName', 'count'),
        'fours': ('fours', 'sum'),
        'sixes': ('sixes', 'sum'),
        'ones': ('ones', 'sum'),
        'twos': ('twos', 'sum'),
        'video_urls': ('video_urls', lambda x: sum(x, []))
    }

    grouped_data = df_area.groupby(
        ['scrM_PlayMIdStrikerName', 'scrM_WagonArea_zName']
    ).agg(**agg_logic).reset_index()

    if grouped_data.empty:
        return {
            'chart_data': chart_data,
            'strikers_data': {}
        }

    # Strike rate calculation
    grouped_data['strike_rate'] = (
        grouped_data['runs'] / grouped_data['balls'] * 100
    ).round(2)

    # Rename for frontend
    grouped_data.rename(columns={
        'scrM_PlayMIdStrikerName': 'striker',
        'scrM_WagonArea_zName': 'area_name'
    }, inplace=True)

    # Build striker-wise dict
    strikers_data = {}
    for striker_name, striker_df in grouped_data.groupby('striker'):
        strikers_data[striker_name] = striker_df.to_dict(orient='records')

    return {
        'chart_data': chart_data,
        'strikers_data': strikers_data
    }


def generate_shottype_report(df):
    if df.empty or 'scrM_ShotType_zName' not in df.columns:
        return None

    df_shottype = df.copy()
    df_shottype = df_shottype.dropna(subset=['scrM_ShotType_zName'])
    df_shottype = df_shottype[df_shottype['scrM_ShotType_zName'].str.strip() != '']

    if df_shottype.empty:
        return None

    # Scoring shots
    df_shottype['ones'] = (df_shottype['scrM_BatsmanRuns'] == 1).astype(int)
    df_shottype['twos'] = (df_shottype['scrM_BatsmanRuns'] == 2).astype(int)
    df_shottype['fours'] = (df_shottype['scrM_IsBoundry'] == 1).astype(int)
    df_shottype['sixes'] = (df_shottype['scrM_IsSixer'] == 1).astype(int)

    # Shot type summary
    shottype_summary = df_shottype.groupby('scrM_ShotType_zName').agg(
        total_runs=('scrM_BatsmanRuns', 'sum'),
        ones=('ones', 'sum'),
        twos=('twos', 'sum'),
        fours=('fours', 'sum'),
        sixes=('sixes', 'sum')
    ).reset_index()

    shottype_summary = shottype_summary[shottype_summary['total_runs'] > 0]

    chart_data = {
        'labels': shottype_summary['scrM_ShotType_zName'].tolist(),
        'series': [
            {'name': 'Ones', 'data': shottype_summary['ones'].tolist()},
            {'name': 'Twos', 'data': shottype_summary['twos'].tolist()},
            {'name': 'Fours', 'data': shottype_summary['fours'].tolist()},
            {'name': 'Sixes', 'data': shottype_summary['sixes'].tolist()}
        ]
    }

    # ✅ Use direct online video URLs instead of offline FileNames
    video_cols = [f'scrM_Video{i}URL' for i in range(1, 7)]
    df_shottype['video_urls'] = df_shottype[video_cols].apply(
        lambda row: [
            str(x)
            for x in row if pd.notna(x) and str(x).strip() != ""
        ],
        axis=1
    )

    # Aggregation logic
    agg_logic = {
        'runs': ('scrM_BatsmanRuns', 'sum'),
        'balls': ('scrM_ShotType_zName', 'count'),
        'fours': ('fours', 'sum'),
        'sixes': ('sixes', 'sum'),
        'ones': ('ones', 'sum'),
        'twos': ('twos', 'sum'),
        'video_urls': ('video_urls', lambda x: sum(x, []))
    }

    grouped_data = df_shottype.groupby(
        ['scrM_PlayMIdStrikerName', 'scrM_ShotType_zName']
    ).agg(**agg_logic).reset_index()

    if grouped_data.empty:
        return {
            'chart_data': chart_data,
            'strikers_data': {}
        }

    # Only keep records with runs > 0
    grouped_data = grouped_data[grouped_data['runs'] > 0].copy()
    grouped_data['strike_rate'] = (
        grouped_data['runs'] / grouped_data['balls'] * 100
    ).round(2)

    # Rename for frontend
    grouped_data.rename(columns={
        'scrM_PlayMIdStrikerName': 'striker',
        'scrM_ShotType_zName': 'shot_type'
    }, inplace=True)

    # Build striker-wise dict
    strikers_data = {}
    for striker_name, striker_df in grouped_data.groupby('striker'):
        strikers_data[striker_name] = striker_df.to_dict(orient='records')

    return {
        'chart_data': chart_data,
        'strikers_data': strikers_data
    }



def generate_deliverytype_report(df):
    if df.empty or 'scrM_DeliveryType_zName' not in df.columns:
        return None

    df_deliverytype = df.copy()
    df_deliverytype = df_deliverytype.dropna(subset=['scrM_DeliveryType_zName'])
    df_deliverytype = df_deliverytype[df_deliverytype['scrM_DeliveryType_zName'].str.strip() != '']

    if df_deliverytype.empty:
        return None

    # Scoring shots
    df_deliverytype['ones'] = (df_deliverytype['scrM_BatsmanRuns'] == 1).astype(int)
    df_deliverytype['twos'] = (df_deliverytype['scrM_BatsmanRuns'] == 2).astype(int)
    df_deliverytype['fours'] = (df_deliverytype['scrM_IsBoundry'] == 1).astype(int)
    df_deliverytype['sixes'] = (df_deliverytype['scrM_IsSixer'] == 1).astype(int)

    # Delivery type summary
    deliverytype_summary = df_deliverytype.groupby('scrM_DeliveryType_zName').agg(
        total_runs=('scrM_BatsmanRuns', 'sum'),
        ones=('ones', 'sum'),
        twos=('twos', 'sum'),
        fours=('fours', 'sum'),
        sixes=('sixes', 'sum')
    ).reset_index()

    # Only keep delivery types with total runs > 0
    deliverytype_summary = deliverytype_summary[deliverytype_summary['total_runs'] > 0]

    chart_data = {
        'labels': deliverytype_summary['scrM_DeliveryType_zName'].tolist(),
        'series': [
            {'name': 'Ones', 'data': deliverytype_summary['ones'].tolist()},
            {'name': 'Twos', 'data': deliverytype_summary['twos'].tolist()},
            {'name': 'Fours', 'data': deliverytype_summary['fours'].tolist()},
            {'name': 'Sixes', 'data': deliverytype_summary['sixes'].tolist()}
        ]
    }

    # ✅ Use direct online video URLs instead of offline FileNames
    video_cols = [f'scrM_Video{i}URL' for i in range(1, 7)]
    df_deliverytype['video_urls'] = df_deliverytype[video_cols].apply(
        lambda row: [
            str(x)
            for x in row if pd.notna(x) and str(x).strip() != ""
        ],
        axis=1
    )

    # Aggregation logic
    agg_logic = {
        'runs': ('scrM_BatsmanRuns', 'sum'),
        'balls': ('scrM_DeliveryType_zName', 'count'),
        'fours': ('fours', 'sum'),
        'sixes': ('sixes', 'sum'),
        'ones': ('ones', 'sum'),
        'twos': ('twos', 'sum'),
        'video_urls': ('video_urls', lambda x: sum(x, []))  # merge all lists
    }

    grouped_data = df_deliverytype.groupby(
        ['scrM_PlayMIdStrikerName', 'scrM_DeliveryType_zName']
    ).agg(**agg_logic).reset_index()

    # If no grouped data, return empty structure
    if grouped_data.empty:
        return {
            'chart_data': chart_data,
            'strikers_data': {}
        }

    # Keep only rows with runs > 0
    grouped_data = grouped_data[grouped_data['runs'] > 0].copy()
    grouped_data['strike_rate'] = (
        grouped_data['runs'] / grouped_data['balls'] * 100
    ).round(2)

    # Rename columns for frontend
    grouped_data.rename(columns={
        'scrM_PlayMIdStrikerName': 'striker',
        'scrM_DeliveryType_zName': 'delivery_type'
    }, inplace=True)

    # Build striker-wise dict
    strikers_data = {}
    for striker_name, striker_df in grouped_data.groupby('striker'):
        strikers_data[striker_name] = striker_df.to_dict(orient='records')

    return {
        'chart_data': chart_data,
        'strikers_data': strikers_data
    }




#-------------------------------------------------------##-------------------------------------------------------##-------------------------------------------------------#
#-------------------------------------------------------##---------- Player Vs Player Report Shreyas-------------##-------------------------------------------------------#
#-------------------------------------------------------##-------------------------------------------------------##-------------------------------------------------------#

import pandas as pd
import json


def render_kpi_table(title, df, collapse_map=None):
    import os
    from urllib.parse import quote

    if df.empty:
        return f"""<div class="text-red-500 text-center">No data available for {title}.</div>"""

    # Drop video cols for main table display
    data_cols = [col for col in df.columns if not col.startswith("scrM_Video")]
    headers = data_cols + ["Videos"]
    rows = df.to_dict(orient='records')

    html = f"""
    <div class="w-full xl:w-1/2">
      <div class="overflow-x-auto rounded-md border border-slate-200 dark:border-zink-600">
        <table class="w-full text-xs bg-custom-50 dark:bg-custom-500/10 min-w-[500px]">
          <thead class="bg-custom-100 dark:bg-custom-500/10">
            <tr>
    """

    for i, col in enumerate(headers):
        align = "text-left" if i == 0 else "text-center"
        html += f"""<th class="px-2.5 py-2 font-semibold text-base border-b border-custom-200 dark:border-custom-900 {align}">{col}</th>"""
    html += "</tr></thead><tbody>"

    for idx, row in enumerate(rows):
        collapse_id = f"collapse-{title.replace(' ', '_')}-{idx}"
        html += f"""<tr class="cursor-pointer hover:bg-custom-100 dark:hover:bg-custom-500/20" onclick="toggleCollapse('{collapse_id}')">"""

        # Normal stats columns
        for i, col in enumerate(data_cols):
            val = row.get(col, "")
            align = "text-left" if i == 0 else "text-center"
            html += f"""<td class="px-2.5 py-2 border-y border-custom-200 dark:border-custom-900 {align} text-base">{val}</td>"""

        # ✅ Secure Video column: use POST form instead of query string
        urls = []
        for i in range(1, 7):
            url_field = f"scrM_Video{i}URL"
            if row.get(url_field):
                urls.append(str(row[url_field]))

        if urls:
            videos_value = ','.join(urls).replace('"', '&quot;')
            video_html = f"""
            <form action="/video_player" method="post" target="_blank" style="display:inline;margin:0;padding:0;">
              <input type="hidden" name="videos" value="{videos_value}">
              <button type="submit" title="Play Video" 
                      style="background:none;border:none;padding:0;cursor:pointer;">
                <img src="/static/video-fill.png" alt="Play" 
                     class="w-5 h-5 inline-block hover:scale-110 transition-transform"/>
              </button>
            </form>
            """
        else:
            video_html = """<span class="text-gray-400">-</span>"""

        html += f"""<td class="px-2.5 py-2 border-y border-custom-200 dark:border-custom-900 text-center">{video_html}</td>"""
        html += "</tr>"

        # Collapse row with PvP info
        row_match = str(row.get("Match", "")).strip()
        collapse_html = collapse_map.get(row_match, '<div class="text-red-500 text-sm">[No PvP Data]</div>') if collapse_map else ""
        html += f"""
        <tr id="{collapse_id}" class="hidden bg-custom-50 dark:bg-custom-500/10">
          <td colspan="{len(headers)}" class="p-3 border-b border-custom-200 dark:border-custom-900 text-slate-700 dark:text-zink-200">
            {collapse_html}
          </td>
        </tr>
        """

    html += "</tbody></table></div></div>"""

    # Collapse JS
    html += """
    <script>
      function toggleCollapse(id) {
        const row = document.getElementById(id);
        if (row) row.classList.toggle('hidden');
      }
    </script>
    """

    return html



import pandas as pd
import numpy as np

def generate_kpi_tables(df, selected_type, collapse_map=None):
    import pandas as pd

    if df.empty:
        return {"No Data": "<p>No data found</p>"}

    tables = {}

    def add_video_columns(summary, group):
        """Push video URLs instead of file names"""
        for i in range(1, 7):
            col_name = f"scrM_Video{i}URL"
            if col_name in group.columns:
                urls = group[col_name].dropna().astype(str).tolist()
                summary.setdefault(col_name, []).append(",".join(urls) if urls else "")

    if selected_type == "batter":
        for batter in df['scrM_PlayMIdStrikerName'].dropna().unique():
            player_df = df[df['scrM_PlayMIdStrikerName'] == batter]
            summary = {
                "Match": [], "Inns": [], "Runs": [], "Balls": [], "S/R": [],
                "Dots": [], "1s": [], "2s": [], "3s": [], "Fours": [], "Sixers": [],
            }

            match_groups = player_df.groupby('scrM_MchMId')
            for _, match_group in match_groups:
                match_name = match_group['MatchName'].iloc[0]
                innings_played = match_group['scrM_InningNo'].nunique()

                for _, group in match_group.groupby('scrM_InningNo'):
                    runs = group['scrM_BatsmanRuns'].sum()
                    balls = len(group)

                    dots = (group['scrM_BatsmanRuns'] == 0).sum()
                    singles = (group['scrM_BatsmanRuns'] == 1).sum()
                    doubles = (group['scrM_BatsmanRuns'] == 2).sum()
                    triples = (group['scrM_BatsmanRuns'] == 3).sum()
                    fours = (group['scrM_BatsmanRuns'] == 4).sum()
                    sixers = (group['scrM_BatsmanRuns'] == 6).sum()

                    summary["Match"].append(match_name)
                    summary["Inns"].append(innings_played)
                    summary["Runs"].append(runs)
                    summary["Balls"].append(balls)
                    summary["S/R"].append(round((runs / balls) * 100, 2) if balls else 0)
                    summary["Dots"].append(dots)
                    summary["1s"].append(singles)
                    summary["2s"].append(doubles)
                    summary["3s"].append(triples)
                    summary["Fours"].append(fours)
                    summary["Sixers"].append(sixers)

                    add_video_columns(summary, group)

            df_table = pd.DataFrame(summary)
            tables[batter] = render_kpi_table(batter, df_table, collapse_map)

    elif selected_type == "bowler":
        for bowler in df['scrM_PlayMIdBowlerName'].dropna().unique():
            player_df = df[df['scrM_PlayMIdBowlerName'] == bowler]
            summary = {
                "Match": [], "Inns": [], "Overs": [], "Runs": [], "Wkts": [], "Eco": [],
                "Dots": [], "1s": [], "2s": [], "3s": [], "Fours": [], "Sixers": [],
                "WD": [], "NB": [],
            }

            match_groups = player_df.groupby('scrM_MchMId')
            for _, match_group in match_groups:
                match_name = match_group['MatchName'].iloc[0]
                innings_bowled = match_group['scrM_InningNo'].nunique()

                for _, group in match_group.groupby('scrM_InningNo'):
                    total_balls = group.shape[0]
                    overs = total_balls // 6 + (total_balls % 6) / 10
                    runs = group['scrM_BatsmanRuns'].sum()
                    wickets = group['scrM_IsWicket'].sum()

                    dots = (group['scrM_BatsmanRuns'] == 0).sum()
                    singles = (group['scrM_BatsmanRuns'] == 1).sum()
                    doubles = (group['scrM_BatsmanRuns'] == 2).sum()
                    triples = (group['scrM_BatsmanRuns'] == 3).sum()
                    fours = (group['scrM_BatsmanRuns'] == 4).sum()
                    sixers = (group['scrM_BatsmanRuns'] == 6).sum()
                    wides = group['scrM_IsWideBall'].sum()
                    noballs = group['scrM_IsNoBall'].sum()

                    summary["Match"].append(match_name)
                    summary["Inns"].append(innings_bowled)
                    summary["Overs"].append(round(overs, 1))
                    summary["Runs"].append(runs)
                    summary["Wkts"].append(wickets)
                    summary["Eco"].append(round(runs / (overs if overs > 0 else 1), 2))
                    summary["Dots"].append(dots)
                    summary["1s"].append(singles)
                    summary["2s"].append(doubles)
                    summary["3s"].append(triples)
                    summary["Fours"].append(fours)
                    summary["Sixers"].append(sixers)
                    summary["WD"].append(wides)
                    summary["NB"].append(noballs)

                    add_video_columns(summary, group)

            df_table = pd.DataFrame(summary)
            tables[bowler] = render_kpi_table(bowler, df_table, collapse_map)

    else:
        tables["Invalid Type"] = "<p>Unknown type selected.</p>"

    return tables







def render_total_kpi_table(title, df):
    if df.empty:
        return f"""<div class="text-red-500 text-center">No total data available for {title}.</div>"""

    headers = df.columns
    row = df.iloc[0].to_dict()

    html = f"""
    <div class="w-full xl:w-1/2 mt-8">
        <div class="overflow-x-auto rounded-md border border-slate-200 dark:border-zink-600">
            <table class="w-full text-xs bg-custom-50 dark:bg-custom-500/10 min-w-[500px]">
                <thead class="ltr:text-left rtl:text-right bg-custom-100 dark:bg-custom-500/10">
                    <tr>
    """

    for col in headers:
        html += f"""<th class="px-2.5 py-2 font-semibold border-b border-custom-200 dark:border-custom-900">{col}</th>"""
    html += "</tr></thead><tbody><tr>"

    for col in headers:
        val = row[col]
        html += f"""<td class="px-2.5 py-2 border-y border-custom-200 dark:border-custom-900">{val}</td>"""

    html += "</tr></tbody></table></div></div>"
    return html



import pandas as pd
import numpy as np

def generate_total_kpi_table(df, selected_type):
    if df.empty:
        return {"No Data": "<p>No data found</p>"}

    tables = {}

    if selected_type == "batter":
        for batter in df['scrM_PlayMIdStrikerName'].dropna().unique():
            player_df = df[df['scrM_PlayMIdStrikerName'] == batter]

            runs = player_df['scrM_BatsmanRuns'].sum()
            balls = len(player_df)
            innings = player_df.groupby(['scrM_MchMId', 'scrM_InningNo']).ngroups

            fours = (player_df['scrM_BatsmanRuns'] == 4).sum()
            sixers = (player_df['scrM_BatsmanRuns'] == 6).sum()
            singles = (player_df['scrM_BatsmanRuns'] == 1).sum()
            doubles = (player_df['scrM_BatsmanRuns'] == 2).sum()
            triples = (player_df['scrM_BatsmanRuns'] == 3).sum()
            dots = (player_df['scrM_BatsmanRuns'] == 0).sum()

            scores_by_innings = player_df.groupby(['scrM_MchMId', 'scrM_InningNo'])['scrM_BatsmanRuns'].sum()
            count_30 = (scores_by_innings >= 30).sum()
            count_50 = (scores_by_innings >= 50).sum()
            count_100 = (scores_by_innings >= 100).sum()

            total_row = {
                "Inns": innings,
                "Runs": runs,
                "Balls": balls,
                "Avg": round(runs / innings, 2) if innings > 0 else 0,
                "S/R": round((runs / balls) * 100, 2) if balls > 0 else 0,
                "Sb%": round(((singles + doubles + triples) / balls) * 100, 2) if balls > 0 else 0,
                "Db%": round((dots / balls) * 100, 2) if balls > 0 else 0,
                "1s%": round((singles / balls) * 100, 2) if balls > 0 else 0,
                "2s%": round((doubles / balls) * 100, 2) if balls > 0 else 0,
                "3s%": round((triples / balls) * 100, 2) if balls > 0 else 0,
                "Fours%": round((fours / balls) * 100, 2) if balls > 0 else 0,
                "Sixers%": round((sixers / balls) * 100, 2) if balls > 0 else 0,
                "30+": count_30,
                "50+": count_50,
                "100+": count_100,
            }

            df_total = pd.DataFrame([total_row])
            tables[batter] = render_total_kpi_table(batter, df_total)

    elif selected_type == "bowler":
        for bowler in df['scrM_PlayMIdBowlerName'].dropna().unique():
            player_df = df[df['scrM_PlayMIdBowlerName'] == bowler]

            total_balls = player_df.shape[0]
            overs = total_balls // 6 + (total_balls % 6) / 10
            runs = player_df['scrM_DelRuns'].sum()
            wickets = player_df['scrM_IsWicket'].sum()

            singles = (player_df['scrM_DelRuns'] == 1).sum()
            doubles = (player_df['scrM_DelRuns'] == 2).sum()
            triples = (player_df['scrM_DelRuns'] == 3).sum()
            dots = (player_df['scrM_DelRuns'] == 0).sum()
            fours = (player_df['scrM_DelRuns'] == 4).sum()
            sixers = (player_df['scrM_DelRuns'] == 6).sum()

            wides = player_df['scrM_IsWideBall'].sum()
            noballs = player_df['scrM_IsNoBall'].sum()

            wickets_by_innings = player_df.groupby(['scrM_MchMId', 'scrM_InningNo'])['scrM_IsWicket'].sum()
            count_2w = (wickets_by_innings >= 2).sum()
            count_3w = (wickets_by_innings >= 3).sum()
            count_5w = (wickets_by_innings >= 5).sum()

            total_row = {
                "Inns": player_df.groupby(['scrM_MchMId', 'scrM_InningNo']).ngroups,
                "Overs": round(overs, 1),
                "Runs": runs,
                "Wkts": wickets,
                "Eco": round(runs / overs, 2) if overs > 0 else 0,
                "Sb%": round(((singles + doubles + triples) / total_balls) * 100, 2) if total_balls > 0 else 0,
                "Db%": round((dots / total_balls) * 100, 2) if total_balls > 0 else 0,
                "Fours%": round((fours / total_balls) * 100, 2) if total_balls > 0 else 0,
                "Sixers%": round((sixers / total_balls) * 100, 2) if total_balls > 0 else 0,
                "WD": wides,
                "NB": noballs,
                "2W+": count_2w,
                "3W+": count_3w,
                "5W+": count_5w,
            }

            df_total = pd.DataFrame([total_row])
            tables[bowler] = render_total_kpi_table(bowler, df_total)

    else:
        tables["Invalid Type"] = "<p>Unknown type selected.</p>"

    return tables



import pandas as pd

import re

def extract_skill_shortname(skill_text):
    """Extract shortname inside parentheses from skill string like 'Right arm medium fast (RAMF)'"""
    if not isinstance(skill_text, str):
        return ""
    match = re.search(r"\((.*?)\)", skill_text)
    return match.group(1) if match else ""


def generate_player_vs_player_table(df, selected_type, batters=None, bowlers=None):
    import pandas as pd

    if df.empty:
        return {}

    tables = {}

    def add_video_columns(summary, group):
        # ✅ now push video URLs instead of file names
        for i in range(1, 7):
            col_name = f"scrM_Video{i}URL"
            if col_name in group.columns:
                urls = group[col_name].dropna().astype(str).tolist()
                summary.setdefault(col_name, []).append(",".join(urls) if urls else "")

    if selected_type == "batter":
        selected_batters = batters if batters else df['scrM_PlayMIdStrikerName'].dropna().unique()
        for batter in selected_batters:
            player_df = df[df['scrM_PlayMIdStrikerName'] == batter]
            summary = {
                "Match": [], "Bowler": [], "Wkts": [], "Runs": [], "Balls": [], "SR": [],
                "Dots": [], "1s": [], "2s": [], "3s": [], "Fours": [], "Sixers": [], "WD": [], "NB": []
            }

            for (match_name, bowler_name), group in player_df.groupby(['MatchName', 'scrM_PlayMIdBowlerName']):
                runs = group['scrM_BatsmanRuns'].sum()
                balls = len(group)
                wkts = group['scrM_IsWicket'].sum()

                # Skill shortname
                bowler_skill_full = group['scrM_BowlerSkill'].iloc[0] if 'scrM_BowlerSkill' in group.columns else ""
                bowler_skill_short = extract_skill_shortname(bowler_skill_full)
                bowler_display = f"{bowler_name} ({bowler_skill_short})" if bowler_skill_short else bowler_name

                summary["Match"].append(match_name)
                summary["Bowler"].append(bowler_display)
                summary["Wkts"].append(wkts)
                summary["Runs"].append(runs)
                summary["Balls"].append(balls)
                summary["SR"].append(round((runs / balls) * 100, 2) if balls else 0)
                summary["Dots"].append((group['scrM_BatsmanRuns'] == 0).sum())
                summary["1s"].append((group['scrM_BatsmanRuns'] == 1).sum())
                summary["2s"].append((group['scrM_BatsmanRuns'] == 2).sum())
                summary["3s"].append((group['scrM_BatsmanRuns'] == 3).sum())
                summary["Fours"].append((group['scrM_BatsmanRuns'] == 4).sum())
                summary["Sixers"].append((group['scrM_BatsmanRuns'] == 6).sum())
                summary["WD"].append(group['scrM_IsWideBall'].sum())
                summary["NB"].append(group['scrM_IsNoBall'].sum())

                add_video_columns(summary, group)

            summary_df = pd.DataFrame(summary)
            tables[batter] = summary_df

    elif selected_type == "bowler":
        selected_bowlers = bowlers if bowlers else df['scrM_PlayMIdBowlerName'].dropna().unique()
        for bowler in selected_bowlers:
            player_df = df[df['scrM_PlayMIdBowlerName'] == bowler]
            summary = {
                "Match": [], "Batter": [], "Wkts": [], "Runs": [], "Balls": [], "Eco": [],
                "Dots": [], "1s": [], "2s": [], "3s": [], "Fours": [], "Sixers": [], "WD": [], "NB": []
            }

            for (match_name, batter_name), group in player_df.groupby(['MatchName', 'scrM_PlayMIdStrikerName']):
                runs = group['scrM_BatsmanRuns'].sum()
                balls = len(group)
                wkts = group['scrM_IsWicket'].sum()
                overs = balls // 6 + (balls % 6) / 10
                eco = round(runs / overs, 2) if overs > 0 else 0

                batter_skill_full = group['scrM_StrikerBatterSkill'].iloc[0] if 'scrM_StrikerBatterSkill' in group.columns else ""
                batter_skill_short = extract_skill_shortname(batter_skill_full)
                batter_display = f"{batter_name} ({batter_skill_short})" if batter_skill_short else batter_name

                summary["Match"].append(match_name)
                summary["Batter"].append(batter_display)
                summary["Wkts"].append(wkts)
                summary["Runs"].append(runs)
                summary["Balls"].append(balls)
                summary["Eco"].append(eco)
                summary["Dots"].append((group['scrM_BatsmanRuns'] == 0).sum())
                summary["1s"].append((group['scrM_BatsmanRuns'] == 1).sum())
                summary["2s"].append((group['scrM_BatsmanRuns'] == 2).sum())
                summary["3s"].append((group['scrM_BatsmanRuns'] == 3).sum())
                summary["Fours"].append((group['scrM_BatsmanRuns'] == 4).sum())
                summary["Sixers"].append((group['scrM_BatsmanRuns'] == 6).sum())
                summary["WD"].append(group['scrM_IsWideBall'].sum())
                summary["NB"].append(group['scrM_IsNoBall'].sum())

                add_video_columns(summary, group)

            summary_df = pd.DataFrame(summary)
            tables[bowler] = summary_df

    return tables


import pandas as pd

from urllib.parse import quote

def render_player_vs_player(title, df):
    import os
    from urllib.parse import quote

    if df.empty:
        return f"""<div class="text-red-500 text-center">No data available for {title}.</div>"""

    # Drop video columns from main data display
    data_cols = [col for col in df.columns if not col.startswith("scrM_Video")]
    headers = data_cols + ["Videos"]

    html = f"""
    <div class="overflow-x-auto rounded-md border border-slate-200 dark:border-zink-600">
      <table class="w-full text-xs bg-custom-50 dark:bg-custom-500/10 min-w-[500px]">
        <thead class="bg-custom-100 dark:bg-custom-500/10">
          <tr>
    """

    # Header row (font size increased → text-base)
    for i, col in enumerate(headers):
        align = "text-left" if i == 0 else "text-center"
        html += f"""<th class="px-2.5 py-2 font-semibold text-base border-b border-custom-200 dark:border-custom-900 {align}">{col}</th>"""
    html += "</tr></thead><tbody>"

    # Data rows
    for _, row in df.iterrows():
        html += """<tr class="hover:bg-custom-100 dark:hover:bg-custom-500/20">"""

        # Normal data cells (font size increased → text-base)
        for i, col in enumerate(data_cols):
            val = row.get(col, "")
            align = "text-left" if i == 0 else "text-center"
            html += f"""<td class="px-2.5 py-2 border-y border-custom-200 dark:border-custom-900 {align} text-base">{val}</td>"""

        # Secure video links via POST form
        urls = []
        for i in range(1, 7):
            url_field = f"scrM_Video{i}URL"
            if row.get(url_field):
                urls.append(str(row[url_field]))

        if urls:
            videos_value = ','.join(urls).replace('"', '&quot;')
            video_html = f"""
            <form action="/video_player" method="post" target="_blank" style="display:inline;margin:0;padding:0;">
              <input type="hidden" name="videos" value="{videos_value}">
              <button type="submit" title="Play Video"
                      style="background:none;border:none;padding:0;cursor:pointer;">
                <img src="/static/video-fill.png" alt="Play"
                     class="w-5 h-5 inline-block hover:scale-110 transition-transform"/>
              </button>
            </form>
            """
        else:
            video_html = """<span class="text-gray-400">-</span>"""

        # Add video column cell (unchanged)
        html += f"""<td class="px-2.5 py-2 border-y border-custom-200 dark:border-custom-900 text-center">{video_html}</td>"""
        html += "</tr>"

    html += "</tbody></table></div>"
    return html


import matplotlib
matplotlib.use('Agg')  # <-- Add this line

# breakdown_data = [
#     {"1s": 2, "2s": 1, "3s": 0, "4s": 1, "6s": 0},
#     {"1s": 1, "2s": 2, "3s": 0, "4s": 0, "6s": 1},
#     {"1s": 0, "2s": 1, "3s": 1, "4s": 1, "6s": 0},
#     {"1s": 3, "2s": 0, "3s": 0, "4s": 0, "6s": 0},
#     {"1s": 1, "2s": 0, "3s": 0, "4s": 2, "6s": 0},
#     {"1s": 2, "2s": 0, "3s": 0, "4s": 1, "6s": 1},
#     {"1s": 1, "2s": 1, "3s": 0, "4s": 0, "6s": 1},
#     {"1s": 0, "2s": 2, "3s": 1, "4s": 0, "6s": 1}
# ]

import matplotlib.pyplot as plt
import numpy as np
import io
import base64

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import io, base64

def generate_radar_chart(player_name, stats, labels, breakdown_data, stance=None):
    import numpy as np
    import matplotlib.pyplot as plt
    import io, base64

    num_vars = len(labels)
    angles = np.linspace(0, 2 * np.pi, num_vars, endpoint=False).tolist()
    angles += angles[:1]

    fig, ax = plt.subplots(figsize=(6, 6), subplot_kw=dict(polar=True))
    ax.set_theta_zero_location("N")
    ax.set_theta_direction(-1)

    ax.set_frame_on(False)
    ax.set_xticks([])
    ax.set_yticks([])
    ax.spines['polar'].set_visible(False)

    # Scaling factor
    scale = 2.0
    ax.set_aspect('equal')

    # Black rim
    rim_radius = 1.03 * scale
    rim_circle = plt.Circle(
        (0, 0),
        rim_radius,
        transform=ax.transData._b,
        color='black',
        linewidth=25,
        fill=False,
        zorder=5,
        clip_on=False
    )
    ax.add_artist(rim_circle)

    # Ground circles
    outer_circle = plt.Circle((0, 0), 1.0 * scale, transform=ax.transData._b, color='#19a94b', zorder=0)
    inner_circle = plt.Circle((0, 0), 0.6 * scale, transform=ax.transData._b, color='#4CAF50', zorder=1)

    # Pitch rectangle
    pitch_width = 0.07 * scale
    pitch_height = 0.3 * scale
    pitch_x = -pitch_width / 2
    pitch_y = -pitch_height / 2
    pitch = plt.Rectangle(
        (pitch_x, pitch_y),
        pitch_width,
        pitch_height,
        transform=ax.transData._b,
        color='burlywood',
        zorder=2
    )

    ax.add_artist(outer_circle)
    ax.add_artist(inner_circle)
    ax.add_artist(pitch)

    # Sector lines (every 45°)
    for angle in np.linspace(0, 2 * np.pi, 9):
        ax.plot([angle, angle], [0, 1.0 * scale], color='white', linewidth=0.7, zorder=3)

    # ===== Sector runs calculation =====
    sector_runs = [
        (bd["1s"]*1) + (bd["2s"]*2) + (bd["3s"]*3) + (bd["4s"]*4) + (bd["6s"]*6)
        for bd in breakdown_data
    ]
    total_sector_runs = sum(sector_runs)

    # ===== Orientation (base RHB, mirror for LHB) =====
    rhb_sector_angles_deg = [112.5, 67.5, 22.5, 337.5, 292.5, 247.5, 202.5, 157.5]

    if stance == "LHB":
        # mapping RHB -> LHB indices (original -> display)
        swap_map = {
            0: 5,  # Mid Wicket <-> Covers
            1: 4,  # Square Leg <-> Point
            2: 7,  # Fine Leg <-> Third Man
            3: 6,  # Third Man <-> Fine Leg
            4: 1,  # Point <-> Square Leg
            5: 0,  # Covers <-> Mid Wicket
            6: 2,  # Long Off <-> Long On
            7: 3   # Long On <-> Long Off
        }
        # IMPORTANT: only reorder runs & breakdown to match the LHB label layout.
        sector_runs = [sector_runs[swap_map[i]] for i in range(8)]
        breakdown_data = [breakdown_data[swap_map[i]] for i in range(8)]

        # Do NOT reorder the angle list — the label/box angle arrays below use the same rhb angles.
        sector_angles_deg = rhb_sector_angles_deg
    else:
        sector_angles_deg = rhb_sector_angles_deg

    # ===== Highlight max runs sector =====
    if total_sector_runs > 0:
        max_index = int(np.argmax(sector_runs))  # index into the reordered sector_runs (if LHB)
        max_angle = np.deg2rad(sector_angles_deg[max_index])
        ax.bar(
            max_angle,
            1.0 * scale,
            width=np.radians(45),
            color='red',
            alpha=0.3,
            zorder=1
        )

    # ===== Fielding position labels =====
    if stance == "LHB":
        position_labels = [
            ("Covers", 112.5, -110, 0.01),
            ("Point", 67.5, -70, 0.01),
            ("Third Man", 22.5, -25, 0.01),
            ("Fine Leg", 337.5, 20, -0.02),
            ("Square Leg", 292.5, 70, -0.03),
            ("Mid Wicket", 247.5, 110, 0.00),
            ("Long On", 202.5, 155, -0.01),
            ("Long Off", 157.5, 200, 0.01)
        ]
    else:  # RHB
        position_labels = [
            ("Mid Wicket", 112.5, -110, 0.01),
            ("Square Leg", 67.5, -70, 0.01),
            ("Fine Leg", 22.5, -25, 0.01),
            ("Third Man", 337.5, 20, -0.02),
            ("Point", 292.5, 70, -0.03),
            ("Covers", 247.5, 110, 0.00),
            ("Long Off", 202.5, 155, -0.01),
            ("Long On", 157.5, 200, 0.01)
        ]

    for text, angle_deg, rotation_deg, dist_offset in position_labels:
        rad = np.deg2rad(angle_deg)
        ax.text(
            rad,
            rim_radius + dist_offset,
            text,
            color='white',
            fontsize=10,
            fontweight='bold',
            ha='center',
            va='center',
            rotation=rotation_deg,
            rotation_mode='anchor',
            zorder=6
        )

    # ===== Runs + % black boxes =====
    if stance == "LHB":
        box_positions = [
            (103.5, 0, 0.70),
            (67.5, 0, 0.70),
            (22.5, 0, 0.80),
            (337.5, 0, 0.80),
            (292.5, 0, 0.75),
            (250.5, 0, 0.70),
            (204.5, 1, 0.59),
            (155.5, 1, 0.59)
        ]
    else:
        box_positions = [
            (103.5, 0, 0.70),
            (67.5, 0, 0.70),
            (22.5, 0, 0.80),
            (337.5, 0, 0.80),
            (292.5, 0, 0.75),
            (250.5, 0, 0.70),
            (204.5, 1, 0.59),
            (155.5, 1, 0.59)
        ]

    total_runs = sum(sector_runs)

    for i, (angle_deg, rotation_deg, dist_offset) in enumerate(box_positions):
        rad = np.deg2rad(angle_deg)
        r = dist_offset * scale
        runs = sector_runs[i]
        percentage = (runs / total_runs * 100) if total_runs > 0 else 0
        label_text = f"{runs} ({percentage:.1f}%)"

        ax.text(
            rad, r,
            label_text,
            color='white',
            fontsize=12,
            fontweight='bold',
            ha='center',
            va='center',
            rotation=rotation_deg,
            rotation_mode='anchor',
            zorder=10
        )

    # 📤 Export as base64
    buf = io.BytesIO()
    plt.tight_layout()
    plt.savefig(buf, format="png", bbox_inches='tight', dpi=150, transparent=True)
    plt.close(fig)
    buf.seek(0)
    encoded = base64.b64encode(buf.read()).decode("utf-8")
    return f"data:image/png;base64,{encoded}"



def get_radar_data_from_player_df(df, player_type):
    if df.empty:
        return None, None, None

    sector_order = [
        "Mid Wicket", "Square Leg", "Fine Leg", "Third Man",
        "Point", "Covers", "Long Off", "Long On"
    ]
    sector_map = {name: i for i, name in enumerate(sector_order)}
    sector_counts = [{"1s": 0, "2s": 0, "3s": 0, "4s": 0, "6s": 0} for _ in range(8)]

    run_col = "scrM_BatsmanRuns"
    for _, row in df.iterrows():
        sector_name = row.get("scrM_WagonArea_zName", None)
        runs = row.get(run_col, 0)

        if pd.isna(sector_name) or sector_name not in sector_map:
            continue

        sector_idx = sector_map[sector_name]
        if runs in [1, 2, 3, 4, 6]:
            key = f"{runs}s" if runs != 6 else "6s"
            sector_counts[sector_idx][key] += 1

    breakdown_data = sector_counts

    if player_type == "batter":
        runs = df['scrM_BatsmanRuns'].sum()
        balls = len(df)
        dots = (df['scrM_BatsmanRuns'] == 0).sum()
        ones = (df['scrM_BatsmanRuns'] == 1).sum()
        twos = (df['scrM_BatsmanRuns'] == 2).sum()
        threes = (df['scrM_BatsmanRuns'] == 3).sum()
        fours = (df['scrM_BatsmanRuns'] == 4).sum()
        sixes = (df['scrM_BatsmanRuns'] == 6).sum()

        stats = [
            runs,
            round((runs / balls) * 100, 2) if balls else 0,   # SR
            round((dots / balls) * 100, 2) if balls else 0,
            round((ones / balls) * 100, 2) if balls else 0,
            round((twos / balls) * 100, 2) if balls else 0,
            round((threes / balls) * 100, 2) if balls else 0,
            round((fours / balls) * 100, 2) if balls else 0,
            round((sixes / balls) * 100, 2) if balls else 0,
        ]
        labels = ['Runs', 'S/R', 'Dots%', '1s%', '2s%', '3s%', 'Fours%', 'Sixers%']

    elif player_type == "bowler":
        runs = df['scrM_BatsmanRuns'].sum()
        balls = df.shape[0]
        wickets = df['scrM_IsWicket'].sum()
        dots = (df['scrM_BatsmanRuns'] == 0).sum()
        ones = (df['scrM_BatsmanRuns'] == 1).sum()
        twos = (df['scrM_BatsmanRuns'] == 2).sum()
        threes = (df['scrM_BatsmanRuns'] == 3).sum()
        fours = (df['scrM_BatsmanRuns'] == 4).sum()
        sixes = (df['scrM_BatsmanRuns'] == 6).sum()

        stats = [
            wickets,
            round(runs / (balls / 6), 2) if balls else 0,  # economy
            round((dots / balls) * 100, 2) if balls else 0,
            round((ones / balls) * 100, 2) if balls else 0,
            round((twos / balls) * 100, 2) if balls else 0,
            round((threes / balls) * 100, 2) if balls else 0,
            round((fours / balls) * 100, 2) if balls else 0,
            round((sixes / balls) * 100, 2) if balls else 0,
        ]
        labels = ['Wickets', 'Eco', 'Dots%', '1s%', '2s%', '3s%', 'Fours%', 'Sixers%']

    else:
        return None, None, None

    return stats, labels, breakdown_data



import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
import io, base64
import os

# ✅ Helper: Scale images consistently
def scaled_offset_image(img, target_size=20, reference_img=None):
    """
    Returns OffsetImage scaled to fixed pixel size (diameter).
    If reference_img is provided, other images will be scaled
    to exactly match the reference image's visual size.
    """
    h, w = img.shape[0], img.shape[1]

    if reference_img is not None:
        ref_h, ref_w = reference_img.shape[0], reference_img.shape[1]
        scale = target_size / max(ref_h, ref_w)
    else:
        scale = target_size / max(h, w)

    return OffsetImage(img, zoom=scale)


def plot_pitch_points_scaled(df, pitch_image_path,
                             wicket_ball_path=None, dot0_ball_path=None,
                             blue1_ball_path=None, pink2_ball_path=None,
                             yellow3_ball_path=None, orange4_ball_path=None,
                             green6_ball_path=None):
    """
    Plots pitch points:
      - 0 → MaroonBall.png
      - 1 → BlueBall.png
      - 2 → PinkBall.png
      - 3 → YellowBall.png
      - 4 → OrangeBall.png
      - 6 → GreenBall.png
      - Wickets → RedBall.png
    """
    # ✅ Fallbacks using resource_path
    if wicket_ball_path is None:
        wicket_ball_path = resource_path("tailwick/static/RedBall_Resized.png")
    if dot0_ball_path is None:
        dot0_ball_path = resource_path("tailwick/static/MaroonBall_Resized.png")
    if blue1_ball_path is None:
        blue1_ball_path = resource_path("tailwick/static/BlueBall_Resized.png")
    if pink2_ball_path is None:
        pink2_ball_path = resource_path("tailwick/static/PinkBall_Resized.png")
    if yellow3_ball_path is None:
        yellow3_ball_path = resource_path("tailwick/static/YellowBall_Resized.png")
    if orange4_ball_path is None:
        orange4_ball_path = resource_path("tailwick/static/OrangeBall_Resized.png")
    if green6_ball_path is None:
        green6_ball_path = resource_path("tailwick/static/GreenBall_Resized.png")

    df = df.copy()
    df['scrM_PitchX'] = pd.to_numeric(df['scrM_PitchX'], errors='coerce')
    df['scrM_PitchY'] = pd.to_numeric(df['scrM_PitchY'], errors='coerce')
    df['scrM_BatsmanRuns'] = pd.to_numeric(df['scrM_BatsmanRuns'], errors='coerce')
    df['scrM_IsWicket'] = pd.to_numeric(df['scrM_IsWicket'], errors='coerce')

    output_width, output_height, dpi = 250, 400, 100
    fig, ax = plt.subplots(figsize=(output_width / dpi, output_height / dpi), dpi=dpi)

    img = mpimg.imread(pitch_image_path)
    img_height_px, img_width_px = img.shape[0], img.shape[1]

    db_width, db_height = 157, 272
    scale_x = img_width_px / db_width
    scale_y = img_height_px / db_height

    ax.imshow(img)

    # Load ball images once
    wicket_img  = mpimg.imread(wicket_ball_path) if os.path.exists(wicket_ball_path) else None
    dot0_img    = mpimg.imread(dot0_ball_path) if os.path.exists(dot0_ball_path) else None
    blue1_img   = mpimg.imread(blue1_ball_path) if os.path.exists(blue1_ball_path) else None
    pink2_img   = mpimg.imread(pink2_ball_path) if os.path.exists(pink2_ball_path) else None
    yellow3_img = mpimg.imread(yellow3_ball_path) if os.path.exists(yellow3_ball_path) else None
    orange4_img = mpimg.imread(orange4_ball_path) if os.path.exists(orange4_ball_path) else None
    green6_img  = mpimg.imread(green6_ball_path) if os.path.exists(green6_ball_path) else None

    for _, row in df.dropna(subset=['scrM_PitchX', 'scrM_PitchY']).iterrows():
        x = row['scrM_PitchX'] * scale_x
        y = row['scrM_PitchY'] * scale_y
        run_val = row['scrM_BatsmanRuns']

        if row['scrM_IsWicket'] == 1 and wicket_img is not None:
            img_used = wicket_img
        elif run_val == 0 and dot0_img is not None:
            img_used = dot0_img
        elif run_val == 1 and blue1_img is not None:
            img_used = blue1_img
        elif run_val == 2 and pink2_img is not None:
            img_used = pink2_img
        elif run_val == 3 and yellow3_img is not None:
            img_used = yellow3_img
        elif run_val == 4 and orange4_img is not None:
            img_used = orange4_img
        elif run_val == 6 and green6_img is not None:
            img_used = green6_img
        else:
            img_used = None

        if img_used is not None:
            imagebox = scaled_offset_image(img_used, target_size=14, reference_img=wicket_img)
            ax.add_artist(AnnotationBbox(imagebox, (x, y), frameon=False, pad=0, zorder=5))
        else:
            ax.scatter(x, y, s=14, c="#000000", zorder=5)

    ax.axis('off')
    plt.tight_layout(pad=0)
    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=dpi, bbox_inches='tight', pad_inches=0, transparent=True)
    plt.close(fig)
    return f"data:image/png;base64,{base64.b64encode(buf.getvalue()).decode()}"



def plot_arrival_points_scaled(df, arrival_image_path,
                               wicket_ball_path=None, dot0_ball_path=None,
                               blue1_ball_path=None, pink2_ball_path=None,
                               yellow3_ball_path=None, orange4_ball_path=None,
                               green6_ball_path=None):
    """
    Plots arrival points with the same ball icons
    """
    if wicket_ball_path is None:
        wicket_ball_path = resource_path("tailwick/static/RedBall_Resized.png")
    if dot0_ball_path is None:
        dot0_ball_path = resource_path("tailwick/static/MaroonBall_Resized.png")
    if blue1_ball_path is None:
        blue1_ball_path = resource_path("tailwick/static/BlueBall_Resized.png")
    if pink2_ball_path is None:
        pink2_ball_path = resource_path("tailwick/static/PinkBall_Resized.png")
    if yellow3_ball_path is None:
        yellow3_ball_path = resource_path("tailwick/static/YellowBall_Resized.png")
    if orange4_ball_path is None:
        orange4_ball_path = resource_path("tailwick/static/OrangeBall_Resized.png")
    if green6_ball_path is None:
        green6_ball_path = resource_path("tailwick/static/GreenBall_Resized.png")

    df = df.copy()
    df['scrM_BatPitchX'] = pd.to_numeric(df['scrM_BatPitchX'], errors='coerce')
    df['scrM_BatPitchY'] = pd.to_numeric(df['scrM_BatPitchY'], errors='coerce')
    df['scrM_BatsmanRuns'] = pd.to_numeric(df['scrM_BatsmanRuns'], errors='coerce')
    df['scrM_IsWicket'] = pd.to_numeric(df['scrM_IsWicket'], errors='coerce')

    output_width, output_height, dpi = 300, 250, 100
    fig, ax = plt.subplots(figsize=(output_width / dpi, output_height / dpi), dpi=dpi)

    img = mpimg.imread(arrival_image_path)
    img_height_px, img_width_px = img.shape[0], img.shape[1]

    db_width, db_height = 157, 132
    scale_x = img_width_px / db_width
    scale_y = img_height_px / db_height

    ax.imshow(img)

    # Load ball images once
    wicket_img  = mpimg.imread(wicket_ball_path) if os.path.exists(wicket_ball_path) else None
    dot0_img    = mpimg.imread(dot0_ball_path) if os.path.exists(dot0_ball_path) else None
    blue1_img   = mpimg.imread(blue1_ball_path) if os.path.exists(blue1_ball_path) else None
    pink2_img   = mpimg.imread(pink2_ball_path) if os.path.exists(pink2_ball_path) else None
    yellow3_img = mpimg.imread(yellow3_ball_path) if os.path.exists(yellow3_ball_path) else None
    orange4_img = mpimg.imread(orange4_ball_path) if os.path.exists(orange4_ball_path) else None
    green6_img  = mpimg.imread(green6_ball_path) if os.path.exists(green6_ball_path) else None

    for _, row in df.dropna(subset=['scrM_BatPitchX', 'scrM_BatPitchY']).iterrows():
        x = row['scrM_BatPitchX'] * scale_x
        y = row['scrM_BatPitchY'] * scale_y
        run_val = row['scrM_BatsmanRuns']

        if row['scrM_IsWicket'] == 1 and wicket_img is not None:
            img_used = wicket_img
        elif run_val == 0 and dot0_img is not None:
            img_used = dot0_img
        elif run_val == 1 and blue1_img is not None:
            img_used = blue1_img
        elif run_val == 2 and pink2_img is not None:
            img_used = pink2_img
        elif run_val == 3 and yellow3_img is not None:
            img_used = yellow3_img
        elif run_val == 4 and orange4_img is not None:
            img_used = orange4_img
        elif run_val == 6 and green6_img is not None:
            img_used = green6_img
        else:
            img_used = None

        if img_used is not None:
            imagebox = scaled_offset_image(img_used, target_size=14, reference_img=wicket_img)
            ax.add_artist(AnnotationBbox(imagebox, (x, y), frameon=False, pad=0, zorder=5))
        else:
            ax.scatter(x, y, s=14, c="#000000", zorder=5)

    ax.axis('off')
    plt.tight_layout(pad=0)
    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=dpi, bbox_inches='tight', pad_inches=0, transparent=True)
    plt.close(fig)
    return f"data:image/png;base64,{base64.b64encode(buf.getvalue()).decode()}"






def get_top_shot_area_data(df, top_n=3, mode="batter"):
    """
    Returns a list of dicts for top_n shot/areas by total runs.
    mode:
      - "batter" → runs scored by batter in each area
      - "bowler" → runs conceded by bowler in each area
    Each dict: 
        {
            'area': area_name,
            '1s_runs': runs_from_1s,
            '1s_count': count_of_1s,
            ...
        }
    """
    if df is None or df.empty:
        return []

    d = df.copy()
    d['scrM_BatsmanRuns'] = pd.to_numeric(d['scrM_BatsmanRuns'], errors='coerce').fillna(0).astype(int)
    d['scrM_WagonArea_zName'] = d['scrM_WagonArea_zName'].fillna('Unknown')

    # Include only deliveries that scored 1,2,3,4,6 runs
    scoring_mask = d['scrM_BatsmanRuns'].isin([1, 2, 3, 4, 6])
    scored = d[scoring_mask]

    if scored.empty:
        return []

    # Group by wagon area for either batter or bowler mode
    # (Logic is actually same: it's just the dataset filtered for that player)
    total_by_area = scored.groupby('scrM_WagonArea_zName')['scrM_BatsmanRuns'].sum().reset_index()
    total_by_area = total_by_area.sort_values(by='scrM_BatsmanRuns', ascending=False).head(top_n)

    result = []
    for _, row in total_by_area.iterrows():
        area = row['scrM_WagonArea_zName']
        area_df = scored[scored['scrM_WagonArea_zName'] == area]

        result.append({
            'area': area,
            '1s_runs': int(area_df.loc[area_df['scrM_BatsmanRuns'] == 1, 'scrM_BatsmanRuns'].sum()),
            '1s_count': int((area_df['scrM_BatsmanRuns'] == 1).sum()),
            '2s_runs': int(area_df.loc[area_df['scrM_BatsmanRuns'] == 2, 'scrM_BatsmanRuns'].sum()),
            '2s_count': int((area_df['scrM_BatsmanRuns'] == 2).sum()),
            '3s_runs': int(area_df.loc[area_df['scrM_BatsmanRuns'] == 3, 'scrM_BatsmanRuns'].sum()),
            '3s_count': int((area_df['scrM_BatsmanRuns'] == 3).sum()),
            '4s_runs': int(area_df.loc[area_df['scrM_BatsmanRuns'] == 4, 'scrM_BatsmanRuns'].sum()),
            '4s_count': int((area_df['scrM_BatsmanRuns'] == 4).sum()),
            '6s_runs': int(area_df.loc[area_df['scrM_BatsmanRuns'] == 6, 'scrM_BatsmanRuns'].sum()),
            '6s_count': int((area_df['scrM_BatsmanRuns'] == 6).sum()),
        })

    return result

def get_top_shot_type_data(df, mode="batter", top_n=3):
    """
    Returns a list of dicts for top_n shot types by total runs.
    Works for both batters (productive shots) and bowlers (runs conceded).
    Each dict same format as get_top_shot_area_data().
    """
    if df is None or df.empty:
        return []

    d = df.copy()
    d['scrM_BatsmanRuns'] = pd.to_numeric(d['scrM_BatsmanRuns'], errors='coerce').fillna(0).astype(int)
    d['scrM_ShotType_zName'] = d['scrM_ShotType_zName'].fillna('Unknown')

    # Only include deliveries with run values we want to count
    scoring_mask = d['scrM_BatsmanRuns'].isin([1, 2, 3, 4, 6])
    scored = d[scoring_mask]

    if scored.empty:
        return []

    # Group by shot type and total runs
    total_by_shot = scored.groupby('scrM_ShotType_zName')['scrM_BatsmanRuns'].sum().reset_index()
    total_by_shot = total_by_shot.sort_values(by='scrM_BatsmanRuns', ascending=False).head(top_n)

    result = []
    for _, row in total_by_shot.iterrows():
        shot = row['scrM_ShotType_zName']
        shot_df = scored[scored['scrM_ShotType_zName'] == shot]

        result.append({
            'shot': shot,
            '1s_runs': int(shot_df.loc[shot_df['scrM_BatsmanRuns'] == 1, 'scrM_BatsmanRuns'].sum()),
            '1s_count': int((shot_df['scrM_BatsmanRuns'] == 1).sum()),
            '2s_runs': int(shot_df.loc[shot_df['scrM_BatsmanRuns'] == 2, 'scrM_BatsmanRuns'].sum()),
            '2s_count': int((shot_df['scrM_BatsmanRuns'] == 2).sum()),
            '3s_runs': int(shot_df.loc[shot_df['scrM_BatsmanRuns'] == 3, 'scrM_BatsmanRuns'].sum()),
            '3s_count': int((shot_df['scrM_BatsmanRuns'] == 3).sum()),
            '4s_runs': int(shot_df.loc[shot_df['scrM_BatsmanRuns'] == 4, 'scrM_BatsmanRuns'].sum()),
            '4s_count': int((shot_df['scrM_BatsmanRuns'] == 4).sum()),
            '6s_runs': int(shot_df.loc[shot_df['scrM_BatsmanRuns'] == 6, 'scrM_BatsmanRuns'].sum()),
            '6s_count': int((shot_df['scrM_BatsmanRuns'] == 6).sum()),
        })

    return result

# Helper function to get wicket counts
def get_wicket_type_counts(df):
    wicket_df = df[df['scrM_IsWicket'] == 1]
    if wicket_df.empty:
        return [], [], [], []

    labels = wicket_df['scrM_DecisionFinal_zName'].dropna().unique().tolist()
    overall_counts = [wicket_df[wicket_df['scrM_DecisionFinal_zName'] == label].shape[0] for label in labels]

    pace_df = wicket_df[wicket_df['scrM_BowlerSkill'].str.upper().str.contains("PACE|FAST", na=False)]
    spin_df = wicket_df[wicket_df['scrM_BowlerSkill'].str.upper().str.contains("SPIN", na=False)]

    pace_counts = [pace_df[pace_df['scrM_DecisionFinal_zName'] == label].shape[0] for label in labels]
    spin_counts = [spin_df[spin_df['scrM_DecisionFinal_zName'] == label].shape[0] for label in labels]

    return labels, overall_counts, pace_counts, spin_counts

def get_pace_spin_wicket_counts(df):
    wicket_df = df[df['scrM_IsWicket'] == 1]
    if wicket_df.empty:
        return ["Pace", "Spin"], [0, 0]
    
    pace_count = wicket_df[wicket_df['scrM_BowlerSkill'].str.upper().str.contains("PACE|FAST", na=False)].shape[0]
    spin_count = wicket_df[wicket_df['scrM_BowlerSkill'].str.upper().str.contains("SPIN", na=False)].shape[0]
    
    return ["Pace", "Spin"], [pace_count, spin_count]

def get_inner_donut_counts(df, selected_type):
    wicket_df = df[df['scrM_IsWicket'] == 1]
    if wicket_df.empty:
        if selected_type == "batter":
            return ["Pace", "Spin"], [0, 0]
        else:
            return ["Right Hand", "Left Hand"], [0, 0]

    if selected_type == "batter":
        pace_count = wicket_df[wicket_df['scrM_BowlerSkill'].str.upper().str.contains("PACE|FAST", na=False)].shape[0]
        spin_count = wicket_df[wicket_df['scrM_BowlerSkill'].str.upper().str.contains("SPIN", na=False)].shape[0]
        return ["Pace", "Spin"], [pace_count, spin_count]
    else:
        # Bowler: classify wickets by batsman skill
        right_hand_count = wicket_df[wicket_df['scrM_StrikerBatterSkill'].str.upper().str.contains("RHB", na=False)].shape[0]
        left_hand_count  = wicket_df[wicket_df['scrM_StrikerBatterSkill'].str.upper().str.contains("LHB", na=False)].shape[0]
        return ["Right Hand", "Left Hand"], [right_hand_count, left_hand_count]



import pandas as pd

def generate_dynamic_strengths_weaknesses(df, player, selected_type, team_total_runs=None):
    if df.empty:
        return "<ul><li>No data available</li></ul>", "<ul><li>No data available</li></ul>"

    strengths, weaknesses = [], []

    if selected_type == "batter":
        if "scrM_BatsmanRuns" not in df.columns:
            return "<ul><li>No batting data available</li></ul>", "<ul><li>No batting data available</li></ul>"

        runs = df["scrM_BatsmanRuns"].sum()
        balls = len(df)

        # --- Early phase scoring ---
        early_phase = df.head(10)
        early_sr = (early_phase["scrM_BatsmanRuns"].sum() / len(early_phase) * 100) if len(early_phase) else 0
        if early_sr > 120:
            strengths.append("Quick starter – strong in first 10 balls.")
        else:
            weaknesses.append("Struggles to accelerate in first 10 balls.")

        # --- Death phase scoring ---
        death_phase = df.tail(10)
        death_sr = (death_phase["scrM_BatsmanRuns"].sum() / len(death_phase) * 100) if len(death_phase) else 0
        if death_sr > 150:
            strengths.append("Explosive in last 10 balls, good finisher.")
        else:
            weaknesses.append("Limited impact in final overs.")

        # --- Pace vs Spin ---
        if "scrM_BowlerSkill" in df.columns:
            skill_group = df.groupby("scrM_BowlerSkill")["scrM_BatsmanRuns"].mean().sort_values()
            if not skill_group.empty:
                best = skill_group.idxmax()
                worst = skill_group.idxmin()
                if best:
                    strengths.append(f"Strong vs {best} bowling.")
                if worst:
                    weaknesses.append(f"Struggles against {worst} bowling.")

        # --- Wagon area scoring zones ---
        if "scrM_WagonArea_zName" in df.columns:
            area_runs = df.groupby("scrM_WagonArea_zName")["scrM_BatsmanRuns"].sum().sort_values()
            if not area_runs.empty:
                best = area_runs.idxmax()
                worst = area_runs.idxmin()
                if best:
                    strengths.append(f"Strong scoring zone: {best}.")
                if worst:
                    weaknesses.append(f"Rarely scores in {worst}.")

        # --- Pitch length analysis ---
        if "scrM_BatPitchArea_zName" in df.columns:
            area_group = df.groupby("scrM_BatPitchArea_zName")["scrM_BatsmanRuns"].mean().sort_values()
            if not area_group.empty:
                best = area_group.idxmax()
                worst = area_group.idxmin()
                if best:
                    strengths.append(f"Scores freely against {best} deliveries.")
                if worst:
                    weaknesses.append(f"Struggles against {worst} lengths.")

        # --- Boundary Hitting Capability ---
        if "scrM_BatsmanRuns" in df.columns:
            boundaries = df[df["scrM_BatsmanRuns"].isin([4, 6])]
            if balls > 0 and (len(boundaries) / balls > 0.2):
                strengths.append("High boundary-hitting capability.")
            else:
                weaknesses.append("Low boundary-hitting frequency.")

        # --- Strike Rotation ---
        if "scrM_BatsmanRuns" in df.columns:
            singles = len(df[df["scrM_BatsmanRuns"] == 1])
            if balls > 0 and (singles / balls > 0.25):
                strengths.append("Good strike rotation ability.")
            else:
                weaknesses.append("Struggles to rotate strike consistently.")

        # --- Conversion Rate (20s → 50s/100s) ---
        innings_runs = runs
        if innings_runs >= 50:
            strengths.append("Good at converting starts into big scores.")
        elif innings_runs >= 20:
            weaknesses.append("Often fails to convert starts into big scores.")

        # --- Contribution to Team Score ---
        if team_total_runs and team_total_runs > 0:
            contribution = (runs / team_total_runs) * 100
            if contribution > 30:
                strengths.append(f"Significant contributor ({contribution:.1f}% of team’s runs).")
            else:
                weaknesses.append(f"Low contribution to team’s total ({contribution:.1f}%).")

        # --- Dismissal Patterns ---
        if "scrM_WicketType" in df.columns:
            dismissals = df["scrM_WicketType"].value_counts()
            if not dismissals.empty:
                worst = dismissals.idxmax()
                if worst:
                    weaknesses.append(f"Frequently dismissed by {worst}.")

        if "scrM_BowlerSkill" in df.columns and "scrM_IsWicketDelivery" in df.columns:
            bowler_outs = df[df["scrM_IsWicketDelivery"] == 1]["scrM_BowlerSkill"].value_counts()
            if not bowler_outs.empty:
                worst = bowler_outs.idxmax()
                if worst:
                    weaknesses.append(f"Gets out most often to {worst} bowlers.")

    elif selected_type == "bowler":
        if "scrM_BatsmanRuns" not in df.columns or "scrM_OverNo" not in df.columns:
            return "<ul><li>No bowling data available</li></ul>", "<ul><li>No bowling data available</li></ul>"

        runs = df["scrM_BatsmanRuns"].sum()
        balls = len(df)

        # --- Early vs Death overs ---
        if balls > 0:
            early = df[df["scrM_OverNo"] <= 2]
            death = df[df["scrM_OverNo"] >= 16]

            eco_early = (early["scrM_BatsmanRuns"].sum() / (len(early)/6)) if len(early) else 0
            eco_death = (death["scrM_BatsmanRuns"].sum() / (len(death)/6)) if len(death) else 0

            if eco_early < 7:
                strengths.append("Tight economy in powerplay overs.")
            else:
                weaknesses.append("Expensive in early overs.")

            if eco_death < 9:
                strengths.append("Effective at death bowling.")
            else:
                weaknesses.append("Leaky at death overs.")

        # --- Effectiveness by batter type ---
        if "scrM_StrikerBatterSkill" in df.columns:
            hand_group = df.groupby("scrM_StrikerBatterSkill")["scrM_BatsmanRuns"].mean().sort_values()
            if not hand_group.empty:
                best = hand_group.idxmin()
                worst = hand_group.idxmax()
                if best:
                    strengths.append(f"Strong against {best} batters.")
                if worst:
                    weaknesses.append(f"Struggles against {worst} batters.")

        # --- Length effectiveness ---
        if "scrM_BatPitchArea_zName" in df.columns:
            area_group = df.groupby("scrM_BatPitchArea_zName")["scrM_BatsmanRuns"].mean().sort_values()
            if not area_group.empty:
                best = area_group.idxmin()
                worst = area_group.idxmax()
                if best:
                    strengths.append(f"Most effective on {best} deliveries.")
                if worst:
                    weaknesses.append(f"Concedes runs on {worst} lengths.")

        # --- Delivery type effectiveness ---
        if "scrM_BowlerDeliveryType" in df.columns:
            del_group = df.groupby("scrM_BowlerDeliveryType")["scrM_BatsmanRuns"].mean().sort_values()
            if not del_group.empty:
                best = del_group.idxmin()
                worst = del_group.idxmax()
                if best:
                    strengths.append(f"Best delivery type: {best}.")
                if worst:
                    weaknesses.append(f"Least effective delivery: {worst}.")

    # --- ✅ Return lists formatted as HTML ---
    strengths_html = f"<ul class='list-disc pl-4 text-sm'>{''.join(f'<li>{pt}</li>' for pt in strengths[:7])}</ul>"
    weaknesses_html = f"<ul class='list-disc pl-4 text-sm'>{''.join(f'<li>{pt}</li>' for pt in weaknesses[:7])}</ul>"

    return strengths_html, weaknesses_html



import re
import math
import pandas as pd

def generate_kpi_with_summary_tables(df, selected_type, player_vs_player_tables=None):
    """
    Drop-in responsive version.
    Keeps your original metrics & charts but makes every container flexible:
    - No fixed widths that cause overflow on tablets
    - Flexbox + wrap everywhere
    - Images are max-width:100% / height:auto
    - ApexCharts boxes are width:100% with min-height
    - Tables remain inside overflow-x-auto wrappers
    """
    import json  # for safe JS arrays
    import pandas as pd
    import re
    import os

    if df.empty:
        return {"No Data": "<p>No data available</p>"}

    # ---------- Helpers ----------
    def first_col(dframe, options, default=None):
        for c in options:
            if c in dframe.columns:
                return c
        return default

    def safe_series(dframe, col, default_val=0):
        if col in dframe.columns:
            return dframe[col].fillna(default_val)
        return pd.Series([default_val] * len(dframe), index=dframe.index)

    def safe_bool_series(dframe, col):
        s = safe_series(dframe, col, 0)
        # Treat anything truthy/nonzero as True
        return (s.astype(float) != 0)

    def safe_div(a, b, places=2, dash_on_zero=False):
        if b == 0:
            return "-" if dash_on_zero else 0
        return round(a / b, places)

    # Canonical columns from your schema
    MATCH_COL   = first_col(df, ["scrM_MatchName", "scrM_MchMId"])
    INN_COL     = first_col(df, ["scrM_InnId", "scrM_InningNo"])
    BAT_RUNS    = "scrM_BatsmanRuns"
    IS_WIDE     = "scrM_IsWideBall"
    IS_NOBALL   = "scrM_IsNoBall"
    IS_VALID    = "scrM_IsValidBall"
    IS_WICKET   = "scrM_IsWicket"
    IS_BWL_WKT  = "scrM_IsBowlerWicket"
    OUT_NAME    = "scrM_PlayMIdWicketName"   # batter out name
    BAT_ID      = first_col(df, ["scrM_PlayMIdStriker", "scrM_PlayMIdStrikerName"])
    BWL_ID      = first_col(df, ["scrM_PlayMIdBowler", "scrM_PlayMIdBowlerName"])
    BAT_NAME    = "scrM_PlayMIdStrikerName"
    BWL_NAME    = "scrM_PlayMIdBowlerName"

    combined_tables = {}

    # Iterate players (batter or bowler, as per selected_type)
    name_col = BAT_NAME if selected_type == "batter" else BWL_NAME

    for player in df[name_col].dropna().unique():
        player_df = df[df[name_col] == player].copy()

        # 🖐 Detect handedness (batter)
        if selected_type == "batter":
            skill_col = "scrM_StrikerBatterSkill"
            if skill_col in player_df.columns and not player_df[skill_col].isna().all():
                batter_skill = str(player_df[skill_col].iloc[0])
            else:
                batter_skill = ""
            is_left_handed = "LHB" in batter_skill.upper()
        else:
            is_left_handed = False

        # 🎯 Player vs Player collapse map
        collapse_map = {}
        if player_vs_player_tables and player in player_vs_player_tables:
            pvp_df = player_vs_player_tables[player]
            pvp_match_col = first_col(pvp_df, ["scrM_MatchName", "scrM_MchMId", "Match", "match"])
            if pvp_match_col and pvp_match_col in pvp_df.columns:
                for match_name, match_df in pvp_df.groupby(pvp_match_col):
                    collapse_map[str(match_name).strip()] = render_player_vs_player(str(match_name), match_df)
            else:
                collapse_map["Summary"] = render_player_vs_player("Summary", player_df)

        # 🧩 KPI + Summary tables (existing helpers)
        kpi_tables = generate_kpi_tables(player_df, selected_type, collapse_map)
        summary_tables = generate_total_kpi_table(player_df, selected_type)
        match_kpi_html = kpi_tables.get(player, "")
        total_kpi_html = summary_tables.get(player, "")

        # ---------- Numbers ----------
        is_wide_s   = safe_bool_series(player_df, IS_WIDE)
        is_nb_s     = safe_bool_series(player_df, IS_NOBALL)
        is_valid_s  = safe_bool_series(player_df, IS_VALID)
        is_wkt_s    = safe_bool_series(player_df, IS_WICKET)
        is_bwl_wkt  = safe_bool_series(player_df, IS_BWL_WKT) if IS_BWL_WKT in player_df.columns else is_wkt_s

        bat_runs_s  = safe_series(player_df, BAT_RUNS, 0).astype(float)

        # Batter: balls faced -> exclude wides
        balls_faced_mask = ~is_wide_s
        balls_faced = int(balls_faced_mask.sum())

        # Bowler: total balls (including wides/noballs) - match KPI table logic
        total_balls = len(player_df)

        # Inns calculation
        group_keys = [c for c in [MATCH_COL, INN_COL] if c is not None]
        Inns = player_df[group_keys].drop_duplicates().shape[0] if group_keys else 1

        if selected_type == "batter":
            Runs = int(bat_runs_s.sum())

            # Dismissals
            if OUT_NAME in player_df.columns:
                dismissals = int(((is_wkt_s) & (player_df[OUT_NAME].fillna("") == player)).sum())
            else:
                dismissals = int(is_wkt_s.sum())

            Avg = safe_div(Runs, dismissals, places=2, dash_on_zero=True)
            SR  = safe_div(Runs * 100.0, balls_faced, places=2, dash_on_zero=True)

            # Scoring / Dot %
            scoring_mask = balls_faced_mask & (bat_runs_s > 0)
            dot_mask     = balls_faced_mask & (bat_runs_s == 0)
            Sb = int(scoring_mask.sum())   # Scoring Balls
            Db = int(dot_mask.sum())       # Dots
            ones = int(((balls_faced_mask) & (bat_runs_s == 1)).sum())
            twos = int(((balls_faced_mask) & (bat_runs_s == 2)).sum())
            threes = int(((balls_faced_mask) & (bat_runs_s == 3)).sum())
            fours = int(((balls_faced_mask) & (bat_runs_s == 4)).sum())
            sixes = int(((balls_faced_mask) & (bat_runs_s == 6)).sum())


            # 30+/50+/100+
            bat_key = BAT_ID if BAT_ID in player_df.columns else BAT_NAME
            bat_gkeys = [k for k in [MATCH_COL, INN_COL, bat_key] if k is not None]
            if bat_gkeys:
                inn_totals = player_df.groupby(bat_gkeys)[BAT_RUNS].sum()
                thirties = int(((inn_totals >= 30) & (inn_totals < 50)).sum())
                fifties  = int(((inn_totals >= 50) & (inn_totals < 100)).sum())
                hundreds = int((inn_totals >= 100).sum())
            else:
                thirties = fifties = hundreds = 0

            row1 = [("Inns", Inns), ("Runs", Runs), ("Balls", balls_faced),
                    ("Avg", Avg), ("S/R", SR), ("SB", Sb), ("Dots", Db)]

            row2 = [("1s", ones), ("2s", twos),
                    ("Fours", fours), ("Sixers", sixes),
                    ("30+", thirties), ("50+", fifties), ("100+", hundreds)]

        else:
            # Bowler - use same logic as KPI tables
            Runs_conc = int(bat_runs_s.sum())
            # Overs calculation: match KPI table format (overs.balls, not decimal)
            Overs = round(total_balls // 6 + (total_balls % 6) / 10, 1)
            Eco = safe_div(Runs_conc, Overs, places=2, dash_on_zero=True)

            # Scoring/Dot vs bowler
            scoring_mask_b = (bat_runs_s > 0)
            dot_mask_b = (bat_runs_s == 0)
            Sb = int(scoring_mask_b.sum())   # Scoring Balls
            Db = int(dot_mask_b.sum())       # Dots

            # Wickets - use same logic as KPI tables
            total_wickets = int(is_wkt_s.sum())

            # 4s/6s - count from all balls like KPI table
            fours = int((bat_runs_s == 4).sum())
            sixes = int((bat_runs_s == 6).sum())

            # Wickets milestones - use same logic as KPI tables
            bwl_key = BWL_ID if BWL_ID in player_df.columns else BWL_NAME
            bwl_gkeys = [k for k in [MATCH_COL, INN_COL, bwl_key] if k is not None]
            if bwl_gkeys:
                tmp = player_df.assign(_wkt=is_wkt_s.astype(int))
                wickets_by_inn = tmp.groupby(bwl_gkeys)["_wkt"].sum()
                two_w   = int(((wickets_by_inn >= 2) & (wickets_by_inn < 3)).sum())
                three_w = int(((wickets_by_inn >= 3) & (wickets_by_inn < 5)).sum())
                five_w  = int((wickets_by_inn >= 5).sum())
            else:
                two_w = three_w = five_w = 0

            row1 = [("Inns", Inns), ("W", total_wickets), ("Runs", Runs_conc), ("Balls", total_balls), ("Overs", Overs),
                    ("Eco", Eco), ("SB", Sb)]

            row2 = [("Dots", Db), ("Fours", fours), ("Sixers", sixes), 
                    ("2w+", two_w), ("3w+", three_w), ("5w+", five_w)]



        # KPI values (NO card — just the rows)
        def build_kpi_row(row_data):
            return "".join(
                f"""<div class="flex-1 min-w-[80px] text-center">
                        <div class="text-xs text-gray-700 dark:text-gray-300">{label}</div>
                        <div class="text-lg font-bold mt-1 text-gray-900 dark:text-white">{value}</div>
                    </div>"""
                for label, value in row_data
            )


        # *** updated: removed inner .card wrapper here ***
        kpi_card_html = f"""
            <div class="kpi-row" style="display:flex;gap:8px;justify-content:space-between;flex-wrap:wrap;">
                {build_kpi_row(row1)}
            </div>
            <div class="kpi-row" style="display:flex;gap:8px;justify-content:space-between;flex-wrap:wrap;margin-top:8px;">
                {build_kpi_row(row2)}
            </div>
        """

        # ---------------- Existing charts (responsive containers) ----------------
        stacked_bar_html = ""
        shot_area_data = get_top_shot_area_data(player_df, mode=("batter" if selected_type == "batter" else "bowler"))
        shot_type_data = get_top_shot_type_data(player_df, mode=("batter" if selected_type == "batter" else "bowler"))

        if shot_area_data and shot_type_data:
            areas = [d["area"] for d in shot_area_data]
            runs_1s_a = [d["1s_runs"] for d in shot_area_data]
            runs_2s_a = [d["2s_runs"] for d in shot_area_data]
            runs_3s_a = [d["3s_runs"] for d in shot_area_data]
            runs_4s_a = [d["4s_runs"] for d in shot_area_data]
            runs_6s_a = [d["6s_runs"] for d in shot_area_data]

            counts_1s_a = [d["1s_count"] for d in shot_area_data]
            counts_2s_a = [d["2s_count"] for d in shot_area_data]
            counts_3s_a = [d["3s_count"] for d in shot_area_data]
            counts_4s_a = [d["4s_count"] for d in shot_area_data]
            counts_6s_a = [d["6s_count"] for d in shot_area_data]

            shots = [d["shot"] for d in shot_type_data]
            runs_1s_s = [d["1s_runs"] for d in shot_type_data]
            runs_2s_s = [d["2s_runs"] for d in shot_type_data]
            runs_3s_s = [d["3s_runs"] for d in shot_type_data]
            runs_4s_s = [d["4s_runs"] for d in shot_type_data]
            runs_6s_s = [d["6s_runs"] for d in shot_type_data]

            counts_1s_s = [d["1s_count"] for d in shot_type_data]
            counts_2s_s = [d["2s_count"] for d in shot_type_data]
            counts_3s_s = [d["3s_count"] for d in shot_type_data]
            counts_4s_s = [d["4s_count"] for d in shot_type_data]
            counts_6s_s = [d["6s_count"] for d in shot_type_data]

            player_id_safe = re.sub(r"[^0-9a-zA-Z_]", "_", player)

            chart_title_areas = "Productive Areas (Top 3)" if selected_type == "batter" else "Runs Conceded Areas (Top 3)"
            chart_title_shots = "Productive Shots (Top 3)" if selected_type == "batter" else "Runs Conceded Shots (Top 3)"

            # Wickets donut data (outer + inner – using your helpers)
            labels, overall_counts, _, _ = get_wicket_type_counts(player_df)
            inner_labels, inner_counts = get_inner_donut_counts(player_df, selected_type)

            # NOTE: To keep layout simple & responsive, donuts are stacked (not overlaid absolutely)
            stacked_bar_html = f"""
            <div class="stacked-bar-container mt-4" style="display:flex;gap:20px;flex-wrap:wrap;">
                <!-- Areas Chart -->
                <div class="card" style="flex:1;min-width:300px;">
                    <div class="card-body">
                        <h6 class="mb-4 text-15">{chart_title_areas}</h6>
                        <div id="stackedChartAreas_{player_id_safe}" style="min-height:320px;"></div>

                        <!-- Custom Capsule Legend (below chart) -->
                        <div style="display:flex;justify-content:center;gap:8px;flex-wrap:wrap;margin-top:10px;font-size:12px;line-height:1.2;">
                            <div style="display:flex;min-width:60px;border-radius:12px;">
                                <!-- Left half -->
                                <span style="flex:1;text-align:center;padding:6px 0;background:#1E90FF;color:#fff;border-radius:12px 0 0 12px;">
                                    1s
                                </span>
                                <!-- Right half -->
                                <span style="flex:1;text-align:center;padding:6px 0;border:1.5px solid #1E90FF;color:#1E90FF;border-radius:0 12px 12px 0;">
                                    {sum(counts_1s_a)}
                                </span>
                            </div>
                            <div style="display:flex;min-width:60px;border-radius:12px;">
                                <!-- Left half -->
                                <span style="flex:1;text-align:center;padding:6px 0;background:#00FF7F;color:#000;border-radius:12px 0 0 12px;">
                                    2s
                                </span>
                                <!-- Right half -->
                                <span style="flex:1;text-align:center;padding:6px 0;border:1.5px solid #00FF7F;color:#00FF7F;border-radius:0 12px 12px 0;">
                                    {sum(counts_2s_a)}
                                </span>
                            </div>
                            <div style="display:flex;min-width:60px;border-radius:12px;">
                                <!-- Left half -->
                                <span style="flex:1;text-align:center;padding:6px 0;background:#FFD700;color:#000;border-radius:12px 0 0 12px;">
                                    3s
                                </span>
                                <!-- Right half -->
                                <span style="flex:1;text-align:center;padding:6px 0;border:1.5px solid #FFD700;color:#FFD700;border-radius:0 12px 12px 0;">
                                    {sum(counts_3s_a)}
                                </span>
                            </div>
                            <div style="display:flex;min-width:60px;border-radius:12px;">
                                <!-- Left half -->
                                <span style="flex:1;text-align:center;padding:6px 0;background:#FFA500;color:#000;border-radius:12px 0 0 12px;">
                                    4s
                                </span>
                                <!-- Right half -->
                                <span style="flex:1;text-align:center;padding:6px 0;border:1.5px solid #FFA500;color:#FFA500;border-radius:0 12px 12px 0;">
                                    {sum(counts_4s_a)}
                                </span>
                            </div>
                            <div style="display:flex;min-width:60px;border-radius:12px;">
                                <!-- Left half -->
                                <span style="flex:1;text-align:center;padding:6px 0;background:#FF0000;color:#fff;border-radius:12px 0 0 12px;">
                                    6s
                                </span>
                                <!-- Right half -->
                                <span style="flex:1;text-align:center;padding:6px 0;border:1.5px solid #FF0000;color:#FF0000;border-radius:0 12px 12px 0;">
                                    {sum(counts_6s_a)}
                                </span>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Shots Chart -->
                <div class="card" style="flex:1;min-width:300px;">
                    <div class="card-body">
                        <h6 class="mb-4 text-15">{chart_title_shots}</h6>
                        <div id="stackedChartShots_{player_id_safe}" style="min-height:320px;"></div>

                        <!-- Custom Capsule Legend (below chart) -->
                        <div style="display:flex;justify-content:center;gap:8px;flex-wrap:wrap;margin-top:10px;font-size:12px;line-height:1.2;">
                            <div style="display:flex;min-width:60px;border-radius:12px;">
                                <!-- Left half -->
                                <span style="flex:1;text-align:center;padding:6px 0;background:#1E90FF;color:#fff;border-radius:12px 0 0 12px;">
                                    1s
                                </span>
                                <!-- Right half -->
                                <span style="flex:1;text-align:center;padding:6px 0;border:1.5px solid #1E90FF;color:#1E90FF;border-radius:0 12px 12px 0;">
                                    {sum(counts_1s_s)}
                                </span>
                            </div>
                            <div style="display:flex;min-width:60px;border-radius:12px;">
                                <!-- Left half -->
                                <span style="flex:1;text-align:center;padding:6px 0;background:#00FF7F;color:#000;border-radius:12px 0 0 12px;">
                                    2s
                                </span>
                                <!-- Right half -->
                                <span style="flex:1;text-align:center;padding:6px 0;border:1.5px solid #00FF7F;color:#00FF7F;border-radius:0 12px 12px 0;">
                                    {sum(counts_2s_s)}
                                </span>
                            </div>
                            <div style="display:flex;min-width:60px;border-radius:12px;">
                                <!-- Left half -->
                                <span style="flex:1;text-align:center;padding:6px 0;background:#FFD700;color:#000;border-radius:12px 0 0 12px;">
                                    3s
                                </span>
                                <!-- Right half -->
                                <span style="flex:1;text-align:center;padding:6px 0;border:1.5px solid #FFD700;color:#FFD700;border-radius:0 12px 12px 0;">
                                    {sum(counts_3s_s)}
                                </span>
                            </div>
                            <div style="display:flex;min-width:60px;border-radius:12px;">
                                <!-- Left half -->
                                <span style="flex:1;text-align:center;padding:6px 0;background:#FFA500;color:#000;border-radius:12px 0 0 12px;">
                                    4s
                                </span>
                                <!-- Right half -->
                                <span style="flex:1;text-align:center;padding:6px 0;border:1.5px solid #FFA500;color:#FFA500;border-radius:0 12px 12px 0;">
                                    {sum(counts_4s_s)}
                                </span>
                            </div>
                            <div style="display:flex;min-width:60px;border-radius:12px;">
                                <!-- Left half -->
                                <span style="flex:1;text-align:center;padding:6px 0;background:#FF0000;color:#fff;border-radius:12px 0 0 12px;">
                                    6s
                                </span>
                                <!-- Right half -->
                                <span style="flex:1;text-align:center;padding:6px 0;border:1.5px solid #FF0000;color:#FF0000;border-radius:0 12px 12px 0;">
                                    {sum(counts_6s_s)}
                                </span>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Wickets Donut -->
                <div class="card" style="flex:1;min-width:300px;position:relative;height:320px;">
                    <div class="card-body">
                        <h6 class="mb-4 text-15">Wickets Breakdown</h6>
                        <div id="outerDonut_{player_id_safe}" style="position:absolute;top:58px;left:0;right:0;height:260px;"></div>
                        <div id="innerDonut_{player_id_safe}" style="position:absolute;top:88px;left:0;right:0;height:160px;"></div>
                    </div>
                </div>

                <script>
                window.addEventListener("load", function() {{
                    if (typeof ApexCharts === 'undefined') {{
                        console.error("ApexCharts library is not loaded!");
                        return;
                    }}
                    var optionsAreas = {{
                        series: [
                            {{ name: '1s', data: {runs_1s_a} }},
                            {{ name: '2s', data: {runs_2s_a} }},
                            {{ name: '3s', data: {runs_3s_a} }},
                            {{ name: '4s', data: {runs_4s_a} }},
                            {{ name: '6s', data: {runs_6s_a} }}
                        ],
                        chart: {{ type: 'bar', height: 300, stacked: true, toolbar: {{ show: false }} }},
                        plotOptions: {{ bar: {{ horizontal: true, barHeight: '50%', dataLabels: {{ total: {{ enabled: true }} }} }} }},
                        xaxis: {{ categories: {areas}, title: {{ text: 'Runs' }} }},
                        legend: {{ show: false }},
                        colors: ['#1E90FF','#00FF7F','#FFD700','#FFA500','#FF0000']
                    }};
                    new ApexCharts(document.querySelector("#stackedChartAreas_{player_id_safe}"), optionsAreas).render();

                    var optionsShots = {{
                        series: [
                            {{ name: '1s', data: {runs_1s_s} }},
                            {{ name: '2s', data: {runs_2s_s} }},
                            {{ name: '3s', data: {runs_3s_s} }},
                            {{ name: '4s', data: {runs_4s_s} }},
                            {{ name: '6s', data: {runs_6s_s} }}
                        ],
                        chart: {{ type: 'bar', height: 300, stacked: true, toolbar: {{ show: false }} }},
                        plotOptions: {{ bar: {{ horizontal: false, columnWidth: '50%', dataLabels: {{ total: {{ enabled: true }} }} }} }},
                        xaxis: {{ categories: {shots}, title: {{ text: 'Runs' }} }},
                        legend: {{ show: false }},
                        colors: ['#1E90FF','#00FF7F','#FFD700','#FFA500','#FF0000']
                    }};
                    new ApexCharts(document.querySelector("#stackedChartShots_{player_id_safe}"), optionsShots).render();

                    var chartColorsOuter = ["#FF4560", "#008FFB", "#00E396", "#FEB019", "#775DD0"];
                    var optionsOuter = {{
                        series: {overall_counts},
                        chart: {{ type: 'donut', height: 260 }},
                        labels: {labels},
                        colors: chartColorsOuter,
                        legend: {{ position: 'bottom' }},
                        plotOptions: {{ pie: {{ donut: {{ size: '70%' }} }} }}
                    }};
                    new ApexCharts(document.querySelector("#outerDonut_{player_id_safe}"), optionsOuter).render();

                    var chartColorsInner = ["#FF5733", "#33C1FF"];
                    var optionsInner = {{
                        series: {inner_counts},
                        chart: {{ type: 'donut', height: 160 }},
                        labels: {inner_labels},
                        colors: chartColorsInner,
                        legend: {{ show: false }},
                        plotOptions: {{ pie: {{ donut: {{ size: '60%' }} }} }}
                    }};
                    new ApexCharts(document.querySelector("#innerDonut_{player_id_safe}"), optionsInner).render();
                }});
                </script>
            </div>
            """





        # 📊 Radar / pitch maps (responsive)
        stats, labels_radar, breakdown_data = get_radar_data_from_player_df(player_df, selected_type)
        radar_chart_html = ""
        if stats and labels_radar and breakdown_data:
            if is_left_handed:
                # Radar chart
                img_url = generate_radar_chart(player, stats, labels_radar, breakdown_data, stance='LHB')
            else:
                img_url = generate_radar_chart(player, stats, labels_radar, breakdown_data, stance='RHB')

            # Pitch map background
            if is_left_handed:
                pitch_img_path = resource_path("tailwick/static/LeftHandPitchPad.jpg")
            else:
                pitch_img_path = resource_path("tailwick/static/RightHandPitchPad.jpg")

            pitch_map_url = plot_pitch_points_scaled(
                player_df[['scrM_PitchX', 'scrM_PitchY', 'scrM_BatsmanRuns', 'scrM_IsWicket']],
                pitch_img_path
            )

            # Arrival map background
            if is_left_handed:
                arrival_img_path = resource_path("tailwick/static/LeftHandArrivalNew.jpg")
            else:
                arrival_img_path = resource_path("tailwick/static/RightHandArrivalNew.jpg")

            arrival_map_url = plot_arrival_points_scaled(
                player_df[['scrM_BatPitchX', 'scrM_BatPitchY', 'scrM_BatsmanRuns', 'scrM_IsWicket']],
                arrival_img_path
            )


            import base64

            # 🔧 Helper to embed static ball icons as base64 <img>
            def embed_ball_icon(path, label):
                with open(path, "rb") as f:
                    encoded = base64.b64encode(f.read()).decode()
                return f"""
                <div style="display:flex;align-items:center;gap:3px;">
                    <img src="data:image/png;base64,{encoded}" alt="{label}" style="width:16px;height:16px;" />
                    <span>{label}</span>
                </div>
                """

            # ✅ Legend for Pitch Pad
            legend_html_pitch = f"""
            <div style="width:100%; max-width:260px; background-color:#dddddd; border-radius:8px; padding:5px;
                        display:flex; justify-content:space-around; align-items:center; margin-top:5px;
                        color:black; font-size:10px; flex-wrap:wrap;">
                {embed_ball_icon(resource_path("tailwick/static/MaroonBall_Resized.png"), "0")}
                {embed_ball_icon(resource_path("tailwick/static/BlueBall_Resized.png"), "1")}
                {embed_ball_icon(resource_path("tailwick/static/PinkBall_Resized.png"), "2")}
                {embed_ball_icon(resource_path("tailwick/static/YellowBall_Resized.png"), "3")}
                {embed_ball_icon(resource_path("tailwick/static/OrangeBall_Resized.png"), "4")}
                {embed_ball_icon(resource_path("tailwick/static/GreenBall_Resized.png"), "6")}
                {embed_ball_icon(resource_path("tailwick/static/RedBall_Resized.png"), "W")}
            </div>
            """

            # ✅ Legend for Arrival Pad
            legend_html_arrival = f"""
            <div style="width:100%; max-width:320px; background-color:#dddddd; border-radius:8px; padding:5px;
                        display:flex; justify-content:space-around; align-items:center; margin-top:5px;
                        color:black; font-size:10px; flex-wrap:wrap;">
                {embed_ball_icon(resource_path("tailwick/static/MaroonBall_Resized.png"), "0")}
                {embed_ball_icon(resource_path("tailwick/static/BlueBall_Resized.png"), "1")}
                {embed_ball_icon(resource_path("tailwick/static/PinkBall_Resized.png"), "2")}
                {embed_ball_icon(resource_path("tailwick/static/YellowBall_Resized.png"), "3")}
                {embed_ball_icon(resource_path("tailwick/static/OrangeBall_Resized.png"), "4")}
                {embed_ball_icon(resource_path("tailwick/static/GreenBall_Resized.png"), "6")}
                {embed_ball_icon(resource_path("tailwick/static/RedBall_Resized.png"), "W")}
            </div>
            """



            # Generate unique chart ID for this player
            import json
            chart_id = f"radar_{player.replace(' ', '_').replace('.', '_')}"
            breakdown_json = json.dumps(breakdown_data)
            stance_value = 'LHB' if is_left_handed else 'RHB'
            
            radar_chart_html = f"""
            <style>
                .charts-container {{ display:flex; flex-direction:column; width:100%; gap:20px; }}
                .first-row {{ display:flex; flex-direction:row; gap:20px; flex-wrap:wrap; }}
                .first-row > div {{ display:flex; flex-direction:column; align-items:center; }}
            </style>
            <div class="charts-container">
                <div class="first-row">
                    <div style="width: 400px;">
                        <img id="{chart_id}" src="{img_url}" alt="Wagon Wheel - {player}" style="width: 100%; height: auto; border-radius: 8px;" />
                        
                        <!-- Checkbox Filters for Radar Chart -->
                        <div style="margin-top: 15px; display: flex; flex-wrap: wrap; justify-content: center; gap: 12px;">
                            <label style="display: inline-flex; align-items: center; cursor: pointer;">
                                <input type="checkbox" class="radar-filter form-checkbox h-4 w-4 text-custom-500 border-slate-300 rounded focus:ring-custom-500" 
                                       data-chart="{chart_id}" value="all" checked 
                                       onchange="updateRadarChart_{chart_id}()">
                                <span style="margin-left: 6px; font-size: 13px; font-weight: 500; color: #475569;">All</span>
                            </label>
                            <label style="display: inline-flex; align-items: center; cursor: pointer;">
                                <input type="checkbox" class="radar-filter form-checkbox h-4 w-4 text-custom-500 border-slate-300 rounded focus:ring-custom-500" 
                                       data-chart="{chart_id}" value="1" 
                                       onchange="updateRadarChart_{chart_id}()">
                                <span style="margin-left: 6px; font-size: 13px; font-weight: 500; color: #475569;">1s</span>
                            </label>
                            <label style="display: inline-flex; align-items: center; cursor: pointer;">
                                <input type="checkbox" class="radar-filter form-checkbox h-4 w-4 text-custom-500 border-slate-300 rounded focus:ring-custom-500" 
                                       data-chart="{chart_id}" value="2" 
                                       onchange="updateRadarChart_{chart_id}()">
                                <span style="margin-left: 6px; font-size: 13px; font-weight: 500; color: #475569;">2s</span>
                            </label>
                            <label style="display: inline-flex; align-items: center; cursor: pointer;">
                                <input type="checkbox" class="radar-filter form-checkbox h-4 w-4 text-custom-500 border-slate-300 rounded focus:ring-custom-500" 
                                       data-chart="{chart_id}" value="3" 
                                       onchange="updateRadarChart_{chart_id}()">
                                <span style="margin-left: 6px; font-size: 13px; font-weight: 500; color: #475569;">3s</span>
                            </label>
                            <label style="display: inline-flex; align-items: center; cursor: pointer;">
                                <input type="checkbox" class="radar-filter form-checkbox h-4 w-4 text-custom-500 border-slate-300 rounded focus:ring-custom-500" 
                                       data-chart="{chart_id}" value="4" 
                                       onchange="updateRadarChart_{chart_id}()">
                                <span style="margin-left: 6px; font-size: 13px; font-weight: 500; color: #475569;">4s</span>
                            </label>
                            <label style="display: inline-flex; align-items: center; cursor: pointer;">
                                <input type="checkbox" class="radar-filter form-checkbox h-4 w-4 text-custom-500 border-slate-300 rounded focus:ring-custom-500" 
                                       data-chart="{chart_id}" value="6" 
                                       onchange="updateRadarChart_{chart_id}()">
                                <span style="margin-left: 6px; font-size: 13px; font-weight: 500; color: #475569;">6s</span>
                            </label>
                        </div>
                        
                        <script>
                        function updateRadarChart_{chart_id}() {{
                            const checkboxes = document.querySelectorAll('input.radar-filter[data-chart="{chart_id}"]');
                            const allCheckbox = document.querySelector('input.radar-filter[data-chart="{chart_id}"][value="all"]');
                            
                            let selectedTypes = [];
                            let allSelected = allCheckbox.checked;
                            
                            checkboxes.forEach(cb => {{
                                if (cb.value !== 'all' && cb.checked) {{
                                    selectedTypes.push(cb.value);
                                }}
                            }});
                            
                            if (allSelected || selectedTypes.length === 0) {{
                                selectedTypes = ['1', '2', '3', '4', '6'];
                                if (allSelected) {{
                                    checkboxes.forEach(cb => {{
                                        if (cb.value !== 'all') cb.checked = false;
                                    }});
                                }}
                            }} else {{
                                allCheckbox.checked = false;
                            }}
                            
                            const breakdownData = {breakdown_json};
                            const filteredBreakdown = breakdownData.map(sector => {{
                                let filtered = {{'1s': 0, '2s': 0, '3s': 0, '4s': 0, '6s': 0}};
                                selectedTypes.forEach(type => {{
                                    const key = type + 's';
                                    if (sector[key] !== undefined) {{
                                        filtered[key] = sector[key];
                                    }}
                                }});
                                return filtered;
                            }});
                            
                            fetch('/regenerate_radar_chart', {{
                                method: 'POST',
                                headers: {{'Content-Type': 'application/json'}},
                                body: JSON.stringify({{
                                    player_name: '{player}',
                                    breakdown_data: filteredBreakdown,
                                    stance: '{stance_value}'
                                }})
                            }})
                            .then(response => response.json())
                            .then(data => {{
                                if (data.success && data.image) {{
                                    document.getElementById('{chart_id}').src = data.image;
                                }}
                            }})
                            .catch(error => console.error('Error updating radar chart:', error));
                        }}
                        </script>
                    </div>
                    <div>
                        <img src="{pitch_map_url}" alt="Pitch Map" style="width: 250px; height: 400px; border-radius: 8px;" />
                        {legend_html_pitch}
                    </div>
                    <div>
                        <img src="{arrival_map_url}" alt="Arrival Zones" style="width: 300px; height: 250px; border-radius: 8px;" />
                        {legend_html_arrival}
                    </div>
                </div>
            </div>
            """

        # ✅ Combined HTML with profile + name and KPI values (no inner card)
        # ✅ Generate strengths & weaknesses dynamically for this player

        def embed_profile_image(path, player, size=60):
            import base64, os
            if os.path.exists(path):
                with open(path, "rb") as f:
                    encoded = base64.b64encode(f.read()).decode()
                src = f"data:image/png;base64,{encoded}"
            else:
                # fallback: use a blank silhouette
                src = "https://via.placeholder.com/60x60.png?text=P"

            return f"""
            <div style="width:{size}px;height:{size}px;border-radius:50%;overflow:hidden;border:2px solid #ccc;">
                <img src="{src}" alt="{player} profile" style="width:100%;height:100%;object-fit:cover;" />
            </div>
            """

        strengths_html, weaknesses_html = generate_dynamic_strengths_weaknesses(player_df, player, selected_type)

        # ✅ Combined HTML with profile + name and KPI values inside a Sky Card background
        # ✅ Combined HTML with profile + name and KPI values inside a Sky Card background
        combined_html = f"""
            <style>
                .pdf-icon-btn {{
                    display:flex;align-items:center;justify-content:center;
                    width:34px;height:34px;border-radius:10px;cursor:pointer;
                    background:rgba(255,255,255,.08);backdrop-filter:blur(2px);
                    border:1px solid rgba(255,255,255,.15);
                    box-shadow:0 2px 8px rgba(0,0,0,.35);
                    transition:transform .18s ease;
                }}
                .pdf-icon-btn:hover {{ transform: translateY(-1px); }}
                .pdf-icon-btn img {{ width:22px;height:22px;pointer-events:none; }}

                /* ✅ Mobile responsiveness fix */
                @media (max-width: 480px) {{
                    /* Stack profile + KPI vertically */
                    .player-card {{
                        flex-direction: column !important;
                        align-items: stretch !important;
                    }}
                    .kpi-col {{
                        flex: 1 1 100% !important;
                        min-width: 0 !important;
                    }}
                    /* Stack Strengths & Weaknesses vertically */
                    .sw-row {{
                        flex-direction: column !important;
                        gap: 12px !important;
                    }}
                    .sw-card {{
                        flex: 1 1 100% !important;
                        min-width: 0 !important;
                    }}
                    /* Prevent horizontal scroll */
                    body, html {{
                        overflow-x: hidden;
                    }}
                }}
            </style>

            <!-- OUTER WRAPPER -->
            <div class="mb-6" style="max-width:100%;overflow:hidden;position:relative;">

                <!-- BLUE CARD -->
                <div class="card bg-sky-500 border-sky-500 dark:bg-sky-800 dark:border-sky-800 player-card"
                    style="border-radius:10px;padding:12px;display:flex;flex-wrap:wrap;gap:12px;align-items:center;justify-content:space-between;">
                    
                    <!-- Profile & Name -->
                    <div style="display:flex;flex-direction:column;align-items:center;flex:0 0 auto;">
                        {embed_profile_image(resource_path("tailwick/static/Default-M.png"), player)}
                        <div style="margin-top:6px;font-weight:600;font-size:14px;color:#ffffff;text-align:center;">{player}</div>
                    </div>

                    <!-- KPI Values -->
                    <div class="kpi-col" style="flex:1 1 220px;min-width:0;max-width:100%;">{kpi_card_html}</div>
                </div>

                <!-- Match KPI Table -->
                <div class="overflow-x-auto rounded-md mb-4" style="max-width:100%;">{match_kpi_html}</div>

                {radar_chart_html}
                {stacked_bar_html}

                <!-- Strengths & Weaknesses -->
                <div class="sw-row" style="display:flex;gap:20px;flex-wrap:wrap;margin-top:20px;">
                    <div class="sw-card card border border-green-400 bg-green-50 dark:bg-green-900 dark:border-green-700"
                        style="flex:1 1 320px;min-width:0;border-radius:10px;padding:12px;">
                        <div class="card-body">
                            <h6 class="mb-3 text-15 font-semibold text-green-700 dark:text-green-300">Strengths</h6>
                            {strengths_html}
                        </div>
                    </div>
                    <div class="sw-card card border border-red-400 bg-red-50 dark:bg-red-900 dark:border-red-700"
                        style="flex:1 1 320px;min-width:0;border-radius:10px;padding:12px;">
                        <div class="card-body">
                            <h6 class="mb-3 text-15 font-semibold text-red-700 dark:text-red-300">Weaknesses</h6>
                            {weaknesses_html}
                        </div>
                    </div>
                </div>
            </div>
        """

        combined_tables[player] = combined_html


    return combined_tables

def get_match_header(match_id):
    """
    Fetch match header info for given match id.
    """
    import pandas as pd
    try:
        conn = get_connection()
        query = """
        WITH LatestScore AS (
            SELECT scrM_DayNo, scrM_SessionNo
            FROM tblscoremaster
            WHERE scrM_MchMId = %s
            ORDER BY scrM_DayNo DESC, scrM_SessionNo DESC
            LIMIT 1
        ),
        LatestInnings AS (
            SELECT mi.inn_Day AS Inn_Day, mi.inn_Session AS Inn_Session
            FROM tblmatchinnings mi
            INNER JOIN tblmatchmaster m ON mi.inn_mchMId = m.mchM_Id
            WHERE m.mchM_Id = %s
            ORDER BY mi.inn_Day DESC, mi.inn_Session DESC
            LIMIT 1
        )
        SELECT
            t.trnM_TournamentName AS TournamentName,
            m.mchM_MatchName AS MatchName,
            DATE_FORMAT(m.mchM_StartDateTime, '%%d-%%b-%%Y') AS MatchDate,
            g.grdM_GroundName AS GroundName,
            m.mchM_ResultRemark AS ResultText,
            CASE
                WHEN m.mchM_IsMatchCompleted = 0
                     AND (ls.scrM_DayNo IS NOT NULL AND ls.scrM_SessionNo IS NOT NULL)
                THEN CONCAT('Day ', ls.scrM_DayNo, ' - Session ', ls.scrM_SessionNo)
                WHEN m.mchM_IsMatchCompleted = 1
                     AND (li.Inn_Day IS NOT NULL AND li.Inn_Session IS NOT NULL)
                THEN CONCAT('Day ', li.Inn_Day, ' - Session ', li.Inn_Session)
                ELSE CASE
                    WHEN m.mchM_IsMatchCompleted = 1 THEN 'Match Ended'
                    ELSE 'Live - Session Info N/A'
                END
            END AS DaySessionText
        FROM tblmatchmaster m
        INNER JOIN tbltournaments t ON m.mchM_TrnMId = t.trnM_Id
        LEFT JOIN tblgroundmaster g ON m.mchM_grdMId = g.grdM_Id
        LEFT JOIN LatestScore ls ON 1=1
        LEFT JOIN LatestInnings li ON 1=1
        WHERE m.mchM_Id = %s
        """
        df = pd.read_sql(query, conn, params=(match_id, match_id, match_id))
        conn.close()
        return df.to_dict(orient="records")[0] if not df.empty else None

    except Exception as e:
        print("❌ Error in get_match_header:", e)
        return None




def get_match_innings(match_id):

    """
    Fetch innings summary for a given match (ordered by Inn_Inning) using match id.
    Returns list of dicts with TeamShortName, InningNo, Runs, Wickets, Overs.
    Works with MySQL (pymysql).
    """
    import pandas as pd

    try:
        conn = get_connection()

        query = """
        SELECT 
            i.Inn_Inning,
            tm.tmM_ShortName AS TeamShortName,
            i.Inn_TotalRuns,
            i.Inn_Wickets,
            i.Inn_Overs,
            i.Inn_DeliveriesOfLastIncompleteOver
        FROM tblmatchinnings i
        INNER JOIN tblmatchmaster m ON i.Inn_mchMId = m.mchM_Id
        INNER JOIN tblteammaster tm ON i.Inn_tmMIdBatting = tm.tmM_Id
        WHERE m.mchM_Id = %s
        ORDER BY i.Inn_Inning
        """

        df = pd.read_sql(query, conn, params=(match_id,))
        conn.close()

        # compute a display-friendly overs string (e.g., 17.2 for 17 overs and 2 deliveries)
        def make_overs_display(row):
            try:
                overs = int(row.get("Inn_Overs") or 0)
            except Exception:
                overs = 0
            try:
                dels = int(row.get("Inn_DeliveriesOfLastIncompleteOver") if row.get("Inn_DeliveriesOfLastIncompleteOver") is not None else 0)
            except Exception:
                dels = 0

            if dels and dels > 0:
                return f"{overs}.{dels}"
            return str(overs)

        if not df.empty:
            df["Inn_OversDisplay"] = df.apply(make_overs_display, axis=1)

        return df.to_dict(orient="records")

    except Exception as e:
        print("❌ Error in get_match_innings:", e)
        return []

    
def get_last_12_deliveries(match_id):
    """
    Fetch last 12 deliveries for a match (for score visualization).
    Works with MySQL (pymysql).
    Uses scrM_MchMId (match_id) ✅
    """

    import pandas as pd

    try:
        conn = get_connection()

        query = """
            SELECT
                scrM_BatsmanRuns,
                scrM_IsWicket,
                scrM_IsNoBall,
                scrM_NoBallRuns,
                scrM_IsWideBall,
                scrM_WideRuns
            FROM tblscoremaster
            WHERE scrM_MchMId = %s
            ORDER BY scrM_DelId DESC
            LIMIT 12
        """

        df = pd.read_sql(query, conn, params=(match_id,))
        conn.close()

        deliveries = []

        for _, row in df.iterrows():
            # ✅ Wicket
            if int(row.get("scrM_IsWicket", 0)) == 1:
                deliveries.append("W")

            # ✅ No ball
            elif int(row.get("scrM_IsNoBall", 0)) == 1:
                nb_runs = int(row["scrM_NoBallRuns"]) if pd.notna(row["scrM_NoBallRuns"]) else 0
                deliveries.append(f"{nb_runs}NB" if nb_runs > 0 else "NB")

            # ✅ Wide
            elif int(row.get("scrM_IsWideBall", 0)) == 1:
                wide_runs = int(row["scrM_WideRuns"]) if pd.notna(row["scrM_WideRuns"]) else 0
                deliveries.append(f"{wide_runs}Wd" if wide_runs > 0 else "Wd")

            # ✅ Normal delivery
            else:
                runs = int(row["scrM_BatsmanRuns"]) if pd.notna(row["scrM_BatsmanRuns"]) else 0
                deliveries.append(str(runs) if runs > 0 else "·")

        # ✅ Reverse so latest ball is on right side
        return list(reversed(deliveries))

    except Exception as e:
        print("❌ Error in get_last_12_deliveries:", e)
        return []



import pyodbc
import pandas as pd

def get_ball_by_ball_data(match_id):
    """
    Fetch ball-by-ball data for a given match using Match ID (scrM_MchMId).
    Works with MySQL (pymysql).
    """
    import pandas as pd

    try:
        conn = get_connection()

        query = """
            SELECT
                scrM_DelId,
                scrM_MchMId,
                scrM_MatchName,
                scrM_InningNo,
                scrM_OverNo,
                scrM_DelNo,
                scrM_PitchXPos,
                scrM_BatPitchXPos,
                scrM_IsBoundry,
                scrM_IsSixer,
                scrM_WideRuns,
                scrM_DeliveryType_zName,
                scrM_tmMIdBowlingName,
                scrM_PlayMIdStriker,
                scrM_PlayMIdStrikerName,
                scrM_PlayMIdNonStrikerName,
                scrM_PlayMIdBowler,
                scrM_PlayMIdBowlerName,
                scrM_tmMIdBattingName,
                scrM_IsValidBall,
                scrM_NoBallRuns,
                scrM_BatsmanRuns,
                scrM_DayNo,
                scrM_SessionNo,
                scrM_DelRuns,
                scrM_IsWicket,
                scrM_IsNoBall,
                scrM_IsWideBall,
                scrM_ByeRuns,
                scrM_LegByeRuns,
                scrM_PenaltyRuns,
                scrM_playMIdCaughtName,
                scrM_playMIdRunOutName,
                scrM_playMIdStumpingName,
                scrM_PitchArea_zName,
                scrM_BatPitchArea_zName,
                scrM_ShotType_zName,
                scrM_BowlerSkill,
                scrM_Wagon_x,
                scrM_Wagon_y,
                scrM_WagonArea_zName,
                scrM_PitchX,
                scrM_PitchY,
                scrM_Video1URL,
                scrM_Video2URL,
                scrM_Video3URL,
                scrM_Video4URL,
                scrM_Video5URL,
                scrM_Video6URL,
                scrM_StrikerBatterSkill
            FROM tblscoremaster
            WHERE scrM_MchMId = %s
            ORDER BY scrM_InningNo, scrM_OverNo, scrM_DelNo
        """

        df = pd.read_sql(query, conn, params=(str(match_id),))
        conn.close()

        # Normalize BatterHand column
        def normalize_hand(val):
            if pd.isna(val):
                return None
            val = str(val).lower()
            if "right" in val:
                return "Right"
            elif "left" in val:
                return "Left"
            return None

        df["BatterHand"] = df["scrM_StrikerBatterSkill"].apply(normalize_hand)

        print(f"✅ Loaded {len(df)} ball-by-ball rows for match_id {match_id}")
        return df

    except Exception as e:
        print("❌ Error fetching ball-by-ball data:", e)
        return pd.DataFrame()







import os
import io
import math
import base64
import time
import pandas as pd
import matplotlib.pyplot as plt
from PIL import Image

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

# if you added global_store.py
from .global_store import BATTER_DATA

# static dir used by generate_wagon_wheel - make sure STATIC_DIR is defined in your utils.py
# If not, set it to the folder you keep the wheel background images in:
# STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
# (adjust above if you already define STATIC_DIR elsewhere)
try:
    STATIC_DIR
except NameError:
    STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


def generate_wagon_wheel(bdf: pd.DataFrame, batter_hand: str, filter_runs=None) -> str:
    """
    Draws wagon wheel lines where ALL shots touch the boundary.
    Returns a base64 PNG exactly 300x300 px.
    If filter_runs is set (1,2,3,4,6), only that run type is drawn.
    """
    # select background
    if str(batter_hand).strip().lower() == "right":
        base_img_path = os.path.join(STATIC_DIR, "RightHandWheel.png")
    elif str(batter_hand).strip().lower() == "left":
        base_img_path = os.path.join(STATIC_DIR, "LeftHandWheel.png")
    else:
        base_img_path = os.path.join(STATIC_DIR, "DefaultWheel.png")

    # Load base image
    try:
        base = Image.open(base_img_path).convert("RGBA")
    except Exception:
        base = Image.new("RGBA", (300, 300), (255, 255, 255, 0))

    if base.size != (300, 300):
        base = base.resize((300, 300), Image.LANCZOS)

    W, H = base.size

    # ✅ Adjust origin slightly UP to match batsman crease (offline logic)
    cx = W // 2
    cy = int(H // 2 - H * 0.045)   # ~7% upward shift (tuned to pitch image)


    rim_thickness = 20
    radius = (min(W, H) / 2) - rim_thickness

    # color map
    color_map = {
        1: "#1E90FF",
        2: "#00FF7F",
        3: "#FFD700",
        4: "#FFA500",
        6: "#FF0000"
    }

    # discover coordinates
    xs, ys = None, None
    if isinstance(bdf, pd.DataFrame):
        if "scrM_Wagon_x" in bdf.columns and "scrM_Wagon_y" in bdf.columns:
            xs = pd.to_numeric(bdf["scrM_Wagon_x"], errors="coerce").dropna()
            ys = pd.to_numeric(bdf["scrM_Wagon_y"], errors="coerce").dropna()
        elif "scrM_ShotX" in bdf.columns and "scrM_ShotY" in bdf.columns:
            xs = pd.to_numeric(bdf["scrM_ShotX"], errors="coerce").dropna()
            ys = pd.to_numeric(bdf["scrM_ShotY"], errors="coerce").dropna()
        elif "Angle" in bdf.columns and "Distance" in bdf.columns:
            angles = np.radians(bdf["Angle"].astype(float).fillna(0))
            dist = bdf["Distance"].astype(float).fillna(0)
            xs = dist * np.cos(angles)
            ys = dist * np.sin(angles)

    if xs is None or ys is None or len(xs) == 0:
        buf = io.BytesIO()
        base.save(buf, format="PNG")
        return base64.b64encode(buf.getvalue()).decode("utf-8")

    # normalize
    cx_db = (xs.min() + xs.max()) / 2.0
    cy_db = (ys.min() + ys.max()) / 2.0
    rx_db = max((xs - cx_db).abs().max(), 1e-6)
    ry_db = max((ys - cy_db).abs().max(), 1e-6)

    mirror = (str(batter_hand).strip().lower() == "left")

    fig = plt.figure(figsize=(3, 3), dpi=100)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.imshow(base, extent=[0, W, H, 0])
    ax.set_xlim(0, W)
    ax.set_ylim(H, 0)
    ax.axis("off")

    # draw shots
    for _, row in bdf.iterrows():
        try:
            runs_int = int(row.get("scrM_BatsmanRuns"))
        except Exception:
            continue

        if runs_int not in color_map:
            continue
        if filter_runs and runs_int != int(filter_runs):
            continue

        if "scrM_Wagon_x" in bdf.columns and "scrM_Wagon_y" in bdf.columns:
            x, y = row.get("scrM_Wagon_x"), row.get("scrM_Wagon_y")
        elif "scrM_ShotX" in bdf.columns and "scrM_ShotY" in bdf.columns:
            x, y = row.get("scrM_ShotX"), row.get("scrM_ShotY")
        elif "Angle" in row:
            try:
                angle = math.radians(float(row.get("Angle", 0)))
                x = math.cos(angle)
                y = math.sin(angle)
            except Exception:
                continue
        else:
            continue

        if x is None or y is None:
            continue

        try:
            xn = (float(x) - cx_db) / rx_db
            yn = (float(y) - cy_db) / ry_db
        except Exception:
            continue

        if mirror:
            xn = -xn

        length = math.sqrt(xn * xn + yn * yn)
        if length == 0:
            continue

        xn /= length
        yn /= length

        # ✅ FIX: ALL shots touch the boundary
        shot_len = radius

        px = cx + xn * shot_len
        py = cy - yn * shot_len

        ax.plot(
            [cx, px], [cy, py],
            color=color_map[runs_int],
            linewidth=1.2,
            alpha=0.95,
            solid_capstyle="round"
        )

    buf = io.BytesIO()
    fig.savefig(
        buf,
        format="png",
        dpi=100,
        facecolor="none",
        edgecolor="none",
        bbox_inches="tight",
        pad_inches=0
    )
    plt.close(fig)

    return base64.b64encode(buf.getvalue()).decode("utf-8")


def generate_player_radar_chart(player_df, player_name, selected_type, selected_team=None, run_filter=None):
    """
    Generate radar chart for individual player (similar to team analysis areawise tab).
    Shows wagon area breakdown with runs and percentages.
    """
    import numpy as np
    import matplotlib.pyplot as plt
    import io
    import base64
    
    matplotlib.use("Agg")
    
    print(f"🔍 generate_player_radar_chart called:")
    print(f"   - Player: {player_name}")
    print(f"   - Input rows: {len(player_df)}")
    print(f"   - Selected team: {selected_team}")
    print(f"   - Run filter: {run_filter}")
    
    # Filter by team if provided
    if selected_team:
        team_col = "scrM_tmMIdBattingName" if selected_type == "batter" else "scrM_tmMIdBowlingName"
        if team_col in player_df.columns:
            before_count = len(player_df)
            player_df = player_df[player_df[team_col] == selected_team].copy()
            print(f"   - After team filter: {len(player_df)} rows (was {before_count})")
    
    # Apply run filter
    if run_filter and run_filter != ["all"]:
        try:
            run_filter_int = [int(x) for x in run_filter if x != "all"]
            if run_filter_int:
                player_df = player_df[player_df["scrM_BatsmanRuns"].isin(run_filter_int)]
        except Exception:
            pass
    
    # Sector definitions
    sectors = ["Mid Wicket", "Square Leg", "Fine Leg", "Third Man",
               "Point", "Covers", "Long Off", "Long On"]
    sector_map = {name: i for i, name in enumerate(sectors)}
    sector_angles_deg = [112.5, 67.5, 22.5, 337.5, 292.5, 247.5, 202.5, 157.5]
    
    # Calculate breakdown per sector
    breakdown = [{"1s":0,"2s":0,"3s":0,"4s":0,"6s":0} for _ in sectors]
    
    for _, row in player_df.iterrows():
        sector = str(row.get("scrM_WagonArea_zName", ""))
        try:
            runs = int(row.get("scrM_BatsmanRuns", 0))
        except:
            runs = 0
        
        if sector in sector_map and runs > 0:
            idx = sector_map[sector]
            if runs == 1: breakdown[idx]["1s"] += 1
            elif runs == 2: breakdown[idx]["2s"] += 1
            elif runs == 3: breakdown[idx]["3s"] += 1
            elif runs == 4: breakdown[idx]["4s"] += 1
            elif runs == 6: breakdown[idx]["6s"] += 1
    
    # Create radar chart
    fig, ax = plt.subplots(figsize=(8,8), subplot_kw=dict(polar=True))
    
    ax.set_theta_zero_location("N")
    ax.set_theta_direction(-1)
    ax.set_xticks([])
    ax.set_yticks([])
    ax.spines['polar'].set_visible(False)
    ax.set_aspect('equal')
    
    scale = 0.9
    
    # Ground
    ax.add_artist(plt.Circle((0,0), 1.0*scale, transform=ax.transData._b,
                             color='#19a94b', zorder=0))
    ax.add_artist(plt.Circle((0,0), 0.6*scale, transform=ax.transData._b,
                             color='#4CAF50', zorder=1))
    
    # Pitch
    ax.add_artist(plt.Rectangle((-0.08*scale/2, -0.33*scale/2),
                                0.08*scale, 0.33*scale,
                                transform=ax.transData._b,
                                color='burlywood', zorder=2))
    
    # Black rim
    rim_radius = 1.10 * scale
    rim_circle = plt.Circle((0, 0), rim_radius, transform=ax.transData._b,
                            color='#6dbc45', linewidth=26, fill=False,
                            zorder=5, clip_on=False)
    ax.add_artist(rim_circle)
    
    # Sector lines
    for angle in np.linspace(0, 2*np.pi, 9):
        ax.plot([angle, angle], [0,1.0*scale], color='white', linewidth=3)
    
    # Calculate runs per sector
    sector_runs = [
        bd["1s"] + bd["2s"]*2 + bd["3s"]*3 + bd["4s"]*4 + bd["6s"]*6
        for bd in breakdown
    ]
    total_runs = sum(sector_runs)
    
    # Highlight max sector
    if total_runs > 0:
        max_i = sector_runs.index(max(sector_runs))
        ax.bar(np.deg2rad(sector_angles_deg[max_i]), 1.0*scale,
               width=np.radians(45), color='red', alpha=0.25)
    
    # Field labels
    label_info = [
        ("Mid Wicket",112.5,-110,-0.02),
        ("Square Leg",67.5,-70,-0.02),
        ("Fine Leg",22.5,-25,0.00),
        ("Third Man",337.5,20,-0.02),
        ("Point",292.5,70,-0.01),
        ("Covers",247.5,110,-0.01),
        ("Long Off",202.5,155,-0.02),
        ("Long On",157.5,200,-0.02)
    ]
    
    for text, ang, rot, off in label_info:
        ax.text(np.deg2rad(ang), rim_radius + off, text,
                color='white', fontsize=16, fontweight='bold',
                ha='center', va='center',
                rotation=rot, rotation_mode='anchor',
                clip_on=False, zorder=10)
    
    # Runs + percentage text
    box_pos = [
        (103.5,0,0.70), (67.5,0,0.70),
        (22.5,0,0.80), (337.5,0,0.80),
        (295.5,0,0.75), (250.5,0,0.70),
        (204.5,1,0.59), (155.5,1,0.59)
    ]
    
    for i,(ang,rot,dist) in enumerate(box_pos):
        r = dist*scale
        runs = sector_runs[i]
        pct = (runs/total_runs*100) if total_runs>0 else 0
        
        ax.text(np.deg2rad(ang), r,
                f"{runs}\n({pct:.1f}%)",
                ha='center', va='center',
                fontsize=19, fontweight='bold',
                color='white', linespacing=1.15)
    
    # Convert to base64
    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=260, transparent=True)
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode()



import plotly.graph_objects as go
import pandas as pd
import plotly.io as pio

import plotly.graph_objects as go
import pandas as pd
import plotly.io as pio

def create_partnership_chart(innings_df, team_name=None):
    # === Handle empty df ===
    if innings_df.empty:
        fig = go.Figure()
        fig.update_layout(
            annotations=[dict(
                text="No Data Available",
                xref="paper", yref="paper",
                showarrow=False,
                font=dict(size=20, color="red"),
                x=0.5, y=0.5
            )],
            height=400, width=800,
            plot_bgcolor="white", paper_bgcolor="white"
        )
        return pio.to_html(fig, full_html=False, include_plotlyjs='cdn')

    # ✅ Calculate Extras
    if set(["scrM_ByeRuns", "scrM_LegByeRuns", "scrM_NoBallRuns",
            "scrM_WideRuns", "scrM_PenaltyRuns"]).issubset(innings_df.columns):
        innings_df["scrM_Extras"] = innings_df[
            ["scrM_ByeRuns", "scrM_LegByeRuns", "scrM_NoBallRuns",
             "scrM_WideRuns", "scrM_PenaltyRuns"]
        ].sum(axis=1)
    else:
        innings_df["scrM_Extras"] = 0

    innings_df["Valid_Ball"] = innings_df.get("scrM_WideRuns", 0) == 0

    innings_df["Partnership_Key"] = innings_df.apply(
        lambda row: "_&_".join(sorted([
            str(row["scrM_PlayMIdStrikerName"]),
            str(row["scrM_PlayMIdNonStrikerName"])
        ])),
        axis=1
    )

    partnerships = []
    for _, group in innings_df[innings_df["Valid_Ball"]].groupby("Partnership_Key"):
        striker = group["scrM_PlayMIdStrikerName"].iloc[0]
        non_striker = group["scrM_PlayMIdNonStrikerName"].iloc[0]

        batter1 = min(striker, non_striker)
        batter2 = max(striker, non_striker)

        batter1_runs = group[group["scrM_PlayMIdStrikerName"] == batter1]["scrM_BatsmanRuns"].sum()
        batter1_balls = group[group["scrM_PlayMIdStrikerName"] == batter1]["Valid_Ball"].sum()

        batter2_runs = group[group["scrM_PlayMIdStrikerName"] == batter2]["scrM_BatsmanRuns"].sum()
        batter2_balls = group[group["scrM_PlayMIdStrikerName"] == batter2]["Valid_Ball"].sum()

        extras = group["scrM_Extras"].sum()
        balls = group["Valid_Ball"].sum()

        partnerships.append({
            "Batter1": batter1,
            "Batter2": batter2,
            "Batter1_Runs": batter1_runs,
            "Batter1_Balls": batter1_balls,
            "Batter2_Runs": batter2_runs,
            "Batter2_Balls": batter2_balls,
            "Extras": extras,
            "Total": batter1_runs + batter2_runs + extras,
            "Balls": balls
        })

    partnerships_df = pd.DataFrame(partnerships)
    if partnerships_df.empty:
        fig = go.Figure()
        fig.update_layout(
            annotations=[dict(
                text="No Data Available",
                xref="paper", yref="paper",
                showarrow=False,
                font=dict(size=20, color="red"),
                x=0.5, y=0.5
            )],
            height=400, width=800,
            plot_bgcolor="white", paper_bgcolor="white"
        )
        return pio.to_html(fig, full_html=False, include_plotlyjs='cdn')

    # ✅ Sort by runs
    partnerships_df = partnerships_df.sort_values(by="Total", ascending=False)

    # ✅ Normalize runs → fractions
    partnerships_df["Batter1_frac"] = partnerships_df["Batter1_Runs"] / partnerships_df["Total"]
    partnerships_df["Extras_frac"]  = partnerships_df["Extras"] / partnerships_df["Total"]
    partnerships_df["Batter2_frac"] = partnerships_df["Batter2_Runs"] / partnerships_df["Total"]

    chart_width = 0.8
    n = len(partnerships_df)
    target_width = min(0.6, chart_width)

    partnerships_df["Batter1_scaled"] = partnerships_df["Batter1_frac"] * target_width
    partnerships_df["Extras_scaled"]  = partnerships_df["Extras_frac"] * target_width
    partnerships_df["Batter2_scaled"] = partnerships_df["Batter2_frac"] * target_width

    fig = go.Figure()

    # === Spacing factor for gaps ===
    strip_gap = 2.5
    y_positions = [i * strip_gap for i in range(n)]
    strip_thickness = 0.7

    # Bars
    fig.add_trace(go.Bar(
        x=-partnerships_df["Batter1_scaled"], y=y_positions,
        orientation="h",
        marker=dict(color="#FF8C00", line=dict(width=0)),
        name="Batter1", hoverinfo="skip", width=strip_thickness
    ))
    fig.add_trace(go.Bar(
        x=partnerships_df["Extras_scaled"], y=y_positions,
        orientation="h",
        marker=dict(color="#32CD32", line=dict(width=0)),
        name="Extras", hoverinfo="skip", width=strip_thickness
    ))
    fig.add_trace(go.Bar(
        x=partnerships_df["Batter2_scaled"], y=y_positions,
        orientation="h",
        marker=dict(color="#1E90FF", line=dict(width=0)),
        name="Batter2", hoverinfo="skip", width=strip_thickness
    ))

    # Annotations
    for i, row in partnerships_df.iterrows():
        y = y_positions[partnerships_df.index.get_loc(i)]

        fig.add_annotation(
            x=-0.8, y=y,
            xanchor="right", align="center",
            text=f"{row['Batter1']}<br><b>{row['Batter1_Runs']} ({row['Batter1_Balls']})</b>",
            showarrow=False,
            font=dict(size=10, color="#FF8C00")
        )

        fig.add_annotation(
            x=0.8, y=y,
            xanchor="left", align="center",
            text=f"{row['Batter2']}<br><b>{row['Batter2_Runs']} ({row['Batter2_Balls']})</b>",
            showarrow=False,
            font=dict(size=10, color="#1E90FF")
        )

        fig.add_annotation(
            x=0, y=y + 0.6,
            xanchor="center", align="center",
            text=f"<b>Partnership - {row['Total']} ({row['Balls']})</b>",
            showarrow=False,
            font=dict(size=10, color="#808080")
        )

        fig.add_annotation(
            x=0, y=y - 0.6,
            xanchor="center", align="center",
            text=f"Extras - {row['Extras']}",
            showarrow=False,
            font=dict(size=10, color="#32CD32")
        )

    # Layout
    fig.update_layout(
        barmode="relative",
        showlegend=False,
        height=max(400, n * 120),
        margin=dict(l=120, r=120, t=20, b=20),
        xaxis=dict(visible=False, range=[-1, 1]),
        yaxis=dict(visible=False, range=[-strip_gap, (n) * strip_gap]),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)"
    )

    # ✅ Return interactive HTML
    return pio.to_html(fig, full_html=False, include_plotlyjs='cdn')






def get_innings_deliveries(match_name, inning_no):
    """
    Returns full ball-by-ball dataframe for a given innings.
    Works with MySQL (pymysql).
    """
    import pandas as pd

    query = """
        SELECT
            scrM_tmMIdBattingName AS scrM_TeamName,
            scrM_PlayMIdStrikerName,
            scrM_PlayMIdNonStrikerName,
            scrM_BatsmanRuns,
            scrM_ByeRuns,
            scrM_LegByeRuns,
            scrM_NoBallRuns,
            scrM_WideRuns,
            scrM_PenaltyRuns
        FROM tblscoremaster
        WHERE scrM_MatchName = %s AND scrM_InningNo = %s
        ORDER BY scrM_OverNo, scrM_DelNo
    """

    try:
        conn = get_connection()
        df = pd.read_sql(query, conn, params=(match_name, inning_no))
        conn.close()
        print(f"✅ Loaded {len(df)} deliveries for match={match_name}, inning={inning_no}")
        return df

    except Exception as e:
        print("❌ Error fetching innings deliveries:", e)
        return pd.DataFrame()

    
import pyodbc

def fetch_metric_videos(batter_id=None, bowler_id=None, metric=None, match_id=None, inning_id=None):
    """
    Fetch video file paths for a specific player + metric (runs, balls, fours, sixes, wickets, dots, maidens etc.)
    Returns a list of valid video paths (absolute or static URLs).
    """
    import pandas as pd, os

    if not (match_id and inning_id):
        print("⚠️ Missing match_id or inning_id")
        return []

    # ✅ ensure numeric
    try:
        match_id = int(match_id)
    except:
        print("⚠️ match_id is not numeric:", match_id)
        return []

    try:
        inning_id = int(inning_id)
    except:
        print("⚠️ inning_id is not numeric:", inning_id)
        return []

    conn = None
    videos = []

    try:
        conn = get_connection()

        # ✅ FIX 1: Match should be by MatchId, NOT MatchName
        # ✅ FIX 2: Innings should match by InnId OR InnNo based on your DB column
        # Here your DB uses scrM_InningNo so keeping it same
        where_clauses = [
            "scrM_MchMId = %s",
            "scrM_InningNo = %s",
            "scrM_IsValidBall = 1"
        ]
        params = [match_id, inning_id]

        # --- Player filter ---
        if batter_id:
            where_clauses.append("scrM_PlayMIdStriker = %s")
            params.append(int(batter_id))
        elif bowler_id:
            where_clauses.append("scrM_PlayMIdBowler = %s")
            params.append(int(bowler_id))

        # --- Metric filter ---
        metric = (metric or "").lower().strip()

        if metric in ["runs", "r"]:
            where_clauses.append("scrM_BatsmanRuns > 0")

        elif metric in ["balls", "b"]:
            # all valid balls already covered by scrM_IsValidBall = 1
            pass

        elif metric in ["fours", "4"]:
            where_clauses.append("scrM_BatsmanRuns = 4")

        elif metric in ["sixes", "6"]:
            where_clauses.append("scrM_BatsmanRuns = 6")

        elif metric in ["dots", "d"]:
            where_clauses.append("""
                (scrM_BatsmanRuns = 0
                 AND scrM_IsWicket = 0
                 AND scrM_WideRuns = 0
                 AND scrM_NoBallRuns = 0
                 AND scrM_ByeRuns = 0
                 AND scrM_LegByeRuns = 0)
            """)

        elif metric in ["wickets", "w"]:
            where_clauses.append("scrM_IsWicket = 1")

        elif metric in ["maidens", "m"]:
            # ✅ Maiden overs only makes sense for bowler
            if not bowler_id:
                print("⚠️ Maiden metric requested but bowler_id missing")
                return []

            maiden_query = f"""
                SELECT scrM_OverNo
                FROM tblscoremaster
                WHERE scrM_MchMId = %s
                  AND scrM_InningNo = %s
                  AND scrM_PlayMIdBowler = %s
                  AND scrM_IsValidBall = 1
                GROUP BY scrM_OverNo
                HAVING SUM(scrM_DelRuns) = 0
            """

            maiden_df = pd.read_sql(
                maiden_query,
                conn,
                params=(match_id, inning_id, int(bowler_id))
            )

            maiden_overs = maiden_df["scrM_OverNo"].tolist()

            if maiden_overs:
                overs_placeholders = ",".join(["%s"] * len(maiden_overs))
                where_clauses.append(f"scrM_OverNo IN ({overs_placeholders})")
                params.extend(maiden_overs)
            else:
                print("⚠️ No maiden overs found.")
                return []

        else:
            print(f"⚠️ Unknown metric received: {metric} (no extra filter applied)")

        # --- Final Query ---
        query = f"""
            SELECT
                scrM_Video1FileName, scrM_Video2FileName, scrM_Video3FileName,
                scrM_Video4FileName, scrM_Video5FileName, scrM_Video6FileName,
                scrM_Video1URL, scrM_Video2URL, scrM_Video3URL,
                scrM_Video4URL, scrM_Video5URL, scrM_Video6URL
            FROM tblscoremaster
            WHERE {" AND ".join(where_clauses)}
        """

        df = pd.read_sql(query, conn, params=tuple(params))

        if df.empty:
            print(f"⚠️ No rows found for metric={metric}, match_id={match_id}, inning_id={inning_id}")
            return []

        # --- Collect Video URLs / Local Videos ---
        for _, row in df.iterrows():
            for i in range(1, 7):
                f_local = row.get(f"scrM_Video{i}FileName")
                f_url = row.get(f"scrM_Video{i}URL")

                if f_url and isinstance(f_url, str) and f_url.strip():
                    videos.append(f_url.strip())

                elif f_local and isinstance(f_local, str) and f_local.strip():
                    local_path = os.path.join("static", "videos", f_local.strip())
                    if os.path.exists(local_path):
                        videos.append(local_path)
                    else:
                        videos.append(os.path.abspath(local_path))

        # De-duplicate
        videos = list(dict.fromkeys(videos))
        print(f"✅ fetch_metric_videos(metric={metric}) -> {len(videos)} files found")
        return videos

    except Exception as e:
        print("❌ fetch_metric_videos error:", e)
        return []

    finally:
        try:
            if conn:
                conn.close()
        except:
            pass











def generate_multi_day_report(df, match_innings=None):
    """
    Build multi-day report, using real scrM_DayNo / scrM_SessionNo if available,
    otherwise fallback to strict 30-overs-per-session buckets (180 legal balls).
    """

    import numpy as np
    from collections import defaultdict
    import plotly.io as pio
    import plotly.graph_objects as go

    if df is None or df.empty:
        return None

    d = df.copy()

    # --- Identify legal balls ---
    wide_col = "scrM_IsWideBall" if "scrM_IsWideBall" in d.columns else None
    noball_col = "scrM_IsNoBall" if "scrM_IsNoBall" in d.columns else None

    is_wide = d[wide_col].fillna(0).astype(int) if wide_col else 0
    is_noball = d[noball_col].fillna(0).astype(int) if noball_col else 0
    d["__is_legal"] = 1 - (np.array(is_wide) | np.array(is_noball))

    # --- Sort in real match order ---
    sort_cols = [c for c in ["scrM_InningNo", "scrM_OverNo", "scrM_DelNo"] if c in d.columns]
    if sort_cols:
        d = d.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)

    # --- Assign Day / Session ---
    if "scrM_DayNo" in d.columns and "scrM_SessionNo" in d.columns:
        # ✅ Use database-provided fields
        d["Day"] = d["scrM_DayNo"].fillna(1).astype(int)
        d["SessionNo"] = d["scrM_SessionNo"].fillna(1).astype(int)
    else:
        # ✅ Fallback: 180-legal-ball sessions
        d["__legal_cum"] = d["__is_legal"].cumsum()
        legal_idx_0 = np.maximum(d["__legal_cum"] - 1, 0)
        session_index = (legal_idx_0 // (30 * 6)).astype(int)  # 180 legal balls

        d["Day"] = (session_index // 3) + 1
        d["SessionNo"] = (session_index % 3) + 1

    # --- Ensure scrM_DayNo / scrM_SessionNo always exist ---
    if "scrM_DayNo" not in d.columns:
        d["scrM_DayNo"] = d["Day"]
    if "scrM_SessionNo" not in d.columns:
        d["scrM_SessionNo"] = d["SessionNo"]

    # --- Build innings → team map from match_innings ---
    inning_team_map = {}
    if match_innings:
        for inn in match_innings:
            inning_team_map[int(inn["Inn_Inning"])] = inn.get("TeamShortName") or inn.get("TeamName")

    # --- Aggregations ---
    run_col = "scrM_DelRuns" if "scrM_DelRuns" in d.columns else ("scrM_BatsmanRuns" if "scrM_BatsmanRuns" in d.columns else None)
    bat_runs_col = "scrM_BatsmanRuns" if "scrM_BatsmanRuns" in d.columns else None
    w_col = "scrM_IsWicket" if "scrM_IsWicket" in d.columns else None

    if run_col is None:
        d["__runs"] = 0
        run_col = "__runs"
    if bat_runs_col is None:
        d["__bruns"] = 0
        bat_runs_col = "__bruns"
    if w_col is None:
        d["__w"] = 0
        w_col = "__w"

    # boundary markers
    for v in [0, 1, 2, 3, 4, 6]:
        col = f"__is_{v}"
        d[col] = (d[bat_runs_col] == v).astype(int)

    agg_keys = ["Day", "SessionNo", "scrM_InningNo"]
    grouped = d.groupby(agg_keys, dropna=False)

    days = defaultdict(lambda: {"sessions": defaultdict(list)})

    for (day, sess_no, inn), g in grouped:
        day, sess_no, inn = int(day), int(sess_no), int(inn)

        legal_balls = int(g["__is_legal"].sum())
        overs_str = f"{legal_balls // 6}.{legal_balls % 6}"
        overs_float = round(legal_balls / 6.0, 1)

        runs = int(g[run_col].sum())
        wkts = int(g[w_col].sum())
        rr = round(runs / overs_float, 2) if overs_float > 0 else 0.0

        team = inning_team_map.get(inn, "")

        zeros  = int(g["__is_0"].sum())
        ones   = int(g["__is_1"].sum())
        twos   = int(g["__is_2"].sum())
        threes = int(g["__is_3"].sum())
        fours  = int(g["__is_4"].sum())
        sixes  = int(g["__is_6"].sum())

        stats_row = {
            "runs": runs,
            "score": f"{runs}/{wkts}",
            "run_rate": rr,
            "fours": fours,
            "sixes": sixes,
            "dot_balls": zeros,
            "ones": ones,
            "twos": twos,
            "threes": threes,
            "overs": overs_str,
            "maiden": 0,
            "wkts": wkts,
            "eco_rate": rr,
            "wide_balls": int(g[wide_col].sum()) if wide_col else 0,
            "no_balls": int(g[noball_col].sum()) if noball_col else 0,
            "runs_saved": 0,
            "runs_given": runs,
            "catches_taken": 0,
            "catches_dropped": 0
        }

        days[day]["sessions"][sess_no].append({
            "innings": inn,
            "batting_team": team,
            "runs": runs,
            "wkts": wkts,
            "overs": overs_float,
            "overs_str": overs_str,
            "rr": rr,
            "zeros": zeros,
            "ones": ones,
            "twos": twos,
            "threes": threes,
            "fours": fours,
            "sixes": sixes,
            "stats_table": stats_row
        })

    # --- Day-level donut chart ---
    chart_htmls = {}
    for day, payload in days.items():
        all_inns = [inn for sess_list in payload["sessions"].values() for inn in sess_list]
        total_runs = sum(x["runs"] for x in all_inns)
        total_wkts = sum(x["wkts"] for x in all_inns)

        labels = ["Total Runs", "Total Wickets"]
        values = [total_runs, total_wkts]
        colors = ["#636EFA", "#EF553B"]

        fig = go.Figure()
        fig.add_trace(go.Pie(
            labels=labels,
            values=values,
            hole=0.5,
            textinfo='percent',
            hoverinfo='label+value+percent',
            marker=dict(colors=colors),
            showlegend=True,
            insidetextfont=dict(color=None),
            outsidetextfont=dict(color="#808080")
        ))

        fig.update_layout(
            title=dict(
                text=f"Day {day}: Total Runs vs Wickets",
                font=dict(color="#808080")  # ✅ neutral heading colour
            ),
            margin=dict(l=10, r=10, t=40, b=10),
            height=250,
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            legend=dict(
                font=dict(color="#808080")  # ✅ legend labels in grey
            )
        )

        chart_htmls[day] = pio.to_html(fig, full_html=False, include_plotlyjs='cdn')

    return {"days": dict(days), "charts": chart_htmls}




import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import io, base64

def map_bowling_type_radar(skill: str) -> str:
    """
    Map detailed bowler skills to Pace or Spin.
    Covers both right-arm and left-arm variations.
    """
    if skill is None:
        return "Unknown"

    skill = skill.strip().lower()

    pace_keywords = ["fast", "medium fast", "medium", "lamf", "ramf", "raf"]
    spin_keywords = ["offbreak", "legbreak", "orthodox", "spinner", "las", "rob", "ralb"]

    if any(k in skill for k in pace_keywords):
        return "Pace"
    elif any(k in skill for k in spin_keywords):
        return "Spin"
    else:
        return "Unknown"


def generate_session_radar_chart(ball_by_ball_df, day, inning, session, team_name="Team", bowler_type=None):
    """
    Radar-style wagon wheel chart for a specific day, inning, and session.
    Can generate Consolidated, Spin-only, or Pace-only charts using bowler_type filter.
    """

    df = ball_by_ball_df.copy()

    # ---- Ensure Day/SessionNo exist ----
    if "Day" not in df.columns or "SessionNo" not in df.columns:
        # Try to derive them if not available
        wide_col = "scrM_IsWideBall" if "scrM_IsWideBall" in df.columns else None
        noball_col = "scrM_IsNoBall" if "scrM_IsNoBall" in df.columns else None
        is_wide = df[wide_col].fillna(0).astype(int) if wide_col else 0
        is_noball = df[noball_col].fillna(0).astype(int) if noball_col else 0
        df["__is_legal"] = 1 - (np.array(is_wide) | np.array(is_noball))

        sort_cols = [c for c in ["scrM_InningNo", "scrM_OverNo", "scrM_DelNo"] if c in df.columns]
        if sort_cols:
            df = df.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)

        df["__legal_cum"] = df["__is_legal"].cumsum()
        legal_idx_0 = np.maximum(df["__legal_cum"] - 1, 0)
        session_index = (legal_idx_0 // (30 * 6)).astype(int)
        df["Day"] = (session_index // 3) + 1
        df["SessionNo"] = (session_index % 3) + 1

    # ---- Handle both derived and DB-provided Day/Session columns ----
    day_col = "Day" if "Day" in df.columns else "scrM_DayNo"
    session_col = "SessionNo" if "SessionNo" in df.columns else "scrM_SessionNo"

    # ---- Filter by day/inning/session ----
    df = df[
        (df[day_col] == day) &
        (df["scrM_InningNo"] == inning) &
        (df[session_col] == session)
    ]

    # ---- Bowler type filter ----
    if bowler_type:
        if "scrM_BowlerSkill" in df.columns:
            df["BowlingType"] = df["scrM_BowlerSkill"].apply(map_bowling_type_radar)
            df = df[df["BowlingType"] == bowler_type]

    # ---- If no data, still return blank chart with "No Data" ----
    if df.empty:
        fig, ax = plt.subplots(figsize=(2.5, 2.5), subplot_kw=dict(polar=True))
        ax.set_theta_zero_location("N")
        ax.set_theta_direction(-1)
        ax.set_frame_on(False)
        ax.set_xticks([]); ax.set_yticks([])
        ax.spines['polar'].set_visible(False)
        ax.text(0.5, 0.5, "No Data", ha="center", va="center", transform=ax.transAxes,
                color="red", fontsize=10, fontweight="bold")
        buf = io.BytesIO()
        plt.tight_layout()
        plt.savefig(buf, format="png", bbox_inches="tight", dpi=150, transparent=True)
        plt.close(fig)
        buf.seek(0)
        return f"data:image/png;base64,{base64.b64encode(buf.read()).decode()}"

    # ---- Sectors ----
    sectors = ["Mid Wicket", "Square Leg", "Fine Leg", "Third Man",
               "Point", "Covers", "Long Off", "Long On"]
    breakdown_data = [{"1s":0,"2s":0,"3s":0,"4s":0,"6s":0} for _ in sectors]
    sector_map = {name: i for i, name in enumerate(sectors)}

    for _, row in df.iterrows():
        sec = str(row.get("scrM_WagonArea_zName", ""))
        runs = int(row.get("scrM_BatsmanRuns", 0))
        if sec in sector_map and runs > 0:
            idx = sector_map[sec]
            if runs == 1: breakdown_data[idx]["1s"] += 1
            elif runs == 2: breakdown_data[idx]["2s"] += 1
            elif runs == 3: breakdown_data[idx]["3s"] += 1
            elif runs == 4: breakdown_data[idx]["4s"] += 1
            elif runs == 6: breakdown_data[idx]["6s"] += 1

    # ---- Start Plot ----
    fig, ax = plt.subplots(figsize=(2.5, 2.5), subplot_kw=dict(polar=True))
    ax.set_theta_zero_location("N")
    ax.set_theta_direction(-1)
    ax.set_frame_on(False)
    ax.set_xticks([]); ax.set_yticks([])
    ax.spines['polar'].set_visible(False)

    scale = 0.9
    ax.set_aspect('equal')

    # ---- Ground visuals ----
    rim_radius = 1.03 * scale
    rim_circle = plt.Circle((0, 0), rim_radius, transform=ax.transData._b,
                            color='black', linewidth=8, fill=False,
                            zorder=5, clip_on=False)
    ax.add_artist(rim_circle)

    ax.add_artist(plt.Circle((0, 0), 1.0 * scale, transform=ax.transData._b,
                             color='#19a94b', zorder=0))
    ax.add_artist(plt.Circle((0, 0), 0.6 * scale, transform=ax.transData._b,
                             color='#4CAF50', zorder=1))
    ax.add_artist(plt.Rectangle((-0.07 * scale / 2, -0.3 * scale / 2),
                                0.07 * scale, 0.3 * scale,
                                transform=ax.transData._b, color='burlywood', zorder=2))

    # ---- Sector lines ----
    for angle in np.linspace(0, 2 * np.pi, 9):
        ax.plot([angle, angle], [0, 1.0 * scale],
                color='white', linewidth=0.5, zorder=3)

    # ---- Highlight max sector ----
    sector_runs = [(bd["1s"] + bd["2s"]*2 + bd["3s"]*3 +
                    bd["4s"]*4 + bd["6s"]*6) for bd in breakdown_data]
    if sum(sector_runs) > 0:
        max_idx = sector_runs.index(max(sector_runs))
        sector_angles_deg = [112.5, 67.5, 22.5, 337.5,
                             292.5, 247.5, 202.5, 157.5]
        ax.bar(np.deg2rad(sector_angles_deg[max_idx]), 1.0 * scale,
               width=np.radians(45), color='red', alpha=0.3, zorder=1)

    # ---- Fielding labels ----
    position_labels = [
        ("Mid Wicket", 112.5, -110, -0.02),
        ("Square Leg", 67.5, -70, -0.02),
        ("Fine Leg", 22.5, -25, 0.00),
        ("Third Man", 337.5, 20, -0.02),
        ("Point", 292.5, 70, -0.01),
        ("Covers", 247.5, 110, -0.01),
        ("Long Off", 202.5, 155, -0.02),
        ("Long On", 157.5, 200, -0.02)
    ]
    for text, angle_deg, rotation_deg, dist_offset in position_labels:
        rad = np.deg2rad(angle_deg)
        ax.text(rad, rim_radius + dist_offset, text,
                color='white', fontsize=5, fontweight='bold',
                ha='center', va='center',
                rotation=rotation_deg, rotation_mode='anchor', zorder=6)

    # ---- Runs + % boxes ----
    total_runs = sum(sector_runs)
    box_positions = [(103.5, 0, 0.70), (67.5, 0, 0.70), (22.5, 0, 0.80), (337.5, 0, 0.80),
                     (295.5, 0, 0.75), (250.5, 0, 0.70), (204.5, 1, 0.59), (155.5, 1, 0.59)]
    for i, (angle_deg, rot, dist) in enumerate(box_positions):
        rad = np.deg2rad(angle_deg)
        r = dist * scale
        runs = sector_runs[i]
        pct = (runs / total_runs * 100) if total_runs > 0 else 0
        ax.text(rad, r, f"{runs} ({pct:.1f}%)",
                color='white', fontsize=5,
                ha='center', va='center',
                rotation=rot, rotation_mode='anchor',
                bbox=dict(facecolor='black', alpha=0.6, boxstyle='round,pad=0.2'))

    # ---- Breakdown per sector ----
    # ---- Breakdown per sector ----
    detail_positions = [
        (113.5, 0, 0.75), (78.5, 0, 0.68), (27.5, 0, 0.68), (332.5, 0, 0.68),
        (285.5, 0, 0.68), (240.5, 0, 0.76), (200.5, 1, 0.72), (158.5, 1, 0.73)
    ]
    for i, (angle_deg, rot, dist) in enumerate(detail_positions):
        rad = np.deg2rad(angle_deg)
        r = dist * scale
        bd = breakdown_data[i]

        # Format without 3s, arranged in two rows
        text = f"1s:{bd['1s']}  2s:{bd['2s']}\n4s:{bd['4s']}  6s:{bd['6s']}"

        ax.text(
            rad, r, text,
            color='white', fontsize=5.5,   # 🔥 Increased font size
            ha='center', va='center',
            rotation=rot, rotation_mode='anchor'
        )


    # ---- Export ----
    buf = io.BytesIO()
    plt.tight_layout()
    plt.savefig(buf, format="png", bbox_inches="tight", dpi=150, transparent=True)
    plt.close(fig)
    buf.seek(0)
    return f"data:image/png;base64,{base64.b64encode(buf.read()).decode()}"



def generate_session_pitchmaps(ball_by_ball_df, day, inning, session,
                               right_pitch_img="tailwick/static/RightHandPitchPad.jpg",
                               left_pitch_img="tailwick/static/LeftHandPitchPad.jpg"):
    """
    Generate Right-hand and Left-hand pitch maps for a given Inn/Day/Session.
    Returns dict with { 'right': base64_img, 'left': base64_img }.
    """

    df = ball_by_ball_df.copy()

    # ---- Ensure Day/SessionNo exist ----
    if "Day" not in df.columns or "SessionNo" not in df.columns:
        wide_col = "scrM_IsWideBall" if "scrM_IsWideBall" in df.columns else None
        noball_col = "scrM_IsNoBall" if "scrM_IsNoBall" in df.columns else None
        is_wide = df[wide_col].fillna(0).astype(int) if wide_col else 0
        is_noball = df[noball_col].fillna(0).astype(int) if noball_col else 0
        df["__is_legal"] = 1 - (np.array(is_wide) | np.array(is_noball))

        sort_cols = [c for c in ["scrM_InningNo", "scrM_OverNo", "scrM_DelNo"] if c in df.columns]
        if sort_cols:
            df = df.sort_values(sort_cols, kind="mergesort").reset_index(drop=True)

        df["__legal_cum"] = df["__is_legal"].cumsum()
        legal_idx_0 = np.maximum(df["__legal_cum"] - 1, 0)
        session_index = (legal_idx_0 // (30 * 6)).astype(int)
        df["Day"] = (session_index // 3) + 1
        df["SessionNo"] = (session_index % 3) + 1

    # ---- Filter ----
    df = df[
        (df["Day"] == day) &
        (df["scrM_InningNo"] == inning) &
        (df["SessionNo"] == session)
    ]
    if df.empty:
        return {"right": None, "left": None}

    # ---- Split by batter hand ----
    right_df = df[df.get("BatterHand", "").str.lower().str.contains("right")]
    left_df  = df[df.get("BatterHand", "").str.lower().str.contains("left")]

    # ---- Call your existing scaled plotting function ----
    right_img = None
    left_img = None

    if not right_df.empty and os.path.exists(right_pitch_img):
        right_img = plot_pitch_points_scaled(right_df, pitch_image_path=right_pitch_img)

    if not left_df.empty and os.path.exists(left_pitch_img):
        left_img = plot_pitch_points_scaled(left_df, pitch_image_path=left_pitch_img)

    return {"right": right_img, "left": left_img}

import pandas as pd

def generate_line_length_table_new(df, day=None, inning=None, session=None):
    """
    Prepare Line & Length summary data for Jinja table rendering
    (filtered by Day / Inning / Session).
    Uses same logic as generate_multi_day_full_layout in kpi_4.py.
    """

    subset = df.copy()

    # ---- Filters ----
    if day is not None and "scrM_DayNo" in subset.columns:
        subset = subset[subset["scrM_DayNo"] == day]
    if inning is not None and "scrM_InningNo" in subset.columns:
        subset = subset[subset["scrM_InningNo"] == inning]
    if session is not None and "scrM_SessionNo" in subset.columns:
        subset = subset[subset["scrM_SessionNo"] == session]

    if subset.empty:
        return []

    # ---- Classify Line (using PitchXPos) ----
    def classify_line(xpos):
        try:
            x = float(xpos)
        except:
            return "Unknown"
        if x < -80: return "Leg. St."
        elif -80 <= x < -40: return "Mid/Leg. St."
        elif -40 <= x < -10: return "Mid. St."
        elif -10 <= x <= 10: return "Off/Mid. St."
        elif 11 <= x <= 40: return "Off St."
        elif 41 <= x <= 80: return "Outside Off St."
        else: return "OnSide"

    subset["LineCategory"] = subset["scrM_PitchXPos"].apply(classify_line)

    # ---- Use Length Category ----
    if "scrM_PitchArea_zName" in subset.columns:
        subset["LengthCategory"] = subset["scrM_PitchArea_zName"]
    else:
        subset["LengthCategory"] = "Unknown"

    # ---- Categories ----
    line_lengths = [
        "Full Length", "Full Toss", "Good Length",
        "Over Pitch", "Short Of Good Length", "Short Pitch"
    ]
    lines = [
        "Leg. St.", "Mid. St.", "Mid/Leg. St.",
        "Off St.", "Off/Mid. St.", "OnSide", "Outside Off St."
    ]

    total_runs = subset["scrM_BatsmanRuns"].sum()
    total_balls = len(subset)

    table = []
    for length in line_lengths:
        length_df = subset[subset["LengthCategory"] == length]

        row = {"Line Length": length}
        for col in lines:
            col_df = length_df[length_df["LineCategory"] == col]
            row[col] = {
                "runs": int(col_df["scrM_BatsmanRuns"].sum()),
                "balls": len(col_df)
            }

        runs = int(length_df["scrM_BatsmanRuns"].sum())
        balls = len(length_df)
        runs_pct = round((runs / total_runs) * 100, 2) if total_runs else 0
        balls_pct = round((balls / total_balls) * 100, 2) if total_balls else 0

        # 👉 Match old report style: Runs (Runs%) and Balls (Balls%)
        row["Runs"] = f"{runs} ({runs_pct}%)"
        row["Balls"] = f"{balls} ({balls_pct}%)"

        table.append(row)

    return table

def generate_partnership_table(df, day=None, inning=None, session=None):
    """
    Generate Partnerships table filtered by Day, Inning, and Session.
    Columns: Batter1, Batter2, Runs+Extras, Balls
    """

    subset = df.copy()

    # ---- Filters ----
    if day is not None and "scrM_DayNo" in subset.columns:
        subset = subset[subset["scrM_DayNo"] == day]
    if inning is not None and "scrM_InningNo" in subset.columns:
        subset = subset[subset["scrM_InningNo"] == inning]
    if session is not None and "scrM_SessionNo" in subset.columns:
        subset = subset[subset["scrM_SessionNo"] == session]

    if subset.empty:
        return []

    partnerships = []
    current_pair = []
    runs_dict, balls_dict = {}, {}
    total_runs, total_balls = 0, 0

    for _, row in subset.iterrows():
        striker = row.get("scrM_PlayMIdStrikerName", "Unknown")
        non_striker = row.get("scrM_PlayMIdNonStrikerName", "Unknown")
        batter_pair = [striker, non_striker]

        # Update runs + balls for striker
        runs_dict[striker] = runs_dict.get(striker, 0) + row.get("scrM_BatsmanRuns", 0)
        balls_dict[striker] = balls_dict.get(striker, 0) + 1

        # Update extras
        extras = 0
        for col in ["scrM_ByeRuns", "scrM_LegByeRuns", "scrM_NoBallRuns", "scrM_WideRuns", "scrM_PenaltyRuns"]:
            if col in row and pd.notna(row[col]):
                extras += row[col]

        total_runs += row.get("scrM_BatsmanRuns", 0) + extras
        total_balls += 1

        # If partnership changes (wicket falls or batter changes)
        if len(current_pair) == 0:
            current_pair = batter_pair

        if striker not in current_pair or non_striker not in current_pair:
            # Save previous partnership
            if len(current_pair) == 2:
                b1, b2 = current_pair
                partnerships.append({
                    "batter1": f"{b1} {runs_dict.get(b1,0)} ({balls_dict.get(b1,0)})",
                    "batter2": f"{b2} {runs_dict.get(b2,0)} ({balls_dict.get(b2,0)})",
                    "runs_extras": total_runs,
                    "balls": total_balls
                })

            # Reset for new pair
            current_pair = batter_pair
            runs_dict, balls_dict = {}, {}
            total_runs, total_balls = 0, 0

    # Last partnership
    if len(current_pair) == 2:
        b1, b2 = current_pair
        partnerships.append({
            "batter1": f"{b1} {runs_dict.get(b1,0)} ({balls_dict.get(b1,0)})",
            "batter2": f"{b2} {runs_dict.get(b2,0)} ({balls_dict.get(b2,0)})",
            "runs_extras": total_runs,
            "balls": total_balls
        })

    return partnerships

def get_match_format_code_by_tournament(tournament_id):
    """
    Fetch the match format code for a given tournament.
    Returns:
        int: Match format code (27/29 = Multiday, 26/28 = Limited Overs) or None if not found.
    Works with MySQL (pymysql).
    """
    import pandas as pd

    try:
        conn = get_connection()
        query = """
            SELECT trnM_MatchFormat_z
            FROM tbltournaments
            WHERE trnM_Id = %s
        """
        result = pd.read_sql(query, conn, params=(tournament_id,))
        conn.close()

        if not result.empty:
            code = int(result.iloc[0]["trnM_MatchFormat_z"])
            # Include known numeric codes: 26=ODI, 28=T20, 167=T10, 27/29=Multi-day/Test
            if code in [26, 27, 28, 29, 167]:
                return code
            else:
                print(f"⚠️ Unknown match format code: {code}")
                return None
        return None

    except Exception as e:
        print("❌ Match format code fetch error:", e)
        return None

    
def get_phase(over_no, total_overs):
    """
    Classify an over into Powerplay, Middle Overs, or Slog Overs.
    Handles both T20 (20 overs) and ODI (50 overs).
    """
    if total_overs == 20:  # T20 match
        if 1 <= over_no <= 6:
            return "Powerplay"
        elif 7 <= over_no <= 15:
            return "Middle Overs"
        elif 16 <= over_no <= 20:
            return "Slog Overs"

    elif total_overs == 50:  # ODI match
        if 1 <= over_no <= 10:
            return "Powerplay"
        elif 11 <= over_no <= 40:
            return "Middle Overs"
        elif 41 <= over_no <= 50:
            return "Slog Overs"

    # fallback for unknown formats
    return "Unknown"

def get_phase_definitions(match_format_code: int):
    """
    Return phase definitions (over ranges) for T20 and ODI formats.

    match_format_code:
        28 = T20 (20 overs)
        26 = ODI (50 overs)
    """
    if match_format_code == 28:  # T20
        return {
            "Overall": (1, 20),
            "Powerplay": (1, 6),
            "Middle Overs": (7, 15),
            "Slog Overs": (16, 20),
        }
    elif match_format_code == 26:  # ODI
        return {
            "Overall": (1, 50),
            "Powerplay": (1, 10),
            "Middle Overs": (11, 40),
            "Slog Overs": (41, 50),
        }
    else:
        return {"Overall": (1, 0)}  # fallback
    
import plotly.graph_objects as go

def no_data_figure(message="No Data Available"):
    """
    Return a Plotly figure with a centered red 'No Data Available' message.
    Can be reused across all chart functions.
    """
    fig = go.Figure()
    fig.add_annotation(
        text=f"<b style='color:red'>{message}</b>",
        x=0.5, y=0.5, xref="paper", yref="paper",
        showarrow=False, font=dict(size=18, color="red"),
        xanchor="center", yanchor="middle"
    )
    fig.update_layout(
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
    )
    return fig



import plotly.graph_objects as go
import pandas as pd
import numpy as np

def safe_team_name(team):
    """Return a simple string name for team whether team is str or dict."""
    if isinstance(team, dict):
        # common keys we've seen in your code
        return str(team.get("name") or team.get("TeamShortName") or team.get("team") or "")
    return str(team or "")


# Runs-per-over chart (phase-aware)
def create_runs_per_over_chart(innings1_df, innings2_df, team1, team2, phase=None, dark_mode=False):
    team1_name = safe_team_name(team1)
    team2_name = safe_team_name(team2)

    # detect max over from available data
    max_over = max(
        int(innings1_df["scrM_OverNo"].max()) if (not innings1_df.empty) else 0,
        int(innings2_df["scrM_OverNo"].max()) if (not innings2_df.empty) else 0
    )
    if max_over == 0:
        return go.Figure()  # nothing to plot

    # choose phase defaults based on inferred match length
    # Handle T10 explicitly (max_over <= 10), then T20 (<=20), then ODI/longer
    if max_over <= 10:
        # T10: Powerplay 1-2, Middle 3-7, Slog 8-10 (or up to max_over)
        phase_map = {
            "overall": (1, max_over),
            "powerplay": (1, min(2, max_over)),
            "middle": (3, min(7, max_over)),
            "slog": (8, max_over),
        }
    elif max_over <= 20:
        phase_map = {
            "overall": (1, max_over),
            "powerplay": (1, min(6, max_over)),
            "middle": (7, min(15, max_over)),
            "slog": (16, max_over),
        }
    else:
        phase_map = {
            "overall": (1, max_over),
            "powerplay": (1, min(10, max_over)),
            "middle": (11, min(40, max_over)),
            "slog": (41, max_over),
        }

    # normalize phase key
    key = (phase or "overall").strip().lower()
    aliases = {
        "powerplay": "powerplay", "pp": "powerplay",
        "middle overs": "middle", "middle": "middle",
        "slog overs": "slog", "slog": "slog",
        "overall": "overall"
    }
    phase_key = aliases.get(key, "overall")
    start_over, end_over = phase_map.get(phase_key, phase_map["overall"])

    # if invalid range
    if start_over > end_over:
        return go.Figure()

    overs = list(range(start_over, end_over + 1))

    # filter only phase overs
    inn1 = innings1_df[innings1_df["scrM_OverNo"].between(start_over, end_over)]
    inn2 = innings2_df[innings2_df["scrM_OverNo"].between(start_over, end_over)]

    runs1 = inn1.groupby("scrM_OverNo")["scrM_DelRuns"].sum().reindex(overs, fill_value=0)
    runs2 = inn2.groupby("scrM_OverNo")["scrM_DelRuns"].sum().reindex(overs, fill_value=0)

    runs1_vals = runs1.values if hasattr(runs1, "values") else np.array(runs1)
    runs2_vals = runs2.values if hasattr(runs2, "values") else np.array(runs2)

    x_base = np.array(overs, dtype=float)
    bar_width = 0.35
    label_color = "#808080"  # fixed neutral color

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=x_base - bar_width / 2,
        y=runs1_vals,
        name=f"{team1_name} ({int(runs1_vals.sum())}/{int(inn1['scrM_IsWicket'].sum() if 'scrM_IsWicket' in inn1 else 0)})",
        marker_color="#002f6c",
        marker_line_width=0
    ))
    fig.add_trace(go.Bar(
        x=x_base + bar_width / 2,
        y=runs2_vals,
        name=f"{team2_name} ({int(runs2_vals.sum())}/{int(inn2['scrM_IsWicket'].sum() if 'scrM_IsWicket' in inn2 else 0)})",
        marker_color="#FF8C00",
        marker_line_width=0
    ))

    # wicket markers
    wk1 = inn1.groupby("scrM_OverNo")["scrM_IsWicket"].sum().to_dict() if not inn1.empty and "scrM_IsWicket" in inn1.columns else {}
    wk2 = inn2.groupby("scrM_OverNo")["scrM_IsWicket"].sum().to_dict() if not inn2.empty and "scrM_IsWicket" in inn2.columns else {}

    for i, over in enumerate(overs):
        if wk1.get(over, 0) > 0:
            fig.add_trace(go.Scatter(
                x=[over - bar_width / 2],
                y=[float(runs1_vals[i])],
                mode="markers",
                marker=dict(color="red", size=10, symbol="circle"),
                showlegend=False
            ))
        if wk2.get(over, 0) > 0:
            fig.add_trace(go.Scatter(
                x=[over + bar_width / 2],
                y=[float(runs2_vals[i])],
                mode="markers",
                marker=dict(color="red", size=10, symbol="circle"),
                showlegend=False
            ))

    # annotations
    for i, over in enumerate(overs):
        if runs1_vals[i] > 0:
            fig.add_annotation(
                x=over - bar_width / 2, y=runs1_vals[i],
                text=str(int(runs1_vals[i])), showarrow=False,
                font=dict(size=11, color=label_color), yshift=12
            )
        if runs2_vals[i] > 0:
            fig.add_annotation(
                x=over + bar_width / 2, y=runs2_vals[i],
                text=str(int(runs2_vals[i])), showarrow=False,
                font=dict(size=11, color=label_color), yshift=12
            )

    y_tick_max = int(max(
        runs1_vals.max() if len(runs1_vals) else 0,
        runs2_vals.max() if len(runs2_vals) else 0
    ))
    y_tick_step = max(1, int(np.ceil(y_tick_max / 5))) if y_tick_max > 0 else 1

    # ✅ fixed layout (no autosize, scrolls on mobile)
    fig.update_layout(
        xaxis=dict(
            title="Over Number",
            tickvals=overs,
            tickmode="array",
            showgrid=False,
            zeroline=False,
            color=label_color,
            range=[overs[0] - 1, overs[-1] + 1]
        ),
        yaxis=dict(
            title="Runs Scored",
            tickvals=list(range(0, y_tick_max + y_tick_step, y_tick_step)),
            showgrid=False,
            zeroline=False,
            color=label_color
        ),
        autosize=False,   # 👈 disable responsive shrink
        width=800,        # 👈 full-size on desktop, scrolls on mobile
        height=500,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            font=dict(color=label_color)
        ),
        margin=dict(l=30, r=30, t=10, b=40),
        barmode="group",
        bargap=0.15
    )

    return fig





def create_run_rate_chart(innings1_df, innings2_df, team1, team2, phase=None, dark_mode=False):
    team1_name = safe_team_name(team1)
    team2_name = safe_team_name(team2)

    fig = go.Figure()

    # detect max over
    max_over = max(
        int(innings1_df["scrM_OverNo"].max()) if (not innings1_df.empty) else 0,
        int(innings2_df["scrM_OverNo"].max()) if (not innings2_df.empty) else 0
    )
    if max_over == 0:
        return fig

    # choose phase map based on match type inferred by max_over
    # Handle T10 explicitly (max_over <= 10), then T20 (<=20), then ODI/longer
    if max_over <= 10:
        phase_map = {
            "overall": (1, max_over),
            "powerplay": (1, min(2, max_over)),
            "middle": (3, min(7, max_over)),
            "slog": (8, max_over),
        }
    elif max_over <= 20:
        phase_map = {
            "overall": (1, max_over),
            "powerplay": (1, min(6, max_over)),
            "middle": (7, min(15, max_over)),
            "slog": (16, max_over),
        }
    else:
        phase_map = {
            "overall": (1, max_over),
            "powerplay": (1, min(10, max_over)),
            "middle": (11, min(40, max_over)),
            "slog": (41, max_over),
        }

    key = (phase or "overall").strip().lower()
    aliases = {
        "powerplay": "powerplay", "pp": "powerplay",
        "middle overs": "middle", "middle": "middle",
        "slog overs": "slog", "slog": "slog",
        "overall": "overall"
    }
    phase_key = aliases.get(key, "overall")
    start_over, end_over = phase_map.get(phase_key, phase_map["overall"])

    if start_over > end_over:
        return fig

    overs = list(range(start_over, end_over + 1))

    # runs per over only for phase
    runs1 = (innings1_df.groupby("scrM_OverNo")["scrM_DelRuns"].sum().reindex(overs, fill_value=0)
             if not innings1_df.empty else np.zeros(len(overs)))
    runs2 = (innings2_df.groupby("scrM_OverNo")["scrM_DelRuns"].sum().reindex(overs, fill_value=0)
             if not innings2_df.empty else np.zeros(len(overs)))

    runs1_vals = np.array(runs1)
    runs2_vals = np.array(runs2)

    # cumulative run rate per over
    denom = np.arange(1, len(overs) + 1)
    crr1 = np.cumsum(runs1_vals) / denom if len(denom) > 0 else np.array([])
    crr2 = np.cumsum(runs2_vals) / denom if len(denom) > 0 else np.array([])

    label_color = "#808080"  # fixed neutral color

    if len(crr1):
        fig.add_trace(go.Scatter(
            x=overs, y=crr1,
            mode="lines+markers+text",
            name=team1_name,
            marker=dict(color="#002f6c"),
            text=[f"{r:.2f}" for r in crr1],
            textposition="top center",
            textfont=dict(size=11, color=label_color)
        ))
    if len(crr2):
        fig.add_trace(go.Scatter(
            x=overs, y=crr2,
            mode="lines+markers+text",
            name=team2_name,
            marker=dict(color="#FF8C00"),
            text=[f"{r:.2f}" for r in crr2],
            textposition="top center",
            textfont=dict(size=11, color=label_color)
        ))

    # wicket markers
    wk1 = innings1_df.groupby("scrM_OverNo")["scrM_IsWicket"].sum().to_dict() if not innings1_df.empty and "scrM_IsWicket" in innings1_df.columns else {}
    wk2 = innings2_df.groupby("scrM_OverNo")["scrM_IsWicket"].sum().to_dict() if not innings2_df.empty and "scrM_IsWicket" in innings2_df.columns else {}

    for i, over in enumerate(overs):
        if wk1.get(over, 0) > 0 and len(crr1) > i:
            fig.add_trace(go.Scatter(
                x=[over], y=[float(crr1[i])],
                mode="markers", marker=dict(color="red", size=10, symbol="circle"),
                showlegend=False
            ))
        if wk2.get(over, 0) > 0 and len(crr2) > i:
            fig.add_trace(go.Scatter(
                x=[over], y=[float(crr2[i])],
                mode="markers", marker=dict(color="red", size=10, symbol="circle"),
                showlegend=False
            ))

    # y-axis range
    max_crr = 0
    if len(crr1): max_crr = max(max_crr, float(np.nanmax(crr1)))
    if len(crr2): max_crr = max(max_crr, float(np.nanmax(crr2)))
    y_max = max_crr + 0.5 if max_crr > 0 else 1.0

    # ✅ fixed size layout — matches bar chart behavior
    fig.update_layout(
        xaxis=dict(
            title="Over Number",
            tickvals=overs,
            tickmode="array",
            showgrid=False,
            zeroline=False,
            color=label_color,
            range=[overs[0] - 0.5, overs[-1] + 0.5]
        ),
        yaxis=dict(
            title="Run Rate (Cumulative)",
            range=[0, y_max],
            showgrid=False,
            zeroline=False,
            color=label_color
        ),
        autosize=False,   # 👈 disable responsive resizing
        width=800,        # 👈 fixed width like runs per over chart
        height=500,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            font=dict(color=label_color)
        ),
        margin=dict(l=30, r=30, t=10, b=40)
    )

    return fig






# 3️⃣ Donut Chart
def create_donut_charts(df, team1, team2):
    team1_name, team2_name = safe_team_name(team1), safe_team_name(team2)

    inning1_df = df[df['scrM_InningNo'] == 1]
    inning2_df = df[df['scrM_InningNo'] == 2]

    def get_distribution(inning_df):
        dots = (inning_df['scrM_BatsmanRuns'] == 0).sum()
        ones = (inning_df['scrM_BatsmanRuns'] == 1).sum()
        twos = (inning_df['scrM_BatsmanRuns'] == 2).sum()
        fours = (inning_df['scrM_BatsmanRuns'] == 4).sum()
        sixes = (inning_df['scrM_BatsmanRuns'] == 6).sum()
        return [dots, ones, twos, fours, sixes]

    runs_inning1 = get_distribution(inning1_df)
    runs_inning2 = get_distribution(inning2_df)

    labels = ['0s', '1s', '2s', '4s', '6s']
    colors = ['#002f6c', '#FF8C00', '#004C99', '#0069B3', '#0099E5']

    fig = go.Figure()

    fig.add_trace(go.Pie(
        labels=labels,
        values=runs_inning1,
        name=f'{team1_name} (Inns 1)',
        hole=0.4,
        textinfo='label+value',
        hoverinfo='label+value+percent',
        domain=dict(x=[0, 0.48]),
        marker=dict(colors=colors),
        showlegend=True,
        insidetextfont=dict(color=None),            # ✅ keep default contrast
        outsidetextfont=dict(color="#808080")       # ✅ neutral grey labels outside
    ))

    fig.add_trace(go.Pie(
        labels=labels,
        values=runs_inning2,
        name=f'{team2_name} (Inns 2)',
        hole=0.4,
        textinfo='label+value',
        hoverinfo='label+value+percent',
        domain=dict(x=[0.52, 1]),
        marker=dict(colors=colors),
        showlegend=True,
        insidetextfont=dict(color=None),
        outsidetextfont=dict(color="#808080")
    ))

    fig.update_layout(
        autosize=True,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=10, r=10, t=40, b=10),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.05,
            xanchor="center",
            x=0.5,
            font=dict(color="#808080")  # ✅ legend labels in grey
        )
    )

    return fig




# 4️⃣ Extra Runs Chart (all extras: wide, no-ball, bye, leg-bye, penalty)
def create_extra_runs_comparison_chart(df, team1, team2):
    team1_name, team2_name = safe_team_name(team1), safe_team_name(team2)

    inning1_df = df[df['scrM_InningNo'] == 1]
    inning2_df = df[df['scrM_InningNo'] == 2]

    wide_runs = [inning1_df['scrM_WideRuns'].sum(), inning2_df['scrM_WideRuns'].sum()]
    noball_runs = [inning1_df['scrM_NoBallRuns'].sum(), inning2_df['scrM_NoBallRuns'].sum()]
    bye_runs = [inning1_df['scrM_ByeRuns'].sum(), inning2_df['scrM_ByeRuns'].sum()]
    legbye_runs = [inning1_df['scrM_LegByeRuns'].sum(), inning2_df['scrM_LegByeRuns'].sum()]
    penalty_runs = [inning1_df['scrM_PenaltyRuns'].sum(), inning2_df['scrM_PenaltyRuns'].sum()]

    fig = go.Figure()

    fig.add_trace(go.Bar(y=[team1_name, team2_name], x=wide_runs, name='Wide Runs',
                         orientation='h', marker=dict(color='#002f6c', line=dict(width=0)),
                         text=wide_runs, textposition='inside'))
    fig.add_trace(go.Bar(y=[team1_name, team2_name], x=noball_runs, name='No Ball Runs',
                         orientation='h', marker=dict(color='#228B22', line=dict(width=0)),
                         text=noball_runs, textposition='inside'))
    fig.add_trace(go.Bar(y=[team1_name, team2_name], x=bye_runs, name='Bye Runs',
                         orientation='h', marker=dict(color='#8B008B', line=dict(width=0)),
                         text=bye_runs, textposition='inside'))
    fig.add_trace(go.Bar(y=[team1_name, team2_name], x=legbye_runs, name='Leg Bye Runs',
                         orientation='h', marker=dict(color='#FF8C00', line=dict(width=0)),
                         text=legbye_runs, textposition='inside'))
    fig.add_trace(go.Bar(y=[team1_name, team2_name], x=penalty_runs, name='Penalty Runs',
                         orientation='h', marker=dict(color='#B22222', line=dict(width=0)),
                         text=penalty_runs, textposition='inside'))

    fig.update_layout(
        autosize=True,
        xaxis_title='Runs',
        yaxis_title='Teams',
        barmode='stack',
        bargap=0.4,
        bargroupgap=0.2,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=30, r=20, t=20, b=30),
        xaxis=dict(
            showgrid=False, zeroline=False, showline=False, ticks='', showticklabels=True,
            title_font=dict(color="#808080"),
            tickfont=dict(color="#808080")
        ),
        yaxis=dict(
            showgrid=False, zeroline=False, showline=False, ticks='', showticklabels=True,
            title_font=dict(color="#808080"),
            tickfont=dict(color="#808080")
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.05,
            xanchor="center",
            x=0.5,
            font=dict(color="#808080")  # ✅ legend text in grey
        )
    )

    return fig


# 5️⃣ Wagon Area Comparison (batting runs only, exclude extras, dynamic areas)
def create_comparison_bar_chart(df, team1, team2):
    team1_name, team2_name = safe_team_name(team1), safe_team_name(team2)

    # ✅ Keep only batting runs (exclude extras)
    inning1_df = df[(df['scrM_InningNo'] == 1) & (df['scrM_BatsmanRuns'].between(0, 6))]
    inning2_df = df[(df['scrM_InningNo'] == 2) & (df['scrM_BatsmanRuns'].between(0, 6))]

    # ✅ Dynamically fetch wagon areas
    valid_areas = df["scrM_WagonArea_zName"].dropna().unique().tolist()

    data1 = inning1_df.groupby("scrM_WagonArea_zName")["scrM_BatsmanRuns"].sum().reset_index()
    data2 = inning2_df.groupby("scrM_WagonArea_zName")["scrM_BatsmanRuns"].sum().reset_index()

    comparison = pd.merge(data1, data2, on="scrM_WagonArea_zName", how="outer").fillna(0)
    comparison.columns = ["Wagon Area", f"{team1_name}", f"{team2_name}"]

    # ✅ Keep only valid areas
    comparison = comparison[comparison["Wagon Area"].isin(valid_areas)]

    fig = go.Figure()

    fig.add_trace(go.Bar(
        x=comparison["Wagon Area"],
        y=comparison[f"{team1_name}"],
        name=team1_name,
        marker=dict(color="#002f6c", line=dict(width=0)),
        text=comparison[f"{team1_name}"],
        textposition="outside",
        textfont=dict(color="#808080")
    ))

    fig.add_trace(go.Bar(
        x=comparison["Wagon Area"],
        y=comparison[f"{team2_name}"],
        name=team2_name,
        marker=dict(color="#FF8C00", line=dict(width=0)),
        text=comparison[f"{team2_name}"],
        textposition="outside",
        textfont=dict(color="#808080")
    ))

    # ✅ Fixed layout (for scrollable mobile behavior)
    fig.update_layout(
        autosize=False,   # disable responsive resizing
        width=800,        # full desktop width; scrolls on mobile
        height=500,
        xaxis_title="Area",
        yaxis_title="Runs",
        barmode="group",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=30, r=30, t=20, b=40),
        xaxis=dict(
            showgrid=False,
            zeroline=False,
            showline=False,
            tickangle=-30,
            ticks='',
            showticklabels=True,
            title_font=dict(color="#808080"),
            tickfont=dict(color="#808080")
        ),
        yaxis=dict(
            showgrid=False,
            zeroline=False,
            ticks='',
            showticklabels=True,
            title_font=dict(color="#808080"),
            tickfont=dict(color="#808080")
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.05,
            xanchor="center",
            x=0.5,
            font=dict(color="#808080")
        ),
    )

    return fig



import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import io, base64
import pandas as pd

def generate_team_comparison_radar(team1_name, team2_name, df):
    """
    Generate two radar-style wagon wheel charts side-by-side:
    - Team 1 runs scored by fielding area
    - Team 2 runs scored by fielding area
    Uses exact same visuals as session radar chart with labels, percentages, breakdowns, etc.
    """

    # ✅ Define fixed sectors in clockwise order
    field_areas = [
        "Mid Wicket", "Square Leg", "Fine Leg", "Third Man",
        "Point", "Covers", "Long Off", "Long On"
    ]
    sector_map = {name: i for i, name in enumerate(field_areas)}

    # Helper: compute per-sector breakdown for a given team
    def compute_breakdown_for_team(team_name):
        team_df = df[df['scrM_tmMIdBattingName'] == team_name]
        breakdown = [{ "1s":0, "2s":0, "3s":0, "4s":0, "6s":0 } for _ in field_areas]

        for _, row in team_df.iterrows():
            sec = str(row.get("scrM_WagonArea_zName", ""))
            runs = int(row.get("scrM_BatsmanRuns", 0))
            if sec in sector_map and runs > 0:
                idx = sector_map[sec]
                if runs == 1: breakdown[idx]["1s"] += 1
                elif runs == 2: breakdown[idx]["2s"] += 1
                elif runs == 3: breakdown[idx]["3s"] += 1
                elif runs == 4 and row.get("scrM_IsBoundry", 0) == 1:
                    breakdown[idx]["4s"] += 1
                elif runs == 6 and row.get("scrM_IsSixer", 0) == 1:
                    breakdown[idx]["6s"] += 1
        return breakdown

    team1_breakdown = compute_breakdown_for_team(team1_name)
    team2_breakdown = compute_breakdown_for_team(team2_name)

    # Function to create a single radar chart for a given team
    def create_team_radar(team_name, breakdown_data):
        fig, ax = plt.subplots(figsize=(2.8, 2.8), subplot_kw=dict(polar=True))
        ax.set_theta_zero_location("N")
        ax.set_theta_direction(-1)
        ax.set_frame_on(False)
        ax.set_xticks([]); ax.set_yticks([])
        ax.spines['polar'].set_visible(False)

        scale = 0.9
        ax.set_aspect('equal')

        # ---- Outer Rim ----
        rim_radius = 1.03 * scale
        rim_circle = plt.Circle((0, 0), rim_radius, transform=ax.transData._b,
                                color='black', linewidth=8, fill=False,
                                zorder=5, clip_on=False)
        ax.add_artist(rim_circle)

        # ---- Ground Circles ----
        ax.add_artist(plt.Circle((0, 0), 1.0 * scale, transform=ax.transData._b,
                                 color='#19a94b', zorder=0))
        ax.add_artist(plt.Circle((0, 0), 0.6 * scale, transform=ax.transData._b,
                                 color='#4CAF50', zorder=1))

        # ---- Pitch Rectangle ----
        ax.add_artist(plt.Rectangle((-0.07 * scale / 2, -0.3 * scale / 2),
                                    0.07 * scale, 0.3 * scale,
                                    transform=ax.transData._b, color='burlywood', zorder=2))

        # ---- Sector lines (every 45 degrees) ----
        for angle in np.linspace(0, 2 * np.pi, 9):
            ax.plot([angle, angle], [0, 1.0 * scale],
                    color='white', linewidth=0.5, zorder=3)

        # ---- Total runs in each sector ----
        sector_runs = [
            (bd["1s"] + bd["2s"]*2 + bd["3s"]*3 + bd["4s"]*4 + bd["6s"]*6)
            for bd in breakdown_data
        ]

        # ---- Highlight maximum sector ----
        if sum(sector_runs) > 0:
            max_idx = sector_runs.index(max(sector_runs))
            sector_angles_deg = [112.5, 67.5, 22.5, 337.5,
                                 292.5, 247.5, 202.5, 157.5]
            ax.bar(np.deg2rad(sector_angles_deg[max_idx]), 1.0 * scale,
                   width=np.radians(45), color='red', alpha=0.3, zorder=1)

        # ---- Fielding Position Labels ----
        position_labels = [
            ("Mid Wicket", 112.5, -110, -0.02),
            ("Square Leg", 67.5, -70, -0.02),
            ("Fine Leg", 22.5, -25, 0.00),
            ("Third Man", 337.5, 20, -0.02),
            ("Point", 292.5, 70, -0.01),
            ("Covers", 247.5, 110, -0.01),
            ("Long Off", 202.5, 155, -0.02),
            ("Long On", 157.5, 200, -0.02)
        ]
        for text, angle_deg, rotation_deg, dist_offset in position_labels:
            rad = np.deg2rad(angle_deg)
            ax.text(rad, rim_radius + dist_offset, text,
                    color='white', fontsize=5, fontweight='bold',
                    ha='center', va='center',
                    rotation=rotation_deg, rotation_mode='anchor', zorder=6)

        # ---- Runs + Percentage Boxes ----
        total_runs = sum(sector_runs)
        box_positions = [
            (103.5, 0, 0.70), (67.5, 0, 0.70), (22.5, 0, 0.80), (337.5, 0, 0.80),
            (295.5, 0, 0.75), (250.5, 0, 0.70), (204.5, 1, 0.59), (155.5, 1, 0.59)
        ]
        for i, (angle_deg, rot, dist) in enumerate(box_positions):
            rad = np.deg2rad(angle_deg)
            r = dist * scale
            runs = sector_runs[i]
            pct = (runs / total_runs * 100) if total_runs > 0 else 0
            ax.text(rad, r, f"{runs} ({pct:.1f}%)",
                    color='white', fontsize=5,
                    ha='center', va='center',
                    rotation=rot, rotation_mode='anchor',
                    bbox=dict(facecolor='black', alpha=0.6, boxstyle='round,pad=0.2'))

        # ---- Detailed Breakdown for each sector ----
        # ---- Detailed Breakdown for each sector ----
        detail_positions = [
            (113.5, 0, 0.75), (78.5, 0, 0.68), (29.5, 0, 0.70), (330.5, 0, 0.70),
            (285.5, 0, 0.68), (241.5, 0, 0.75), (200.5, 1, 0.72), (158.5, 1, 0.73)
        ]
        for i, (angle_deg, rot, dist) in enumerate(detail_positions):
            rad = np.deg2rad(angle_deg)
            r = dist * scale
            bd = breakdown_data[i]

            # 🔹 Modified text layout: remove 3s, split into two lines
            text = f"1s:{bd['1s']}  2s:{bd['2s']}\n4s:{bd['4s']}  6s:{bd['6s']}"

            ax.text(rad, r, text, color='white', fontsize=5.5,
                    ha='center', va='center',
                    rotation=rot, rotation_mode='anchor')

        # ---- Export to Base64 ----
        buf = io.BytesIO()
        plt.tight_layout()
        plt.savefig(buf, format="png", bbox_inches="tight", dpi=150, transparent=True)
        plt.close(fig)
        buf.seek(0)
        return base64.b64encode(buf.read()).decode("utf-8")

    # Create both charts
    team1_img = create_team_radar(team1_name, team1_breakdown)
    team2_img = create_team_radar(team2_name, team2_breakdown)

    return team1_img, team2_img

# 🥧 Player Contribution Donut
def create_player_contribution_donut(df, team, inning_no, phase_name="Overall"):
    """
    Donut chart: % contribution of each batter in runs scored during a phase.
    df: phase-filtered dataframe (already restricted to this phase)
    team: team object or name
    inning_no: 1 or 2
    """
    team_name = safe_team_name(team)

    # Filter by inning
    inn_df = df[df["scrM_InningNo"] == inning_no]
    if inn_df.empty:
        return no_data_figure("No Data Available")

    # Runs per batter
    batter_runs = (
        inn_df.groupby("scrM_PlayMIdStrikerName")["scrM_BatsmanRuns"]
        .sum()
        .reset_index()
    )

    if batter_runs.empty:
        return no_data_figure("No Data Available")

    labels = batter_runs["scrM_PlayMIdStrikerName"].tolist()
    values = batter_runs["scrM_BatsmanRuns"].tolist()

    # Reuse comparative donut colors
    colors = [
        '#002f6c', '#FF8C00', '#004C99', '#0069B3', '#0099E5',
        '#22CCEE', '#FF4477', '#44BB99', '#AA3377', '#EE7733'
    ]

    fig = go.Figure()

    fig.add_trace(go.Pie(
        labels=labels,
        values=values,
        hole=0.4,
        textinfo='percent',
        hoverinfo='label+value+percent',
        marker=dict(colors=colors),
        showlegend=True,
        insidetextfont=dict(color=None),
        outsidetextfont=dict(color="#808080")
    ))

    fig.update_layout(
        autosize=True,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=10, r=10, t=40, b=10),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.05,
            xanchor="center",
            x=0.5,
            font=dict(color="#808080")
        )
    )

    return fig





import pandas as pd

# 📊 Batting Summary Table (phase-wise, inning-wise)
def generate_batting_summary(df, team, inning_no, phase_name="Overall"):
    """
    Batting Summary for a given inning & phase.
    Returns Pandas DataFrame with: Batter | Runs | Balls | 4s | 6s
    """
    team_name = safe_team_name(team)

    # Filter by inning
    inn_df = df[df["scrM_InningNo"] == inning_no]
    if inn_df.empty:
        return pd.DataFrame(columns=["Batter", "Runs", "Balls", "4s", "6s"])

    # Group stats per batter
    grouped = (
        inn_df.groupby("scrM_PlayMIdStrikerName")
        .agg(
            Runs=("scrM_BatsmanRuns", "sum"),
            Balls=("scrM_IsValidBall", "sum"),  # ✅ only count legal balls
            Fours=("scrM_BatsmanRuns", lambda x: (x == 4).sum()),
            Sixes=("scrM_BatsmanRuns", lambda x: (x == 6).sum())
        )
        .reset_index()
    )

    grouped.rename(columns={"scrM_PlayMIdStrikerName": "Batter"}, inplace=True)

    # Sort by Runs (descending)
    grouped = grouped.sort_values(by="Runs", ascending=False).reset_index(drop=True)

    return grouped

from tailwick.utils import no_data_figure  # ✅ import your shared helper

def create_bowling_dotball_donut(df, team, inning_no, phase_name="Overall"):
    """
    Donut chart: % of dot balls bowled by each bowler in the phase.
    df: phase-filtered dataframe (already restricted to this phase)
    team: team name
    inning_no: 1 or 2 (which batting innings this team bowled in)
    """
    team_name = safe_team_name(team)

    # Filter only this inning (bowling side = opposite of batting team)
    inn_df = df[df["scrM_InningNo"] == inning_no]
    if inn_df.empty:
        return no_data_figure()

    # Dot balls per bowler (valid deliveries only)
    dotballs = (
        inn_df[inn_df["scrM_IsValidBall"] == 1]
        .groupby("scrM_PlayMIdBowlerName")
        .apply(lambda x: (x["scrM_BatsmanRuns"] == 0).sum())
        .reset_index(name="DotBalls")
    )

    if dotballs.empty:
        return no_data_figure()

    labels = dotballs["scrM_PlayMIdBowlerName"].tolist()
    values = dotballs["DotBalls"].tolist()

    # Donut slice colors
    colors = [
        '#002f6c', '#FF8C00', '#004C99', '#0069B3', '#0099E5',
        '#22CCEE', '#FF4477', '#44BB99', '#AA3377', '#EE7733'
    ]

    fig = go.Figure()
    fig.add_trace(go.Pie(
        labels=labels,
        values=values,
        hole=0.4,
        textinfo="percent",                 # ✅ only show percentages
        hoverinfo="label+value+percent",    # ✅ full info on hover
        marker=dict(colors=colors),
        showlegend=True,
        insidetextfont=dict(color=None),      # ✅ auto contrast
        outsidetextfont=dict(color="#808080") # ✅ neutral grey
    ))

    fig.update_layout(
        autosize=True,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=10, r=10, t=40, b=10),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.05,
            xanchor="center",
            x=0.5,
            font=dict(color="#808080")
        )
    )
    return fig




def build_bowling_summary(df, team_name, inning_no, phase_name="Overall"):
    """
    Returns per-bowler summary for a phase:
    Bowler, Overs, Runs, W, NB, Wd, Fours, Sixes, Dots, Maiden, Econ
    """
    if df.empty:
        return pd.DataFrame(columns=["Bowler","Overs","Runs","W","NB","Wd","Fours","Sixes","Dots","Maiden","Econ"])

    # ✅ Only legal balls
    legal = df[df["scrM_IsValidBall"] == 1]

    # Group by bowler
    summary = []
    for bowler, group in df.groupby("scrM_PlayMIdBowlerName"):
        balls = len(group[group["scrM_IsValidBall"] == 1])
        overs = f"{balls//6}.{balls%6}" if balls else "0.0"

        runs = group["scrM_DelRuns"].sum()
        wkts = group["scrM_IsWicket"].sum()
        wides = group["scrM_IsWideBall"].sum()
        noballs = group["scrM_IsNoBall"].sum()
        fours = (group["scrM_BatsmanRuns"] == 4).sum()
        sixes = (group["scrM_BatsmanRuns"] == 6).sum()
        dots = (group["scrM_BatsmanRuns"] == 0).sum()

        # ✅ Maiden check (no runs in that over, excluding extras)
        maiden = 0
        for over, over_group in group.groupby("scrM_OverNo"):
            runs_in_over = over_group["scrM_BatsmanRuns"].sum() + over_group["scrM_ByeRuns"].sum() + over_group["scrM_LegByeRuns"].sum()
            if runs_in_over == 0:
                maiden += 1

        econ = round(runs / (balls/6), 2) if balls else 0

        summary.append({
            "Bowler": bowler,
            "Overs": overs,
            "Runs": runs,
            "W": wkts,
            "NB": noballs,
            "Wd": wides,
            "Fours": fours,
            "Sixes": sixes,
            "Dots": dots,
            "Maiden": maiden,
            "Econ": econ,
        })

    return pd.DataFrame(summary)

def map_bowling_type(skill: str) -> str:
    """
    Map detailed bowler skills to Pace or Spin.
    Covers both right-arm and left-arm variations.
    """
    if skill is None:
        return "Unknown"

    skill = skill.strip().lower()

    pace_keywords = ["fast", "medium fast", "medium", "lamf", "ramf", "raf"]
    spin_keywords = ["offbreak", "legbreak", "orthodox", "spinner", "las", "rob", "ralb"]

    if any(k in skill for k in pace_keywords):
        return "Pace"
    elif any(k in skill for k in spin_keywords):
        return "Spin"
    else:
        return "Unknown"


def generate_batting_vs_pace_spin(df, team_name, inning_no, phase_name="Overall"):
    """
    Generate batting performance vs Pace & Spin for a team in a given phase.
    Returns:
        pace_table, spin_table -> pandas DataFrames
    """

    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    # ✅ Map bowler skill to Pace/Spin
    df["BowlingType"] = df["scrM_BowlerSkill"].apply(map_bowling_type)

    # Keep only Pace & Spin
    df = df[df["BowlingType"].isin(["Pace", "Spin"])]

    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    # ✅ Legal balls
    df["IsLegal"] = df["scrM_IsValidBall"] == 1

    # Batting stats per batter vs type
    batting_stats = (
        df.groupby(["BowlingType", "scrM_PlayMIdStrikerName"])
        .agg(
            Runs=("scrM_BatsmanRuns", "sum"),
            Balls=("IsLegal", "sum"),
            Dots=("scrM_BatsmanRuns", lambda x: (x == 0).sum()),
            Fours=("scrM_BatsmanRuns", lambda x: (x == 4).sum()),
            Sixes=("scrM_BatsmanRuns", lambda x: (x == 6).sum()),
        )
        .reset_index()
    )

    # ✅ Strike rate
    batting_stats["SR"] = batting_stats.apply(
        lambda row: round((row.Runs / row.Balls) * 100, 2) if row.Balls > 0 else 0,
        axis=1,
    )

    batting_stats.rename(columns={"scrM_PlayMIdStrikerName": "Batter"}, inplace=True)

    # ✅ Split Pace & Spin tables
    pace_table = batting_stats[batting_stats["BowlingType"] == "Pace"][
        ["Batter", "Runs", "Balls", "SR", "Dots", "Fours", "Sixes"]
    ]
    spin_table = batting_stats[batting_stats["BowlingType"] == "Spin"][
        ["Batter", "Runs", "Balls", "SR", "Dots", "Fours", "Sixes"]
    ]

    return pace_table, spin_table


def map_bowling_type(skill: str) -> str:
    """
    Map detailed bowler skills to Pace or Spin.
    Covers both right-arm and left-arm variations.
    """
    if skill is None:
        return "Unknown"

    skill = skill.strip().lower()

    pace_keywords = ["fast", "medium fast", "medium", "lamf", "ramf", "raf"]
    spin_keywords = ["offbreak", "legbreak", "orthodox", "spinner", "las", "rob", "ralb"]

    if any(k in skill for k in pace_keywords):
        return "Pace"
    elif any(k in skill for k in spin_keywords):
        return "Spin"
    else:
        return "Unknown"


from tailwick.utils import no_data_figure  # ✅ import the shared helper

def create_vs_pace_spin_chart(df, team_name, phase_name="Overall"):
    """
    Horizontal bar chart: Runs vs Pace & Spin with wickets shown as red dots.
    Clean, no grid lines, no white borders, only transparent background.
    Legends styled like player contribution donut (horizontal above chart).
    """
    if df.empty:
        return no_data_figure()

    # ✅ Detect correct wicket column
    if "scrM_IsBowlerWicket" in df.columns:
        wicket_col = "scrM_IsBowlerWicket"
    elif "scrM_IsWicket" in df.columns:
        wicket_col = "scrM_IsWicket"
    else:
        return no_data_figure("No valid wicket column found")

    # ✅ Map bowler skill into Pace/Spin
    df["BowlingType"] = df["scrM_BowlerSkill"].apply(map_bowling_type)
    df = df[df["BowlingType"].isin(["Pace", "Spin"])]  # keep only Pace/Spin

    if df.empty:
        return no_data_figure()

    colors = {"Pace": "#002f6c", "Spin": "#FF8C00"}

    grouped = (
        df.groupby("BowlingType")
        .agg(
            Runs=("scrM_BatsmanRuns", "sum"),
            Wickets=(wicket_col, "sum")
        )
        .reset_index()
    )

    if grouped.empty:
        return no_data_figure()

    fig = go.Figure()
    wicket_legend_added = False

    for _, row in grouped.iterrows():
        grp = row["BowlingType"]
        runs = row["Runs"]
        wkts = row["Wickets"]

        # Runs bar
        fig.add_trace(
            go.Bar(
                y=[grp],
                x=[runs],
                name=f"{grp} Runs",
                orientation="h",
                marker=dict(color=colors.get(grp, "#888")),
                text=[runs],
                textposition="inside",
                textfont=dict(color="white"),  # ✅ inside stays white
            )
        )

        # Red dot for wickets
        if wkts > 0:
            fig.add_trace(
                go.Scatter(
                    y=[grp],
                    x=[runs + max(5, runs * 0.05)],
                    mode="markers",
                    marker=dict(color="red", size=14, symbol="circle"),
                    name="Wickets" if not wicket_legend_added else None,
                    showlegend=not wicket_legend_added,
                )
            )
            wicket_legend_added = True

    fig.update_layout(
        barmode="group",
        xaxis=dict(
            title=dict(text="Runs", font=dict(color="#808080")),
            showgrid=False,
            showline=False,
            zeroline=False,
            showticklabels=True,
            tickfont=dict(color="#808080")
        ),
        yaxis=dict(
            title=dict(text="Bowling Type", font=dict(color="#808080")),
            showgrid=False,
            showline=False,
            zeroline=False,
            showticklabels=True,
            tickfont=dict(color="#808080")
        ),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=10, r=10, t=40, b=10),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.05,
            xanchor="center",
            x=0.5,
            font=dict(color="#808080"),
            bgcolor="rgba(0,0,0,0)",
            bordercolor="rgba(0,0,0,0)"
        )
    )

    return fig



def create_phase_partnership_chart(phase_df, team_name, phase_name="Overall"):
    """
    Partnership strip chart for a given team in a particular phase.
    Shows Batter1 vs Batter2 contributions + Extras with runs/balls annotations.
    """
    import plotly.graph_objects as go
    import pandas as pd

    # === Handle empty df ===
    if phase_df.empty:
        return no_data_figure()

    # ✅ Calculate Extras
    extras_cols = ["scrM_ByeRuns", "scrM_LegByeRuns", "scrM_NoBallRuns",
                   "scrM_WideRuns", "scrM_PenaltyRuns"]
    for col in extras_cols:
        if col not in phase_df.columns:
            phase_df[col] = 0
    phase_df["scrM_Extras"] = phase_df[extras_cols].sum(axis=1)

    # ✅ Valid balls
    phase_df["Valid_Ball"] = phase_df["scrM_IsValidBall"] == 1

    # ✅ Partnership key
    phase_df["Partnership_Key"] = phase_df.apply(
        lambda row: "_&_".join(sorted([
            str(row["scrM_PlayMIdStrikerName"]),
            str(row["scrM_PlayMIdNonStrikerName"])
        ])),
        axis=1
    )

    # Build partnerships
    partnerships = []
    for _, group in phase_df[phase_df["Valid_Ball"]].groupby("Partnership_Key"):
        striker = group["scrM_PlayMIdStrikerName"].iloc[0]
        non_striker = group["scrM_PlayMIdNonStrikerName"].iloc[0]

        batter1, batter2 = sorted([striker, non_striker])

        batter1_runs = group[group["scrM_PlayMIdStrikerName"] == batter1]["scrM_BatsmanRuns"].sum()
        batter1_balls = group[group["scrM_PlayMIdStrikerName"] == batter1]["Valid_Ball"].sum()

        batter2_runs = group[group["scrM_PlayMIdStrikerName"] == batter2]["scrM_BatsmanRuns"].sum()
        batter2_balls = group[group["scrM_PlayMIdStrikerName"] == batter2]["Valid_Ball"].sum()

        extras = group["scrM_Extras"].sum()
        balls = group["Valid_Ball"].sum()

        partnerships.append({
            "Batter1": batter1,
            "Batter2": batter2,
            "Batter1_Runs": batter1_runs,
            "Batter1_Balls": batter1_balls,
            "Batter2_Runs": batter2_runs,
            "Batter2_Balls": batter2_balls,
            "Extras": extras,
            "Total": batter1_runs + batter2_runs + extras,
            "Balls": balls
        })

    partnerships_df = pd.DataFrame(partnerships)
    if partnerships_df.empty:
        return no_data_figure()

    # ✅ Sort by total runs
    partnerships_df = partnerships_df.sort_values(by="Total", ascending=False)

    # ✅ Normalize to fractions
    partnerships_df["Batter1_frac"] = partnerships_df["Batter1_Runs"] / partnerships_df["Total"]
    partnerships_df["Extras_frac"]  = partnerships_df["Extras"] / partnerships_df["Total"]
    partnerships_df["Batter2_frac"] = partnerships_df["Batter2_Runs"] / partnerships_df["Total"]

    # ✅ Scale for strip width
    chart_width = 0.8
    n = len(partnerships_df)
    target_width = min(0.6, chart_width)

    partnerships_df["Batter1_scaled"] = partnerships_df["Batter1_frac"] * target_width
    partnerships_df["Extras_scaled"]  = partnerships_df["Extras_frac"] * target_width
    partnerships_df["Batter2_scaled"] = partnerships_df["Batter2_frac"] * target_width

    fig = go.Figure()

    # === Spacing factor ===
    strip_gap = 2.5
    y_positions = [i * strip_gap for i in range(n)]
    strip_thickness = 0.7

    # === Bars ===
    fig.add_trace(go.Bar(
        x=-partnerships_df["Batter1_scaled"], y=y_positions,
        orientation="h", marker=dict(color="#FF8C00", line=dict(width=0)),
        name="Batter1", hoverinfo="skip", width=strip_thickness
    ))
    fig.add_trace(go.Bar(
        x=partnerships_df["Extras_scaled"], y=y_positions,
        orientation="h", marker=dict(color="#32CD32", line=dict(width=0)),
        name="Extras", hoverinfo="skip", width=strip_thickness
    ))
    fig.add_trace(go.Bar(
        x=partnerships_df["Batter2_scaled"], y=y_positions,
        orientation="h", marker=dict(color="#1E90FF", line=dict(width=0)),
        name="Batter2", hoverinfo="skip", width=strip_thickness
    ))

    # === Annotations ===
    for i, row in partnerships_df.iterrows():
        y = y_positions[partnerships_df.index.get_loc(i)]

        fig.add_annotation(
            x=-0.8, y=y, xanchor="right", align="center",
            text=f"{row['Batter1']}<br><b>{row['Batter1_Runs']} ({row['Batter1_Balls']})</b>",
            showarrow=False, font=dict(size=10, color="#FF8C00")
        )

        fig.add_annotation(
            x=0.8, y=y, xanchor="left", align="center",
            text=f"{row['Batter2']}<br><b>{row['Batter2_Runs']} ({row['Batter2_Balls']})</b>",
            showarrow=False, font=dict(size=10, color="#1E90FF")
        )

        fig.add_annotation(
            x=0, y=y + 0.6, xanchor="center", align="center",
            text=f"<b>Partnership - {row['Total']} ({row['Balls']})</b>",
            showarrow=False, font=dict(size=10, color="#808080")
        )

        fig.add_annotation(
            x=0, y=y - 0.6, xanchor="center", align="center",
            text=f"Extras - {row['Extras']}",
            showarrow=False, font=dict(size=10, color="#32CD32")
        )

    # === Layout ===
    fig.update_layout(
        barmode="relative",
        showlegend=False,
        height=max(400, n * 120),
        margin=dict(l=120, r=120, t=40, b=20),
        xaxis=dict(visible=False, range=[-1, 1]),
        yaxis=dict(visible=False, range=[-strip_gap, (n) * strip_gap]),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)"
    )

    return fig


import plotly.express as px

def generate_delivery_type_distribution(df, team_name=None, phase_label=None):
    if df.empty or "scrM_DeliveryType_zName" not in df.columns:
        return None

    counts = df["scrM_DeliveryType_zName"].fillna("Unknown").value_counts(normalize=True) * 100
    data = counts.reset_index()
    data.columns = ["Delivery_Type", "Percentage"]

    if data.empty:
        return None

    title = "Delivery Type Distribution"
    if team_name:
        title = f"{team_name} - {title}"
    if phase_label:
        title += f" ({phase_label})"

    colors = [
        '#002f6c', '#FF8C00', '#004C99', '#0069B3', '#0099E5',
        '#22CCEE', '#FF4477', '#44BB99', '#AA3377', '#EE7733'
    ]

    fig = px.bar(
        data,
        x="Delivery_Type",
        y="Percentage",
        text=data["Percentage"].apply(lambda x: f"{x:.1f}%"),
        color="Delivery_Type",
        color_discrete_sequence=colors
    )
    fig.update_traces(
        textposition="outside",
        textfont=dict(color="#808080"),
        showlegend=False
    )

    fig.update_layout(
        title=dict(
            text=title,
            x=0.5,
            font=dict(size=14, color="#808080")
        ),
        autosize=True,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=10, r=10, t=50, b=10),
        xaxis=dict(
            title=dict(text="Delivery Type", font=dict(color="#808080")),
            tickfont=dict(color="#808080"),
            showgrid=False,
            zeroline=False
        ),
        yaxis=dict(
            title=dict(text="Percentage (%)", font=dict(color="#808080")),
            tickfont=dict(color="#808080"),
            showgrid=False,
            zeroline=False
        ),
        showlegend=False
    )
    return fig


def generate_pitch_area_distribution(df, team_name=None, phase_label=None):
    if df.empty or "scrM_PitchArea_zName" not in df.columns:
        return None

    counts = df["scrM_PitchArea_zName"].fillna("Unknown").value_counts(normalize=True) * 100
    data = counts.reset_index()
    data.columns = ["Pitch_Area", "Percentage"]

    if data.empty:
        return None

    title = "Pitch Area Distribution"
    if team_name:
        title = f"{team_name} - {title}"
    if phase_label:
        title += f" ({phase_label})"

    colors = [
        '#002f6c', '#FF8C00', '#004C99', '#0069B3', '#0099E5',
        '#22CCEE', '#FF4477', '#44BB99', '#AA3377', '#EE7733'
    ]

    fig = px.bar(
        data,
        y="Pitch_Area", x="Percentage", orientation="h",
        text=data["Percentage"].apply(lambda x: f"{x:.1f}%"),
        color="Pitch_Area",
        color_discrete_sequence=colors
    )
    fig.update_traces(
        textposition="outside",
        textfont=dict(color="#808080"),
        showlegend=False
    )

    fig.update_layout(
        title=dict(
            text=title,
            x=0.5,
            font=dict(size=14, color="#808080")
        ),
        autosize=True,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=10, r=10, t=50, b=10),
        xaxis=dict(
            title=dict(text="Percentage (%)", font=dict(color="#808080")),
            tickfont=dict(color="#808080"),
            showgrid=False,
            zeroline=False
        ),
        yaxis=dict(
            title=dict(text="Pitch Area", font=dict(color="#808080")),
            tickfont=dict(color="#808080"),
            showgrid=False,
            zeroline=False
        ),
        showlegend=False
    )
    return fig

def build_bowling_runs_conceded_summary(df, team_name, inning_no, phase_name="Overall"):
    """
    Build runs conceded breakdown + bowler stats for a team in a given phase.
    Returns:
        team_data (pd.DataFrame) → bowler stats with overs classification
    """
    if df.empty:
        return pd.DataFrame()

    # Filter to correct inning
    inn_df = df[df["scrM_InningNo"] == inning_no]
    if inn_df.empty:
        return pd.DataFrame()

    # ✅ Aggregate runs per over per bowler
    runs_per_over = inn_df.groupby(
        ["scrM_PlayMIdBowlerName", "scrM_OverNo"]
    )["scrM_DelRuns"].sum().reset_index()

    # ✅ Classify overs
    less_than_6 = runs_per_over[runs_per_over["scrM_DelRuns"] < 6] \
        .groupby("scrM_PlayMIdBowlerName").size().reset_index(name="< 6 Runs Overs")
    equal_6 = runs_per_over[runs_per_over["scrM_DelRuns"] == 6] \
        .groupby("scrM_PlayMIdBowlerName").size().reset_index(name="= 6 Runs Overs")
    greater_6 = runs_per_over[runs_per_over["scrM_DelRuns"] > 6] \
        .groupby("scrM_PlayMIdBowlerName").size().reset_index(name="> 6 Runs Overs")

    # ✅ Bowler stats
    bowler_stats = runs_per_over.groupby("scrM_PlayMIdBowlerName").agg(
        Overs=("scrM_OverNo", "nunique"),
        Runs_Conceded=("scrM_DelRuns", "sum"),
    ).reset_index()

    # ✅ Actual Maidens (overs where total runs = 0)
    maidens = runs_per_over[runs_per_over["scrM_DelRuns"] == 0] \
        .groupby("scrM_PlayMIdBowlerName").size().reset_index(name="Maidens")

    # ✅ Merge all
    final = bowler_stats.merge(maidens, on="scrM_PlayMIdBowlerName", how="left").fillna(0)
    final = final.merge(less_than_6, on="scrM_PlayMIdBowlerName", how="left").fillna(0)
    final = final.merge(equal_6, on="scrM_PlayMIdBowlerName", how="left").fillna(0)
    final = final.merge(greater_6, on="scrM_PlayMIdBowlerName", how="left").fillna(0)

    # ✅ Ensure integers for counts
    for col in ["Maidens", "< 6 Runs Overs", "= 6 Runs Overs", "> 6 Runs Overs"]:
        final[col] = final[col].astype(int)

    # Rename for display
    final.rename(columns={
        "scrM_PlayMIdBowlerName": "Bowler",
        "Runs_Conceded": "Runs Conceded",
    }, inplace=True)

    return final


def create_runs_conceded_chart_and_table(df, team_name, inning_no, phase_name="Overall"):
    """
    Create chart + return DataFrame for Tailwind rendering.
    """
    team_data = build_bowling_runs_conceded_summary(df, team_name, inning_no, phase_name)
    if team_data.empty:
        return go.Figure(), pd.DataFrame()  # return empty DF if no data

    # --- Chart ---
    melted = team_data.melt(
        id_vars=["Bowler"],
        value_vars=["< 6 Runs Overs", "= 6 Runs Overs", "> 6 Runs Overs"]
    )

    fig = px.bar(
        melted, 
        x="Bowler", 
        y="value", 
        color="variable", 
        barmode="group",
        labels={"value": "Number of Overs"},  
        color_discrete_map={
            "< 6 Runs Overs": "#002f6c",
            "= 6 Runs Overs": "#0099E5",
            "> 6 Runs Overs": "#FF8C00"
        }
    )

    # ✅ Bar labels grey
    fig.update_traces(
        texttemplate="%{y}", 
        textposition="outside", 
        textfont=dict(color="#808080")
    )

    # ✅ Force y-axis to show only integers
    y_max = melted["value"].max()

    fig.update_layout(
        title_x=0.5,
        title_font=dict(size=20, family="Arial, sans-serif", color="#808080"),
        autosize=True,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=40, r=40, t=60, b=40),
        xaxis=dict(
            showgrid=False,
            zeroline=False,
            color="#808080",
            title_font=dict(color="#808080"),
            tickfont=dict(color="#808080")
        ),
        yaxis=dict(
            showgrid=False,
            zeroline=False,
            color="#808080",
            title_font=dict(color="#808080"),
            tickfont=dict(color="#808080"),
            dtick=1,              # ✅ force step = 1
            rangemode="tozero",   # ✅ always start at 0
            range=[0, y_max + 1]  # ✅ clean integer top range
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.05,
            xanchor="center",
            x=0.5,
            title_text="",  # ✅ remove "Runs Category" label
            font=dict(color="#808080")
        )
    )

    # ✅ Return chart + DataFrame
    return fig, team_data


import plotly.express as px
import plotly.graph_objects as go
import pandas as pd


def build_boundaries_per_ball_summary(df, team_name, inning_no, phase_name="Overall"):
    """
    Build a summary of boundaries (4s & 6s) conceded by each bowler,
    broken down by the ball number within the over.
    """

    # ✅ Filter by inning and bowling team
    inn_df = df[(df["scrM_InningNo"] == inning_no) & (df["scrM_tmMIdBowlingName"] == team_name)]

    # If no data, return empty
    if inn_df.empty:
        return pd.DataFrame()

    inn_df = inn_df.copy()

    # ✅ Ball number within the over
    inn_df["scrM_BallNo"] = inn_df.groupby(["scrM_OverNo", "scrM_PlayMIdBowlerName"]).cumcount() + 1
    inn_df = inn_df[inn_df["scrM_BallNo"].between(1, 6)]

    # ✅ Boundary filter using IsBoundry or IsSixer flags
    boundary_df = inn_df[(inn_df["scrM_IsBoundry"] == 1) | (inn_df["scrM_IsSixer"] == 1)]

    if boundary_df.empty:
        return pd.DataFrame()

    # ✅ Group boundaries by bowler and ball position
    boundary_summary = (
        boundary_df.groupby(["scrM_PlayMIdBowlerName", "scrM_BallNo"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )

    # ✅ Ensure all six ball positions exist
    for i in range(1, 7):
        if i not in boundary_summary.columns:
            boundary_summary[i] = 0

    # ✅ Rename columns safely
    boundary_summary.rename(
        columns={
            "scrM_PlayMIdBowlerName": "Bowler",
            1: "1st",
            2: "2nd",
            3: "3rd",
            4: "4th",
            5: "5th",
            6: "6th",
        },
        inplace=True,
    )

    # ✅ Reorder columns so they always appear sequentially
    column_order = ["Bowler", "1st", "2nd", "3rd", "4th", "5th", "6th"]
    boundary_summary = boundary_summary[column_order]

    return boundary_summary




def create_boundaries_per_ball_chart(team_data, team_name):
    """
    Generate a stacked bar chart showing boundaries conceded by each bowler,
    broken down by ball number in the over, with scroll-friendly layout.
    """
    if team_data.empty:
        print(f"No data to plot for {team_name}. Returning empty figure.")
        return go.Figure()

    # 🎨 Custom color palette for balls 1–6
    custom_colors = {
        "1st": "#002f6c", "2nd": "#FF8C00", "3rd": "#004C99",
        "4th": "#0069B3", "5th": "#0099E5", "6th": "#EE553B"
    }

    # Melt for long-form Plotly Express input
    melted = team_data.melt(
        id_vars=["Bowler"],
        value_vars=["1st", "2nd", "3rd", "4th", "5th", "6th"],
        var_name="scrM_BallNo",
        value_name="Boundary Count"
    )

    # ✅ Create stacked bar chart
    fig = px.bar(
        melted,
        x="Bowler",
        y="Boundary Count",
        color="scrM_BallNo",
        barmode="stack",
        color_discrete_map=custom_colors
    )

    # ✅ Add data labels inside bars
    fig.update_traces(
        texttemplate="%{y}",
        textposition="inside",
        insidetextanchor="middle",
        textfont=dict(color="white", size=11)
    )

    # ✅ Fixed layout for consistent width (scrollable on mobile)
    y_max = melted["Boundary Count"].max()
    fig.update_layout(
        autosize=False,
        width=800,      # fixed width for desktop, scroll on mobile
        height=500,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=30, r=30, t=40, b=60),
        xaxis=dict(
            showgrid=False,
            zeroline=False,
            showline=False,
            tickfont=dict(color="#808080"),
            title_font=dict(color="#808080"),
        ),
        yaxis=dict(
            showgrid=False,
            zeroline=False,
            tickfont=dict(color="#808080"),
            title_font=dict(color="#808080"),
            dtick=1,
            rangemode="tozero",
            range=[0, y_max + 1]
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.05,
            xanchor="center",
            x=0.5,
            title_text="",
            font=dict(color="#808080"),
            bgcolor="rgba(0,0,0,0)"
        )
    )

    return fig




def create_boundaries_conceded_chart_and_table(df, team_name, inning_no, phase_name="Overall"):
    """
    Wrapper: Generates both the summary table and chart
    """
    team_data = build_boundaries_per_ball_summary(df, team_name, inning_no, phase_name)

    if team_data.empty:
        print(f"[{team_name}] No boundary data found for inning {inning_no} ({phase_name}).")
        return go.Figure(), pd.DataFrame()

    fig = create_boundaries_per_ball_chart(team_data, team_name)
    return fig, team_data



def get_players_by_tournament(tournament_id):
    """
    Fetch aggregated player batting stats for the selected tournament.
    Works without team selection — all players in the tournament.
    Normalizes scrM_StrikerBatterSkill into RHB/LHB.
    Works with MySQL (pymysql).
    """
    import pandas as pd

    try:
        conn = get_connection()

        query = """
            WITH player_innings AS (
                SELECT
                    s.scrM_PlayMIdStrikerName AS player_name,
                    s.scrM_tmMIdBattingName AS team_short_name,
                    CASE 
                        WHEN s.scrM_StrikerBatterSkill LIKE '%%Right%%' THEN 'RHB'
                        WHEN s.scrM_StrikerBatterSkill LIKE '%%Left%%' THEN 'LHB'
                        ELSE 'UNK'
                    END AS batter_skill,
                    s.scrM_InnId,
                    SUM(s.scrM_BatsmanRuns) AS runs_in_inn,
                    COUNT(s.scrM_DelId) AS balls_in_inn,
                    SUM(CASE WHEN s.scrM_BatsmanRuns = 0 THEN 1 ELSE 0 END) AS dots_in_inn,
                    SUM(CASE WHEN s.scrM_IsBoundry = 1 THEN 1 ELSE 0 END) AS fours_in_inn,
                    SUM(CASE WHEN s.scrM_IsSixer = 1 THEN 1 ELSE 0 END) AS sixes_in_inn,
                    MAX(CAST(s.scrM_IsWicket AS UNSIGNED)) AS got_out
                FROM tblscoremaster s
                WHERE s.scrM_TrnMId = %s
                  AND s.scrM_IsValidBall = 1
                GROUP BY s.scrM_PlayMIdStrikerName, s.scrM_tmMIdBattingName, 
                         CASE 
                            WHEN s.scrM_StrikerBatterSkill LIKE '%%Right%%' THEN 'RHB'
                            WHEN s.scrM_StrikerBatterSkill LIKE '%%Left%%' THEN 'LHB'
                            ELSE 'UNK'
                         END,
                         s.scrM_InnId
            )
            SELECT
                player_name,
                team_short_name,
                batter_skill,
                COUNT(scrM_InnId) AS innings,
                SUM(runs_in_inn) AS runs,
                SUM(CASE WHEN got_out = 0 THEN 1 ELSE 0 END) AS not_outs,
                SUM(balls_in_inn) AS balls,
                SUM(dots_in_inn) AS dots,
                SUM(fours_in_inn) AS fours,
                SUM(sixes_in_inn) AS sixes,
                ROUND(
                    CASE WHEN SUM(balls_in_inn) > 0
                         THEN (SUM(runs_in_inn) * 100.0 / SUM(balls_in_inn))
                         ELSE 0 END, 2
                ) AS strike_rate,
                ROUND(
                    CASE WHEN (COUNT(scrM_InnId) - SUM(CASE WHEN got_out = 0 THEN 1 ELSE 0 END)) > 0
                         THEN (SUM(runs_in_inn) * 1.0 /
                              (COUNT(scrM_InnId) - SUM(CASE WHEN got_out = 0 THEN 1 ELSE 0 END)))
                         ELSE 0 END, 2
                ) AS average
            FROM player_innings
            GROUP BY player_name, team_short_name, batter_skill
            ORDER BY runs DESC
        """

        df = pd.read_sql(query, conn, params=(tournament_id,))
        conn.close()

        # Cast numeric fields properly
        int_cols = ["innings", "runs", "not_outs", "balls", "dots", "fours", "sixes"]
        for col in int_cols:
            df[col] = df[col].fillna(0).astype(int)

        float_cols = ["strike_rate", "average"]
        for col in float_cols:
            df[col] = df[col].fillna(0).astype(float)

        print(f"✅ Loaded {len(df)} players for tournament {tournament_id}")
        return df.to_dict(orient="records")

    except Exception as e:
        print("❌ Error fetching players by tournament:", e)
        return []


def get_players_by_team(tournament_id, team_name):
    """
    Fetch aggregated player batting stats for a specific tournament AND team.
    Normalizes scrM_StrikerBatterSkill into RHB/LHB.
    Works with MySQL (pymysql).
    """
    import pandas as pd

    try:
        conn = get_connection()

        query = """
            WITH player_innings AS (
                SELECT
                    s.scrM_PlayMIdStrikerName AS player_name,
                    s.scrM_tmMIdBattingName AS team_short_name,
                    CASE 
                        WHEN s.scrM_StrikerBatterSkill LIKE '%%Right%%' THEN 'RHB'
                        WHEN s.scrM_StrikerBatterSkill LIKE '%%Left%%' THEN 'LHB'
                        ELSE 'UNK'
                    END AS batter_skill,
                    s.scrM_InnId,
                    SUM(s.scrM_BatsmanRuns) AS runs_in_inn,
                    COUNT(s.scrM_DelId) AS balls_in_inn,
                    SUM(CASE WHEN s.scrM_BatsmanRuns = 0 THEN 1 ELSE 0 END) AS dots_in_inn,
                    SUM(CASE WHEN s.scrM_IsBoundry = 1 THEN 1 ELSE 0 END) AS fours_in_inn,
                    SUM(CASE WHEN s.scrM_IsSixer = 1 THEN 1 ELSE 0 END) AS sixes_in_inn,
                    MAX(CAST(s.scrM_IsWicket AS UNSIGNED)) AS got_out
                FROM tblscoremaster s
                WHERE s.scrM_TrnMId = %s
                  AND s.scrM_tmMIdBattingName = %s
                  AND s.scrM_IsValidBall = 1
                GROUP BY s.scrM_PlayMIdStrikerName, s.scrM_tmMIdBattingName, 
                         CASE 
                            WHEN s.scrM_StrikerBatterSkill LIKE '%%Right%%' THEN 'RHB'
                            WHEN s.scrM_StrikerBatterSkill LIKE '%%Left%%' THEN 'LHB'
                            ELSE 'UNK'
                         END,
                         s.scrM_InnId
            )
            SELECT
                player_name,
                team_short_name,
                batter_skill,
                COUNT(scrM_InnId) AS innings,
                SUM(runs_in_inn) AS runs,
                SUM(CASE WHEN got_out = 0 THEN 1 ELSE 0 END) AS not_outs,
                SUM(balls_in_inn) AS balls,
                SUM(dots_in_inn) AS dots,
                SUM(fours_in_inn) AS fours,
                SUM(sixes_in_inn) AS sixes,
                ROUND(
                    CASE WHEN SUM(balls_in_inn) > 0
                         THEN (SUM(runs_in_inn) * 100.0 / SUM(balls_in_inn))
                         ELSE 0 END, 2
                ) AS strike_rate,
                ROUND(
                    CASE WHEN (COUNT(scrM_InnId) - SUM(CASE WHEN got_out = 0 THEN 1 ELSE 0 END)) > 0
                         THEN (SUM(runs_in_inn) * 1.0 /
                              (COUNT(scrM_InnId) - SUM(CASE WHEN got_out = 0 THEN 1 ELSE 0 END)))
                         ELSE 0 END, 2
                ) AS average
            FROM player_innings
            GROUP BY player_name, team_short_name, batter_skill
            ORDER BY runs DESC
        """

        df = pd.read_sql(query, conn, params=(tournament_id, team_name))
        conn.close()

        # Cast numeric fields properly
        int_cols = ["innings", "runs", "not_outs", "balls", "dots", "fours", "sixes"]
        for col in int_cols:
            df[col] = df[col].fillna(0).astype(int)

        float_cols = ["strike_rate", "average"]
        for col in float_cols:
            df[col] = df[col].fillna(0).astype(float)

        print(f"✅ Loaded {len(df)} players for team {team_name} in tournament {tournament_id}")
        return df.to_dict(orient="records")

    except Exception as e:
        print("❌ Error fetching players by team:", e)
        return []


def map_bowling_type_1(skill):
    """
    Normalize scrM_BowlerSkill into PACE or SPIN.
    """
    if not skill:
        return "UNK"
    skill = skill.lower()
    if "spin" in skill:
        return "SPIN"
    if "fast" in skill or "medium" in skill or "pace" in skill or "seam" in skill:
        return "PACE"
    return "UNK"


def get_bowlers_by_tournament(tournament_id):
    """
    Fetch aggregated player bowling stats for the selected tournament.
    Returns overs as 'O.B' (string like '14.2' = 14 overs and 2 balls).
    """
    import pandas as pd

    try:
        conn = get_connection()
        query = """
            SELECT
                s.scrM_PlayMIdBowlerName AS player_name,
                s.scrM_tmMIdBowlingName AS team_short_name,
                s.scrM_BowlerSkill,
                s.scrM_InnId,
                SUM(s.scrM_BatsmanRuns + COALESCE(s.scrM_LegByeRuns,0) + COALESCE(s.scrM_IsNoBall,0) + COALESCE(s.scrM_IsWideBall,0)) AS runs_in_inn,
                COUNT(s.scrM_DelId) AS balls_in_inn,
                SUM(CASE WHEN s.scrM_BatsmanRuns = 0 THEN 1 ELSE 0 END) AS dots_in_inn,
                SUM(CAST(s.scrM_IsWicket AS UNSIGNED)) AS wkts_in_inn
            FROM tblscoremaster s
            WHERE s.scrM_TrnMId = %s
              AND s.scrM_IsValidBall = 1
            GROUP BY s.scrM_PlayMIdBowlerName, s.scrM_tmMIdBowlingName, s.scrM_BowlerSkill, s.scrM_InnId
        """
        df = pd.read_sql(query, conn, params=(tournament_id,))
        conn.close()

        if df.empty:
            return []

        # Normalize bowler skill (requires map_bowling_type_1 present)
        df["bowler_skill"] = df["scrM_BowlerSkill"].apply(map_bowling_type_1)

        # Aggregate across innings
        agg = df.groupby(["player_name", "team_short_name", "bowler_skill"], as_index=False).agg(
            innings=("scrM_InnId", "count"),
            runs=("runs_in_inn", "sum"),
            wkts=("wkts_in_inn", "sum"),
            balls=("balls_in_inn", "sum"),
            dots=("dots_in_inn", "sum"),
        )

        # Convert numeric columns to ints (safe)
        int_cols = ["innings", "runs", "wkts", "balls", "dots"]
        for col in int_cols:
            agg[col] = agg[col].fillna(0).astype(int)

        # Calculate overs as O.B string (e.g. "14.2" = 14 overs and 2 balls)
        def balls_to_overs_str(total_balls):
            try:
                total_balls = int(total_balls)
            except Exception:
                return "0.0"
            overs = total_balls // 6
            rem = total_balls % 6
            return f"{overs}.{rem}"

        agg["overs"] = agg["balls"].apply(balls_to_overs_str)

        # Economy, Strike Rate (balls per wicket), Average (runs per wicket)
        agg["eco"] = agg.apply(lambda r: round((r["runs"] * 6.0 / r["balls"]), 2) if r["balls"] > 0 else 0.0, axis=1)
        agg["strike_rate"] = agg.apply(lambda r: round((r["balls"] / r["wkts"]), 2) if r["wkts"] > 0 else 0.0, axis=1)
        agg["average"] = agg.apply(lambda r: round((r["runs"] / r["wkts"]), 2) if r["wkts"] > 0 else 0.0, axis=1)

        # Ensure floats for these derived metrics
        float_cols = ["eco", "strike_rate", "average"]
        for col in float_cols:
            agg[col] = agg[col].fillna(0).astype(float)

        # Final columns ordering (optional)
        final_cols = [
            "player_name", "team_short_name", "bowler_skill",
            "innings", "overs", "balls", "runs", "wkts", "dots",
            "eco", "strike_rate", "average"
        ]
        agg = agg[final_cols]

        print(f"✅ Loaded {len(agg)} bowlers for tournament {tournament_id}")
        return agg.to_dict(orient="records")

    except Exception as e:
        print("❌ Error fetching bowlers by tournament:", e)
        return []



def get_bowlers_by_team(tournament_id, team_name):
    """
    Fetch aggregated player bowling stats for a specific tournament AND team.
    Returns overs as 'O.B' (string like '14.2' = 14 overs and 2 balls).
    """
    import pandas as pd

    try:
        conn = get_connection()
        query = """
            SELECT
                s.scrM_PlayMIdBowlerName AS player_name,
                s.scrM_tmMIdBowlingName AS team_short_name,
                s.scrM_BowlerSkill,
                s.scrM_InnId,
                SUM(s.scrM_BatsmanRuns + COALESCE(s.scrM_LegByeRuns,0) + COALESCE(s.scrM_IsNoBall,0) + COALESCE(s.scrM_IsWideBall,0)) AS runs_in_inn,
                COUNT(s.scrM_DelId) AS balls_in_inn,
                SUM(CASE WHEN s.scrM_BatsmanRuns = 0 THEN 1 ELSE 0 END) AS dots_in_inn,
                SUM(CAST(s.scrM_IsWicket AS UNSIGNED)) AS wkts_in_inn
            FROM tblscoremaster s
            WHERE s.scrM_TrnMId = %s
              AND s.scrM_tmMIdBowlingName = %s
              AND s.scrM_IsValidBall = 1
            GROUP BY s.scrM_PlayMIdBowlerName, s.scrM_tmMIdBowlingName, s.scrM_BowlerSkill, s.scrM_InnId
        """
        df = pd.read_sql(query, conn, params=(tournament_id, team_name))
        conn.close()

        if df.empty:
            return []

        # Normalize bowler skill
        df["bowler_skill"] = df["scrM_BowlerSkill"].apply(map_bowling_type_1)

        # Aggregate across innings
        agg = df.groupby(["player_name", "team_short_name", "bowler_skill"], as_index=False).agg(
            innings=("scrM_InnId", "count"),
            runs=("runs_in_inn", "sum"),
            wkts=("wkts_in_inn", "sum"),
            balls=("balls_in_inn", "sum"),
            dots=("dots_in_inn", "sum"),
        )

        # Convert numeric columns to ints
        int_cols = ["innings", "runs", "wkts", "balls", "dots"]
        for col in int_cols:
            agg[col] = agg[col].fillna(0).astype(int)

        # Calculate overs as O.B string
        def balls_to_overs_str(total_balls):
            try:
                total_balls = int(total_balls)
            except Exception:
                return "0.0"
            overs = total_balls // 6
            rem = total_balls % 6
            return f"{overs}.{rem}"

        agg["overs"] = agg["balls"].apply(balls_to_overs_str)

        # Economy, Strike Rate, Average
        agg["eco"] = agg.apply(lambda r: round((r["runs"] * 6.0 / r["balls"]), 2) if r["balls"] > 0 else 0.0, axis=1)
        agg["strike_rate"] = agg.apply(lambda r: round((r["balls"] / r["wkts"]), 2) if r["wkts"] > 0 else 0.0, axis=1)
        agg["average"] = agg.apply(lambda r: round((r["runs"] / r["wkts"]), 2) if r["wkts"] > 0 else 0.0, axis=1)

        # Ensure floats for derived metrics
        float_cols = ["eco", "strike_rate", "average"]
        for col in float_cols:
            agg[col] = agg[col].fillna(0).astype(float)

        # Final columns ordering
        final_cols = [
            "player_name", "team_short_name", "bowler_skill",
            "innings", "overs", "balls", "runs", "wkts", "dots",
            "eco", "strike_rate", "average"
        ]
        agg = agg[final_cols]

        print(f"✅ Loaded {len(agg)} bowlers for team {team_name} in tournament {tournament_id}")
        return agg.to_dict(orient="records")

    except Exception as e:
        print("❌ Error fetching bowlers by team:", e)
        return []




def get_offline_video_path(del_id, row, parent_path):
    """
    Try to resolve the correct offline video path for a delivery.
    row: dict from tblscoremaster (with scrM_Video1FileName..6FileName)
    parent_path: str from tblSettingsMaster.setM_VideoPath
    """
    file_candidates = [
        row.get("scrM_Video1FileName"),
        row.get("scrM_Video2FileName"),
        row.get("scrM_Video3FileName"),
        row.get("scrM_Video4FileName"),
        row.get("scrM_Video5FileName"),
        row.get("scrM_Video6FileName"),
    ]
    for f in file_candidates:
        if f and (str(del_id) in f):
            return os.path.join(parent_path, f)
    return None

    
def get_parent_video_path_from_db():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT TOP 1 setM_VideoPath FROM tblsettingsmaster")
    row = cursor.fetchone()
    return row[0] if row and row[0] else ""

# def get_dismissal_text(d):
#     """
#     Given a row with scrM_IsWicket=1, return a readable dismissal string.
#     """
#     bowler = d.get("scrM_PlayMIdBowlerName") or ""
#     batter = d.get("scrM_PlayMIdStrikerName") or ""
    
#     # Caught
#     if pd.notna(d.get("scrM_playMIdCaughtName")) and d["scrM_playMIdCaughtName"]:
#         return f"c {d['scrM_playMIdCaughtName']} b {bowler}"
    
#     # Bowled
#     if d.get("scrM_DecisionFinal_zName") == "Bowled" or (d.get("scrM_IsBowlerWicket") == 1 and not d.get("scrM_playMIdCaughtName")):
#         return f"b {bowler}"
    
#     # LBW
#     if d.get("scrM_DecisionFinal_zName") == "LBW":
#         return f"lbw b {bowler}"
    
#     # Stumped
#     if pd.notna(d.get("scrM_playMIdStumpingName")) and d["scrM_playMIdStumpingName"]:
#         return f"st {d['scrM_playMIdStumpingName']} b {bowler}"
    
#     # Run Out
#     if pd.notna(d.get("scrM_playMIdRunOutName")) and d["scrM_playMIdRunOutName"]:
#         return f"run out ({d['scrM_playMIdRunOutName']})"
    
#     # Default – unknown but wicket
#     return f"b {bowler}" if bowler else "out"

import json, time, tempfile, os
from pathlib import Path
from typing import Any, Dict, Optional

# ---------------- Cache constants & helpers ----------------
SCORECARD_CACHE_DIR = Path(__file__).resolve().parent / "scorecard_cache"
SCORECARD_CACHE_DIR.mkdir(parents=True, exist_ok=True)

def sanitize_filename(name: str) -> str:
    return "".join([c if c.isalnum() or c in ("-", "_") else "_" for c in str(name)])[:200]

def safe_write_json(path: Path, data: Dict[str, Any]) -> None:
    """Atomic JSON write to avoid partial reads."""
    tmp = None
    try:
        tmp = tempfile.NamedTemporaryFile('w', delete=False, dir=str(path.parent), encoding='utf-8')
        tmp.write(json.dumps(data, indent=2, ensure_ascii=False))
        tmp.flush(); tmp.close()
        os.replace(tmp.name, str(path))
    finally:
        if tmp and os.path.exists(tmp.name):
            try:
                os.remove(tmp.name)
            except Exception:
                pass

# Simple lock using O_EXCL on .lock file (works across processes)
def acquire_lock(lock_path: Path, timeout: float = 10.0) -> bool:
    start = time.time()
    while True:
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(fd)
            return True
        except FileExistsError:
            if time.time() - start > timeout:
                return False
            time.sleep(0.08)  # small wait and retry

def release_lock(lock_path: Path) -> None:
    try:
        if lock_path.exists():
            lock_path.unlink()
    except Exception:
        pass

def _innings_cache_path(match_id: str, inn_no: int) -> Path:
    """
    Cache filename should always use Match ID internally.
    Example: 880_inn_1.json
    """
    safe_mid = str(match_id).strip()
    return SCORECARD_CACHE_DIR / f"{safe_mid}_inn_{inn_no}.json"

def get_match_name_by_id(match_id):
    """
    Returns mchM_MatchName for given match_id.
    Used only for display.
    """
    try:
        if not match_id:
            return None

        conn = get_connection()
        df = pd.read_sql("""
            SELECT mchM_Id, mchM_MatchName
            FROM tblmatchmaster
            WHERE mchM_Id = %s
            LIMIT 1
        """, conn, params=(int(match_id),))
        conn.close()

        if df.empty:
            return None

        return str(df.iloc[0]["mchM_MatchName"]).strip()

    except Exception as e:
        print("⚠️ get_match_name_by_id error:", e)
        return None


def _match_cache_path(match_name: str) -> Path:
    return SCORECARD_CACHE_DIR / f"{sanitize_filename(match_name)}.json"

def _file_age_seconds(p: Path) -> Optional[float]:
    try:
        return time.time() - p.stat().st_mtime
    except Exception:
        return None
    
def _to_int(val):
    try:
        return int(val) if val is not None and val != "" else 0
    except Exception:
        return 0

def _to_float(val):
    try:
        return float(val) if val is not None and val != "" else 0.0
    except Exception:
        return 0.0


def _build_innings_json(match_name: str, inn_no: int) -> dict:
    """
    ✅ FINAL UPDATED VERSION (Match-ID Safe + Display Friendly)

    - Supports match_id OR match_name input
    - Resolves match_id & match_display_name properly
    - Always fetches ball-by-ball using match_id ✅
    - Fixes blank scorecard issue ✅
    - Adds display fields inside meta for UI:
        ✅ MatchId
        ✅ MatchName
        ✅ BattingTeamName
        ✅ BowlingTeamName
    """

    import pandas as pd

    print(f"✅ Building innings JSON for {match_name}, innings {inn_no}")

    global BATTER_DATA

    # ✅ clear only at innings 1
    if inn_no == 1:
        try:
            BATTER_DATA.clear()
        except Exception:
            pass

    # --------------------------------------------------------
    # ✅ STEP 1: Resolve match_id & match_display_name
    # --------------------------------------------------------
    input_val = str(match_name).strip()
    match_id = None
    match_display_name = None

    try:
        conn = get_connection()

        # CASE 1: numeric => match_id
        if input_val.isdigit():
            match_id = str(input_val)

            df_match = pd.read_sql(
                """
                SELECT mchM_Id, mchM_MatchName
                FROM tblmatchmaster
                WHERE mchM_Id = %s
                LIMIT 1
                """,
                conn,
                params=(int(match_id),)
            )

            if not df_match.empty:
                match_display_name = str(df_match.iloc[0]["mchM_MatchName"]).strip()

        # CASE 2: match_name => resolve match_id
        else:
            match_display_name = input_val

            df_match = pd.read_sql(
                """
                SELECT mchM_Id, mchM_MatchName
                FROM tblmatchmaster
                WHERE mchM_MatchName = %s
                LIMIT 1
                """,
                conn,
                params=(match_display_name,)
            )

            if not df_match.empty:
                match_id = str(df_match.iloc[0]["mchM_Id"]).strip()
                match_display_name = str(df_match.iloc[0]["mchM_MatchName"]).strip()

        conn.close()

    except Exception as e:
        print("❌ Error resolving match_id/match_name:", e)
        try:
            conn.close()
        except Exception:
            pass

    if not match_id:
        return {
            "inn_no": inn_no,
            "batters": [],
            "bowlers": [],
            "meta": {
                "MatchId": None,
                "MatchName": input_val
            },
            "partnership_chart": None,
            "fall_of_wickets": [],
            "_error": f"match_id not found for match={input_val}"
        }

    # --------------------------------------------------------
    # ✅ Helper: dismissal string builder
    # --------------------------------------------------------
    def build_dismissal(row) -> str:
        mode = str(row.get("scrM_DecisionFinal_zName", "")).strip().lower()
        bowler = str(row.get("scrM_PlayMIdBowlerName", "")).strip()

        if mode == "caught":
            caught = str(row.get("scrM_playMIdCaughtName", "")).strip()
            fielder = caught or str(row.get("scrM_PlayMIdFielderName", "")).strip()

            if fielder and bowler and fielder.lower() == bowler.lower():
                return f"c & b {bowler}"

            return f"c {fielder} b {bowler}"

        if mode == "bowled":
            return f"b {bowler}" if bowler else "bowled"

        if mode == "lbw":
            return f"lbw b {bowler}" if bowler else "lbw"

        if "run out" in mode:
            fielder = str(row.get("scrM_playMIdRunOutName", "")).strip()
            return f"run out ({fielder})" if fielder else "run out"

        if mode == "stumped":
            keeper = str(row.get("scrM_playMIdStumpingName", "")).strip()
            return f"st {keeper} b {bowler}" if keeper else f"st ? b {bowler}"

        if mode == "hit wicket":
            return f"hit wicket b {bowler}" if bowler else "hit wicket"

        return str(row.get("scrM_DecisionFinal_zName", "")) or ""

    # --------------------------------------------------------
    # ✅ STEP 2: Load Ball-by-ball using match_id ✅
    # --------------------------------------------------------
    try:
        # ✅ IMPORTANT FIX:
        # get_ball_by_ball_details expects match_ids list and uses scrM_MchMId internally
        bbb_df = get_ball_by_ball_details([match_id], inning=inn_no)

        batters = []
        bowlers = []

        if bbb_df is None or bbb_df.empty:
            return {
                "inn_no": inn_no,
                "batters": [],
                "bowlers": [],
                "meta": {
                    "MatchId": str(match_id),
                    "MatchName": str(match_display_name or input_val)
                },
                "partnership_chart": None,
                "fall_of_wickets": []
            }

        # safe numeric conversion
        for c in ["scrM_OverNo", "scrM_DelNo", "scrM_BatsmanRuns", "scrM_DelRuns"]:
            if c in bbb_df.columns:
                bbb_df[c] = pd.to_numeric(bbb_df[c], errors="coerce").fillna(0)

        if "scrM_OverNo" in bbb_df.columns and "scrM_DelNo" in bbb_df.columns:
            bbb_df["scrM_OverNo"] = bbb_df["scrM_OverNo"].astype(int)
            bbb_df["scrM_DelNo"] = bbb_df["scrM_DelNo"].astype(int)

        # ensure correct order
        bbb_df = bbb_df.sort_values(["scrM_OverNo", "scrM_DelNo"], ascending=[True, True])

        # cumulative team runs
        bbb_df["TeamRuns"] = bbb_df["scrM_DelRuns"].cumsum()

        # --------------------------------------------------------
        # ✅ Extract Teams for Display
        # --------------------------------------------------------
        batting_team_name = None
        bowling_team_name = None

        try:
            if "scrM_tmMIdBattingName" in bbb_df.columns:
                batting_team_name = (
                    bbb_df["scrM_tmMIdBattingName"].dropna().astype(str).iloc[0].strip()
                    if not bbb_df["scrM_tmMIdBattingName"].dropna().empty else None
                )
            if "scrM_tmMIdBowlingName" in bbb_df.columns:
                bowling_team_name = (
                    bbb_df["scrM_tmMIdBowlingName"].dropna().astype(str).iloc[0].strip()
                    if not bbb_df["scrM_tmMIdBowlingName"].dropna().empty else None
                )
        except Exception:
            batting_team_name = batting_team_name
            bowling_team_name = bowling_team_name

        # --------------------------------------------------------
        # ✅ STEP 3: Batting order = first striker appearance
        # --------------------------------------------------------
        batting_sequence = []
        seen_batters = set()

        for _, row in bbb_df.iterrows():
            striker = str(row.get("scrM_PlayMIdStrikerName", "")).strip()
            if striker and striker not in seen_batters:
                batting_sequence.append(striker)
                seen_batters.add(striker)

        # --------------------------------------------------------
        # ✅ STEP 4: BATTERS TABLE
        # --------------------------------------------------------
        for name in batting_sequence:
            g = bbb_df[bbb_df["scrM_PlayMIdStrikerName"] == name]
            if g.empty:
                continue

            runs = int(pd.to_numeric(g.get("scrM_BatsmanRuns", 0), errors="coerce").fillna(0).sum())

            # balls faced (exclude wides)
            balls = int((pd.to_numeric(g.get("scrM_IsWideBall", 0), errors="coerce").fillna(0) == 0).sum())

            fours = int(pd.to_numeric(g.get("scrM_IsBoundry", 0), errors="coerce").fillna(0).sum()) if "scrM_IsBoundry" in g.columns else int((g["scrM_BatsmanRuns"] == 4).sum())
            sixes = int(pd.to_numeric(g.get("scrM_IsSixer", 0), errors="coerce").fillna(0).sum()) if "scrM_IsSixer" in g.columns else int((g["scrM_BatsmanRuns"] == 6).sum())

            sr = round((runs / max(1, balls)) * 100, 2) if balls else 0.0

            # dismissal
            dismissal = "not out"
            wk_df = bbb_df[
                (pd.to_numeric(bbb_df.get("scrM_IsWicket", 0), errors="coerce").fillna(0).astype(int) == 1) &
                (bbb_df["scrM_PlayMIdStrikerName"] == name)
            ]

            if not wk_df.empty:
                try:
                    dismissal = build_dismissal(wk_df.iloc[0])
                except Exception:
                    dismissal = "out"

            # dots
            dots = int(
                g[
                    (pd.to_numeric(g.get("scrM_BatsmanRuns", 0), errors="coerce").fillna(0) == 0) &
                    (pd.to_numeric(g.get("scrM_WideRuns", 0), errors="coerce").fillna(0) == 0) &
                    (pd.to_numeric(g.get("scrM_NoBallRuns", 0), errors="coerce").fillna(0) == 0) &
                    (pd.to_numeric(g.get("scrM_ByeRuns", 0), errors="coerce").fillna(0) == 0) &
                    (pd.to_numeric(g.get("scrM_LegByeRuns", 0), errors="coerce").fillna(0) == 0) &
                    (pd.to_numeric(g.get("scrM_PenaltyRuns", 0), errors="coerce").fillna(0) == 0)
                ].shape[0]
            )

            ones = int((pd.to_numeric(g.get("scrM_BatsmanRuns", 0), errors="coerce").fillna(0) == 1).sum())
            twos = int((pd.to_numeric(g.get("scrM_BatsmanRuns", 0), errors="coerce").fillna(0) == 2).sum())
            threes = int((pd.to_numeric(g.get("scrM_BatsmanRuns", 0), errors="coerce").fillna(0) == 3).sum())

            bdry_balls = fours + sixes
            bdry_pct = round(bdry_balls / max(1, balls) * 100, 2)
            bdry_freq = round(balls / max(1, bdry_balls), 1) if bdry_balls else 0
            db_pct = round(dots / max(1, balls) * 100, 2)
            db_freq = round(balls / max(1, dots), 1) if dots else 0

            # batter id
            first_row = g.iloc[0]
            batter_id = str(int(pd.to_numeric(first_row.get("scrM_PlayMIdStriker", -1), errors="coerce") or -1))

            # Batter vs Bowler table
            bowler_table = []
            if "scrM_PlayMIdBowlerName" in g.columns:
                for bowler_name, gb in g.groupby("scrM_PlayMIdBowlerName"):
                    b_runs = int(pd.to_numeric(gb.get("scrM_BatsmanRuns", 0), errors="coerce").fillna(0).sum())
                    b_balls = int((pd.to_numeric(gb.get("scrM_IsWideBall", 0), errors="coerce").fillna(0) == 0).sum())

                    b_dots = int(
                        gb[
                            (pd.to_numeric(gb.get("scrM_BatsmanRuns", 0), errors="coerce").fillna(0) == 0) &
                            (pd.to_numeric(gb.get("scrM_WideRuns", 0), errors="coerce").fillna(0) == 0) &
                            (pd.to_numeric(gb.get("scrM_NoBallRuns", 0), errors="coerce").fillna(0) == 0) &
                            (pd.to_numeric(gb.get("scrM_ByeRuns", 0), errors="coerce").fillna(0) == 0) &
                            (pd.to_numeric(gb.get("scrM_LegByeRuns", 0), errors="coerce").fillna(0) == 0) &
                            (pd.to_numeric(gb.get("scrM_PenaltyRuns", 0), errors="coerce").fillna(0) == 0)
                        ].shape[0]
                    )

                    b_ones = int((pd.to_numeric(gb.get("scrM_BatsmanRuns", 0), errors="coerce").fillna(0) == 1).sum())
                    b_twos = int((pd.to_numeric(gb.get("scrM_BatsmanRuns", 0), errors="coerce").fillna(0) == 2).sum())
                    b_fours = int((pd.to_numeric(gb.get("scrM_BatsmanRuns", 0), errors="coerce").fillna(0) == 4).sum())
                    b_sixes = int((pd.to_numeric(gb.get("scrM_BatsmanRuns", 0), errors="coerce").fillna(0) == 6).sum())

                    bowler_table.append({
                        "bowler": str(bowler_name).strip(),
                        "runs": b_runs,
                        "balls": b_balls,
                        "dots": b_dots,
                        "ones": b_ones,
                        "twos": b_twos,
                        "fours": b_fours,
                        "sixes": b_sixes
                    })

            # Wagon wheel images
            ww_images = {}

            batter_hand = "Right"
            if "scrM_BatterHand" in g.columns:
                try:
                    non_na = g["scrM_BatterHand"].dropna()
                    if not non_na.empty:
                        batter_hand = str(non_na.iloc[0]).strip() or "Right"
                except Exception:
                    batter_hand = "Right"

            try:
                ww_images["all"] = generate_wagon_wheel(g, batter_hand)
                for run_type in [1, 2, 3, 4, 6]:
                    ww_images[str(run_type)] = generate_wagon_wheel(g, batter_hand, filter_runs=run_type)
            except Exception:
                ww_images.setdefault("all", "")
                for run_type in [1, 2, 3, 4, 6]:
                    ww_images.setdefault(str(run_type), "")

            batters.append({
                "id": batter_id,
                "name": name,
                "PlayMId": int(pd.to_numeric(first_row.get("scrM_PlayMIdStriker", -1), errors="coerce") or -1),
                "runs": runs,
                "balls": balls,
                "fours": fours,
                "sixes": sixes,
                "sr": sr,
                "dismissal": dismissal or "",
                "dots": dots,
                "ones": ones,
                "twos": twos,
                "threes": threes,
                "bdry_pct": bdry_pct,
                "bdry_freq": bdry_freq,
                "db_pct": db_pct,
                "db_freq": db_freq,
                "bowler_table": bowler_table,
                "wagon_wheel": ww_images,
                "hand": batter_hand
            })

            # register batter df for wagonwheel
            try:
                BATTER_DATA[batter_id] = {"df": g.copy(), "hand": batter_hand}
            except Exception:
                pass

        # --------------------------------------------------------
        # ✅ STEP 5: BOWLERS TABLE
        # --------------------------------------------------------
        try:
            if "scrM_PlayMIdBowlerName" in bbb_df.columns:
                bowler_groups = bbb_df.groupby("scrM_PlayMIdBowlerName")
            else:
                bowler_groups = []

            for bowler_name, gb in bowler_groups:
                bowler_name = str(bowler_name).strip() or "Unknown"

                legal_mask = (
                    (pd.to_numeric(gb.get("scrM_IsWideBall", 0), errors="coerce").fillna(0) == 0) &
                    (pd.to_numeric(gb.get("scrM_IsNoBall", 0), errors="coerce").fillna(0) == 0)
                )
                legal_balls = int(legal_mask.sum())
                overs_str = f"{legal_balls // 6}.{legal_balls % 6}"

                total_del_runs = int(pd.to_numeric(gb.get("scrM_DelRuns", 0), errors="coerce").fillna(0).sum())
                bye_sum = int(pd.to_numeric(gb.get("scrM_ByeRuns", 0), errors="coerce").fillna(0).sum())
                legbye_sum = int(pd.to_numeric(gb.get("scrM_LegByeRuns", 0), errors="coerce").fillna(0).sum())
                penalty_sum = int(pd.to_numeric(gb.get("scrM_PenaltyRuns", 0), errors="coerce").fillna(0).sum())

                runs_conceded = int(total_del_runs - bye_sum - legbye_sum - penalty_sum)

                maidens = 0
                if "scrM_OverNo" in gb.columns:
                    maidens = int((gb.groupby("scrM_OverNo")["scrM_DelRuns"].sum() == 0).sum())

                decisions = gb.get("scrM_DecisionFinal_zName", pd.Series([""] * len(gb), index=gb.index))
                decisions = decisions.astype(str).str.lower()
                is_wicket_series = pd.to_numeric(gb.get("scrM_IsWicket", 0), errors="coerce").fillna(0).astype(int)

                wickets = int(((is_wicket_series == 1) & decisions.isin(
                    ["bowled", "lbw", "caught", "stumped", "hit wicket", "caught and bowled"]
                )).sum())

                wd_sum = int(pd.to_numeric(gb.get("scrM_WideRuns", 0), errors="coerce").fillna(0).sum())
                nb_sum = int(pd.to_numeric(gb.get("scrM_NoBallRuns", 0), errors="coerce").fillna(0).sum())

                dots = int(
                    gb[
                        (pd.to_numeric(gb.get("scrM_BatsmanRuns", 0), errors="coerce").fillna(0) == 0) &
                        (pd.to_numeric(gb.get("scrM_WideRuns", 0), errors="coerce").fillna(0) == 0) &
                        (pd.to_numeric(gb.get("scrM_NoBallRuns", 0), errors="coerce").fillna(0) == 0) &
                        (pd.to_numeric(gb.get("scrM_ByeRuns", 0), errors="coerce").fillna(0) == 0) &
                        (pd.to_numeric(gb.get("scrM_LegByeRuns", 0), errors="coerce").fillna(0) == 0) &
                        (pd.to_numeric(gb.get("scrM_PenaltyRuns", 0), errors="coerce").fillna(0) == 0)
                    ].shape[0]
                )

                fours = int((pd.to_numeric(gb.get("scrM_BatsmanRuns", 0), errors="coerce").fillna(0) == 4).sum())
                sixes = int((pd.to_numeric(gb.get("scrM_BatsmanRuns", 0), errors="coerce").fillna(0) == 6).sum())

                bdry_balls = fours + sixes
                bdry_pct = round(bdry_balls / max(1, legal_balls) * 100, 2)
                bdry_freq = round(legal_balls / max(1, bdry_balls), 1) if bdry_balls else 0
                db_pct = round(dots / max(1, legal_balls) * 100, 2)
                db_freq = round(legal_balls / max(1, dots), 1) if dots else 0

                econ = round(runs_conceded / (legal_balls / 6), 2) if legal_balls else 0.0

                vs_batters = []
                if "scrM_PlayMIdStrikerName" in gb.columns:
                    for batter_name, gb2 in gb.groupby("scrM_PlayMIdStrikerName"):
                        batter_display = str(batter_name).strip() or "Unknown"

                        b_balls = int((pd.to_numeric(gb2.get("scrM_IsWideBall", 0), errors="coerce").fillna(0) == 0).sum())
                        b_runs = int(pd.to_numeric(gb2.get("scrM_BatsmanRuns", 0), errors="coerce").fillna(0).sum())

                        decisions2 = gb2.get("scrM_DecisionFinal_zName", pd.Series([""] * len(gb2), index=gb2.index))
                        decisions2 = decisions2.astype(str).str.lower()

                        b_wickets = int(((pd.to_numeric(gb2.get("scrM_IsWicket", 0), errors="coerce").fillna(0).astype(int) == 1) & decisions2.isin(
                            ["bowled", "lbw", "caught", "stumped", "hit wicket", "caught and bowled"]
                        )).sum())

                        wd_b = int(pd.to_numeric(gb2.get("scrM_WideRuns", 0), errors="coerce").fillna(0).sum())
                        nb_b = int(pd.to_numeric(gb2.get("scrM_NoBallRuns", 0), errors="coerce").fillna(0).sum())
                        econ_b = round(b_runs / (b_balls / 6), 2) if b_balls else 0.0
                        b_fours = int((pd.to_numeric(gb2.get("scrM_BatsmanRuns", 0), errors="coerce").fillna(0) == 4).sum())
                        b_sixes = int((pd.to_numeric(gb2.get("scrM_BatsmanRuns", 0), errors="coerce").fillna(0) == 6).sum())

                        vs_batters.append({
                            "batter": batter_display,
                            "balls": b_balls,
                            "runs": b_runs,
                            "wickets": b_wickets,
                            "wd": wd_b,
                            "nb": nb_b,
                            "econ": econ_b,
                            "fours": b_fours,
                            "sixes": b_sixes
                        })

                bowler_id = None
                if "scrM_PlayMIdBowler" in gb.columns:
                    bid_series = gb["scrM_PlayMIdBowler"].dropna()
                    if not bid_series.empty:
                        try:
                            bowler_id = int(pd.to_numeric(bid_series.iloc[0], errors="coerce"))
                        except Exception:
                            bowler_id = None

                bowlers.append({
                    "id": bowler_id,
                    "name": bowler_name,
                    "overs": overs_str,
                    "maidens": maidens,
                    "runs": runs_conceded,
                    "wickets": wickets,
                    "econ": econ,
                    "dots": dots,
                    "fours": fours,
                    "sixes": sixes,
                    "bdry_pct": bdry_pct,
                    "bdry_freq": bdry_freq,
                    "db_pct": db_pct,
                    "db_freq": db_freq,
                    "wd": wd_sum,
                    "nb": nb_sum,
                    "vs_batters": vs_batters
                })

        except Exception as e:
            print("❌ Error building bowlers:", e)
            bowlers = []

        # --------------------------------------------------------
        # ✅ STEP 6: Match innings meta + partnership chart
        # --------------------------------------------------------
        partnership_html, innings_meta = None, {}

        try:
            inns = get_match_innings(match_display_name or match_id)
            if hasattr(inns, "to_dict"):
                inns = inns.to_dict(orient="records")

            innings_meta = next(
                (i for i in inns if int(i.get("Inn_Inning", -1)) == int(inn_no)),
                {}
            )
        except Exception:
            innings_meta = {}

        try:
            partnership_html = create_partnership_chart(bbb_df, batting_team_name)
        except Exception as e:
            print(f"⚠️ Partnership chart error (innings {inn_no}):", e)
            partnership_html = None

        # --------------------------------------------------------
        # ✅ STEP 7: Fall of Wickets
        # --------------------------------------------------------
        fall_of_wickets = []
        try:
            dismissals = bbb_df[pd.to_numeric(bbb_df.get("scrM_IsWicket", 0), errors="coerce").fillna(0) == 1].copy()

            for idx, (_, row) in enumerate(dismissals.iterrows(), start=1):
                dismissal = build_dismissal(row)
                fall_of_wickets.append({
                    "wkt_no": idx,
                    "batter": str(row.get("scrM_PlayMIdStrikerName", "")),
                    "runs": int(row.get("TeamRuns", 0) or 0),
                    "over_str": f"{max(0, int(row.get('scrM_OverNo', 0)) - 1)}.{int(row.get('scrM_DelNo', 0))}",
                    "dismissal": dismissal
                })
        except Exception:
            fall_of_wickets = []

        # --------------------------------------------------------
        # ✅ STEP 8: Summary
        # --------------------------------------------------------
        summary = {}
        try:
            total_runs = int(pd.to_numeric(bbb_df.get("TeamRuns", 0), errors="coerce").fillna(0).max())
            overs = int(pd.to_numeric(bbb_df.get("scrM_OverNo", 0), errors="coerce").fillna(0).max())

            extras = int(
                pd.to_numeric(bbb_df.get("scrM_NoBallRuns", 0), errors="coerce").fillna(0).sum() +
                pd.to_numeric(bbb_df.get("scrM_WideRuns", 0), errors="coerce").fillna(0).sum() +
                pd.to_numeric(bbb_df.get("scrM_ByeRuns", 0), errors="coerce").fillna(0).sum() +
                pd.to_numeric(bbb_df.get("scrM_LegByeRuns", 0), errors="coerce").fillna(0).sum() +
                pd.to_numeric(bbb_df.get("scrM_PenaltyRuns", 0), errors="coerce").fillna(0).sum()
            )

            crr = round(total_runs / max(1, overs), 2) if overs else 0.0

            fows = [0] + [w["runs"] for w in fall_of_wickets] + [total_runs]
            highest_pship = max(fows[i + 1] - fows[i] for i in range(len(fows) - 1)) if len(fows) > 1 else total_runs

            summary = {
                "TotalRuns": total_runs,
                "TotalOvers": overs,
                "Extras": extras,
                "CRR": float(crr),
                "HighestPartnership": highest_pship
            }
        except Exception:
            summary = {}

        final_meta = {}
        final_meta.update(innings_meta or {})
        final_meta.update(summary or {})
        final_meta["MatchId"] = str(match_id)
        final_meta["MatchName"] = str(match_display_name or input_val)
        final_meta["BattingTeamName"] = batting_team_name
        final_meta["BowlingTeamName"] = bowling_team_name

        return {
            "inn_no": inn_no,
            "batters": batters,
            "bowlers": bowlers,
            "meta": final_meta,
            "partnership_chart": partnership_html,
            "fall_of_wickets": fall_of_wickets
        }

    except Exception as ex:
        print("❌ Error building innings JSON:", ex)
        return {
            "inn_no": inn_no,
            "batters": [],
            "bowlers": [],
            "meta": {
                "MatchId": str(match_id),
                "MatchName": str(match_display_name or input_val)
            },
            "partnership_chart": None,
            "fall_of_wickets": [],
            "_error": str(ex)
        }







def _build_match_json_from_innings(match_name: str, innings_jsons: list, header_json: dict, last12: list) -> Dict:
    return {
        "match_header": header_json,
        "innings": innings_jsons,
        "last_12_balls": last12 or [],
        "_meta": {
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime()),
            "match_name": match_name
        }
    }



def _compute_match_status(innings_jsons: list[dict]) -> str:
    """
    Compute live match status/result based on innings JSON data.
    """
    try:
        if not innings_jsons:
            return "No innings data available"

        inn1 = next((i for i in innings_jsons if i["inn_no"] == 1), None)
        inn2 = next((i for i in innings_jsons if i["inn_no"] == 2), None)

        if not inn1:
            return "No data yet"

        team1 = inn1["meta"].get("TeamShortName", "Team1")
        runs1 = inn1["meta"].get("TotalRuns", 0)
        overs1 = inn1["meta"].get("TotalOvers", 0)

        # If only first innings exists
        if not inn2 or not inn2.get("meta"):
            crr = inn1["meta"].get("CRR", 0.0)
            projected = round(crr * 20, 0)  # assume 20 overs
            return f"{team1} projected score: {projected}"

        team2 = inn2["meta"].get("TeamShortName", "Team2")
        runs2 = inn2["meta"].get("TotalRuns", 0)
        overs2 = inn2["meta"].get("TotalOvers", 0)

        target = runs1 + 1

        # If 2nd innings has just started
        if overs2 == 0 and runs2 == 0:
            return f"{team1} scored {runs1}. {team2} yet to bat."

        # If team 2 already ahead
        if runs2 >= target:
            return f"{team2} won by chasing successfully"

        # If 2nd innings finished (all overs or all wickets)
        if overs2 >= 20 or inn2["meta"].get("WicketsLost", 0) >= 10:
            if runs2 < runs1:
                return f"{team1} won by {runs1 - runs2} runs"
            elif runs2 == runs1:
                return "Match tied"
            else:
                return f"{team2} won by chasing successfully"

        # Otherwise still chasing
        need = target - runs2
        return f"{team2} need {need} runs to win"

    except Exception as e:
        return f"Status unavailable ({e})"


from typing import Dict

def generate_scorecard_json(match_name: str, live: bool = True, force: bool = False) -> Dict:
    """
    ✅ UPDATED VERSION (FINAL):
    - Accepts match_id OR match_name as input
    - Resolves match_id + match_display_name from DB
    - Uses innings cache keyed ONLY by match_id ✅
    - Builds innings using match_id ✅ (tblscoremaster uses scrM_MchMId)
    - Ensures match_header always contains:
        ✅ MatchId
        ✅ MatchName  (for display)
    - ✅ LAST 12 BALLS fetched inning-wise from tblscoremaster ✅
    """

    import time
    import json
    import pandas as pd

    # -------------------------------------------------
    # ✅ STEP 0: Resolve match_id and match_display_name
    # -------------------------------------------------
    original_input = str(match_name).strip()
    match_id = None
    match_display_name = None

    try:
        conn = get_connection()

        # Only allow numeric match_id
        if original_input.isdigit():
            match_id = str(original_input).strip()
            df_m = pd.read_sql(
                """
                SELECT mchM_Id, mchM_MatchName
                FROM tblmatchmaster
                WHERE mchM_Id = %s
                LIMIT 1
                """,
                conn,
                params=(int(match_id),)
            )
            if not df_m.empty:
                match_display_name = str(df_m.iloc[0]["mchM_MatchName"]).strip()
        else:
            # If not numeric, fail
            match_id = None
        conn.close()
    except Exception as e:
        print("❌ Error resolving match_id:", e)
        try:
            conn.close()
        except Exception:
            pass

    # ✅ Hard fail safe
    if not match_id:
        return {
            "match_header": {
                "MatchId": None,
                "MatchName": original_input
            },
            "innings": [],
            "last_12_balls": {},
            "MatchStatus": "Match Not Found",
            "_meta": {
                "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime()),
                "match_name": original_input,
                "match_id": None,
                "match_finished": False,
                "_error": "match_id could not be resolved"
            },
        }

    # -------------------------------------------------
    # ✅ STEP 1: MATCH HEADER
    # -------------------------------------------------
    header_json = {}
    try:
        header_raw = get_match_header(match_id)
        if hasattr(header_raw, "to_dict"):
            header_json = header_raw.to_dict()
        elif isinstance(header_raw, dict):
            header_json = header_raw
        else:
            header_json = {
                k: getattr(header_raw, k)
                for k in dir(header_raw)
                if not k.startswith("_") and not callable(getattr(header_raw, k))
            }
    except Exception as e:
        print("⚠️ get_match_header failed:", e)
        header_json = {}

    # ✅ Always attach these
    header_json["MatchId"] = str(match_id)
    header_json["MatchName"] = str(match_display_name or original_input)

    result_text = header_json.get("ResultText") or header_json.get("Result")
    match_finished = bool(result_text)

    # -------------------------------------------------
    # ✅ STEP 2: INNINGS META
    # -------------------------------------------------
    innings_meta = []
    try:
        innings_raw = get_match_innings(match_id)
        if hasattr(innings_raw, "to_dict"):
            innings_meta = innings_raw.to_dict(orient="records")
        elif isinstance(innings_raw, (list, tuple)):
            innings_meta = [dict(x) if not isinstance(x, dict) else x for x in innings_raw]
        else:
            innings_meta = [dict(innings_raw)]
    except Exception as e:
        print("⚠️ get_match_innings failed:", e)
        innings_meta = []

    innings_jsons = []

    # -------------------------------------------------
    # ✅ STEP 3: PER INNINGS BUILD (CACHE BY match_id ✅)
    # -------------------------------------------------
    for inn_meta in innings_meta:
        inn_no = int(inn_meta.get("Inn_Inning", inn_meta.get("inn_no", -1)))
        if inn_no < 0:
            continue

        inn_cache = _innings_cache_path(match_id, inn_no)
        lock_path = inn_cache.with_suffix(".lock")

        # ---- detect innings completion ----
        inn_finished = False
        if inn_meta.get("Inn_Status") and str(inn_meta.get("Inn_Status")).lower() in (
            "complete", "completed", "finished", "closed"):
            inn_finished = True
        try:
            inns = get_match_innings(match_id)
            if hasattr(inns, "to_dict"):
                inns = inns.to_dict(orient="records")
            innings_meta = next(
                (i for i in inns if int(i.get("Inn_Inning", -1)) == int(inn_no)),
                {}
            )
        except Exception:
            innings_meta = {}

        # ---- rebuild decision ----
        need_rebuild = force or not inn_cache.exists()

        if inn_cache.exists() and not inn_finished:
            age = _file_age_seconds(inn_cache) or 999999
            if live and age > 60:
                need_rebuild = True

        # ---- build & cache ----
        if need_rebuild:
            got_lock = acquire_lock(lock_path, timeout=8.0)
            if got_lock:
                try:
                    inn_json = _build_innings_json(match_id, inn_no)

                    if inn_json.get("_error"):
                        print(f"⚠️ Skipping cache for innings {inn_no}: {inn_json['_error']}")
                    else:
                        inn_json["match_id"] = str(match_id)
                        inn_json["match_name"] = str(match_display_name or original_input)
                        safe_write_json(inn_cache, inn_json)

                finally:
                    release_lock(lock_path)

        # ---- load innings json ----
        try:
            with open(inn_cache, "r", encoding="utf-8") as f:
                inn_json = json.load(f)
        except Exception:
            inn_json = _build_innings_json(match_id, inn_no)

        if inn_json.get("_error"):
            print(f"⚠️ Skipping innings {inn_no} in response: {inn_json['_error']}")
            continue

        innings_jsons.append(inn_json)

    # -------------------------------------------------
    # ✅ STEP 4: LAST 12 BALLS (INNING-WISE ✅)
    # -------------------------------------------------
    last12_by_inning = {}

    try:
        conn = get_connection()

        # If innings_meta is empty, still try to detect innings from DB
        inn_list = []
        for inn_meta in innings_meta:
            inn_no = int(inn_meta.get("Inn_Inning", inn_meta.get("inn_no", -1)))
            if inn_no > 0:
                inn_list.append(inn_no)

        if not inn_list:
            df_inns = pd.read_sql(
                """
                SELECT DISTINCT scrM_InningNo
                FROM tblscoremaster
                WHERE scrM_MchMId = %s
                ORDER BY scrM_InningNo
                """,
                conn,
                params=(int(match_id),)
            )
            inn_list = [int(x) for x in df_inns["scrM_InningNo"].tolist() if x]

        for inn_no in inn_list:

            q_last12 = """
                SELECT
                    scrM_DelRuns,
                    scrM_IsWicket
                FROM tblscoremaster
                WHERE scrM_MchMId = %s
                  AND scrM_InningNo = %s
                  AND scrM_IsValidBall = 1
                ORDER BY scrM_OverNo DESC, scrM_DelNo DESC
                LIMIT 12
            """

            df_last = pd.read_sql(q_last12, conn, params=(int(match_id), int(inn_no)))

            balls = []
            if not df_last.empty:
                for _, r in df_last.iterrows():
                    if int(r.get("scrM_IsWicket") or 0) == 1:
                        balls.append("W")
                    else:
                        try:
                            balls.append(str(int(r.get("scrM_DelRuns") or 0)))
                        except Exception:
                            balls.append("0")

                balls = list(reversed(balls))

            last12_by_inning[str(inn_no)] = balls

        conn.close()

        print("✅ Last 12 balls inning-wise loaded:", last12_by_inning)

    except Exception as e:
        print("⚠️ LAST 12 BALLS inning-wise query failed:", e)
        try:
            conn.close()
        except Exception:
            pass
        last12_by_inning = {}

    # -------------------------------------------------
    # ✅ STEP 5: MATCH STATUS
    # -------------------------------------------------
    match_status = result_text if match_finished else _compute_match_status(innings_jsons)

    # -------------------------------------------------
    # ✅ FINAL RESPONSE
    # -------------------------------------------------
    return {
        "match_header": header_json,
        "innings": innings_jsons,
        "last_12_balls": last12_by_inning,  # ✅ dictionary by inning
        "MatchStatus": match_status,
        "_meta": {
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime()),
            "match_name": str(match_display_name or original_input),
            "match_id": str(match_id),
            "match_finished": match_finished,
        },
    }








import pandas as pd
import math

import pandas as pd
from collections import defaultdict

def generate_heatmap_matrix(df, selected_metric=None, is_single_match=False, selected_type="batter"):
    """
    ✅ Python Line & Length Heatmap Generator
    - Builds heatmap_data (zone wise)
    - Computes per-ball zone_key (IMPORTANT for click-to-video mapping)
    - Keeps empty zones blank (display_text=None, metric_value=None)

    Returns:
        {
          "heatmap_data": {...},
          "totals": {...},
          "df_with_zone": df_ll   ✅ contains zone_key for each ball
        }
    """

    import pandas as pd

    if df is None or df.empty or 'scrM_PitchX' not in df.columns or 'scrM_PitchY' not in df.columns:
        return {
            "heatmap_data": {},
            "totals": {"balls": 0, "runs": 0, "boundaries": 0, "wickets": 0},
            "df_with_zone": pd.DataFrame()
        }

    df_ll = df.copy()

    # ✅ Convert to numeric + drop NaNs
    df_ll['scrM_PitchX'] = pd.to_numeric(df_ll['scrM_PitchX'], errors='coerce')
    df_ll['scrM_PitchY'] = pd.to_numeric(df_ll['scrM_PitchY'], errors='coerce')
    df_ll = df_ll.dropna(subset=['scrM_PitchX', 'scrM_PitchY'])

    if df_ll.empty:
        return {
            "heatmap_data": {},
            "totals": {"balls": 0, "runs": 0, "boundaries": 0, "wickets": 0},
            "df_with_zone": pd.DataFrame()
        }

    # ✅ Define bins (MUST MATCH frontend zone display)
    line_bins = [-float('inf'), 50, 70, 80, 84, 88, 95, float('inf')]
    line_labels = [
        'Way Outside Off', 'Outside Off', 'Just Outside Off',
        'Off Stump', 'Middle Stump', 'Leg Stump', 'Outside Leg'
    ]

    length_bins = [-float('inf'), 93, 107.5, 128, 150.5, 177, 205, float('inf')]
    length_labels = [
        'Fulltoss', 'Yorker', 'Full Length',
        'Overpitch', 'Good Length', 'Short of Good', 'Short Pitch'
    ]

    # ✅ Create line/length zones
    df_ll['LineZone'] = pd.cut(df_ll['scrM_PitchX'], bins=line_bins, labels=line_labels, right=False)
    df_ll['LengthZone'] = pd.cut(df_ll['scrM_PitchY'], bins=length_bins, labels=length_labels, right=False)

    # ✅ Create combined zone_key (THIS IS YOUR PYTHON HEATMAP CELL KEY)
    df_ll['zone_key'] = df_ll['LengthZone'].astype(str) + '-' + df_ll['LineZone'].astype(str)

    # ✅ Keep old name also (for safety)
    df_ll['Zone'] = df_ll['zone_key']

    # ✅ Boundary & wicket flags
    df_ll['fours'] = (df_ll.get('scrM_IsBoundry', 0) == 1).astype(int)
    df_ll['sixes'] = (df_ll.get('scrM_IsSixer', 0) == 1).astype(int)
    df_ll['boundaries'] = df_ll['fours'] + df_ll['sixes']

    # ✅ wicket logic
    if 'scrM_IsWicket' in df_ll.columns:
        df_ll['is_wicket'] = df_ll['scrM_IsWicket'].fillna(0).astype(int)
    elif 'scrM_IsWicketBall' in df_ll.columns:
        df_ll['is_wicket'] = df_ll['scrM_IsWicketBall'].fillna(0).astype(int)
    elif 'scrM_WicketType' in df_ll.columns:
        df_ll['is_wicket'] = df_ll['scrM_WicketType'].apply(
            lambda x: 0 if pd.isna(x) or str(x).strip() in ("", "0", "None", "nan") else 1
        ).astype(int)
    else:
        df_ll['is_wicket'] = 0

    # ✅ totals
    total_balls = len(df_ll)
    total_runs = int(df_ll['scrM_BatsmanRuns'].sum()) if 'scrM_BatsmanRuns' in df_ll.columns else 0
    total_boundaries = int(df_ll['boundaries'].sum())
    total_wickets = int(df_ll['is_wicket'].sum())

    # ✅ Zone aggregation
    zone_summary = df_ll.groupby('zone_key').agg(
        balls=('zone_key', 'count'),
        runs=('scrM_BatsmanRuns', 'sum'),
        boundaries=('boundaries', 'sum'),
        wickets=('is_wicket', 'sum')
    ).reset_index()

    heatmap_data = {}

    # ✅ Create skeleton with predefined zones (blank boxes)
    all_zones = [f"{length}-{line}" for length in length_labels for line in line_labels]
    for zone in all_zones:
        heatmap_data[zone] = {
            'balls': 0,
            'runs': 0,
            'boundaries': 0,
            'wickets': 0,
            'display_text': None,   # ✅ blank cell
            'metric_value': None
        }

    # ✅ Fill zones where balls exist
    for _, row in zone_summary.iterrows():
        zone = row['zone_key']
        balls = int(row['balls'])
        runs = int(row['runs'])
        wickets = int(row['wickets'])
        boundaries = int(row['boundaries'])

        # ✅ dots count
        dots = df_ll[(df_ll['zone_key'] == zone) & (df_ll['scrM_BatsmanRuns'] == 0)].shape[0]

        if balls == 0:
            continue

        metric_value = None
        display_text = f"{balls} b<br>{runs} r<br>{boundaries} B, {wickets} W"

        # ✅ Default (no metric) should color by runs
        if not selected_metric:
            metric_value = runs
        else:
            try:
                if selected_metric == "Economy":
                    metric_value = round((runs / (balls / 6)), 2) if balls > 0 else None
                    display_text = f"Eco<br>{metric_value}" if metric_value is not None else None

                elif selected_metric == "SR Bowler":
                    if wickets > 0:
                        metric_value = round((balls / wickets), 2)
                        display_text = f"SR<br>{metric_value}"
                    else:
                        metric_value = None
                        display_text = None

                elif selected_metric == "Strike Rate":
                    if selected_type == "bowler":
                        # ✅ Bowling strike rate = balls/wickets
                        if wickets > 0:
                            metric_value = round((balls / wickets), 2)
                            display_text = f"BSR<br>{metric_value}"
                        else:
                            metric_value = None
                            display_text = None
                    else:
                        # ✅ Batting SR = runs/balls * 100
                        metric_value = round((runs / balls) * 100, 2) if balls > 0 else None
                        display_text = f"SR<br>{metric_value}" if metric_value is not None else None

                elif selected_metric == "Dot Ball %":
                    if is_single_match:
                        metric_value = int(dots)
                        display_text = f"Dots<br>{metric_value}"
                    else:
                        metric_value = round((dots / balls) * 100, 2) if balls > 0 else None
                        display_text = f"Dot%<br>{metric_value}" if metric_value is not None else None

                elif selected_metric == "Boundary %":
                    if is_single_match:
                        metric_value = int(boundaries)
                        display_text = f"Bound<br>{metric_value}"
                    else:
                        metric_value = round((boundaries / balls) * 100, 2) if balls > 0 else None
                        display_text = f"Bound%<br>{metric_value}" if metric_value is not None else None

                elif selected_metric == "Average":
                    metric_value = round((runs / wickets), 2) if wickets > 0 else None
                    display_text = f"Avg<br>{metric_value}" if metric_value is not None else None

                elif selected_metric == "Balls Per Dismissal":
                    metric_value = round((balls / wickets), 2) if wickets > 0 else None
                    display_text = f"BPD<br>{metric_value}" if metric_value is not None else None

                elif selected_metric == "Ball %":
                    if is_single_match:
                        metric_value = int(balls)
                        display_text = f"Balls<br>{metric_value}"
                    else:
                        metric_value = round((balls / total_balls) * 100, 2) if total_balls > 0 else None
                        display_text = f"Ball%<br>{metric_value}" if metric_value is not None else None

                elif selected_metric == "Wicket":
                    metric_value = int(wickets)
                    display_text = f"<b>{wickets}</b><br>Wkt" if wickets > 0 else None

            except Exception as e:
                print(f"⚠️ Metric calc failed for zone={zone}: {e}")
                metric_value = None
                display_text = None

        heatmap_data[zone] = {
            'balls': balls,
            'runs': runs,
            'boundaries': boundaries,
            'wickets': wickets,
            'display_text': display_text,
            'metric_value': metric_value
        }

    totals = {
        'balls': total_balls,
        'runs': total_runs,
        'boundaries': total_boundaries,
        'wickets': total_wickets
    }

    return {
        "heatmap_data": heatmap_data,
        "totals": totals,
        "df_with_zone": df_ll   # ✅ CRITICAL FOR CLICK-TO-VIDEO
    }



def get_team_inning_distribution(tournament_id, team_name, matches):
    """
    Returns:
    - count_batting_first
    - count_batting_second

    Correct logic:
    • If team batted in Inning 1 → Batting 1st
    • If team batted in Inning 2 → Batting 2nd
    """

    import pandas as pd

    if not tournament_id or not team_name or not matches:
        return 0, 0

    try:
        conn = get_connection()

        placeholders = ",".join("%s" for _ in matches)

        query = f"""
            SELECT DISTINCT
                scrM_MatchName,
                scrM_InningNo
            FROM tblscoremaster
            WHERE scrM_TrnMId = %s
              AND scrM_MatchName IN ({placeholders})
              AND scrM_tmMIdBattingName = %s
        """

        params = [int(tournament_id), *matches, team_name]

        df = pd.read_sql(query, conn, params=params)
        conn.close()

        if df.empty:
            return 0, 0

        # Count unique matches where team batted in each inning
        batting_first = (
            df[df["scrM_InningNo"] == 1]["scrM_MatchName"]
            .nunique()
        )

        batting_second = (
            df[df["scrM_InningNo"] == 2]["scrM_MatchName"]
            .nunique()
        )

        return int(batting_first), int(batting_second)

    except Exception as e:
        print("❌ get_team_inning_distribution error:", e)
        return 0, 0


def get_powerplay_stats(trn_id, team, matches):
    if not trn_id or not team or not matches:
        print(f"[DEBUG] get_powerplay_stats: Missing parameter(s): trn_id={trn_id}, team={team}, matches={matches}")
        return None

    try:
        conn = get_connection()
        placeholders = ",".join("%s" for _ in matches)

        # Fetch Powerplay (1–6), Middle Overs (7–15), Slog Overs (16–20)
        query = f"""
            SELECT 
                scrM_MatchName,
                scrM_InningNo,
                scrM_BatsmanRuns,
                scrM_IsWicket,
                scrM_IsBoundry,
                scrM_IsSixer,
                scrM_OverNo,
                CASE WHEN scrM_BatsmanRuns = 0 THEN 1 ELSE 0 END AS DotBall
            FROM tblscoremaster
            WHERE scrM_TrnMId = %s
              AND scrM_MatchName IN ({placeholders})
              AND scrM_tmMIdBattingName = %s
              AND scrM_IsValidBall = 1
        """

        params = [trn_id] + list(matches) + [team]
        print(f"[DEBUG] get_powerplay_stats: trn_id={trn_id}, team={team}, matches={matches}")
        print(f"[DEBUG] get_powerplay_stats: SQL query: {query}")
        print(f"[DEBUG] get_powerplay_stats: params={params}")
        df = pd.read_sql(query, conn, params=tuple(params))
        print(f"[DEBUG] get_powerplay_stats: DataFrame shape: {df.shape}")
        if not df.empty:
            print(f"[DEBUG] get_powerplay_stats: DataFrame head:\n{df.head()}")
        else:
            print(f"[DEBUG] get_powerplay_stats: DataFrame is EMPTY!")
        conn.close()

        result = {}

        def calc(df_subset, tag):
            if df_subset.empty:
                return {tag: {"avg_runs": 0, "sr": 0, "dots": 0, "wkts": 0, "fours": 0, "sixes": 0}}

            total_runs = df_subset.scrM_BatsmanRuns.sum()
            total_balls = len(df_subset)
            match_count = df_subset.scrM_MatchName.nunique() if "scrM_MatchName" in df_subset.columns else 1
            return {tag: {
                "avg_runs": round(total_runs / match_count, 2) if match_count > 0 else 0,
                "sr": round((total_runs / total_balls) * 100, 2) if total_balls > 0 else 0,
                "dots": round(df_subset.DotBall.sum() / match_count, 2) if match_count > 0 else 0,
                "wkts": round(df_subset.scrM_IsWicket.sum() / match_count, 2) if match_count > 0 else 0,
                "fours": round(df_subset.scrM_IsBoundry.sum() / match_count, 2) if match_count > 0 else 0,
                "sixes": round(df_subset.scrM_IsSixer.sum() / match_count, 2) if match_count > 0 else 0
            }}

        for inning in [1, 2]:
            pp = df[(df.scrM_OverNo <= 6) & (df.scrM_InningNo == inning)]
            mo = df[(df.scrM_OverNo >= 7) & (df.scrM_OverNo <= 15) & (df.scrM_InningNo == inning)]
            so = df[(df.scrM_OverNo >= 16) & (df.scrM_OverNo <= 20) & (df.scrM_InningNo == inning)]

            result.update(calc(pp, f"pp{inning}"))
            result.update(calc(mo, f"mo{inning}"))
            result.update(calc(so, f"so{inning}"))  # 🔥 NEW — slog overs stored

        return result

    except Exception as e:
        print("⚠️ POWERPLAY ERROR:", e)
        return None
    
def get_phase_stats_bowling(trn_id, team, matches):
    if not trn_id or not team or not matches:
        return None
    
    try:
        conn = get_connection()
        placeholders = ",".join("%s" for _ in matches)

        query = f"""
            SELECT 
                scrM_MatchName,
                scrM_InningNo,
                scrM_BatsmanRuns,     -- runs conceded
                scrM_IsWicket,        -- wicket by bowler
                scrM_IsBoundry,       -- 4 conceded
                scrM_IsSixer,         -- 6 conceded
                scrM_OverNo,
                CASE WHEN scrM_BatsmanRuns = 0 THEN 1 ELSE 0 END AS DotBall
            FROM tblscoremaster
            WHERE scrM_TrnMId = %s
              AND scrM_MatchName IN ({placeholders})
              AND scrM_tmMIdBowlingName = %s    -- 🔥 bowling filter
              AND scrM_IsValidBall = 1
        """

        params = [trn_id] + list(matches) + [team]
        df = pd.read_sql(query, conn, params=tuple(params))
        conn.close()

        result = {}

        def calc(df_subset, tag):
            if df_subset.empty:
                return {tag: {"econ": 0, "avg_runs": 0, "dots": 0, "wkts": 0, "fours": 0, "sixes": 0}}

            runs = df_subset.scrM_BatsmanRuns.sum()
            balls = len(df_subset)
            match_count = df_subset.scrM_MatchName.nunique() if "scrM_MatchName" in df_subset.columns else 1
            return {
                tag: {
                    "econ": round((runs / balls) * 6, 2),
                    "avg_runs": round(runs / match_count, 2) if match_count > 0 else 0,
                    "dots": round(df_subset.DotBall.sum() / match_count, 2) if match_count > 0 else 0,
                    "wkts": round(df_subset.scrM_IsWicket.sum() / match_count, 2) if match_count > 0 else 0,
                    "fours": round(df_subset.scrM_IsBoundry.sum() / match_count, 2) if match_count > 0 else 0,
                    "sixes": round(df_subset.scrM_IsSixer.sum() / match_count, 2) if match_count > 0 else 0
                }
            }


        for inning in [1, 2]:
            pp = df[(df.scrM_OverNo <= 6) & (df.scrM_InningNo == inning)]
            mo = df[(df.scrM_OverNo >= 7) & (df.scrM_OverNo <= 15) & (df.scrM_InningNo == inning)]
            so = df[(df.scrM_OverNo >= 16) & (df.scrM_OverNo <= 20) & (df.scrM_InningNo == inning)]

            result.update(calc(pp, f"pp{inning}"))
            result.update(calc(mo, f"mo{inning}"))
            result.update(calc(so, f"so{inning}"))

        return result

    except Exception as e:
        print("⚠️ BOWLING PHASE ERROR:", e)
        return None
    
def get_team_bowling_inning_distribution(trn_id, team, matches):
    conn = get_connection()
    placeholders = ",".join("%s" for _ in matches)

    query = f"""
        SELECT scrM_MatchName, MIN(scrM_InningNo) AS first_inning
        FROM tblscoremaster
                WHERE scrM_TrnMId = %s
                    AND scrM_tmMIdBowlingName = %s
          AND scrM_MatchName IN ({placeholders})
        GROUP BY scrM_MatchName
    """

    params = [trn_id, team] + list(matches)
    df = pd.read_sql(query, conn, params=params)
    conn.close()

    bowling_first = (df.first_inning == 1).sum()
    bowling_second = (df.first_inning == 2).sum()

    return bowling_first, bowling_second

def get_order_wise_batting_full(df):
    """
    Batting order buckets based on batting position:
      Top   : bat_pos 1-3
      Middle: bat_pos 4-7
      Lower : bat_pos 8-10

    Returns:
      {
        "1": {"Top": {...}, "Middle": {...}, "Lower": {...}},   # Inning 1
        "2": {"Top": {...}, "Middle": {...}, "Lower": {...}},   # Inning 2
      }
    """
    import numpy as np
    import pandas as pd

    result = {
        "1": {"Top": {}, "Middle": {}, "Lower": {}},   # Inning 1
        "2": {"Top": {}, "Middle": {}, "Lower": {}},   # Inning 2
    }

    if df is None or df.empty:
        return result

    # ------- assign batting position per Match + Inning -------
    # sort balls
    df = df.sort_values(
        ["scrM_MatchName", "scrM_InningNo", "scrM_OverNo", "scrM_DelNo"],
        ignore_index=True
    )

    def _assign_bat_pos(g):
        order_map = {}
        next_pos = 1
        positions = []
        for name in g["scrM_PlayMIdStrikerName"]:
            if name not in order_map:
                order_map[name] = next_pos
                next_pos += 1
            positions.append(order_map[name])
        g["bat_pos"] = positions
        return g

    df = df.groupby(["scrM_MatchName", "scrM_InningNo"], group_keys=False).apply(_assign_bat_pos)

    # ------- calculate stats per innings + order bucket -------
    order_ranges = {
        "Top": (1, 3),
        "Middle": (4, 7),
        "Lower": (8, 10),
    }

    for inn in [1, 2]:
        inn_df = df[df["scrM_InningNo"] == inn]

        for order, (lo, hi) in order_ranges.items():
            d = inn_df[(inn_df["bat_pos"] >= lo) & (inn_df["bat_pos"] <= hi)]

            total_balls = len(d)
            total_runs = d["scrM_BatsmanRuns"].sum()

            if total_balls == 0:
                # fill zeros
                result[str(inn)][order] = {
                    "Avg": 0.0,
                    "SR": 0.0,
                    "Dots": 0,
                    "Wkts": 0,
                    "4s": 0,
                    "6s": 0,
                    "30+": 0,
                    "50+": 0,
                    "100+": 0,
                }
                continue

            # average runs per batter in that bucket
            avg_runs = round(
                total_runs / (len(d["scrM_PlayMIdStrikerName"].unique()) + 0.0001),
                2,
            )
            # strike rate
            sr = round((total_runs / (total_balls + 0.0001)) * 100, 2)

            res = {
                "Avg": avg_runs,
                "SR": sr,
                "Dots": int((d["scrM_DelRuns"] == 0).sum()),
                "Wkts": int((d["scrM_IsWicket"] == 1).sum()),
                "4s": int((d["scrM_IsBoundry"] == 1).sum()),
                "6s": int((d["scrM_IsSixer"] == 1).sum()),
            }

            # 30+, 50+, 100+ by batter
            player_runs = d.groupby("scrM_PlayMIdStrikerName")["scrM_BatsmanRuns"].sum()
            res["30+"] = int(((player_runs >= 30) & (player_runs < 50)).sum())
            res["50+"] = int(((player_runs >= 50) & (player_runs < 100)).sum())
            res["100+"] = int((player_runs >= 100).sum())

            result[str(inn)][order] = res

    return result


def get_order_wise_bowling_full(df):
    """
    Bowling vs Top/Middle/Lower order batters.

    Batting order of the STRIKER (bat_pos) is used as:
      Top   : 1-3
      Middle: 4-7
      Lower : 8-10

    Returns:
      {
        "1": {"Top": {...}, "Middle": {...}, "Lower": {...}},   # Inning 1
        "2": {"Top": {...}, "Middle": {...}, "Lower": {...}},   # Inning 2
      }
    """
    import numpy as np
    import pandas as pd

    result = {
        "1": {"Top": {}, "Middle": {}, "Lower": {}},
        "2": {"Top": {}, "Middle": {}, "Lower": {}},
    }

    if df is None or df.empty:
        return result

    # ------- assign batting position based on striker per Match + Inning -------
    df = df.sort_values(
        ["scrM_MatchName", "scrM_InningNo", "scrM_OverNo", "scrM_DelNo"],
        ignore_index=True
    )

    def _assign_bat_pos(g):
        order_map = {}
        next_pos = 1
        positions = []
        for name in g["scrM_PlayMIdStrikerName"]:
            if name not in order_map:
                order_map[name] = next_pos
                next_pos += 1
            positions.append(order_map[name])
        g["bat_pos"] = positions
        return g

    df = df.groupby(["scrM_MatchName", "scrM_InningNo"], group_keys=False).apply(_assign_bat_pos)

    order_ranges = {
        "Top": (1, 3),
        "Middle": (4, 7),
        "Lower": (8, 10),
    }

    for inn in [1, 2]:
        inn_df = df[df["scrM_InningNo"] == inn]

        for order, (lo, hi) in order_ranges.items():
            d = inn_df[(inn_df["bat_pos"] >= lo) & (inn_df["bat_pos"] <= hi)]

            if d.empty:
                result[str(inn)][order] = {
                    "AvgConceded": 0.0,
                    "Econ": 0.0,
                    "Dots": 0,
                    "Wkts": 0,
                    "4s": 0,
                    "6s": 0,
                    "2W+": 0,
                    "3W+": 0,
                    "5W+": 0,
                }
                continue

            total_runs = d["scrM_DelRuns"].sum()
            overs = len(d) / 6.0

            econ = round(total_runs / (overs + 0.0001), 2)
            avg = round(total_runs / (int((d["scrM_IsWicket"] == 1).sum()) + 0.0001), 2)

            res = {
                "AvgConceded": avg,
                "Econ": econ,
                "Dots": int((d["scrM_DelRuns"] == 0).sum()),
                "Wkts": int((d["scrM_IsWicket"] == 1).sum()),
                "4s": int((d["scrM_IsBoundry"] == 1).sum()),
                "6s": int((d["scrM_IsSixer"] == 1).sum()),
            }

            # wicket brackets per bowler vs this order
            bowler_wkts = d.groupby("scrM_PlayMIdBowlerName")["scrM_IsWicket"].sum()
            res["2W+"] = int(((bowler_wkts >= 2) & (bowler_wkts < 3)).sum())
            res["3W+"] = int(((bowler_wkts >= 3) & (bowler_wkts < 5)).sum())
            res["5W+"] = int((bowler_wkts >= 5).sum())

            result[str(inn)][order] = res

    return result

def map_bowling_category(skill: str) -> str:
    """
    Map detailed bowler skills to Pace or Spin.
    Supports full right-arm/left-arm variations from all major cricket DBs.
    """

    if not skill:
        return "Unknown"

    s = skill.strip().lower()

    # --------------------
    # PACE CATEGORIES
    # --------------------
    pace_keywords = [
        # Generic words
        "fast", "medium", "pace", "quick",

        # Right arm pace variations
        "raf", "ramf", "ram", "rfm", "rfs", "r-fast", "r-medium",
        "right arm fast", "right arm medium", "right arm medium fast",
        "right arm fast medium",

        # Left arm pace variations
        "laf", "lamf", "lam", "lfm", "l-fast", "l-medium",
        "left arm fast", "left arm medium", "left arm medium fast",
        "left arm fast medium",

        # Abbreviations used in DB
        "fm", "mf", "sfm", "lfm", "rfm", "mfm",
    ]

    # --------------------
    # SPIN CATEGORIES
    # --------------------
    spin_keywords = [
        # Off-spin
        "offbreak", "off break", "off-spin", "off spin",
        "rob", "raob", "ro", "r off break", "right arm off break",

        # Leg-spin
        "legbreak", "leg break", "leg-spin", "leg spin",
        "ralb", "rolb", "r leg break", "right arm leg break",

        # Orthodox
        "orthodox", "sla", "slow left arm", "left arm orthodox",
        "las", "laob", "left arm orthodox spin",

        # Chinaman / wrist spin
        "chinaman", "slc", "left arm chinaman", "wrist spin",
        "lacb", "lac", "left arm wrist spin",

        # Generic spin terms
        "spinner", "spin",
    ]

    # --------------------
    # Decision
    # --------------------
    if any(k in s for k in pace_keywords):
        return "Pace"
    if any(k in s for k in spin_keywords):
        return "Spin"

    return "Unknown"

# utils.py  (place near generate_batting_vs_pace_spin)
import pandas as pd

def map_bowling_category(skill: str) -> str:
    """
    Map detailed bowler skills to 'Pace' or 'Spin'.
    Supports full right-arm/left-arm variations commonly used in cricket DBs.
    """
    if not skill:
        return "Unknown"

    s = str(skill).strip().lower()

    # PACE keywords (broad)
    pace_keywords = [
        "fast", "medium", "pace", "quick",
        "raf", "ramf", "ram", "rfm", "rfs", "r-fast", "r-medium",
        "right arm fast", "right arm medium", "right arm medium fast", "right arm fast medium",
        "laf", "lamf", "lam", "lfm", "l-fast", "l-medium",
        "left arm fast", "left arm medium", "left arm medium fast", "left arm fast medium",
        "fm", "mf", "sfm", "lfm", "rfm", "mfm"
    ]

    # SPIN keywords (broad)
    spin_keywords = [
        # Off-spin
        "offbreak", "off break", "off-spin", "off spin", "rob", "raob", "right arm off break",
        # Leg-spin
        "legbreak", "leg break", "leg-spin", "leg spin", "ralb", "rolb", "right arm leg break",
        # Orthodox / slow left arm
        "orthodox", "sla", "slow left arm", "left arm orthodox", "las", "laob",
        # Chinaman / wrist spin
        "chinaman", "slc", "left arm chinaman", "wrist spin", "left arm wrist",
        # Generic
        "spinner", "spin"
    ]

    if any(k in s for k in pace_keywords):
        return "Pace"
    if any(k in s for k in spin_keywords):
        return "Spin"
    return "Unknown"


def aggregate_batting_vs_type(df):
    """
    Aggregates delivery-level batting data to compute:
      - avg_runs per match
      - strike rate (total runs / legal balls * 100)
      - dots, wickets, fours, sixes (totals)
    Input df: delivery-level dataframe already filtered by (team batting) AND (BowlingType = Pace/Spin)
    Returns dict: {avg_runs, sr, dots, wkts, fours, sixes, matches}
    """
    import pandas as _pd

    out = {
        "avg_runs": 0.0,
        "sr": 0.0,
        "dots": 0,
        "wkts": 0,
        "fours": 0,
        "sixes": 0,
        "matches": 0,
    }

    if df is None or df.empty:
        return out

    # Defensive numeric conversions
    for col in ["scrM_BatsmanRuns", "scrM_DelRuns", "scrM_IsWicket"]:
        if col in df.columns:
            df[col] = _pd.to_numeric(df[col], errors="coerce").fillna(0)

    total_runs = int(df.get("scrM_BatsmanRuns", 0).sum())

    # legal balls = deliveries that are not wides and not no-balls
    legal_balls = int(((~df.get("scrM_IsWideBall", 0).astype(bool)) & (~df.get("scrM_IsNoBall", 0).astype(bool))).sum())

    dots = int((df.get("scrM_DelRuns", 0) == 0).sum()) if "scrM_DelRuns" in df.columns else 0
    wkts = int(df.get("scrM_IsWicket", 0).sum()) if "scrM_IsWicket" in df.columns else 0
    fours = int((df.get("scrM_BatsmanRuns", 0) == 4).sum())
    sixes = int((df.get("scrM_BatsmanRuns", 0) == 6).sum())

    matches_cnt = int(df["scrM_MatchName"].nunique()) if "scrM_MatchName" in df.columns else 1

    avg_runs = round(total_runs / max(matches_cnt, 1), 2)
    sr = round((total_runs / legal_balls) * 100, 2) if legal_balls > 0 else 0.0

    out.update({
        "avg_runs": avg_runs,
        "sr": sr,
        "dots": dots,
        "wkts": wkts,
        "fours": fours,
        "sixes": sixes,
        "matches": matches_cnt,
    })
    return out


def generate_bowling_vs_pace_spin(df, team_name, inning_no=None, phase_name="Overall"):
    """
    Delivery-based bowling summary for selected team when bowling.
    Returns (pace_summary, spin_summary) each containing:
      {overs, runs, wkts, nb, wd, dots, fours, sixes, econ}

    FIXES APPLIED:
    - Wide/NoBall rows now included (after SQL filter removed).
    - Dot balls exclude wides/noballs.
    - Boundaries count correctly even on no-ball hits.
    """
    import pandas as _pd

    # Empty summary template
    empty = {
        "overs": "0.0",
        "runs": 0,
        "wkts": 0,
        "nb": 0,
        "wd": 0,
        "dots": 0,
        "fours": 0,
        "sixes": 0,
        "econ": 0
    }

    # If no data, return empty
    if df is None or df.empty:
        return empty, empty

    # Ensure numeric columns exist
    numeric_cols = [
        "scrM_BatsmanRuns", "scrM_WideRuns", "scrM_NoBallRuns",
        "scrM_ByeRuns", "scrM_LegByeRuns", "scrM_IsWicket",
        "scrM_IsNoBall", "scrM_IsWideBall"
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = _pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Filter only balls where team is bowling
    df_bowl = df[df.get("scrM_tmMIdBowlingName") == team_name].copy()

    if df_bowl.empty:
        return empty, empty

    # Bowling Type (pace/spin)
    if "scrM_BowlerSkill" in df_bowl.columns:
        df_bowl["BowlingType"] = df_bowl["scrM_BowlerSkill"].apply(map_bowling_category)
    else:
        df_bowl["BowlingType"] = "Unknown"

    # ------------------------------------------------------------------
    # Summary function (Pace / Spin group)
    # ------------------------------------------------------------------
    def summarize(gdf):
        if gdf is None or gdf.empty:
            return empty.copy()

        # TOTAL RUNS conceded (including all extras)
        runs = int(
            gdf.get("scrM_BatsmanRuns", 0).sum()
            + gdf.get("scrM_WideRuns", 0).sum()
            + gdf.get("scrM_NoBallRuns", 0).sum()
            + gdf.get("scrM_ByeRuns", 0).sum()
            + gdf.get("scrM_LegByeRuns", 0).sum()
        )

        # LEGAL BALLS only (wide/no-ball do NOT count)
        legal_balls = int(
            ((gdf["scrM_IsWideBall"] == 0) & (gdf["scrM_IsNoBall"] == 0)).sum()
        )

        overs = f"{legal_balls//6}.{legal_balls%6}" if legal_balls else "0.0"
        econ = round(runs / (legal_balls / 6), 2) if legal_balls else 0

        # Wickets
        wkts = int(gdf.get("scrM_IsWicket", 0).sum())

        # No-balls & wides (now correctly counted)
        nb = int(gdf.get("scrM_IsNoBall", 0).sum())
        wd = int(gdf.get("scrM_IsWideBall", 0).sum())

        # DOT BALLS — must exclude wides / no-balls
        dots = int(
            ((gdf["scrM_BatsmanRuns"] == 0)
             & (gdf["scrM_IsWideBall"] == 0)
             & (gdf["scrM_IsNoBall"] == 0))
            .sum()
        )

        # FOURS — include no-ball boundary hits
        fours = int(
            (
                (gdf["scrM_BatsmanRuns"] == 4)
                | (gdf["scrM_BatsmanRuns"] == 4)  # boundary on no-ball allowed
            ).sum()
        )

        # SIXES — include no-ball sixes
        sixes = int(
            (
                (gdf["scrM_BatsmanRuns"] == 6)
                | (gdf["scrM_BatsmanRuns"] == 6)
            ).sum()
        )

        return {
            "overs": overs,
            "runs": runs,
            "wkts": wkts,
            "nb": nb,
            "wd": wd,
            "dots": dots,
            "fours": fours,
            "sixes": sixes,
            "econ": econ,
        }

    # Final pace & spin summaries
    pace_summary = summarize(df_bowl[df_bowl["BowlingType"] == "Pace"])
    spin_summary = summarize(df_bowl[df_bowl["BowlingType"] == "Spin"])

    return pace_summary, spin_summary


def create_team_vs_opponent_runs_per_over_chart(df_team, df_opponent, team_name, phase=None, dark_mode=False):
    """
    TEAM ANALYSIS — Runs Per Over Chart (AVERAGE PER MATCH)
    Includes: average wickets, correct markers, full UI styling
    """

    import numpy as np
    import plotly.graph_objects as go

    # Labels
    team1_name = safe_team_name(team_name)
    team2_name = "Opponents"

    # Detect highest over
    max_over = max(
        int(df_team["scrM_OverNo"].max()) if not df_team.empty else 0,
        int(df_opponent["scrM_OverNo"].max()) if not df_opponent.empty else 0
    )
    if max_over == 0:
        return go.Figure()

    # Phase Map
    if max_over <= 20:
        phase_map = {
            "overall": (1, max_over),
            "powerplay": (1, min(6, max_over)),
            "middle": (7, min(15, max_over)),
            "slog": (16, max_over),
        }
    else:
        phase_map = {
            "overall": (1, max_over),
            "powerplay": (1, min(10, max_over)),
            "middle": (11, min(40, max_over)),
            "slog": (41, max_over),
        }

    key = (phase or "overall").lower().strip()
    aliases = {
        "pp": "powerplay", "powerplay": "powerplay",
        "middle": "middle", "middle overs": "middle",
        "slog": "slog", "slog overs": "slog",
        "overall": "overall"
    }
    phase_key = aliases.get(key, "overall")

    start_over, end_over = phase_map[phase_key]
    overs = list(range(int(start_over), int(end_over) + 1))

    # =====================================================
    # AVERAGE RUNS & AVERAGE WICKETS PER MATCH
    # =====================================================
    def avg_values(df):
        """ Returns average runs & average wickets per match per over """
        if df.empty:
            return np.zeros(len(overs)), np.zeros(len(overs))

        match_groups = df.groupby("scrM_MatchName")

        all_runs = []
        all_wkts = []

        for _, match_df in match_groups:
            part = match_df[match_df["scrM_OverNo"].between(start_over, end_over)]

            runs = (
                part.groupby("scrM_OverNo")["scrM_DelRuns"].sum()
                .reindex(overs, fill_value=0)
                .astype(float)
            )
            wkts = (
                part.groupby("scrM_OverNo")["scrM_IsWicket"].sum()
                .reindex(overs, fill_value=0)
                .astype(float)
            )

            all_runs.append(runs.values)
            all_wkts.append(wkts.values)

        # AVERAGE across matches
        return np.mean(all_runs, axis=0), np.mean(all_wkts, axis=0)

    team_vals, team_wk_avg = avg_values(df_team)
    opp_vals, opp_wk_avg = avg_values(df_opponent)

    # =====================================================
    # BUILD CHART
    # =====================================================
    x_base = np.array(overs, dtype=float)
    bar_width = 0.42
    label_color = "#00E676" if dark_mode else "#00C853"

    fig = go.Figure()

    # TEAM bars
    fig.add_trace(go.Bar(
        x=(x_base - bar_width / 2),
        y=team_vals,
        name=team1_name,
        marker_color="#1976D2",
    ))

    # OPPONENT bars
    fig.add_trace(go.Bar(
        x=(x_base + bar_width / 2),
        y=opp_vals,
        name=team2_name,
        marker_color="#FF9800",
    ))

    # =====================================================
    # WICKET MARKERS (only if avg ≥ 1 wicket)
    # =====================================================
    for i, ov in enumerate(overs):
        if team_wk_avg[i] >= 1:
            fig.add_trace(go.Scatter(
                x=[ov - bar_width / 2],
                y=[team_vals[i]],
                mode="markers",
                marker=dict(color="red", size=14, symbol="circle"),
                showlegend=False
            ))

        if opp_wk_avg[i] >= 1:
            fig.add_trace(go.Scatter(
                x=[ov + bar_width / 2],
                y=[opp_vals[i]],
                mode="markers",
                marker=dict(color="red", size=14, symbol="circle"),
                showlegend=False
            ))

    # =====================================================
    # VALUE LABELS
    # =====================================================
    for i, ov in enumerate(overs):
        fig.add_annotation(
            x=ov - bar_width / 2,
            y=team_vals[i] + 0.8,
            text=str(math.floor(team_vals[i] + 0.5)),
            showarrow=False,
            font=dict(size=14, color=label_color),
        )
        fig.add_annotation(
            x=ov + bar_width / 2,
            y=opp_vals[i] + 0.8,
            text=str(math.floor(opp_vals[i] + 0.5)),
            showarrow=False,
            font=dict(size=14, color=label_color),
        )

    # =====================================================
    # LAYOUT
    # =====================================================
    y_max = float(max(max(team_vals), max(opp_vals)) + 5)
    y_step = int(max(2, int(np.ceil(y_max / 8))))

    fig.update_layout(
        autosize=True,
        width=None,
        height=520,

        xaxis=dict(
            title=dict(text="Over Number", font=dict(size=18, color=label_color)),
            tickvals=overs,
            tickfont=dict(size=16, color=label_color),
            showgrid=False,
        ),

        yaxis=dict(
            title=dict(text="Runs (Average)", font=dict(size=18, color=label_color)),
            tickvals=list(range(0, int(y_max) + y_step, y_step)),
            tickfont=dict(size=16, color=label_color),
            showgrid=False,
        ),

        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.08,
            font=dict(size=16, color=label_color),
        ),

        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",

        barmode="group",
        bargap=0.18,

        margin=dict(l=0, r=0, t=40, b=60),
    )

    return fig



def create_team_vs_opponent_run_rate_chart(df_team, df_opponent, team_name, phase=None, dark_mode=False):
    """
    TEAM ANALYSIS — Correct Cumulative Run Rate Chart
    Uses AVERAGE cumulative RR across multiple matches (not total runs)
    """

    import numpy as np
    import pandas as pd
    import plotly.graph_objects as go

    team1_name = safe_team_name(team_name)
    team2_name = "Opponents"

    # --------------------------
    # DETECT MAX OVER
    # --------------------------
    max_over = 0
    if not df_team.empty:
        max_over = max(max_over, int(df_team["scrM_OverNo"].max()))
    if not df_opponent.empty:
        max_over = max(max_over, int(df_opponent["scrM_OverNo"].max()))

    if max_over == 0:
        return go.Figure()

    # --------------------------
    # PHASE MAP
    # --------------------------
    if max_over <= 20:
        phase_map = {
            "overall": (1, max_over),
            "powerplay": (1, min(6, max_over)),
            "middle": (7, min(15, max_over)),
            "slog": (16, max_over),
        }
    else:
        phase_map = {
            "overall": (1, max_over),
            "powerplay": (1, min(10, max_over)),
            "middle": (11, min(40, max_over)),
            "slog": (41, max_over),
        }

    key = (phase or "overall").lower().strip()
    aliases = {
        "pp": "powerplay",
        "powerplay": "powerplay",
        "middle overs": "middle",
        "middle": "middle",
        "slog overs": "slog",
        "slog": "slog",
        "overall": "overall",
    }
    phase_key = aliases.get(key, "overall")
    start_over, end_over = phase_map[phase_key]

    overs = list(range(start_over, end_over + 1))

    # =============================================================
    #               ⭐ MATCH-BY-MATCH CUMULATIVE RR (CORRECT)
    # =============================================================

    def compute_matchwise_avg_rr(df):
        """
        For each match → cumulative RR
        Then average RR across matches.
        """
        if df.empty:
            return np.zeros(len(overs))

        match_rr = []

        for match_name, g in df.groupby("scrM_MatchName"):
            runs_per_over = (
                g.groupby("scrM_OverNo")["scrM_DelRuns"].sum()
                .reindex(overs, fill_value=0)
                .values
            )
            denom = np.arange(1, len(overs) + 1)
            crr = np.cumsum(runs_per_over) / denom
            match_rr.append(crr)

        match_rr = np.array(match_rr)
        avg_rr = match_rr.mean(axis=0)   # ⭐ average across matches
        return avg_rr

    # TEAM RR
    crr_team = compute_matchwise_avg_rr(df_team)

    # OPPONENT RR
    crr_oppo = compute_matchwise_avg_rr(df_opponent)

    # --------------------------
    # FIGURE START
    # --------------------------
    fig = go.Figure()

    label_color = "#00C853"

    # TEAM CURVE (blue)
    fig.add_trace(go.Scatter(
        x=overs, y=crr_team,
        mode="lines+markers+text",
        name=team1_name,
        marker=dict(color="#1976D2"),
        text=[f"{x:.2f}" for x in crr_team],
        textposition="top center",
        textfont=dict(size=16, color=label_color)
    ))

    # OPPONENT CURVE (orange)
    fig.add_trace(go.Scatter(
        x=overs, y=crr_oppo,
        mode="lines+markers+text",
        name=team2_name,
        marker=dict(color="#FF9800"),
        text=[f"{x:.2f}" for x in crr_oppo],
        textposition="top center",
        textfont=dict(size=16, color=label_color)
    ))

    y_max = max(np.nanmax(crr_team), np.nanmax(crr_oppo)) + 0.5

    # -------------------------------------------------------------
    # STYLE EXACTLY LIKE YOUR RUNS-PER-OVER GRAPH
    # -------------------------------------------------------------
    fig.update_layout(
        autosize=True,
        width=None,
        height=520,

        xaxis=dict(
            title=dict(text="Over Number", font=dict(size=18, color=label_color)),
            tickvals=overs,
            tickfont=dict(size=16, color=label_color),
            showgrid=False,
            zeroline=False,
            showline=False,
        ),

        yaxis=dict(
            title=dict(text="Run Rate (Cumulative)", font=dict(size=18, color=label_color)),
            tickfont=dict(size=16, color=label_color),
            range=[0, y_max],
            showgrid=False,
            zeroline=False,
            showline=False,
        ),

        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            font=dict(size=16, color=label_color),
        ),

        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=40, b=60),
    )

    return fig


# utils.py  — ADD THIS FUNCTION (paste into utils.py)
# Requires: matplotlib, numpy, io, base64, pandas
import numpy as np
import matplotlib.pyplot as plt
import io, base64
import pandas as pd

# paste/replace this in utils.py
import numpy as np
import matplotlib.pyplot as plt
import io, base64

def generate_team_wagon_radar(
    team_name,
    df,
    mode="batting",
    stance=None,
    size_inches=8,
    dpi=260,
    run_filter=None
):
    """
    FINAL UPDATED VERSION
    ---------------------
    - Supports run filtering (1,2,3,4,6) just like session radar.
    - Does NOT blank out radar when filtered values are small.
    - Perfect percentage calculation for multiselect.
    - Layout identical to your session radar chart.
    """

    import numpy as np
    import matplotlib.pyplot as plt
    import io, base64
    import pandas as pd

    # ----------------------------
    # NORMALIZE RUN FILTER
    # ----------------------------
    rf = None
    if run_filter is not None:
        if isinstance(run_filter, str):
            if run_filter.lower() == "all":
                rf = None
            else:
                try:
                    rf = {int(run_filter)}
                except:
                    rf = None
        elif isinstance(run_filter, (list, tuple, set)):
            cleaned = []
            for x in run_filter:
                if str(x).lower() != "all":
                    try:
                        cleaned.append(int(x))
                    except:
                        pass
            rf = set(cleaned) if cleaned else None

    # Radar sectors
    sectors = [
        "Mid Wicket",
        "Square Leg",
        "Fine Leg",
        "Third Man",
        "Point",
        "Covers",
        "Long Off",
        "Long On"
    ]

    # ----------------------------
    # EMPTY CHECK
    # ----------------------------
    if df is None or df.empty:
        fig, ax = plt.subplots(figsize=(size_inches, size_inches), subplot_kw=dict(polar=True))
        ax.set_theta_zero_location("N")
        ax.set_theta_direction(-1)
        ax.set_xticks([]); ax.set_yticks([])
        try:
            ax.spines["polar"].set_visible(False)
        except:
            pass

        ax.text(0.5, 0.5, "No Data",
                ha="center", va="center",
                transform=ax.transAxes,
                color="white", fontsize=22, fontweight="bold")

        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=dpi, transparent=True, bbox_inches="tight")
        plt.close(fig)
        buf.seek(0)
        return "data:image/png;base64," + base64.b64encode(buf.read()).decode()

    # ----------------------------
    # REQUIRED COLUMNS
    # ----------------------------
    area_col = "scrM_WagonArea_zName"
    runs_col = "scrM_BatsmanRuns"

    df = df.copy()

    if area_col not in df.columns or runs_col not in df.columns:
        fig, ax = plt.subplots(figsize=(size_inches, size_inches), subplot_kw=dict(polar=True))
        ax.text(0.5, 0.5, "Missing wagon area / runs column",
                ha="center", va="center", fontsize=12)
        ax.axis("off")
        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=dpi, transparent=True, bbox_inches="tight")
        plt.close(fig)
        buf.seek(0)
        return "data:image/png;base64," + base64.b64encode(buf.read()).decode()

    # Clean values
    df[area_col] = df[area_col].astype(str).str.strip()
    df[runs_col] = pd.to_numeric(df[runs_col], errors="coerce").fillna(0).astype(int)

    # NEW IMPORTANT FIX — FILTER ONLY VALID SECTORS
    valid_sectors = [
        "Mid Wicket", "Square Leg", "Fine Leg", "Third Man",
        "Point", "Covers", "Long Off", "Long On"
    ]
    df = df[df[area_col].isin(valid_sectors)]


    # ----------------------------
    # APPLY RUN FILTER
    # ----------------------------
    if rf:
        df = df[df[runs_col].isin(rf)]

    # ----------------------------
    # AGGREGATION
    # ----------------------------
    breakdown_data = [{"1s":0,"2s":0,"3s":0,"4s":0,"6s":0} for _ in sectors]
    sector_map = {s:i for i,s in enumerate(sectors)}

    for _, row in df.iterrows():
        sec = row[area_col]
        r = int(row[runs_col])

        if sec in sector_map:
            idx = sector_map[sec]
            if r == 1: breakdown_data[idx]["1s"] += 1
            elif r == 2: breakdown_data[idx]["2s"] += 1
            elif r == 3: breakdown_data[idx]["3s"] += 1
            elif r == 4: breakdown_data[idx]["4s"] += 1
            elif r == 6: breakdown_data[idx]["6s"] += 1

    # ----------------------------
    # COMPUTE SECTOR RUNS
    # ----------------------------
    sector_runs = [
        bd["1s"] + bd["2s"]*2 + bd["3s"]*3 + bd["4s"]*4 + bd["6s"]*6
        for bd in breakdown_data
    ]

    # ----------------------------
    # FIX: PREVENT RADAR FROM COLLAPSING
    # ----------------------------
    total_runs = sum(sector_runs)
    if total_runs == 0:
        total_runs = 1  # avoids division by zero & keeps radar visible

    # ----------------------------
    # BEGIN PLOTTING
    # ----------------------------
    fig, ax = plt.subplots(figsize=(size_inches, size_inches), subplot_kw=dict(polar=True))
    ax.set_theta_zero_location("N")
    ax.set_theta_direction(-1)
    ax.set_xticks([]); ax.set_yticks([])
    try:
        ax.spines['polar'].set_visible(False)
    except:
        pass

    scale = 0.9
    ax.set_aspect("equal")

    # rim & ground
    rim_radius = 1.10 * scale
    ax.add_artist(plt.Circle((0,0), rim_radius, transform=ax.transData._b,
                             color="#6dbc45", linewidth=26, fill=False, zorder=5, clip_on=False))

    ax.add_artist(plt.Circle((0,0), 1.0*scale, transform=ax.transData._b, color="#19a94b", zorder=0))
    ax.add_artist(plt.Circle((0,0), 0.6*scale, transform=ax.transData._b, color="#4CAF50", zorder=1))

    # pitch
    ax.add_artist(plt.Rectangle((-0.08*scale/2, -0.33*scale/2),
                                0.08*scale, 0.33*scale,
                                transform=ax.transData._b,
                                color="burlywood", zorder=2))

    # sector lines
    for ang in np.linspace(0, 2*np.pi, 9):
        ax.plot([ang, ang], [0, 1.0*scale], color="white", linewidth=3, zorder=3)

    # highlight max sector
    max_idx = int(np.argmax(sector_runs))
    sector_angles_deg = [112.5, 67.5, 22.5, 337.5, 292.5, 247.5, 202.5, 157.5]
    ax.bar(
        np.deg2rad(sector_angles_deg[max_idx]),
        1.0 * scale,
        width=np.radians(45),
        color="red",
        alpha=0.25,
        zorder=1
    )

    # labels
    label_specs = [
        ("Mid Wicket",112.5,-110,-0.02),
        ("Square Leg",67.5,-70,-0.02),
        ("Fine Leg",22.5,-25,0.00),
        ("Third Man",337.5,20,-0.02),
        ("Point",292.5,70,-0.01),
        ("Covers",247.5,110,-0.01),
        ("Long Off",202.5,155,-0.02),
        ("Long On",157.5,200,-0.02)
    ]
    for t,deg,rot,off in label_specs:
        ax.text(np.deg2rad(deg), rim_radius+off, t,
                color="white", fontsize=16, fontweight="bold",
                ha="center", va="center",
                rotation=rot, rotation_mode="anchor", zorder=6)

    # runs + percentages
    box_positions = [
        (103.5,0,0.70),(67.5,0,0.70),
        (22.5,0,0.80),(337.5,0,0.80),
        (295.5,0,0.75),(250.5,0,0.70),
        (204.5,1,0.59),(155.5,1,0.59)
    ]

    for i,(deg,rot,dist) in enumerate(box_positions):
        rad = np.deg2rad(deg)
        r = dist * scale
        runs = sector_runs[i]
        pct = runs / total_runs * 100

        ax.text(
            rad, r,
            f"{runs}\n({pct:.1f}%)",
            color="white", fontsize=19, fontweight="bold",
            ha="center", va="center",
            rotation=0, linespacing=1.15
        )

    # save final image
    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=dpi, transparent=True, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)

    return "data:image/png;base64," + base64.b64encode(buf.read()).decode()

def compute_area_stats(df, area_col):
    if df.empty:
        return []

    area_groups = df.groupby(area_col)

    results = []

    for area, group in area_groups:
        balls = len(group)
        runs = group["scrM_BatsmanRuns"].sum()

        dots = len(group[group["scrM_BatsmanRuns"] == 0])
        ones = len(group[group["scrM_BatsmanRuns"] == 1])
        twos = len(group[group["scrM_BatsmanRuns"] == 2])
        threes = len(group[group["scrM_BatsmanRuns"] == 3])
        fours = len(group[group["scrM_IsBoundry"] == 1])
        sixes = len(group[group["scrM_IsSixer"] == 1])

        sr = round((runs / balls * 100), 1) if balls > 0 else 0
        eco = round((runs / (balls / 6)), 1) if balls > 0 else 0

        bdry_balls = fours + sixes
        bdry_pct = round((bdry_balls / balls * 100), 1) if balls > 0 else 0
        runs_pct = 0  # computed later after totals

        results.append({
            "Area": area or "Unknown",
            "Runs": runs,
            "Balls": balls,
            "SR": sr,
            "Eco": eco,
            "Dots": dots,
            "1s": ones,
            "2s": twos,
            "3s": threes,
            "4s": fours,
            "6s": sixes,
            "BdryPct": bdry_pct
        })

    total_runs = sum(r["Runs"] for r in results)

    for r in results:
        r["RunsPct"] = round(r["Runs"] / total_runs * 100, 1) if total_runs > 0 else 0

    return results

def get_powerplay_stats(trn_id, team, matches):
    if not trn_id or not team or not matches:
        print(f"[DEBUG] get_powerplay_stats: Missing parameter(s): trn_id={trn_id}, team={team}, matches={matches}")
        return None

    try:
        conn = get_connection()
        placeholders = ",".join("%s" for _ in matches)

        # Fetch Powerplay (1–6), Middle Overs (7–15), Slog Overs (16–20)
        query = f"""
            SELECT 
                scrM_MatchName,
                scrM_InningNo,
                scrM_BatsmanRuns,
                scrM_IsWicket,
                scrM_IsBoundry,
                scrM_IsSixer,
                scrM_OverNo,
                CASE WHEN scrM_BatsmanRuns = 0 THEN 1 ELSE 0 END AS DotBall
            FROM tblscoremaster
            WHERE scrM_TrnMId = %s
              AND scrM_MatchName IN ({placeholders})
              AND scrM_tmMIdBattingName = %s
              AND scrM_IsValidBall = 1
        """

        params = [trn_id] + list(matches) + [team]
        print(f"[DEBUG] get_powerplay_stats: trn_id={trn_id}, team={team}, matches={matches}")
        print(f"[DEBUG] get_powerplay_stats: SQL query: {query}")
        print(f"[DEBUG] get_powerplay_stats: params={params}")
        df = pd.read_sql(query, conn, params=tuple(params))
        print(f"[DEBUG] get_powerplay_stats: DataFrame shape: {df.shape}")
        if not df.empty:
            print(f"[DEBUG] get_powerplay_stats: DataFrame head:\n{df.head()}")
        else:
            print(f"[DEBUG] get_powerplay_stats: DataFrame is EMPTY!")
        conn.close()

        result = {}

        def calc(df_subset, tag):
            if df_subset.empty:
                return {tag: {"avg_runs": 0, "sr": 0, "dots": 0, "wkts": 0, "fours": 0, "sixes": 0}}

            total_runs = df_subset.scrM_BatsmanRuns.sum()
            total_balls = len(df_subset)
            match_count = df_subset.scrM_MatchName.nunique() if "scrM_MatchName" in df_subset.columns else 1
            return {tag: {
                "avg_runs": round(total_runs / match_count, 2) if match_count > 0 else 0,
                "sr": round((total_runs / total_balls) * 100, 2) if total_balls > 0 else 0,
                "dots": round(df_subset.DotBall.sum() / match_count, 2) if match_count > 0 else 0,
                "wkts": round(df_subset.scrM_IsWicket.sum() / match_count, 2) if match_count > 0 else 0,
                "fours": round(df_subset.scrM_IsBoundry.sum() / match_count, 2) if match_count > 0 else 0,
                "sixes": round(df_subset.scrM_IsSixer.sum() / match_count, 2) if match_count > 0 else 0
            }}

        for inning in [1, 2]:
            pp = df[(df.scrM_OverNo <= 6) & (df.scrM_InningNo == inning)]
            mo = df[(df.scrM_OverNo >= 7) & (df.scrM_OverNo <= 15) & (df.scrM_InningNo == inning)]
            so = df[(df.scrM_OverNo >= 16) & (df.scrM_OverNo <= 20) & (df.scrM_InningNo == inning)]

            result.update(calc(pp, f"pp{inning}"))
            result.update(calc(mo, f"mo{inning}"))
            result.update(calc(so, f"so{inning}"))  # 🔥 NEW — slog overs stored

        return result

    except Exception as e:
        print("⚠️ POWERPLAY ERROR:", e)
        return None




# Fetch powerplay stats for a given match, team, and over range
def get_powerplay_stats_ISPL(match_name, team_id, pp_from, pp_to):
    """
    Returns a dict of aggregated powerplay stats for a given match and team ID.
    """
    import pandas as pd
    try:
        conn = get_connection()
        query = """
            SELECT
                SUM(CASE WHEN scrM_IsWicket = 1 THEN 1 ELSE 0 END) AS Wkts,
                SUM(scrM_DelRuns) AS Runs,
                SUM(CASE WHEN scrM_IsBoundry = 1 THEN 1 ELSE 0 END) AS Fours,
                SUM(CASE WHEN scrM_IsSixer = 1 THEN 1 ELSE 0 END) AS Sixes,
                SUM(scrM_WideRuns) AS WD,
                SUM(scrM_NoBallRuns) AS NB,
                SUM(CASE WHEN scrM_DeliveryType_zName = 'FH' THEN 1 ELSE 0 END) AS FH
            FROM tblscoremaster
            WHERE scrM_MatchName = %s
              AND scrM_tmMIdBatting = %s
              AND scrM_OverNo >= %s - 1
              AND scrM_OverNo < %s
              
        """
        df = pd.read_sql(query, conn, params=(match_name, team_id, pp_from, pp_to))
        conn.close()
        if df.empty:
            return {'Wkts': 0, 'Runs': 0, 'Fours': 0, 'Sixes': 0, 'WD': 0, 'NB': 0, 'FH': 0}
        row = df.iloc[0]
        return {
            'Wkts': int(row['Wkts']) if pd.notnull(row['Wkts']) else 0,
            'Runs': int(row['Runs']) if pd.notnull(row['Runs']) else 0,
            'Fours': int(row['Fours']) if pd.notnull(row['Fours']) else 0,
            'Sixes': int(row['Sixes']) if pd.notnull(row['Sixes']) else 0,
            'WD': int(row['WD']) if pd.notnull(row['WD']) else 0,
            'NB': int(row['NB']) if pd.notnull(row['NB']) else 0,
            'FH': int(row['FH']) if pd.notnull(row['FH']) else 0,
        }
    except Exception as e:
        print(f"❌ Error in get_powerplay_stats: {e}")
        return {'Wkts': 0, 'Runs': 0, 'Fours': 0, 'Sixes': 0, 'WD': 0, 'NB': 0, 'FH': 0}


def get_powerplay_overs(match_name, team_id):
    """
    Returns (powerplay_list, sql_debug) where powerplay_list is a list of
    {PowerplayNo, From, To} dicts and sql_debug contains the query, params and raw result.
    """
    import pandas as pd
    try:
        conn = get_connection()
        query = """
            SELECT Inn_PP1From, Inn_PP1To, Inn_PP2From, Inn_PP2To
            FROM tblmatchinnings i
            INNER JOIN tblmatchmaster m ON i.Inn_mchMId = m.mchM_Id
            WHERE m.mchM_MatchName = %s AND i.Inn_tmMIdBatting = %s AND (i.Inn_Inning = 1 OR i.Inn_Inning = 2)
        """
        params = (match_name, team_id)
        df = pd.read_sql(query, conn, params=params)
        conn.close()
        sql_debug = {'query': query, 'params': params, 'result': df.to_dict('records')}
        if df.empty:
            return [], sql_debug
        row = df.iloc[0]
        result = []
        if pd.notnull(row.get('Inn_PP1From')) and pd.notnull(row.get('Inn_PP1To')):
            result.append({'PowerplayNo': 1, 'From': int(row['Inn_PP1From']), 'To': int(row['Inn_PP1To'])})
        if pd.notnull(row.get('Inn_PP2From')) and pd.notnull(row.get('Inn_PP2To')):
            result.append({'PowerplayNo': 2, 'From': int(row['Inn_PP2From']), 'To': int(row['Inn_PP2To'])})
        return result, sql_debug
    except Exception as e:
        print(f"❌ Error in get_powerplay_overs: {e}")
        return [], {'query': '', 'params': (), 'result': str(e)}


def get_tapeball_deliveries(match_name, team_id=None):
    """
    Returns (deliveries_list, summary_dict) for deliveries where scrM_IsTapeOver = 1
    filtered by match_name and optionally team_id (batting or bowling).
    summary_dict contains {'Runs', 'Wkts', 'Balls'}.
    """
    import pandas as pd
    try:
        conn = get_connection()
        if team_id:
            # Filter by batting team (same behaviour as powerplay / fiftyover reports)
            query = """
                SELECT
                    scrM_OverNo, scrM_DelNo, scrM_PlayMIdStrikerName AS Batter,
                    scrM_PlayMIdBowlerName AS Bowler, scrM_DelRuns, scrM_IsWicket,
                    scrM_WideRuns, scrM_NoBallRuns, scrM_DeliveryType_zName
                FROM tblscoremaster
                WHERE scrM_MatchName = %s AND scrM_IsTapeOver = 1
                  AND scrM_tmMIdBatting = %s
                ORDER BY scrM_OverNo, scrM_DelNo
            """
            params = (match_name, team_id)
        else:
            query = """
                SELECT
                    scrM_OverNo, scrM_DelNo, scrM_PlayMIdStrikerName AS Batter,
                    scrM_PlayMIdBowlerName AS Bowler, scrM_DelRuns, scrM_IsWicket,
                    scrM_WideRuns, scrM_NoBallRuns, scrM_DeliveryType_zName
                FROM tblscoremaster
                WHERE scrM_MatchName = %s AND scrM_IsTapeOver = 1
                ORDER BY scrM_OverNo, scrM_DelNo
            """
            params = (match_name,)

        df = pd.read_sql(query, conn, params=params)
        conn.close()

        if df.empty:
            return [], {'Runs': 0, 'Wkts': 0, 'Balls': 0}

        deliveries = []
        for _, r in df.iterrows():
            deliveries.append({
                'Over': int(r['scrM_OverNo']) if pd.notnull(r['scrM_OverNo']) else None,
                'Ball': int(r['scrM_DelNo']) if pd.notnull(r['scrM_DelNo']) else None,
                'Batter': r.get('Batter'),
                'Bowler': r.get('Bowler'),
                'Runs': int(r['scrM_DelRuns']) if pd.notnull(r['scrM_DelRuns']) else 0,
                'IsWicket': int(r['scrM_IsWicket']) if pd.notnull(r['scrM_IsWicket']) else 0,
                'Wide': int(r['scrM_WideRuns']) if pd.notnull(r['scrM_WideRuns']) else 0,
                'NoBall': int(r['scrM_NoBallRuns']) if pd.notnull(r['scrM_NoBallRuns']) else 0,
                'DeliveryType': r.get('scrM_DeliveryType_zName')
            })

        total_runs = int(df['scrM_DelRuns'].sum()) if 'scrM_DelRuns' in df.columns else 0
        total_wkts = int(df['scrM_IsWicket'].sum()) if 'scrM_IsWicket' in df.columns else 0
        total_balls = len(df)

        summary = {'Runs': total_runs, 'Wkts': int(total_wkts), 'Balls': int(total_balls)}
        return deliveries, summary
    except Exception as e:
        print(f"❌ Error in get_tapeball_deliveries: {e}")
        return [], {'Runs': 0, 'Wkts': 0, 'Balls': 0}
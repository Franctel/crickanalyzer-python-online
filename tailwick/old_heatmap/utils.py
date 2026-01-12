import pyodbc
import pandas as pd
import matplotlib
import dash_table, html
matplotlib.use('Agg')  # <-- Add this line

import os, sys
from pathlib import Path

# ----------------- Config + Connection Helper -----------------
# def load_config():
#     cfg_path = Path(__file__).resolve().parent / "config.json"
#     with open(cfg_path, encoding="utf-8") as f:
#         return json.load(f)

# def get_connection():
#     cfg = load_config()
#     try:
#         connection_string = (
#             f"DRIVER={cfg['driver']};"
#             f"SERVER={cfg['server']};"
#             f"DATABASE={cfg['database']};"
#             f"Trusted_Connection={cfg['trusted_connection']};"
#         )
#         return pyodbc.connect(connection_string)
#     except Exception as e:
#         print("❌ Database connection failed:", e)
#         return None

# Base directory (works in dev and PyInstaller)
# def resource_path(relative_path):
#     try:
#         base_path = sys._MEIPASS  # PyInstaller temp folder
#     except Exception:
#         base_path = os.path.abspath(".")
#     return os.path.join(base_path, relative_path)

# ----------------- Config + Connection Helper -----------------
from pathlib import Path
import os, sys, json, pyodbc

# Global DB override (set by apps.py endpoint)
db_override = {
    "instance": None,
    "ip": None,
    "username": None,
    "password": None
}

def load_config():
    cfg_path = Path(__file__).resolve().parent / "config.json"
    with open(cfg_path, encoding="utf-8") as f:
        return json.load(f)

def get_connection():
    cfg = load_config()

    try:
        # Case 1: Remote override
        if db_override.get("instance") and db_override.get("ip"):
            instance = db_override["instance"]

            # If full instance string provided (like HOST\SQLEXPRESS), keep only the instance part
            if "\\" in instance:
                instance = instance.split("\\")[-1]

            # Try "IP\INSTANCE" first, fallback to "IP,1433"
            server_with_instance = f"{db_override['ip']}\\{instance}"
            server_with_port = f"{db_override['ip']},1433"

            def make_conn_str(server):
                return (
                    f"DRIVER={cfg['driver']};"
                    f"SERVER={server};"
                    f"DATABASE={cfg['database']};"
                    f"UID={db_override.get('username')};"
                    f"PWD={db_override.get('password')};"
                )

            try:
                print(f"📡 Trying remote SQL Server: {server_with_instance}")
                return pyodbc.connect(make_conn_str(server_with_instance), timeout=5)
            except Exception as e1:
                print("⚠️ Remote instance connection failed:", e1)

                try:
                    print(f"📡 Retrying with IP + port: {server_with_port}")
                    return pyodbc.connect(make_conn_str(server_with_port), timeout=5)
                except Exception as e2:
                    print("❌ Remote IP:port connection failed:", e2)
                    return None

        # Case 2: Local standalone (Trusted Connection)
        else:
            connection_string = (
                f"DRIVER={cfg['driver']};"
                f"SERVER={cfg['server']};"
                f"DATABASE={cfg['database']};"
                f"Trusted_Connection={cfg['trusted_connection']};"
            )
            print(f"✅ Using local standalone DB: {cfg['server']}")
            return pyodbc.connect(connection_string)

    except Exception as e:
        print("❌ Database connection failed:", e)
        return None


# Base directory (works in dev and PyInstaller)
def resource_path(relative_path):
    try:
        base_path = sys._MEIPASS  # PyInstaller temp folder
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)




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
    try:
        conn = get_connection()
        query = """
            SELECT DISTINCT scrM_tmMIdBattingName
            FROM tblScoreMaster
            WHERE scrM_tmMIdBattingName IS NOT NULL
        """
        df = pd.read_sql(query, conn)
        conn.close()

        teams = sorted(df['scrM_tmMIdBattingName'].dropna().unique())
        return teams

    except Exception as e:
        print("Failed to fetch teams:", e)
        return []


def get_all_tournaments():
    try:
        conn = get_connection()
        query = """
            SELECT t.trnM_Id, t.trnM_TournamentName
            FROM tblTournaments t
            WHERE t.trnM_TournamentName IS NOT NULL
            ORDER BY t.trnM_TournamentName
        """
        df = pd.read_sql(query, conn)
        conn.close()

        tournaments = [
            {"value": int(row['trnM_Id']), "label": row['trnM_TournamentName']}
            for _, row in df.iterrows()
        ]
        print("✅ Loaded tournaments:", tournaments)
        return tournaments
    except Exception as e:
        print("❌ Failed to fetch tournaments:", e)
        return []


def get_data_from_db(team1, team2):
    try:
        conn = get_connection()
        print("DB connected!")

        query = """
            SELECT *
            FROM tblScoreMaster
            WHERE scrM_tmMIdBattingName IN (?, ?)
            AND scrM_tmMIdBowlingName IN (?, ?)
            AND scrM_IsValidBall = 1
        """

        df = pd.read_sql(query, conn, params=[team1, team2, team1, team2])
        conn.close()

        print(f"Loaded {len(df)} rows from DB.")
        return df
    except Exception as e:
        print("❌ DB error:", e)
        return pd.DataFrame()


def get_match_format_by_tournament(tournament_id):
    try:
        conn = get_connection()
        result = pd.read_sql("""
            SELECT z.z_Name AS format
            FROM tblTournaments t
            JOIN tblZ z ON t.trnM_MatchFormat_z = z.z_Id
            WHERE t.trnM_Id = ?
        """, conn, params=[tournament_id])
        conn.close()
        return result.iloc[0]['format'] if not result.empty else None
    except Exception as e:
        print("Match format fetch error:", e)
        return None


# ✅ Get teams based on selected tournament (year-specific)
def get_teams_by_tournament(tournament_id):
    try:
        conn = get_connection()
        query = """
            SELECT DISTINCT scrM_tmMIdBattingName 
            FROM tblScoreMaster 
            WHERE scrM_TrnMId = ? AND scrM_tmMIdBattingName IS NOT NULL
        """
        df = pd.read_sql(query, conn, params=[tournament_id])
        conn.close()
        print(f"✅ Teams fetched for tournament {tournament_id}: {len(df)}")
        return sorted(df['scrM_tmMIdBattingName'].dropna().unique())
    except Exception as e:
        print("❌ Teams by tournament error:", e)
        return []


# ✅ Get matches by selected tournament only (when team not yet chosen)
def get_matches_by_tournament(tournament_id):
    try:
        conn = get_connection()
        query = """
            SELECT DISTINCT scrM_MatchName
            FROM tblScoreMaster
            WHERE scrM_TrnMId = ? AND scrM_MatchName IS NOT NULL
            ORDER BY scrM_MatchName
        """
        df = pd.read_sql(query, conn, params=[tournament_id])
        conn.close()
        print(f"✅ Matches for tournament {tournament_id}: {len(df)}")
        return sorted(df['scrM_MatchName'].dropna().unique())
    except Exception as e:
        print("❌ Matches by tournament error:", e)
        return []


# ✅ Get matches based on selected tournament and team (year-filtered)
def get_matches_by_tournament_and_team(tournament_id, team_name):
    try:
        conn = get_connection()
        query = """
            SELECT DISTINCT scrM_MatchName
            FROM tblScoreMaster
            WHERE scrM_TrnMId = ?
              AND (scrM_tmMIdBattingName = ? OR scrM_tmMIdBowlingName = ?)
              AND scrM_MatchName IS NOT NULL
            ORDER BY scrM_MatchName
        """
        df = pd.read_sql(query, conn, params=[tournament_id, team_name, team_name])
        conn.close()
        print(f"✅ Matches for team {team_name} in tournament {tournament_id}: {len(df)}")
        return sorted(df['scrM_MatchName'].dropna().unique())
    except Exception as e:
        print("❌ Matches by tournament and team error:", e)
        return []


# ✅ (Optional fallback) Get matches by team across all tournaments
def get_matches_by_team(team):
    try:
        conn = get_connection()
        query = """
            SELECT DISTINCT scrM_MatchName 
            FROM tblScoreMaster 
            WHERE (scrM_tmMIdBattingName = ? OR scrM_tmMIdBowlingName = ?)
              AND scrM_MatchName IS NOT NULL
            ORDER BY scrM_MatchName
        """
        df = pd.read_sql(query, conn, params=[team, team])
        conn.close()
        print(f"✅ Matches by team {team}: {len(df)}")
        return sorted(df['scrM_MatchName'].dropna().unique())
    except Exception as e:
        print("❌ Matches by team error:", e)
        return []

 
def get_days_innings_sessions_by_matches(matches):
    try:
        if not matches:
            return [], [], []

        placeholders = ",".join("?" for _ in matches)
        query = f"""
            SELECT DISTINCT scrM_DayNo, scrM_InningNo, scrM_SessionNo
            FROM tblScoreMaster
            WHERE scrM_MatchName IN ({placeholders})
        """
        
        conn = get_connection()

        df = pd.read_sql(query, conn, params=matches)
        conn.close()

        days = sorted(df['scrM_DayNo'].dropna().unique(), key=lambda x: int(x))
        innings = sorted(df['scrM_InningNo'].dropna().unique(), key=lambda x: int(x))
        sessions = sorted(df['scrM_SessionNo'].dropna().unique(), key=lambda x: int(x))


        return days, innings, sessions

    except Exception as e:
        print("Error getting days/innings/sessions:", e)
        return [], [], []




def get_players_by_match(matches, day=None, inning=None, session=None):
    if not matches:
        return [], []

    try:
        conn = get_connection()

        placeholders = ','.join(['?'] * len(matches))
        query = f"""
            SELECT DISTINCT scrM_PlayMIdStrikerName AS Batter,
                            scrM_PlayMIdBowlerName AS Bowler
            FROM tblScoreMaster
            WHERE scrM_MatchName IN ({placeholders})
              AND scrM_PlayMIdStrikerName IS NOT NULL
              AND scrM_PlayMIdBowlerName IS NOT NULL
        """
        params = list(matches)

        if day:
            query += " AND scrM_DayNo = ?"
            params.append(int(day))
        if inning:
            query += " AND scrM_InningNo = ?"
            params.append(int(inning))
        if session:
            query += " AND scrM_SessionNo = ?"
            params.append(int(session))

        df = pd.read_sql(query, conn, params=params)
        conn.close()

        # ✅ Normalize names by stripping whitespace
        df["Batter"] = df["Batter"].str.strip()
        df["Bowler"] = df["Bowler"].str.strip()

        batters = sorted(df["Batter"].dropna().unique().tolist())
        bowlers = sorted(df["Bowler"].dropna().unique().tolist())
        return batters, bowlers

    except Exception as e:
        print("get_players_by_match Error:", e)
        return [], []




def get_filtered_score_data(
    conn, match_names, batters=None, bowlers=None, inning=None,
    session=None, day=None, phase=None, from_over=None, to_over=None,
    type=None, ball_phase=None
):
    """
    Fetch score rows filtered by provided match names and optional filters.
    FIX APPLIED:
      - Removed scrM_IsValidBall = 1 so wides/noballs are included.
      - Added safety normalization for IsValidBall.
    Returns a pandas DataFrame.
    """
    import pandas as pd

    # defensive: must have at least one match
    if not match_names:
        print("get_filtered_score_data: no match_names provided - returning empty DataFrame")
        return pd.DataFrame()

    # -------------------------------
    # BASE QUERY (NO VALID BALL FILTER)
    # -------------------------------
    placeholders = ",".join(["?"] * len(match_names))
    query = f"""
    SELECT *
    FROM tblScoreMaster
    WHERE scrM_MatchName IN ({placeholders})
    """
    params = list(match_names)

    # -------------------------------
    # OPTIONAL FILTERS
    # -------------------------------
    if batters:
        q = ",".join(["?"] * len(batters))
        query += f" AND scrM_PlayMIdStrikerName IN ({q})"
        params.extend(batters)

    if bowlers:
        q = ",".join(["?"] * len(bowlers))
        query += f" AND scrM_PlayMIdBowlerName IN ({q})"
        params.extend(bowlers)

    if inning is not None and str(inning).strip() != "":
        query += " AND scrM_InningNo = ?"
        params.append(int(inning))

    if session is not None and str(session).strip() != "":
        query += " AND scrM_SessionNo = ?"
        params.append(int(session))

    if day is not None and str(day).strip() != "":
        query += " AND scrM_DayNo = ?"
        params.append(int(day))

    if from_over is not None and to_over is not None and str(from_over).strip() != "" and str(to_over).strip() != "":
        query += " AND scrM_OverNo BETWEEN ? AND ?"
        params.extend([int(from_over), int(to_over)])

    # -------------------------------
    # EXECUTE QUERY
    # -------------------------------
    try:
        print("get_filtered_score_data: executing SQL with params:", params[:20], " (truncated if long)")
        df = pd.read_sql(query, conn, params=params)
    except Exception as e:
        print("get_filtered_score_data: SQL execution failed:", e)
        return pd.DataFrame()

    if df.empty:
        print("get_filtered_score_data: returned empty DataFrame")
        return df

    # -------------------------------
    # POST PROCESSING
    # -------------------------------
    
    # ✅ Normalize player names by stripping whitespace
    if 'scrM_PlayMIdStrikerName' in df.columns:
        df['scrM_PlayMIdStrikerName'] = df['scrM_PlayMIdStrikerName'].str.strip()
    if 'scrM_PlayMIdBowlerName' in df.columns:
        df['scrM_PlayMIdBowlerName'] = df['scrM_PlayMIdBowlerName'].str.strip()

    # Alias used by other functions
    if "scrM_MatchName" in df.columns:
        df["MatchName"] = df["scrM_MatchName"]
    else:
        df["MatchName"] = ""

    # Ensure scrM_IsValidBall exists & numeric for downstream logic
    if "scrM_IsValidBall" in df.columns:
        try:
            df["scrM_IsValidBall"] = pd.to_numeric(df["scrM_IsValidBall"], errors="coerce").fillna(0).astype(int)
        except Exception:
            pass
    else:
        df["scrM_IsValidBall"] = 1   # default if not present

    # Normalize numeric fields
    for col in ("scrM_OverNo", "scrM_DelNo", "scrM_InningNo", "scrM_BatsmanRuns"):
        if col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
            except Exception:
                pass

    # Compute ball_index (OverNo - 1)*6 + DelNo
    if "scrM_OverNo" in df.columns and "scrM_DelNo" in df.columns:
        try:
            df["ball_index"] = (df["scrM_OverNo"] - 1) * 6 + df["scrM_DelNo"]
        except Exception:
            df["ball_index"] = None
    else:
        df["ball_index"] = None

    return df





import pandas as pd
import pyodbc

def get_ball_by_ball_details(
    match_names, batters=None, bowlers=None, inning=None,
    session=None, day=None, from_over=None, to_over=None
):
    """
    Fetch ball-by-ball details from SQL Server with optional filters.
    Uses scrM_VideoXFileName columns for offline video playback
    instead of scrM_VideoXURL, and adds commentary text.
    """

    if not match_names:
        return pd.DataFrame()

    conn = None
    try:
        conn = get_connection()

        # Base query
        match_placeholders = ','.join(['?'] * len(match_names))
        query = f"""
            SELECT
                s.scrM_MatchName,
                s.scrM_DayNo,
                s.scrM_SessionNo,
                s.scrM_InningNo,
                s.scrM_OverNo,
                s.scrM_DelNo,
                s.scrM_DelId,
                s.scrM_PlayMIdStrikerName,
                s.scrM_PlayMIdBowlerName,
                s.scrM_BatsmanRuns,
                s.scrM_DecisionFinal_zName,
                s.scrM_DecisionFinal_z,
                s.scrM_ShotType_zName,
                s.scrM_DeliveryType_zName,
                s.scrM_WagonArea_zName,
                s.scrM_PitchX,
                s.scrM_PitchY,
                s.scrM_PitchXPos AS scrM_Line,
                s.scrM_PitchYPos AS scrM_Length,
                s.scrM_IsNoBall,
                s.scrM_IsWideBall,
                s.scrM_LegByeRuns,
                s.scrM_IsBoundry,
                s.scrM_IsSixer,
                s.scrM_IsWicket,
                s.scrM_PlayMIdWicket,
                s.scrM_PlayMIdWicketName,
                s.scrM_playMIdCaughtName,
                s.scrM_PlayMIdFielderName,
                s.scrM_playMIdRunOutName,
                s.scrM_playMIdStumpingName,
                s.scrM_FieldingType_z,
                s.scrM_FieldingType_zName,
                s.scrM_Video1FileName,
                s.scrM_Video2FileName,
                s.scrM_Video3FileName,
                s.scrM_Video4FileName,
                s.scrM_Video5FileName,
                s.scrM_Video6FileName,
                s.scrM_tmMIdBattingName,
                s.scrM_tmMIdBowlingName,
                s.scrM_IsBeaten,
                s.scrM_IsUncomfort,
                s.scrM_StrikerBatterSkill,
                s.scrM_BowlerSkill,
                s.scrM_RunsSavedOrGiven
            FROM tblScoreMaster s
            WHERE s.scrM_MatchName IN ({match_placeholders})
        """

        # Collect params
        params = list(match_names)

        # Apply filters
        if batters:
            query += " AND s.scrM_PlayMIdStrikerName IN ({})".format(
                ','.join(['?'] * len(batters))
            )
            params.extend(batters)

        if bowlers:
            query += " AND s.scrM_PlayMIdBowlerName IN ({})".format(
                ','.join(['?'] * len(bowlers))
            )
            params.extend(bowlers)

        if inning:
            query += " AND s.scrM_InningNo = ?"
            params.append(inning)

        if session:
            query += " AND s.scrM_SessionNo = ?"
            params.append(session)

        if day:
            query += " AND s.scrM_DayNo = ?"
            params.append(day)

        if from_over and to_over:
            query += " AND s.scrM_OverNo BETWEEN ? AND ?"
            params.extend([int(from_over), int(to_over)])

        # Order by ball sequence
        query += " ORDER BY s.scrM_InningNo, s.scrM_OverNo, s.scrM_DelNo"

        # Run query safely with params
        df = pd.read_sql(query, conn, params=params)

        # ✅ Normalize player names by stripping whitespace
        if not df.empty:
            if 'scrM_PlayMIdStrikerName' in df.columns:
                df['scrM_PlayMIdStrikerName'] = df['scrM_PlayMIdStrikerName'].str.strip()
            if 'scrM_PlayMIdBowlerName' in df.columns:
                df['scrM_PlayMIdBowlerName'] = df['scrM_PlayMIdBowlerName'].str.strip()
        
        # Add commentary column using updated generate_commentary
        if not df.empty:
            df["commentary"] = df.apply(generate_commentary, axis=1)

        return df

    except Exception as e:
        print(f"Error in get_ball_by_ball_details: {e}")
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
#             FROM tblScoreMaster
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
#             FROM tblScoreMaster
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
#             FROM tblTournaments t
#             LEFT JOIN tblZ z ON t.trnM_MatchFormat_z = z.z_Id
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
#             FROM tblTournaments t
#             JOIN tblZ z ON t.trnM_MatchFormat_z = z.z_Id
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
#             FROM tblScoreMaster 
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
#             FROM tblScoreMaster 
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
#             FROM tblScoreMaster
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
#             FROM tblScoreMaster
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
#         "SELECT mchM_Id, mchM_MatchName FROM tblMatchMaster WHERE mchM_MatchName IN ({})".format(
#             ','.join(['?'] * len(match_names))
#         ),
#         conn, params=match_names
#     )
#     match_id_map = dict(zip(match_df['mchM_MatchName'], match_df['mchM_Id']))
#     match_ids = list(match_id_map.values())

#     # 🔍 Base query
#     query = """
#     SELECT * FROM tblScoreMaster
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
    """Generates a descriptive commentary string for a given ball without embedding video play icons."""

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
        # Keep OUT highlighted but no icons
        commentary += f', <span class="text-red-500 font-semibold">{wicket_details}</span>'
    else:
        if runs == 1:
            commentary += ", 1 run"
        elif runs > 1:
            commentary += f", {runs} runs"
        else:
            commentary += ", no run"

    # 🚫 REMOVED: Video play button HTML injection
    # We now rely solely on the runs oval button to open the advanced video player.

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










def generate_line_length_report(df):
    """
    Generates Line & Length heatmap + table + pitch_points.
    pitch_points now ONLY includes fields needed for offline video lookup,
    i.e., scrM_DelId + match + inning info. No video filename dependency.
    """
    if df.empty or 'scrM_PitchX' not in df.columns or 'scrM_PitchY' not in df.columns:
        return None

    df_ll = df.copy()

    # Clean coordinates
    df_ll['scrM_PitchX'] = pd.to_numeric(df_ll['scrM_PitchX'], errors='coerce')
    df_ll['scrM_PitchY'] = pd.to_numeric(df_ll['scrM_PitchY'], errors='coerce')
    df_ll = df_ll.dropna(subset=['scrM_PitchX', 'scrM_PitchY'])
    # Remove balls where both X and Y are 0 (garbage values)
    df_ll = df_ll[~((df_ll['scrM_PitchX'] == 0) & (df_ll['scrM_PitchY'] == 0))]
    if df_ll.empty:
        return None

    # Line Zones
    line_bins = [-float('inf'), 50, 70, 80, 84, 88, 95, float('inf')]
    line_labels = ['Way Outside Off', 'Outside Off', 'Just Outside Off',
                   'Off Stump', 'Middle Stump', 'Leg Stump', 'Outside Leg']


    # Length Zones (adjusted to match actual image zones)
    # Image Y: Fulltoss 0-186, Yorker 186-215, Full Length 215-256, Overpitch 256-301,
    # Good Length 301-354, Short of Good 354-410, Short Pitch 410+
    # Data is scaled: image height 560px, data height 280 (so divide by 2)
    length_bins = [
        -float('inf'),
        93,    # 186/2, Fulltoss ends, Yorker starts
        107.5, # 215/2, Yorker ends, Full Length starts
        128,   # 256/2, Full Length ends, Overpitch starts
        150.5, # 301/2, Overpitch ends, Good Length starts
        177,   # 354/2, Good Length ends, Short of Good starts
        205,   # 410/2, Short of Good ends, Short Pitch starts
        float('inf')
    ]
    length_labels = [
        'Fulltoss',
        'Yorker',
        'Full Length',
        'Overpitch',
        'Good Length',
        'Short of Good',
        'Short Pitch'
    ]
    length_labels = [
        'Fulltoss',
        'Yorker',
        'Full Length',
        'Overpitch',
        'Good Length',
        'Short of Good',
        'Short Pitch'
    ]

    df_ll['LineZone'] = pd.cut(df_ll['scrM_PitchX'], bins=line_bins, labels=line_labels, right=False)
    df_ll['LengthZone'] = pd.cut(df_ll['scrM_PitchY'], bins=length_bins, labels=length_labels, right=False)
    df_ll['LengthZone'] = pd.Categorical(df_ll['LengthZone'], categories=length_labels, ordered=True)
    df_ll['Zone'] = df_ll['LengthZone'].astype(str) + '-' + df_ll['LineZone'].astype(str)

    # Boundary & wicket counts
    df_ll['fours'] = (df_ll.get('scrM_IsBoundry', 0) == 1).astype(int)
    df_ll['sixes'] = (df_ll.get('scrM_IsSixer', 0) == 1).astype(int)
    df_ll['boundaries'] = df_ll['fours'] + df_ll['sixes']

    total_balls = len(df_ll)
    total_runs = df_ll.get('scrM_BatsmanRuns', 0).sum()
    total_boundaries = df_ll['boundaries'].sum()
    total_wickets = df_ll.get('scrM_IsWicket', 0).sum()

    zone_summary = df_ll.groupby('Zone').agg(
        balls=('Zone', 'count'),
        runs=('scrM_BatsmanRuns', 'sum'),
        boundaries=('boundaries', 'sum'),
        wickets=('scrM_IsWicket', 'sum')
    ).reset_index()

    if total_balls > 0:
        zone_summary['balls_percentage'] = (zone_summary['balls'] / total_balls * 100)
    else:
        zone_summary['balls_percentage'] = 0

    # Heatmap output structure
    heatmap_data = {}
    all_zones = [f"{length}-{line}" for length in length_labels for line in line_labels]
    for zone in all_zones:
        heatmap_data[zone] = {'balls': 0, 'runs': 0, 'boundaries': 0, 'wickets': 0, 'balls_percentage': 0.0}

    for _, row in zone_summary.iterrows():
        z = row['Zone']
        if z in heatmap_data:
            heatmap_data[z] = {
                'balls': int(row['balls']),
                'runs': int(row['runs']),
                'boundaries': int(row['boundaries']),
                'wickets': int(row['wickets']),
                'balls_percentage': round(row['balls_percentage'], 1)
            }

    table_data = [{
        'Zone': z,
        'balls': d['balls'],
        'runs': d['runs'],
        'boundaries': d['boundaries'],
        'wickets': d['wickets'],
        'balls_percentage': d['balls_percentage']
    } for z, d in heatmap_data.items()]

    totals = {
        'balls': int(total_balls),
        'runs': int(total_runs),
        'boundaries': int(total_boundaries),
        'wickets': int(total_wickets)
    }

    # ✅ Include only fields required for *offline video matching*
    pitch_point_cols = [
        'scrM_PitchX', 'scrM_PitchY',
        'scrM_BatsmanRuns', 'scrM_IsWicket',
        'scrM_StrikerBatterSkill', 'scrM_BowlerSkill',
        'scrM_DelId', 'scrM_MatchName', 'scrM_InningNo',
        'scrM_PlayMIdStriker', 'scrM_PlayMIdBowler',
        'Zone', 'LineZone', 'LengthZone'
    ]
    pitch_point_cols = [c for c in pitch_point_cols if c in df_ll.columns]

    # Add zone labels to each pitch point for frontend sync
    pitch_points = df_ll[pitch_point_cols].to_dict(orient='records')

    return {
        'heatmap_data': heatmap_data,
        'table_data': table_data,
        'totals': totals,
        'pitch_points': pitch_points
    }






def generate_areawise_report(df):
    """
    Area-wise report for batter shot regions.
    Fully offline video logic: NO scrM_Video*FileName usage.
    Uses scrM_DelId + scrM_MatchName + scrM_InningNo to play clips.
    """
    import pandas as pd

    if df.empty or 'scrM_WagonArea_zName' not in df.columns:
        return None

    df_area = df.copy()
    df_area = df_area.dropna(subset=['scrM_WagonArea_zName'])
    df_area = df_area[df_area['scrM_WagonArea_zName'].str.strip() != '']

    if df_area.empty:
        return None

    # Normal scoring tags
    df_area['ones']  = (df_area['scrM_BatsmanRuns'] == 1).astype(int)
    df_area['twos']  = (df_area['scrM_BatsmanRuns'] == 2).astype(int)
    df_area['fours'] = (df_area['scrM_IsBoundry'] == 1).astype(int)
    df_area['sixes'] = (df_area['scrM_IsSixer'] == 1).astype(int)

    # =======================
    # CHART DATA (unchanged)
    # =======================
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
            {'name': 'Ones',  'data': area_summary['ones'].tolist()},
            {'name': 'Twos',  'data': area_summary['twos'].tolist()},
            {'name': 'Fours', 'data': area_summary['fours'].tolist()},
            {'name': 'Sixes', 'data': area_summary['sixes'].tolist()}
        ]
    }

    # ============================
    # TABLE DATA WITH DELIVERY IDs
    # ============================
    # group by: Batter + Area + Match + Inning
    group_cols = [
        'scrM_PlayMIdStrikerName',
        'scrM_WagonArea_zName',
        'scrM_MatchName',
        'scrM_InningNo'
    ]

    grouped = df_area.groupby(group_cols).agg(
        runs=('scrM_BatsmanRuns', 'sum'),
        balls=('scrM_WagonArea_zName', 'count'),
        fours=('fours', 'sum'),
        sixes=('sixes', 'sum'),
        ones=('ones', 'sum'),
        twos=('twos', 'sum'),
        delivery_ids=('scrM_DelId', lambda x: [int(v) for v in x.dropna().tolist() if int(v) > 0])
    ).reset_index()

    if grouped.empty:
        return {'chart_data': chart_data, 'strikers_data': {}}

    # strike rate
    grouped['strike_rate'] = (grouped['runs'] / grouped['balls'] * 100).round(2)

    # rename fields for UI
    grouped.rename(columns={
        'scrM_PlayMIdStrikerName': 'striker',
        'scrM_WagonArea_zName': 'area_name',
        'scrM_MatchName': 'match_id',
        'scrM_InningNo': 'inning_id'
    }, inplace=True)

    # organize nested dict by striker (same UI structure as before)
    strikers_data = {}
    for striker_name, gdf in grouped.groupby('striker'):
        strikers_data[striker_name] = gdf.to_dict(orient='records')

    return {
        'chart_data': chart_data,
        'strikers_data': strikers_data
    }



def generate_shottype_report(df):
    """
    Shot Type report with OFFLINE playback support.
    Does NOT use scrM_Video*FileName.
    Uses scrM_DelId + scrM_MatchName + scrM_InningNo to fetch local videos.
    """
    import pandas as pd

    if df.empty or 'scrM_ShotType_zName' not in df.columns:
        return None

    df_st = df.copy()
    df_st = df_st.dropna(subset=['scrM_ShotType_zName'])
    df_st = df_st[df_st['scrM_ShotType_zName'].str.strip() != '']

    if df_st.empty:
        return None

    # Score breakdown
    df_st['ones']  = (df_st['scrM_BatsmanRuns'] == 1).astype(int)
    df_st['twos']  = (df_st['scrM_BatsmanRuns'] == 2).astype(int)
    df_st['fours'] = (df_st['scrM_IsBoundry'] == 1).astype(int)
    df_st['sixes'] = (df_st['scrM_IsSixer'] == 1).astype(int)

    # For charts
    shottype_summary = df_st.groupby('scrM_ShotType_zName').agg(
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
            {'name': 'Ones',  'data': shottype_summary['ones'].tolist()},
            {'name': 'Twos',  'data': shottype_summary['twos'].tolist()},
            {'name': 'Fours', 'data': shottype_summary['fours'].tolist()},
            {'name': 'Sixes', 'data': shottype_summary['sixes'].tolist()},
        ]
    }

    # ✅ MAIN CHANGE — group WITH match + inning + delivery IDs
    grouped = df_st.groupby(
        ['scrM_PlayMIdStrikerName', 'scrM_ShotType_zName', 'scrM_MatchName', 'scrM_InningNo']
    ).agg(
        runs=('scrM_BatsmanRuns', 'sum'),
        balls=('scrM_ShotType_zName', 'count'),
        fours=('fours', 'sum'),
        sixes=('sixes', 'sum'),
        ones=('ones', 'sum'),
        twos=('twos', 'sum'),
        delivery_ids=('scrM_DelId', lambda x: [int(v) for v in x.dropna().tolist() if int(v) > 0])
    ).reset_index()

    if grouped.empty:
        return {'chart_data': chart_data, 'strikers_data': {}}

    grouped = grouped[grouped['runs'] > 0].copy()
    grouped['strike_rate'] = (grouped['runs'] / grouped['balls'] * 100).round(2)

    grouped.rename(columns={
        'scrM_PlayMIdStrikerName': 'striker',
        'scrM_ShotType_zName': 'shot_type',
        'scrM_MatchName': 'match_id',
        'scrM_InningNo': 'inning_id'
    }, inplace=True)

    # Convert into dict grouped by batter
    strikers_data = {}
    for name, gdf in grouped.groupby('striker'):
        strikers_data[name] = gdf.to_dict(orient='records')

    return {
        'chart_data': chart_data,
        'strikers_data': strikers_data
    }



def generate_deliverytype_report(df):
    import pandas as pd

    if df.empty:
        return None

    # -------- Coalesce delivery type safely ----------
    # Preferred column, then fallback, finally "Unknown"
    def _clean_series(s):
        return s.fillna("").astype(str).str.strip()

    dt = _clean_series(df.get('scrM_DeliveryType_zName', pd.Series(index=df.index)))
    bt = _clean_series(df.get('scrM_BallType_zName', pd.Series(index=df.index)))
    coalesced = dt.where(dt.ne(""), bt)
    coalesced = coalesced.where(coalesced.ne(""), "Unknown")

    df_dt = df.copy()
    df_dt['DeliveryTypeName'] = coalesced

    # score tags (same as other tabs)
    df_dt['ones']  = (df_dt['scrM_BatsmanRuns'] == 1).astype(int)
    df_dt['twos']  = (df_dt['scrM_BatsmanRuns'] == 2).astype(int)
    df_dt['fours'] = (df_dt['scrM_IsBoundry'] == 1).astype(int)
    df_dt['sixes'] = (df_dt['scrM_IsSixer'] == 1).astype(int)

    # -------- Main chart (hide zero-only labels later via JS if you like) ----------
    summary = df_dt.groupby('DeliveryTypeName').agg(
        total_runs=('scrM_BatsmanRuns', 'sum'),
        ones=('ones', 'sum'),
        twos=('twos', 'sum'),
        fours=('fours', 'sum'),
        sixes=('sixes', 'sum')
    ).reset_index()

    # keep rows even if all zero; UI still renders and filters work
    chart_data = {
        'labels': summary['DeliveryTypeName'].tolist(),
        'series': [
            {'name': 'Ones',  'data': summary['ones'].tolist()},
            {'name': 'Twos',  'data': summary['twos'].tolist()},
            {'name': 'Fours', 'data': summary['fours'].tolist()},
            {'name': 'Sixes', 'data': summary['sixes'].tolist()},
        ]
    }

    # -------- Table by striker (with delivery_ids + match/inning) ----------
    grouped = df_dt.groupby(
        ['scrM_PlayMIdStrikerName', 'DeliveryTypeName', 'scrM_MatchName', 'scrM_InningNo']
    ).agg(
        runs=('scrM_BatsmanRuns', 'sum'),
        balls=('DeliveryTypeName', 'count'),
        fours=('fours', 'sum'),
        sixes=('sixes', 'sum'),
        ones=('ones', 'sum'),
        twos=('twos', 'sum'),
        delivery_ids=('scrM_DelId', lambda x: [int(v) for v in pd.to_numeric(x, errors='coerce').dropna().astype(int).tolist()])
    ).reset_index()

    # compute SR; keep zero-run rows so filters still show the bucket
    grouped['strike_rate'] = (grouped['runs'] / grouped['balls'] * 100).round(2).replace([pd.NA, pd.NaT], 0)

    grouped.rename(columns={
        'scrM_PlayMIdStrikerName': 'striker',
        'DeliveryTypeName': 'delivery_type',
        'scrM_MatchName': 'match_id',
        'scrM_InningNo': 'inning_id'
    }, inplace=True)

    strikers_data = { name: g.to_dict(orient='records')
                      for name, g in grouped.groupby('striker') }

    return {'chart_data': chart_data, 'strikers_data': strikers_data}






#-------------------------------------------------------##-------------------------------------------------------##-------------------------------------------------------#
#-------------------------------------------------------##---------- Player Vs Player Report Shreyas-------------##-------------------------------------------------------#
#-------------------------------------------------------##-------------------------------------------------------##-------------------------------------------------------#

import pandas as pd
import json


def render_kpi_table(title, df, collapse_map=None):
    """
    Render KPI table. For batters, plays videos filtered by batter only.
    For bowlers, plays videos filtered by bowler only.
    """
    from urllib.parse import quote
    from markupsafe import Markup

    if df is None or df.empty:
        return Markup(f"<div class='text-red-500 text-center text-xl'>No data available for {title}.</div>")

    # Detect mode
    is_batter_mode = "S/R" in df.columns or ("Runs" in df.columns and "Eco" not in df.columns)
    is_bowler_mode = "Eco" in df.columns or "Wkts" in df.columns

    # Filter out unnecessary technical columns
    data_cols = [
        col for col in df.columns
        if col not in (
            "scrM_MatchName", "scrM_DelId",
            "scrM_InningNo", "scrM_PlayMIdStriker", "scrM_PlayMIdBowler"
        )
    ]
    headers = data_cols + ["Videos"]
    rows = df.to_dict(orient="records")

    def make_play_link(match_name, delivery_id, inning_val="", batter_val="", bowler_val=""):
        """Generate proper video link based on mode."""
        if not match_name or not delivery_id:
            return "<span class='text-gray-400 text-xl'>-</span>"

        qs = [("match_id", match_name)]
        qs.append(("inning_id", str(inning_val)))
        qs.append(("delivery_id", str(delivery_id)))

        if is_batter_mode and batter_val:
            qs.append(("batter_id", str(batter_val)))
        elif is_bowler_mode and bowler_val:
            qs.append(("bowler_id", str(bowler_val)))

        qstr = "&".join(f"{quote(k)}={quote(v)}" for k,v in qs)

        return (
            f'<a href="/video_player?{qstr}" target="_blank" class="inline-block mx-0.5">'
            f'<img src="/static/video-fill_1.png" alt="Play" '
            f'class="w-7 h-7 inline-block hover:scale-125 transition-transform brightness-125"/>'
            f'</a>'
        )

    html = []
    html.append("""
    <div class="w-full xl:w-1/2">
      <div class="overflow-x-auto rounded-md border border-slate-200 dark:border-zink-600">
        <table class="w-full text-xl bg-custom-50 dark:bg-custom-500/10 min-w-[500px]">
          <thead class="bg-custom-100 dark:bg-custom-500/10"><tr>
    """)

    for i, col in enumerate(headers):
        align = "text-left" if i == 0 else "text-center"
        html.append(
            f"<th class='px-3 py-3 text-2xl font-semibold border-b border-custom-200 dark:border-custom-900 {align}'>{col}</th>"
        )

    html.append("</tr></thead><tbody>")

    for idx, row in enumerate(rows):
        collapse_id = f"collapse-{title.replace(' ', '_')}-{idx}"
        html.append(
            f"<tr class='cursor-pointer hover:bg-custom-100 dark:hover:bg-custom-500/20 text-xl'"
            f" onclick=\"toggleCollapse('{collapse_id}')\">"
        )

        for i, col in enumerate(data_cols):
            val = row.get(col, "")
            align = "text-left" if i == 0 else "text-center"
            html.append(
                f"<td class='px-3 py-3 border-y border-custom-200 dark:border-custom-900 {align}'>{val}</td>"
            )

        match_name = str(row.get("scrM_MatchName") or row.get("Match") or row.get("match_name") or "").strip()
        delivery_id = row.get("scrM_DelId") or row.get("DelId") or row.get("delivery_id") or ""
        inning_val = row.get("scrM_InningNo") or row.get("Inning") or row.get("inning_id") or ""
        batter_val = row.get("scrM_PlayMIdStriker") or row.get("BatterId") or ""
        bowler_val = row.get("scrM_PlayMIdBowler") or row.get("BowlerId") or ""

        video_html = (
            make_play_link(match_name, delivery_id, inning_val, batter_val, bowler_val)
            if match_name and delivery_id else "<span class='text-gray-400 text-xl'>-</span>"
        )
        html.append(
            f"<td class='px-3 py-3 border-y border-custom-200 dark:border-custom-900 text-center'>{video_html}</td></tr>"
        )

        # Collapse Row Section
        collapse_key = f"{match_name}__{inning_val}"
        collapse_html = (collapse_map.get(collapse_key) if collapse_map else "") or \
                        '<div class="text-red-500 text-xl">[No PvP Data]</div>'

        html.append(f"""
        <tr id="{collapse_id}" class="hidden bg-custom-50 dark:bg-custom-500/10">
          <td colspan="{len(headers)}" class="p-4 border-b border-custom-200 dark:border-custom-900 text-xl text-slate-700 dark:text-zink-200">
            {collapse_html}
          </td>
        </tr>
        """)

    html.append("</tbody></table></div></div>")
    html.append("""
    <script>
      function toggleCollapse(id){
        const row=document.getElementById(id);
        if(row) row.classList.toggle('hidden');
      }
    </script>
    """)
    return Markup("".join(html))







import pandas as pd
import numpy as np

def generate_kpi_tables(df, selected_type, collapse_map=None):
    import pandas as pd

    if df.empty:
        return {"No Data": "<p>No data found</p>"}

    tables = {}

    # ============================================
    # 🚹 BATTER MODE — KEEP WD/NB AS IT IS
    # ============================================
    if selected_type == "batter":
        for batter in df["scrM_PlayMIdStrikerName"].dropna().unique():
            player_df = df[df["scrM_PlayMIdStrikerName"] == batter]
            summary = {
                "scrM_MatchName": [],
                "scrM_DelId": [],
                "scrM_InningNo": [],
                "scrM_PlayMIdStriker": [],
                "scrM_PlayMIdBowler": [],
                "Match": [], "Inns": [], "Runs": [], "Balls": [], "S/R": [],
                "Dots": [], "1s": [], "2s": [], "3s": [], "Fours": [], "Sixers": []
            }

            for (match_name, inning_no), group in player_df.groupby(["scrM_MatchName", "scrM_InningNo"]):
                runs = group["scrM_BatsmanRuns"].sum()
                balls = len(group)
                sr = round((runs / balls) * 100, 2) if balls else 0

                dots = (group["scrM_BatsmanRuns"] == 0).sum()
                singles = (group["scrM_BatsmanRuns"] == 1).sum()
                doubles = (group["scrM_BatsmanRuns"] == 2).sum()
                triples = (group["scrM_BatsmanRuns"] == 3).sum()
                fours = (group["scrM_BatsmanRuns"] == 4).sum()
                sixers = (group["scrM_BatsmanRuns"] == 6).sum()

                first_ball = group.iloc[0]
                summary["scrM_MatchName"].append(match_name)
                summary["scrM_DelId"].append(first_ball.get("scrM_DelId"))
                summary["scrM_InningNo"].append(inning_no)
                summary["scrM_PlayMIdStriker"].append(first_ball.get("scrM_PlayMIdStriker"))
                summary["scrM_PlayMIdBowler"].append(first_ball.get("scrM_PlayMIdBowler"))

                summary["Match"].append(match_name)
                summary["Inns"].append(inning_no)
                summary["Runs"].append(runs)
                summary["Balls"].append(balls)
                summary["S/R"].append(sr)
                summary["Dots"].append(dots)
                summary["1s"].append(singles)
                summary["2s"].append(doubles)
                summary["3s"].append(triples)
                summary["Fours"].append(fours)
                summary["Sixers"].append(sixers)

            df_table = pd.DataFrame(summary)

            # 🔹 Batter mode → DO NOT REMOVE WD/NB (your requirement)
            tables[batter] = render_kpi_table(batter, df_table, collapse_map)

    # ============================================
    # 🎯 BOWLER MODE — REMOVE WD/NB FROM KPI
    # ============================================
    elif selected_type == "bowler":
        for bowler in df["scrM_PlayMIdBowlerName"].dropna().unique():
            player_df = df[df["scrM_PlayMIdBowlerName"] == bowler]
            summary = {
                "scrM_MatchName": [],
                "scrM_DelId": [],
                "scrM_InningNo": [],
                "scrM_PlayMIdStriker": [],
                "scrM_PlayMIdBowler": [],
                "Match": [], "Inns": [], "Overs": [], "Runs": [], "Wkts": [], "Eco": [],
                "Dots": [], "1s": [], "2s": [], "3s": [], "Fours": [], "Sixers": [],
                "WD": [], "NB": []   # WD & NB still calculated here
            }

            for (match_name, inning_no), group in player_df.groupby(["scrM_MatchName", "scrM_InningNo"]):
                # ✅ Count only valid balls (exclude wides and no-balls)
                if 'scrM_IsValidBall' in group.columns:
                    valid_balls = (group['scrM_IsValidBall'] == 1).sum()
                else:
                    # Fallback: exclude wides and no-balls manually
                    valid_balls = ((group.get('scrM_IsWideBall', 0) == 0) & (group.get('scrM_IsNoBall', 0) == 0)).sum()
                
                overs = valid_balls // 6 + (valid_balls % 6) / 10
                runs = group["scrM_BatsmanRuns"].sum()
                wkts = group["scrM_IsWicket"].sum()
                eco = round(runs / (overs if overs > 0 else 1), 2)

                dots = (group["scrM_BatsmanRuns"] == 0).sum()
                singles = (group["scrM_BatsmanRuns"] == 1).sum()
                doubles = (group["scrM_BatsmanRuns"] == 2).sum()
                triples = (group["scrM_BatsmanRuns"] == 3).sum()
                fours = (group["scrM_BatsmanRuns"] == 4).sum()
                sixers = (group["scrM_BatsmanRuns"] == 6).sum()
                wides = group["scrM_IsWideBall"].sum()
                noballs = group["scrM_IsNoBall"].sum()

                first_ball = group.iloc[0]
                summary["scrM_MatchName"].append(match_name)
                summary["scrM_DelId"].append(first_ball.get("scrM_DelId"))
                summary["scrM_InningNo"].append(inning_no)
                summary["scrM_PlayMIdStriker"].append(first_ball.get("scrM_PlayMIdStriker"))
                summary["scrM_PlayMIdBowler"].append(first_ball.get("scrM_PlayMIdBowler"))

                summary["Match"].append(match_name)
                summary["Inns"].append(inning_no)
                summary["Overs"].append(round(overs, 1))
                summary["Runs"].append(runs)
                summary["Wkts"].append(wkts)
                summary["Eco"].append(eco)
                summary["Dots"].append(dots)
                summary["1s"].append(singles)
                summary["2s"].append(doubles)
                summary["3s"].append(triples)
                summary["Fours"].append(fours)
                summary["Sixers"].append(sixers)
                summary["WD"].append(wides)
                summary["NB"].append(noballs)

            df_table = pd.DataFrame(summary)

            # 🚨 REMOVE WD & NB from KPI TABLE (Bowler vs Batter only)
            df_table.drop(columns=["WD", "NB"], inplace=True, errors="ignore")

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

            # ✅ Count only valid balls (exclude wides and no-balls)
            if 'scrM_IsValidBall' in player_df.columns:
                valid_balls = (player_df['scrM_IsValidBall'] == 1).sum()
            else:
                # Fallback: exclude wides and no-balls manually
                valid_balls = ((player_df.get('scrM_IsWideBall', 0) == 0) & (player_df.get('scrM_IsNoBall', 0) == 0)).sum()
            
            total_balls = valid_balls  # Use valid balls for percentage calculations
            overs = valid_balls // 6 + (valid_balls % 6) / 10
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

    # ============================
    # BATTER MODE (Batter vs Bowlers)
    # ============================
    if selected_type == "batter":
        selected_batters = batters if batters else df['scrM_PlayMIdStrikerName'].dropna().unique()
        
        for batter in selected_batters:
            player_df = df[df['scrM_PlayMIdStrikerName'] == batter]

            summary = {
                "Match": [], "Inning": [], "Bowler": [], "Wkts": [], "Runs": [], "Balls": [], "SR": [],
                "Dots": [], "1s": [], "2s": [], "3s": [], "Fours": [], "Sixers": [],
                "WD": [], "NB": [], "scrM_MatchName": [],
                "scrM_DelId": [], "scrM_InningNo": [], "scrM_PlayMIdStriker": [], "scrM_PlayMIdBowler": []
            }

            # Group by Match + Inning + Bowler
            for (match_name, inning_no, bowler_name), group in player_df.groupby(
                ['scrM_MatchName', 'scrM_InningNo', 'scrM_PlayMIdBowlerName']
            ):
                
                runs = group['scrM_BatsmanRuns'].sum()
                # ✅ Batter balls: exclude wides only (no-balls count as balls faced)
                if 'scrM_IsWideBall' in group.columns:
                    balls = len(group[group['scrM_IsWideBall'] == 0])
                else:
                    balls = len(group)
                wkts = group['scrM_IsWicket'].sum()

                # Bowler label with skill
                bowler_skill = str(group['scrM_BowlerSkill'].iloc[0]).strip() if 'scrM_BowlerSkill' in group.columns else ""
                if "(" in bowler_skill and ")" in bowler_skill:
                    bowler_skill = bowler_skill[bowler_skill.find("(")+1:bowler_skill.find(")")]
                bowler_display = f"{bowler_name} ({bowler_skill.upper()})" if bowler_skill else bowler_name

                summary["Match"].append(match_name)
                summary["Inning"].append(inning_no)
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

                summary["scrM_MatchName"].append(match_name)
                summary["scrM_DelId"].append(group["scrM_DelId"].iloc[0])
                summary["scrM_InningNo"].append(inning_no)
                summary["scrM_PlayMIdStriker"].append(group["scrM_PlayMIdStriker"].iloc[0])
                summary["scrM_PlayMIdBowler"].append(group["scrM_PlayMIdBowler"].iloc[0])

            # Create DataFrame
            df_final = pd.DataFrame(summary)

            # ❌ REMOVE WD / NB COLUMNS FROM PvP TABLE
            df_final.drop(columns=["WD", "NB"], inplace=True, errors="ignore")

            tables[batter] = df_final

    # ============================
    # BOWLER MODE (Bowler vs Batters)
    # ============================
    elif selected_type == "bowler":
        selected_bowlers = bowlers if bowlers else df['scrM_PlayMIdBowlerName'].dropna().unique()

        for bowler in selected_bowlers:
            player_df = df[df['scrM_PlayMIdBowlerName'] == bowler]

            summary = {
                "Match": [], "Inning": [], "Batter": [], "Wkts": [], "Runs": [], "Balls": [], "Eco": [],
                "Dots": [], "1s": [], "2s": [], "3s": [], "Fours": [], "Sixers": [],
                "WD": [], "NB": [], "scrM_MatchName": [],
                "scrM_DelId": [], "scrM_InningNo": [], "scrM_PlayMIdStriker": [], "scrM_PlayMIdBowler": []
            }

            # Group by Match + Inning + Batter
            for (match_name, inning_no, batter_name), group in player_df.groupby(
                ['scrM_MatchName', 'scrM_InningNo', 'scrM_PlayMIdStrikerName']
            ):

                runs = group['scrM_BatsmanRuns'].sum()
                # ✅ Bowler balls: only count valid balls (exclude wides and no-balls)
                if 'scrM_IsValidBall' in group.columns:
                    valid_balls = len(group[group['scrM_IsValidBall'] == 1])
                else:
                    # Fallback: exclude wides and no-balls manually
                    valid_balls = len(group[(group.get('scrM_IsWideBall', 0) == 0) & (group.get('scrM_IsNoBall', 0) == 0)])
                
                balls = valid_balls
                wkts = group['scrM_IsWicket'].sum()
                
                # Calculate overs from valid balls
                overs = balls // 6 + (balls % 6) / 10
                eco = round(runs / overs, 2) if overs else 0

                batter_skill = str(group['scrM_StrikerBatterSkill'].iloc[0]).strip()
                if "(" in batter_skill and ")" in batter_skill:
                    batter_skill = batter_skill[batter_skill.find("(")+1:batter_skill.find(")")]
                batter_display = f"{batter_name} ({batter_skill.upper()})" if batter_skill else batter_name

                summary["Match"].append(match_name)
                summary["Inning"].append(inning_no)
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

                summary["scrM_MatchName"].append(match_name)
                summary["scrM_DelId"].append(group["scrM_DelId"].iloc[0])
                summary["scrM_InningNo"].append(inning_no)
                summary["scrM_PlayMIdStriker"].append(group["scrM_PlayMIdStriker"].iloc[0])
                summary["scrM_PlayMIdBowler"].append(group["scrM_PlayMIdBowler"].iloc[0])

            # Create DataFrame
            df_final = pd.DataFrame(summary)

            # ❌ REMOVE WD / NB COLUMNS FROM PvP TABLE
            df_final.drop(columns=["WD", "NB"], inplace=True, errors="ignore")

            tables[bowler] = df_final

    return tables






import pandas as pd

from urllib.parse import quote

def render_player_vs_player(title, df):
    """
    Player vs Player — same as KPI font size +3,
    NOW text color style matches KPI.
    """
    from urllib.parse import quote
    from markupsafe import Markup

    if df is None or df.empty:
        return Markup(f"<div class='text-red-500 text-center text-lg'>No data available for {title}.</div>")

    hidden_cols = ["scrM_MatchName","scrM_DelId","scrM_InningNo","scrM_PlayMIdStriker","scrM_PlayMIdBowler"]
    data_cols = [col for col in df.columns if not any(h in col for h in hidden_cols)]
    headers   = data_cols + ["Videos"]


    # ====================== PLAY ICON ======================
    def make_play_link(match_name, inning_val="", batter_val="", bowler_val=""):
        if not match_name or not batter_val:
            return "<span class='text-gray-400'>-</span>"

        from urllib.parse import quote
        qs=[("match_id",str(match_name))]
        if inning_val: qs.append(("inning_id",str(inning_val)))
        if batter_val: qs.append(("batter_id",str(batter_val)))
        if bowler_val: qs.append(("bowler_id",str(bowler_val)))

        qstr="&".join(f"{quote(k)}={quote(v)}" for k,v in qs)

        return(
           f'<a href="/video_player?{qstr}" target="_blank" class="inline-block">'
           f'<img src="/static/video-fill_1.png" class="w-6 h-6 hover:scale-125 transition-transform"/>'
           f'</a>'
        )


    # ======================  TABLE ======================
    html=[]
    html.append("""
    <div class="w-full xl:w-1/2">
      <div class="overflow-x-auto rounded-md border border-slate-200 dark:border-zink-600">
        <table class="w-full text-base text-slate-900 dark:text-zinc-100 min-w-[600px]"> 
          <thead class="bg-custom-100 dark:bg-custom-500/10 text-slate-900 dark:text-white">
            <tr>
    """)

    for i,col in enumerate(headers):
        align="text-left" if i==0 else "text-center"
        html.append(f"<th class='px-4 py-3 text-xl font-semibold border-b border-custom-200 dark:border-custom-900 {align}'>{col}</th>")

    html.append("</tr></thead><tbody>")


    for _,row in df.iterrows():
        html.append("<tr class='hover:bg-custom-100 dark:hover:bg-custom-500/20 text-lg text-slate-800 dark:text-zinc-100'>")

        for i,col in enumerate(data_cols):
            val=row.get(col,"")
            align="text-left" if i==0 else "text-center"
            html.append(f"<td class='px-4 py-3 border-y border-custom-200 dark:border-custom-900 {align}'>{val}</td>")

        match_name=str(row.get('scrM_MatchName') or "").strip()
        inning_val=str(row.get("scrM_InningNo") or "")
        batter_val=str(row.get("scrM_PlayMIdStriker") or "")
        bowler_val=str(row.get("scrM_PlayMIdBowler") or "")

        html.append(f"<td class='px-4 py-3 border-y border-custom-200 dark:border-custom-900 text-center'>{ make_play_link(match_name, inning_val, batter_val, bowler_val) }</td></tr>")


    html.append("</tbody></table></div></div>")
    return Markup("".join(html))












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
            fontsize=10,
            ha='center',
            va='center',
            rotation=rotation_deg,
            rotation_mode='anchor',
            bbox=dict(facecolor='black', alpha=0.6, boxstyle='round,pad=0.7'),
            zorder=10
        )

    # ===== Breakdown text under boxes =====
    if stance == "LHB":
        detail_positions = [
            (116.5, 0, 0.68),
            (78.5, 0, 0.68),
            (29.5, 0, 0.70),
            (330.5, 0, 0.70),
            (283.5, 0, 0.68),
            (239.5, 0, 0.72),
            (200.5, 1, 0.72),
            (163.5, 1, 0.70)
        ]
    else:
        detail_positions = [
            (116.5, 0, 0.68),
            (78.5, 0, 0.68),
            (29.5, 0, 0.70),
            (330.5, 0, 0.70),
            (283.5, 0, 0.68),
            (239.5, 0, 0.72),
            (200.5, 1, 0.72),
            (163.5, 1, 0.70)
        ]

    for i, (angle_deg, rotation_deg, dist_offset) in enumerate(detail_positions):
        rad = np.deg2rad(angle_deg)
        r = dist_offset * scale
        bd = breakdown_data[i]

        breakdown_text = (
            f"1s:{bd['1s']}  2s:{bd['2s']}\n"
            f"4s:{bd['4s']}  6s:{bd['6s']}"
        )

        ax.text(
            rad, r,
            breakdown_text,
            color='white',
            fontsize=11,
            ha='center',
            va='center',
            rotation=rotation_deg,
            rotation_mode='anchor',
            zorder=11
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
    strengths_html = f"<ul class='list-disc pl-4' style='font-size:18px'>{''.join(f'<li>{pt}</li>' for pt in strengths[:7])}</ul>"
    weaknesses_html = f"<ul class='list-disc pl-4' style='font-size:18px'>{''.join(f'<li>{pt}</li>' for pt in weaknesses[:7])}</ul>"


    return strengths_html, weaknesses_html



import re
import math
import pandas as pd

def generate_kpi_with_summary_tables(df, selected_type, player_vs_player_tables=None, selected_team=None, selected_matches=None):
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

    # ✅ Filter by selected team to show only that team's players
    if selected_team:
        team_col = "scrM_tmMIdBattingName" if selected_type == "batter" else "scrM_tmMIdBowlingName"
        if team_col in df.columns:
            df = df[df[team_col] == selected_team].copy()
            print(f"✅ Filtered player cards for team: {selected_team}, Rows: {len(df)}")

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
        # 🎯 Player vs Player collapse map (fixed: match + inning)
        collapse_map = {}
        if player_vs_player_tables and player in player_vs_player_tables:
            pvp_df = player_vs_player_tables[player]
            if not pvp_df.empty:
                for (match_name, inning_no), sub_df in pvp_df.groupby(["scrM_MatchName", "scrM_InningNo"]):
                    key = f"{match_name}__{inning_no}"
                    collapse_map[key] = render_player_vs_player(f"{match_name} - Inn {inning_no}", sub_df)

            else:
                collapse_map["Summary"] = render_player_vs_player("Summary", pvp_df)

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

        # Batter: balls faced -> exclude wides only (no-balls count as balls faced)
        balls_faced_mask = ~is_wide_s
        balls_faced = int(balls_faced_mask.sum())

        # ✅ Always compute batter overs from actual balls faced (don't trust pre-computed column)
        full_overs = balls_faced // 6
        remaining_balls = balls_faced % 6
        batter_overs = float(f"{full_overs}.{remaining_balls}")


        # Bowler: legal balls
        legal_balls_mask = is_valid_s if IS_VALID in player_df.columns else (~is_wide_s & ~is_nb_s)
        legal_balls = int(legal_balls_mask.sum())

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
            # Bowler
            Runs_conc = int(bat_runs_s.sum())
            # ✅ Always compute Overs from actual valid balls (don't trust pre-computed column)
            full_overs = legal_balls // 6
            remaining_balls = legal_balls % 6
            Overs = float(f"{full_overs}.{remaining_balls}")  # e.g., 12 balls = 2.0 overs

            # Economy rate using true overs
            Eco = safe_div(Runs_conc, (full_overs + remaining_balls / 6.0), places=2, dash_on_zero=True)


            # Scoring/Dot vs bowler
            scoring_mask_b = legal_balls_mask & (bat_runs_s > 0)
            dot_mask_b     = legal_balls_mask & (bat_runs_s == 0)
            Sb = int(scoring_mask_b.sum())   # Scoring Balls
            Db = int(dot_mask_b.sum())       # Dots

            # Wickets
            total_wickets = int(is_bwl_wkt.sum()) if IS_BWL_WKT in player_df.columns else int(is_wkt_s.sum())

            # 4s/6s %
            fours = int(((legal_balls_mask) & (bat_runs_s == 4)).sum())
            sixes = int(((legal_balls_mask) & (bat_runs_s == 6)).sum())

            # Wickets milestones
            bwl_key = BWL_ID if BWL_ID in player_df.columns else BWL_NAME
            bwl_gkeys = [k for k in [MATCH_COL, INN_COL, bwl_key] if k is not None]
            if bwl_gkeys:
                wkt_flag = is_bwl_wkt if IS_BWL_WKT in player_df.columns else is_wkt_s
                tmp = player_df.assign(_wkt=wkt_flag.astype(int))
                wickets_by_inn = tmp.groupby(bwl_gkeys)["_wkt"].sum()
                two_w   = int(((wickets_by_inn >= 2) & (wickets_by_inn < 3)).sum())
                three_w = int(((wickets_by_inn >= 3) & (wickets_by_inn < 5)).sum())
                five_w  = int((wickets_by_inn >= 5).sum())
            else:
                two_w = three_w = five_w = 0

            row1 = [("Inns", Inns), ("W", total_wickets), ("Runs", Runs_conc), ("Overs", Overs),
                    ("Eco", Eco), ("SB", Sb)]

            row2 = [("Dots", Db), ("Fours", fours), ("Sixers", sixes), 
                    ("2w+", two_w), ("3w+", three_w), ("5w+", five_w)]



        # KPI values (NO card — just the rows)
        def build_kpi_row(row_data):
            return "".join(
                f"""<div class="flex-1 min-w-[80px] text-center">
                        <div class="text-xl font-semibold text-gray-200">{label}</div>   <!-- text-lg → text-xl -->
                        <div class="text-2xl font-bold mt-1 text-white">{value}</div>
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

                        <!-- Title increased by 2 -->
                        <h6 class="mb-4 font-semibold" style="font-size: 23px;">{chart_title_areas}</h6>

                        <div id="stackedChartAreas_{player_id_safe}" style="min-height:320px;"></div>

                        <!-- Capsule Legend +2 bigger -->
                        <div style="display:flex;justify-content:center;gap:8px;flex-wrap:wrap;margin-top:10px;
                                    font-size:18px;line-height:1.55;">

                            <div style="display:flex;min-width:65px;border-radius:14px;">
                                <span style="flex:1;text-align:center;padding:9px 0;background:#1E90FF;color:#fff;
                                            border-radius:14px 0 0 14px;font-size:19px;">1s</span>
                                <span style="flex:1;text-align:center;padding:9px 0;border:2px solid #1E90FF;color:#1E90FF;
                                            border-radius:0 14px 14px 0;font-size:19px;">{sum(counts_1s_a)}</span>
                            </div>

                            <div style="display:flex;min-width:65px;border-radius:14px;">
                                <span style="flex:1;text-align:center;padding:9px 0;background:#00FF7F;color:#000;
                                            border-radius:14px 0 0 14px;font-size:19px;">2s</span>
                                <span style="flex:1;text-align:center;padding:9px 0;border:2px solid #00FF7F;color:#00FF7F;
                                            border-radius:0 14px 14px 0;font-size:19px;">{sum(counts_2s_a)}</span>
                            </div>

                            <div style="display:flex;min-width:65px;border-radius:14px;">
                                <span style="flex:1;text-align:center;padding:9px 0;background:#FFD700;color:#000;
                                            border-radius:14px 0 0 14px;font-size:19px;">3s</span>
                                <span style="flex:1;text-align:center;padding:9px 0;border:2px solid #FFD700;color:#FFD700;
                                            border-radius:0 14px 14px 0;font-size:19px;">{sum(counts_3s_a)}</span>
                            </div>

                            <div style="display:flex;min-width:65px;border-radius:14px;">
                                <span style="flex:1;text-align:center;padding:9px 0;background:#FFA500;color:#000;
                                            border-radius:14px 0 0 14px;font-size:19px;">4s</span>
                                <span style="flex:1;text-align:center;padding:9px 0;border:2px solid #FFA500;color:#FFA500;
                                            border-radius:0 14px 14px 0;font-size:19px;">{sum(counts_4s_a)}</span>
                            </div>

                            <div style="display:flex;min-width:65px;border-radius:14px;">
                                <span style="flex:1;text-align:center;padding:9px 0;background:#FF0000;color:#fff;
                                            border-radius:14px 0 0 14px;font-size:19px;">6s</span>
                                <span style="flex:1;text-align:center;padding:9px 0;border:2px solid #FF0000;color:#FF0000;
                                            border-radius:0 14px 14px 0;font-size:19px;">{sum(counts_6s_a)}</span>
                            </div>

                        </div>
                    </div>
                </div>


                <!-- Shots Chart -->
                <div class="card" style="flex:1;min-width:300px;">
                    <div class="card-body">

                        <!-- Title increased by 2 -->
                        <h6 class="mb-4 font-semibold" style="font-size: 23px;">{chart_title_shots}</h6>

                        <div id="stackedChartShots_{player_id_safe}" style="min-height:320px;"></div>

                        <!-- Capsule Legend upgraded same as Areas (+2 size everywhere) -->
                        <div style="display:flex;justify-content:center;gap:8px;flex-wrap:wrap;margin-top:10px;
                                    font-size:18px;line-height:1.55;">

                            <div style="display:flex;min-width:65px;border-radius:14px;">
                                <span style="flex:1;text-align:center;padding:9px 0;background:#1E90FF;color:#fff;
                                            border-radius:14px 0 0 14px;font-size:19px;">1s</span>
                                <span style="flex:1;text-align:center;padding:9px 0;border:2px solid #1E90FF;color:#1E90FF;
                                            border-radius:0 14px 14px 0;font-size:19px;">{sum(counts_1s_s)}</span>
                            </div>

                            <div style="display:flex;min-width:65px;border-radius:14px;">
                                <span style="flex:1;text-align:center;padding:9px 0;background:#00FF7F;color:#000;
                                            border-radius:14px 0 0 14px;font-size:19px;">2s</span>
                                <span style="flex:1;text-align:center;padding:9px 0;border:2px solid #00FF7F;color:#00FF7F;
                                            border-radius:0 14px 14px 0;font-size:19px;">{sum(counts_2s_s)}</span>
                            </div>

                            <div style="display:flex;min-width:65px;border-radius:14px;">
                                <span style="flex:1;text-align:center;padding:9px 0;background:#FFD700;color:#000;
                                            border-radius:14px 0 0 14px;font-size:19px;">3s</span>
                                <span style="flex:1;text-align:center;padding:9px 0;border:2px solid #FFD700;color:#FFD700;
                                            border-radius:0 14px 14px 0;font-size:19px;">{sum(counts_3s_s)}</span>
                            </div>

                            <div style="display:flex;min-width:65px;border-radius:14px;">
                                <span style="flex:1;text-align:center;padding:9px 0;background:#FFA500;color:#000;
                                            border-radius:14px 0 0 14px;font-size:19px;">4s</span>
                                <span style="flex:1;text-align:center;padding:9px 0;border:2px solid #FFA500;color:#FFA500;
                                            border-radius:0 14px 14px 0;font-size:19px;">{sum(counts_4s_s)}</span>
                            </div>

                            <div style="display:flex;min-width:65px;border-radius:14px;">
                                <span style="flex:1;text-align:center;padding:9px 0;background:#FF0000;color:#fff;
                                            border-radius:14px 0 0 14px;font-size:19px;">6s</span>
                                <span style="flex:1;text-align:center;padding:9px 0;border:2px solid #FF0000;color:#FF0000;
                                            border-radius:0 14px 14px 0;font-size:19px;">{sum(counts_6s_s)}</span>
                            </div>

                        </div>
                    </div>
                </div>


                <!-- Wickets Donut -->
                <div class="card" style="flex:1;min-width:300px;position:relative;height:320px;">
                    <div class="card-body">
                        <h6 class="mb-4 font-semibold" style="font-size: 23px;">Wickets Breakdown</h6>
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

                        chart: {{ 
                            type: 'bar', 
                            height: 300, 
                            stacked: true, 
                            toolbar: {{ show: false }} 
                        }},

                        // Font size +1 everywhere
                        xaxis: {{ 
                            categories: {areas}, 
                            title: {{ text: 'Runs', style: {{ fontSize: '21px', fontWeight: 600, color:'#00C853' }} }},
                            labels: {{ style: {{ fontSize: '21px', fontWeight: 500, color:'#00C853' }} }} 
                        }},

                        yaxis: {{
                            labels: {{ style: {{ fontSize: '21px', fontWeight: 500, color:'#00C853' }} }} 
                        }},

                        dataLabels: {{
                            enabled: true,
                            style: {{ fontSize: '21px', fontWeight: '700', color:'#00C853' }}  
                        }},

                        plotOptions: {{ 
                            bar: {{ 
                                horizontal: true, 
                                barHeight: '50%',
                                dataLabels: {{ total: {{ enabled: true, style: {{ fontSize: '22px', fontWeight: '700', color:'#00C853' }} }} }} 
                            }} 
                        }},

                        legend: {{
                            show: false,
                            fontSize: '21px'
                        }},

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
                        chart: {{ 
                            type: 'bar', 
                            height: 300, 
                            stacked: true, 
                            toolbar: {{ show: false }} 
                        }},

                        // Font size +1 everywhere
                        xaxis: {{ 
                            categories: {shots}, 
                            labels: {{ style: {{ fontSize: '21px', fontWeight: 500, color:'#00C853' }} }} 
                        }},

                        yaxis: {{
                            title: {{ text: 'Runs', style: {{ fontSize: '21px', fontWeight: 600, color:'#00C853' }} }},
                            labels: {{ style: {{ fontSize: '21px', fontWeight: 500, color:'#00C853' }} }} 
                        }},

                        dataLabels: {{
                            enabled: true,
                            style: {{ fontSize: '21px', fontWeight: '700', color:'#00C853' }}  
                        }},

                        plotOptions: {{ 
                            bar: {{ 
                                horizontal: false, 
                                barHeight: '50%',
                                dataLabels: {{ total: {{ enabled: true, style: {{ fontSize: '22px', fontWeight: '700', color:'#00C853' }} }} }} 
                            }} 
                        }},

                        legend: {{
                            show: false,
                            fontSize: '21px'
                        }},

                        colors: ['#1E90FF','#00FF7F','#FFD700','#FFA500','#FF0000']
                    }};
                    new ApexCharts(document.querySelector("#stackedChartShots_{player_id_safe}"), optionsShots).render();

                    var chartColorsOuter = ["#FF4560", "#008FFB", "#00E396", "#FEB019", "#775DD0"];
                    var optionsOuter = {{
                        series: {overall_counts},
                        chart: {{ type: 'donut', height: 360 }},
                        labels: {labels},
                        colors: chartColorsOuter,
                        legend: {{ position: 'bottom' }},
                        plotOptions: {{ pie: {{ donut: {{ size: '70%' }} }} }}
                    }};
                    new ApexCharts(document.querySelector("#outerDonut_{player_id_safe}"), optionsOuter).render();

                    var chartColorsInner = ["#FF5733", "#33C1FF"];
                    var optionsInner = {{
                        series: {inner_counts},
                        chart: {{ type: 'donut', height: 260 }},
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
                pitch_img_path = resource_path("tailwick/static/LeftHandPitchPad_1.png")
            else:
                pitch_img_path = resource_path("tailwick/static/RightHandPitchPad_1.png")

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



            # ✅ Generate radar chart with team filtering
            print(f"🎯 Generating initial radar for {player}: player_df rows = {len(player_df)}")
            print(f"🎯 Player data matches: {player_df['scrM_MatchName'].unique().tolist() if 'scrM_MatchName' in player_df.columns else 'N/A'}")
            radar_img = generate_player_radar_chart(player_df, player, selected_type, selected_team)

            # ✅ Create filter checkboxes for radar chart (centered below radar)
            filter_checkboxes = f"""
            <div style="margin-top: 15px; display: flex; gap: 12px; flex-wrap: wrap; align-items: center; justify-content: center;">
                <label style="display: flex; align-items: center; gap: 6px; cursor: pointer; font-size: 18px; color: #e0e0e0;">
                    <input type="checkbox" class="player-radar-filter" data-player="{player}" value="all" checked 
                           style="width: 22px; height: 22px; cursor: pointer; transform: scale(1.15);" />
                    All
                </label>
                <label style="display: flex; align-items: center; gap: 6px; cursor: pointer; font-size: 18px; color: #e0e0e0;">
                    <input type="checkbox" class="player-radar-filter" data-player="{player}" value="1" 
                           style="width: 22px; height: 22px; cursor: pointer; transform: scale(1.15);" />
                    1s
                </label>
                <label style="display: flex; align-items: center; gap: 6px; cursor: pointer; font-size: 18px; color: #e0e0e0;">
                    <input type="checkbox" class="player-radar-filter" data-player="{player}" value="2" 
                           style="width: 22px; height: 22px; cursor: pointer; transform: scale(1.15);" />
                    2s
                </label>
                <label style="display: flex; align-items: center; gap: 6px; cursor: pointer; font-size: 18px; color: #e0e0e0;">
                    <input type="checkbox" class="player-radar-filter" data-player="{player}" value="3" 
                           style="width: 22px; height: 22px; cursor: pointer; transform: scale(1.15);" />
                    3s
                </label>
                <label style="display: flex; align-items: center; gap: 6px; cursor: pointer; font-size: 18px; color: #e0e0e0;">
                    <input type="checkbox" class="player-radar-filter" data-player="{player}" value="4" 
                           style="width: 22px; height: 22px; cursor: pointer; transform: scale(1.15);" />
                    4s
                </label>
                <label style="display: flex; align-items: center; gap: 6px; cursor: pointer; font-size: 18px; color: #e0e0e0;">
                    <input type="checkbox" class="player-radar-filter" data-player="{player}" value="6" 
                           style="width: 22px; height: 22px; cursor: pointer; transform: scale(1.15);" />
                    6s
                </label>
            </div>
            """

            radar_chart_html = f"""
            <style>
                .charts-container {{ display:flex; flex-direction:column; width:100%; gap:20px; }}
                .first-row {{ display:flex; flex-direction:row; gap:20px; flex-wrap:wrap; }}
                .first-row > div {{ display:flex; flex-direction:column; align-items:center; }}
                .player-radar-filter:checked {{ accent-color: #4CAF50; }}
            </style>
            <div class="charts-container">
                <div class="first-row">
                    <div style="width: 600px; display: flex; flex-direction: column; align-items: center;">
                        <img id="player_radar_{player.replace(' ', '_')}" src="data:image/png;base64,{radar_img}" alt="Radar Chart - {player}" 
                             style="width: 100%; height: auto; border-radius: 8px;" />
                        {filter_checkboxes}
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
            
            <script>
            (function() {{
                const playerName = '{player}';
                const playerSafe = playerName.replace(/ /g, '_');
                const selectedType = '{selected_type}';
                const selectedTeam = '{selected_team or ""}';
                const selectedMatches = {json.dumps(selected_matches or [])};
                const checkboxes = document.querySelectorAll(`.player-radar-filter[data-player="${{playerName}}"]`);
                const radarImg = document.getElementById(`player_radar_${{playerSafe}}`);
                
                checkboxes.forEach(cb => {{
                    cb.addEventListener('change', function() {{
                        const allCb = document.querySelector(`.player-radar-filter[data-player="${{playerName}}"][value="all"]`);
                        const otherCbs = Array.from(checkboxes).filter(x => x.value !== 'all');
                        
                        if (this.value === 'all' && this.checked) {{
                            otherCbs.forEach(x => x.checked = false);
                        }} else if (this.value !== 'all' && this.checked) {{
                            if (allCb) allCb.checked = false;
                        }} else if (this.value !== 'all' && !this.checked) {{
                            if (!otherCbs.some(x => x.checked) && allCb) {{
                                allCb.checked = true;
                            }}
                        }}
                        
                        // Get selected filters
                        const selected = Array.from(checkboxes)
                            .filter(x => x.checked)
                            .map(x => x.value);
                        
                        // Update radar chart via AJAX
                        fetch('/apps/api/player_radar_filter', {{
                            method: 'POST',
                            headers: {{ 'Content-Type': 'application/json' }},
                            body: JSON.stringify({{
                                player_name: playerName,
                                filters: selected,
                                selected_type: selectedType,
                                selected_team: selectedTeam,
                                selected_matches: selectedMatches
                            }})
                        }})
                        .then(res => res.json())
                        .then(data => {{
                            if (data.radar_img) {{
                                radarImg.src = 'data:image/png;base64,' + data.radar_img;
                            }}
                        }})
                        .catch(err => console.error('Radar filter error:', err));
                    }});
                }});
            }})();
            </script>
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
            </style>

            <!-- OUTER WRAPPER -->
            <div class="mb-6" style="max-width:100%;overflow:hidden; position:relative;">

                <!-- BLUE CARD -->
                <div class="card bg-sky-500 border-sky-500 dark:bg-sky-800 dark:border-sky-800" 
                    style="border-radius:10px;padding:12px;display:flex;flex-wrap:wrap;gap:12px;align-items:center;justify-content:space-between;">
                    
                    <!-- Profile & Name -->
                    <div style="display:flex;flex-direction:column;align-items:center;flex:0 0 auto;">
                        {embed_profile_image(resource_path("tailwick/static/Default-M.png"), player)}
                        <div style="margin-top:6px;font-weight:700;font-size:20px;color:#ffffff;text-align:center;">{player}</div>
                    </div>

                    <!-- KPI Values -->
                    <div style="flex:1;min-width:260px;max-width:100%;">{kpi_card_html}</div>
                </div>

                <!-- Match KPI Table -->
                <div class="overflow-x-auto rounded-md mb-4" style="max-width:100%;">{match_kpi_html}</div>

                {radar_chart_html}
                {stacked_bar_html}

                <!-- Strengths & Weaknesses -->
                <div style="display:flex;gap:20px;flex-wrap:wrap;margin-top:20px;">
                    <div class="card border border-green-400 bg-green-50 dark:bg-green-900 dark:border-green-700" 
                        style="flex:1;min-width:280px;border-radius:10px;padding:12px;">
                        <div class="card-body">

                            <!-- 🔥 Heading font increased by +4 -->
                            <h6 class="mb-3 font-semibold text-green-700 dark:text-green-300" style="font-size:19px;">Strengths</h6>

                            {strengths_html}
                        </div>
                    </div>

                    <div class="card border border-red-400 bg-red-50 dark:bg-red-900 dark:border-red-700" 
                        style="flex:1;min-width:280px;border-radius:10px;padding:12px;">
                        <div class="card-body">

                            <!-- 🔥 Heading font increased by +4 -->
                            <h6 class="mb-3 font-semibold text-red-700 dark:text-red-300" style="font-size:19px;">Weaknesses</h6>

                            {weaknesses_html}
                        </div>
                    </div>
                </div>

            </div>
        """

        combined_tables[player] = combined_html

    return combined_tables

def get_match_header(match_name):
    """
    Fetch tournament, match info, ground, session, and result remark for header card
    """
    try:
        conn = get_connection()


        query = """
        WITH LatestScore AS (
            SELECT TOP 1 scrM_DayNo, scrM_SessionNo
            FROM tblScoreMaster
            WHERE scrM_MatchName = ?
            ORDER BY scrM_DayNo DESC, scrM_SessionNo DESC
        ),
        LatestInnings AS (
            SELECT TOP 1 Inn_Day, Inn_Session
            FROM tblMatchInnings mi
            INNER JOIN tblMatchMaster mm ON mi.Inn_mchMId = mm.mchM_Id
            WHERE mm.mchM_MatchName = ?
            ORDER BY Inn_Day DESC, Inn_Session DESC
        )
        SELECT 
            t.trnM_TournamentName AS TournamentName,
            m.mchM_MatchName AS MatchName,
            CONVERT(VARCHAR(11), m.mchM_StartDateTime, 113) AS MatchDate,
            g.grdM_GroundName AS GroundName,
            m.mchM_ResultRemark AS ResultText,   -- 🆕 Match Result
            CASE 
                WHEN m.mchM_IsMatchCompleted = 0 
                     AND (ls.scrM_DayNo IS NOT NULL AND ls.scrM_SessionNo IS NOT NULL)
                THEN 'Day ' + CAST(ls.scrM_DayNo AS VARCHAR(10)) + ' - Session ' + CAST(ls.scrM_SessionNo AS VARCHAR(10))
                WHEN m.mchM_IsMatchCompleted = 1 
                     AND (li.Inn_Day IS NOT NULL AND li.Inn_Session IS NOT NULL)
                THEN 'Day ' + CAST(li.Inn_Day AS VARCHAR(10)) + ' - Session ' + CAST(li.Inn_Session AS VARCHAR(10))
                ELSE CASE WHEN m.mchM_IsMatchCompleted = 1 THEN 'Match Ended' ELSE 'Live - Session Info N/A' END
            END AS DaySessionText
        FROM tblMatchMaster m
        INNER JOIN tblTournaments t ON m.mchM_TrnMId = t.trnM_Id
        LEFT JOIN tblGroundMaster g ON m.mchM_grdMId = g.grdM_Id
        LEFT JOIN LatestScore ls ON 1=1
        LEFT JOIN LatestInnings li ON 1=1
        WHERE m.mchM_MatchName = ?
        """

        df = pd.read_sql(query, conn, params=[match_name, match_name, match_name])
        conn.close()

        if df.empty:
            return None
        return df.iloc[0].to_dict()

    except Exception as e:
        print("Error in get_match_header:", e)
        return None


def get_match_innings(match_name):
    """
    Fetch innings summary for a given match (ordered by Inn_Inning).
    Returns list of dicts with TeamShortName, InningNo, Runs, Wickets, Overs.
    """
    try:
        conn = get_connection()


        query = """
        SELECT 
            i.Inn_Inning,
            tm.tmM_ShortName AS TeamShortName,
            i.Inn_TotalRuns,
            i.Inn_Wickets,
            i.Inn_Overs
        FROM tblMatchInnings i
        INNER JOIN tblMatchMaster m ON i.Inn_mchMId = m.mchM_Id
        INNER JOIN tblTeamMaster tm ON i.Inn_tmMIdBatting = tm.tmM_Id
        WHERE m.mchM_MatchName = ?
        ORDER BY i.Inn_Inning
        """
        df = pd.read_sql(query, conn, params=[match_name])
        conn.close()

        return df.to_dict(orient="records")

    except Exception as e:
        print("Error in get_match_innings:", e)
        return []
    
def get_last_12_deliveries(match_name):
    try:
        conn = get_connection()

        query = """
            SELECT TOP 12
                scrM_BatsmanRuns,
                scrM_IsWicket,
                scrM_IsNoBall,
                scrM_NoBallRuns,
                scrM_IsWideBall,
                scrM_WideRuns
            FROM tblScoreMaster
            WHERE scrM_MatchName = ?
            ORDER BY scrM_InningNo DESC, scrM_OverNo DESC, scrM_DelNo DESC
        """
        df = pd.read_sql(query, conn, params=[match_name])
        conn.close()

        deliveries = []
        for _, row in df.iterrows():
            if row['scrM_IsWicket'] == 1:
                deliveries.append("W")
            elif row['scrM_IsNoBall'] == 1:
                nb_runs = int(row['scrM_NoBallRuns']) if row['scrM_NoBallRuns'] else 0
                deliveries.append(f"{nb_runs}NB" if nb_runs > 0 else "NB")
            elif row['scrM_IsWideBall'] == 1:
                wide_runs = int(row['scrM_WideRuns']) if row['scrM_WideRuns'] else 0
                deliveries.append(f"{wide_runs}Wd" if wide_runs > 0 else "Wd")
            else:
                runs = int(row['scrM_BatsmanRuns'])
                deliveries.append(str(runs) if runs > 0 else "·")  # dot ball

        return list(reversed(deliveries))  # reverse to show latest on right
    except Exception as e:
        print("Error in get_last_12_deliveries:", e)
        return []

import pyodbc
import pandas as pd

def get_ball_by_ball_data(match_name):
    """
    Fetch ball-by-ball data for a given match using MatchName.
    """
    try:
        conn = get_connection()

        query = """
            SELECT
                scrM_DelId,
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
                scrM_IsNoBall,
                scrM_IsWideBall,
                scrM_ByeRuns,
                scrM_LegByeRuns,
                scrM_PenaltyRuns,
                scrM_IsWicket,
                scrM_DecisionFinal_zName,
                scrM_DecisionFinal_z,
                scrM_PlayMIdWicket,
                scrM_PlayMIdWicketName,
                scrM_playMIdCaughtName,
                scrM_playMIdRunOutName,
                scrM_PlayMIdFielderName,
                scrM_playMIdStumpingName,
                scrM_FieldingType_z,
                scrM_FieldingType_zName,
                scrM_PitchArea_zName,
                scrM_BatPitchArea_zName,
                scrM_ShotType_zName,
                scrM_BowlerSkill,
                scrM_Wagon_x,
                scrM_Wagon_y,
                scrM_WagonArea_zName,
                scrM_PitchX,
                scrM_PitchY,
                scrM_StrikerBatterSkill,
                scrM_RunsSavedOrGiven
            FROM tblScoreMaster
            WHERE scrM_MatchName = ?
            ORDER BY scrM_InningNo, scrM_OverNo, scrM_DelNo
        """

        df = pd.read_sql(query, conn, params=[match_name])

        # Normalize BatterHand column (Right / Left)
        def normalize_hand(val):
            if pd.isna(val):
                return None
            v = str(val).lower()
            if "right" in v:
                return "Right"
            if "left" in v:
                return "Left"
            return None

        df["BatterHand"] = df["scrM_StrikerBatterSkill"].apply(normalize_hand)

        return df

    except Exception as e:
        print("Error fetching ball-by-ball data:", e)
        return pd.DataFrame()






import os, io, base64, math
import pandas as pd
import matplotlib.pyplot as plt
from PIL import Image

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")

def generate_wagon_wheel(bdf: pd.DataFrame, batter_hand: str = None, mode: str = "batter", filter_runs=None) -> str:
    """
    Draws wagon wheel lines for batters (runs scored) or bowlers (runs conceded).
    mode = "batter" or "bowler"
    Returns a base64 PNG 300x300 px.
    """

    import io
    import math
    import base64
    import pandas as pd
    import matplotlib.pyplot as plt
    from PIL import Image
    import os

    # ✅ Background image logic
    if mode == "batter":
        if batter_hand == "Right":
            base_img_path = os.path.join(STATIC_DIR, "RightHandWheel_1.png")
        elif batter_hand == "Left":
            base_img_path = os.path.join(STATIC_DIR, "LeftHandWheel_1.png")
        else:
            base_img_path = os.path.join(STATIC_DIR, "DefaultWheel_1.png")
    else:
        # For bowler mode: determine which hand they conceded more runs against
        if "scrM_StrikerBatterSkill" in bdf.columns and not bdf.empty:
            try:
                runs_by_hand = bdf.groupby("scrM_StrikerBatterSkill")["scrM_BatsmanRuns"].sum()
                right_runs = runs_by_hand.get("Right", 0)
                left_runs = runs_by_hand.get("Left", 0)
                
                if right_runs > left_runs:
                    base_img_path = os.path.join(STATIC_DIR, "RightHandWheel_1.png")
                elif left_runs > right_runs:
                    base_img_path = os.path.join(STATIC_DIR, "LeftHandWheel_1.png")
                else:
                    base_img_path = os.path.join(STATIC_DIR, "DefaultWheel_1.png")
            except Exception:
                base_img_path = os.path.join(STATIC_DIR, "DefaultWheel_1.png")
        else:
            base_img_path = os.path.join(STATIC_DIR, "DefaultWheel_1.png")

    base = Image.open(base_img_path).convert("RGBA")
    if base.size != (300, 300):
        base = base.resize((300, 300), Image.LANCZOS)
    W, H = base.size
    cx, cy = W // 2, H // 2
    
    # ✅ Apply Y offset for batsman position (14px above center)
    batsman_y_offset = -14
    cy = cy + batsman_y_offset

    rim_thickness = 20
    radius = (min(W, H) / 2) - rim_thickness

    # ✅ Color map for all run types
    color_map = {0: "#808080", 1: "#1E90FF", 2: "#00FF7F", 3: "#FFD700", 4: "#FFA500", 6: "#FF0000"}

    # ✅ Determine which runs to highlight based on filter
    highlight_runs = None
    if filter_runs is not None:
        try:
            highlight_runs = int(filter_runs)
        except Exception:
            pass

    xs = pd.to_numeric(bdf.get("scrM_Wagon_x"), errors="coerce").dropna()
    ys = pd.to_numeric(bdf.get("scrM_Wagon_y"), errors="coerce").dropna()
    if xs.empty or ys.empty:
        buf = io.BytesIO()
        base.save(buf, format="PNG")
        return base64.b64encode(buf.getvalue()).decode("utf-8")

    # ✅ Center and normalize data points
    cx_db = (xs.min() + xs.max()) / 2.0
    cy_db = (ys.min() + ys.max()) / 2.0
    rx_db = max((xs - cx_db).abs().max(), 1e-6)
    ry_db = max((ys - cy_db).abs().max(), 1e-6)

    # ✅ Mirror for left-hand batters only
    mirror = (str(batter_hand).strip().lower() == "left" and mode == "batter")

    # ✅ Create figure
    fig = plt.figure(figsize=(3, 3), dpi=100)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.imshow(base, extent=[0, W, H, 0])
    ax.set_xlim(0, W)
    ax.set_ylim(H, 0)
    ax.axis("off")

    # ✅ Draw wagon wheel lines - show ALL lines, highlight filtered ones
    for _, row in bdf.iterrows():
        runs = row.get("scrM_BatsmanRuns")
        
        # Skip runs not in our color map (except when checking for highlight)
        if runs not in color_map and runs != 0:
            continue

        x, y = row.get("scrM_Wagon_x"), row.get("scrM_Wagon_y")
        if pd.isna(x) or pd.isna(y):
            continue

        xn = (float(x) - cx_db) / rx_db
        yn = (float(y) - cy_db) / ry_db

        if mirror:
            xn = -xn

        length = math.sqrt(xn**2 + yn**2)
        if length == 0:
            continue
        xn /= length
        yn /= length

        px = cx + xn * radius
        py = cy - yn * radius

        # ✅ Determine line appearance based on filter
        if highlight_runs is None:
            # "All" filter: Show all run types (1,2,3,4,6) normally with their respective colors
            if runs in [1, 2, 3, 4, 6]:
                line_color = color_map[runs]
                line_alpha = 0.95 if mode == "batter" else 0.9
                line_width = 1.2 if mode == "batter" else 1.4
            else:
                # Skip dots (0) and other runs when showing "All"
                continue
        elif runs == highlight_runs:
            # Show ONLY the filtered run type with same thickness as "All"
            line_color = color_map.get(runs, "#808080")
            line_alpha = 1.0
            line_width = 1.2 if mode == "batter" else 1.4
        else:
            # Skip other run types - don't show them at all
            continue

        # ✅ Flip direction for bowlers to show conceded lines inward
        if mode == "bowler":
            ax.plot([px, cx], [py, cy],
                    color=line_color,
                    linewidth=line_width,
                    alpha=line_alpha,
                    solid_capstyle="round")
        else:
            ax.plot([cx, px], [cy, py],
                    color=line_color,
                    linewidth=line_width,
                    alpha=line_alpha,
                    solid_capstyle="round")

    # ✅ Convert to base64 image
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=100, facecolor="none", edgecolor="none", bbox_inches="tight", pad_inches=0)
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



def create_partnership_chart(innings_df, team_name):
    import plotly.graph_objects as go
    import pandas as pd

    # === Handle empty df ===
    if innings_df.empty:
        fig = go.Figure()
        fig.update_layout(
            annotations=[dict(
                text="No Data Available",
                xref="paper", yref="paper",
                showarrow=False,
                font=dict(size=26, color="red"),   # Increased 24→26
                x=0.5, y=0.5
            )],
            height=400, width=800,
            plot_bgcolor="white", paper_bgcolor="white"
        )
        return fig

    # Calculate Extras
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
                font=dict(size=26, color="red"),   # Increased
                x=0.5, y=0.5
            )],
            height=400, width=800,
            plot_bgcolor="white", paper_bgcolor="white"
        )
        return fig

    # Sort by runs
    partnership_sequence = []

    for pair in partnerships:
        b1 = pair["Batter1"]
        b2 = pair["Batter2"]

        # First ball where this pair appeared
        first_ball = innings_df[
            ((innings_df["scrM_PlayMIdStrikerName"] == b1) & (innings_df["scrM_PlayMIdNonStrikerName"] == b2)) |
            ((innings_df["scrM_PlayMIdStrikerName"] == b2) & (innings_df["scrM_PlayMIdNonStrikerName"] == b1))
        ].index.min()

        partnership_sequence.append((first_ball, pair))

    # Sort by appearance (batting order sequence)
    partnership_sequence.sort(key=lambda x: x[0])

    # Convert back to dataframe
    partnerships_df = pd.DataFrame([p for _, p in partnership_sequence])

    # Normalize runs → fractions
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

    strip_gap = 2.5
    y_positions = [(n-1-i) * strip_gap for i in range(n)]
    strip_thickness = 0.7

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


    # 🔥 EVERY TEXT SIZE +2
    for i, row in partnerships_df.iterrows():
        y = y_positions[partnerships_df.index.get_loc(i)]

        fig.add_annotation(
            x=-0.8, y=y, xanchor="right", align="center",
            text=f"{row['Batter1']}<br><b>{row['Batter1_Runs']} ({row['Batter1_Balls']})</b>",
            showarrow=False, font=dict(size=16, color="#FF8C00")   # ← 14→16
        )

        fig.add_annotation(
            x=0.8, y=y, xanchor="left", align="center",
            text=f"{row['Batter2']}<br><b>{row['Batter2_Runs']} ({row['Batter2_Balls']})</b>",
            showarrow=False, font=dict(size=16, color="#1E90FF")   # ← 14→16
        )

        fig.add_annotation(
            x=0, y=y + 0.6, xanchor="center",
            text=f"<b>Partnership - {row['Total']} ({row['Balls']})</b>",
            showarrow=False, font=dict(size=16, color="#808080")   # ← 14→16
        )

        fig.add_annotation(
            x=0, y=y - 0.6, xanchor="center",
            text=f"Extras - {row['Extras']}",
            showarrow=False, font=dict(size=16, color="#32CD32")   # ← 14→16
        )


    shapes = [dict(type="line", x0=-1, x1=1, xref="paper",
                   y0=y+strip_gap/2, y1=y+strip_gap/2, yref="y",
                   line=dict(color="rgba(0,0,0,0)", width=1))
              for y in y_positions]

    fig.update_layout(
        barmode="relative",
        showlegend=False,
        height=max(400, n * 120),
        margin=dict(l=120, r=120, t=20, b=20),
        xaxis=dict(visible=False, range=[-1, 1]),
        yaxis=dict(visible=False, range=[-strip_gap, (n) * strip_gap]),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        shapes=shapes
    )
    return fig




def get_innings_deliveries(match_name, inning_no):
    """
    Returns full ball-by-ball dataframe for a given innings.
    """
    import pyodbc, pandas as pd

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
        FROM tblScoreMaster
        WHERE scrM_MatchName = ? AND scrM_InningNo = ?
        ORDER BY scrM_OverNo, scrM_DelNo
    """

    try:
        conn = get_connection()

        df = pd.read_sql(query, conn, params=[match_name, inning_no])
        conn.close()
        return df
    except Exception as e:
        print("Error fetching innings deliveries:", e)
        return pd.DataFrame()
    
import pyodbc

# utils.py
import os, re, urllib.parse
from typing import List, Optional

def fetch_metric_videos(
    batter_id: Optional[int] = None,
    bowler_id: Optional[int] = None,
    metric: Optional[str] = None,
    match_id: Optional[str] = None,
    inning_id: Optional[int] = None,
) -> List[str]:
    """
    Return ABSOLUTE file paths for clips that match the given filters.
    Mirrors the /video_player logic but returns paths ready for zipping.
    """
    from .utils import get_video_base_path, get_connection  # adjust if your imports differ

    def apply_metric_filter_to_sql(sql: str) -> str:
        m = (metric or "").strip().upper()

        if m in ("B", "BALL", "BALLS"):
            return sql

        if m in ("D", "DOT", "DOTS"):
            sql += """
                AND scrM_BatsmanRuns = 0
                AND scrM_ByeRuns = 0
                AND scrM_LegByeRuns = 0
                AND scrM_WideRuns = 0
                AND scrM_NoBallRuns = 0
                AND scrM_IsWicket = 0
            """
        elif m in ("1", "1S"):
            sql += " AND scrM_BatsmanRuns = 1 AND scrM_ExtrasRuns = 0"
        elif m in ("2", "2S"):
            sql += " AND scrM_BatsmanRuns = 2 AND scrM_ExtrasRuns = 0"
        elif m in ("3", "3S"):
            sql += " AND scrM_BatsmanRuns = 3 AND scrM_ExtrasRuns = 0"
        elif m in ("4", "4S", "FOURS"):
            sql += " AND scrM_BatsmanRuns = 4 AND scrM_IsBoundry = 1"
        elif m in ("6", "6S", "SIXES"):
            sql += " AND scrM_IsSixer = 1"
        elif m in ("W", "WK", "WKT", "WICKETS"):
            sql += " AND scrM_IsWicket = 1"
        elif m in ("R", "RUNS"):
            sql += " AND scrM_BatsmanRuns > 0 AND scrM_IsWicket = 0"
        elif m in ("EXTRAS",):
            sql += " AND scrM_ExtrasRuns > 0"
        elif m in ("WD", "WIDE", "WIDES"):
            sql += " AND scrM_ExtrasType_zName = 'WD'"
        elif m in ("NB", "NO BALL", "NO-BALL", "NO_BALL"):
            sql += " AND scrM_ExtrasType_zName = 'NB'"
        elif m in ("LB", "LEGBYE", "LEGBYES"):
            sql += " AND scrM_ExtrasType_zName = 'LB'"
        elif m in ("BYE", "BYES"):
            sql += " AND scrM_ExtrasType_zName = 'B'"
        elif m in ("M", "MAIDEN"):
            sql += " AND scrM_OverRuns = 0"

        return sql

    base_path = get_video_base_path()
    if not base_path or not os.path.isdir(base_path):
        return []

    # find the specific match folder under base path (like your /video_player route)
    match_folder = None
    for entry in os.listdir(base_path):
        entry_path = os.path.join(base_path, entry)
        if os.path.isdir(entry_path) and match_id and match_id.lower() in entry.lower():
            match_folder = entry_path
            break
    if not match_folder:
        return []

    # fetch delivery IDs
    delivery_ids: List[str] = []
    try:
        conn = get_connection()
        cur = conn.cursor()

        sql = """
            SELECT scrM_DelId
            FROM tblScoreMaster
            WHERE scrM_MatchName = ?
              AND scrM_IsValidBall = 1
        """
        params = [os.path.basename(match_folder)]

        if inning_id:
            sql += " AND scrM_InningNo = ?"
            params.append(int(inning_id))

        if batter_id:
            sql += " AND scrM_PlayMIdStriker = ?"
            params.append(int(batter_id))

        if bowler_id:
            sql += " AND scrM_PlayMIdBowler = ?"
            params.append(int(bowler_id))

        sql = apply_metric_filter_to_sql(sql)
        cur.execute(sql, params)
        delivery_ids = [str(r[0]) for r in cur.fetchall()]
        cur.close()
        conn.close()
    except Exception as e:
        print("fetch_metric_videos DB error:", e)
        return []

    if not delivery_ids:
        return []

    # map delivery IDs to actual files in the match folder
    files: List[str] = []
    valid_exts = (".mp4", ".mov", ".mkv")
    for root, _, fs in os.walk(match_folder):
        for f in fs:
            if not f.lower().endswith(valid_exts):
                continue
            for did in delivery_ids:
                if f.lower().startswith(did.lower()):
                    files.append(os.path.join(root, f))
                    break

    # sort by delivery number in the path
    def extract_num(p):
        m = re.search(r"[\\/]([0-9]+)", p)
        return int(m.group(1)) if m else 0

    files.sort(key=extract_num)
    return files






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
        d["Day"] = d["scrM_DayNo"].fillna(1).astype(int)
        d["SessionNo"] = d["scrM_SessionNo"].fillna(1).astype(int)
    else:
        d["__legal_cum"] = d["__is_legal"].cumsum()
        legal_idx_0 = np.maximum(d["__legal_cum"] - 1, 0)
        session_index = (legal_idx_0 // (30 * 6)).astype(int)

        d["Day"] = (session_index // 3) + 1
        d["SessionNo"] = (session_index % 3) + 1

    if "scrM_DayNo" not in d.columns:
        d["scrM_DayNo"] = d["Day"]
    if "scrM_SessionNo" not in d.columns:
        d["scrM_SessionNo"] = d["SessionNo"]

    # --- Team name map ---
    inning_team_map = {}
    if match_innings:
        for inn in match_innings:
            inning_team_map[int(inn["Inn_Inning"])] = inn.get("TeamShortName") or inn.get("TeamName")

    # --- Required columns ---
    run_col = "scrM_DelRuns" if "scrM_DelRuns" in d.columns else None
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

        # LEGAL BALLS
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

        # MAIDENS
        maiden_map = calculate_maidens(g)
        total_maidens = sum(maiden_map.values()) if maiden_map else 0

        # NO BALLS
        no_balls = int(g[noball_col].sum()) if noball_col else 0

        # ---------------------------------------------------------
        # ✔ CORRECT RSOG LOGIC (Runs Saved / Runs Given)
        # ---------------------------------------------------------
        if "scrM_RunsSavedOrGiven" in g.columns:
            rsog = g["scrM_RunsSavedOrGiven"].fillna(0).astype(float)
            runs_saved = int(rsog[rsog > 0].sum())           # positive ➝ saved
            runs_given = abs(int(rsog[rsog < 0].sum()))      # negative ➝ given (convert to positive)
        else:
            runs_saved = 0
            runs_given = 0

        # Catches Taken
        # ✔ Catch Taken → scrM_DecisionFinal_z == 42
        if "scrM_DecisionFinal_z" in g.columns:
            catches_taken = int(g["scrM_DecisionFinal_z"].fillna(0).astype(int).eq(42).sum())
        else:
            catches_taken = 0

        # ✔ Catch Dropped → scrM_FieldingType_z == 62
        if "scrM_FieldingType_z" in g.columns:
            catches_dropped = int(g["scrM_FieldingType_z"].fillna(0).astype(int).eq(62).sum())
        else:
            catches_dropped = 0


        # ---------------------------------------------------------
        # FINAL STATS ROW
        # ---------------------------------------------------------
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
            "maiden": total_maidens,
            "wkts": wkts,
            "eco_rate": rr,
            "wide_balls": int(g[wide_col].sum()) if wide_col else 0,
            "no_balls": no_balls,
            "runs_saved": runs_saved,      # ✔ Correct
            "runs_given": runs_given,      # ✔ Correct
            "catches_taken": catches_taken,
            "catches_dropped": catches_dropped
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

    # --- Day donut chart ---
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
                font=dict(color="#808080")
            ),
            margin=dict(l=10, r=10, t=40, b=10),
            height=250,
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            legend=dict(
                font=dict(color="#808080")
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


def generate_session_radar_chart(
    ball_by_ball_df,
    day,
    inning,
    session,
    team_name="Team",
    bowler_type=None,
    run_filter=None
):
    """
    Radar-style session wagon wheel chart with optional run filtering.
    BIG SIZE VERSION (Option A: 600x600).
    """

    import numpy as np
    import matplotlib.pyplot as plt
    import io, base64

    df = ball_by_ball_df.copy()

    # ------------------------------
    # NORMALISING run_filter INPUT
    # ------------------------------
    if run_filter is None or str(run_filter).lower() == "all":
        run_set = None
    else:
        if isinstance(run_filter, (list, tuple, set)):
            run_set = set(int(x) for x in run_filter)
        else:
            s = str(run_filter)
            if "," in s:
                run_set = set(int(x.strip()) for x in s.split(",") if x.strip())
            else:
                run_set = set([int(s.strip())])

    # ------------------------------
    # Extract Day / Session columns
    # ------------------------------
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

    day_col = "Day"
    session_col = "SessionNo"

    df = df[
        (df[day_col] == day) &
        (df["scrM_InningNo"] == inning) &
        (df[session_col] == session)
    ]

    # ------------------------------
    # RUN FILTER
    # ------------------------------
    if run_set:
        df = df[df["scrM_BatsmanRuns"].isin(run_set)]

    # ------------------------------
    # Bowler Type filter
    # ------------------------------
    if bowler_type:
        if "scrM_BowlerSkill" in df.columns:
            df["BowlingType"] = df["scrM_BowlerSkill"].apply(map_bowling_type_radar)
            df = df[df["BowlingType"] == bowler_type]

    # ------------------------------
    # No Data Chart
    # ------------------------------
    if df.empty:
        fig, ax = plt.subplots(figsize=(8, 8), subplot_kw=dict(polar=True))  # bigger
        ax.set_theta_zero_location("N")
        ax.set_theta_direction(-1)
        ax.set_xticks([]); ax.set_yticks([])
        ax.spines['polar'].set_visible(False)
        ax.text(0.5, 0.5, "No Data", ha="center", va="center",
                transform=ax.transAxes, color="red", fontsize=24, fontweight="bold")
        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=260, transparent=True)
        plt.close(fig)
        buf.seek(0)
        return f"data:image/png;base64,{base64.b64encode(buf.read()).decode()}"

    # ------------------------------
    # Sector breakdown
    # ------------------------------
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

    # ------------------------------
    # Start Plot  (BIGGER SIZE)
    # ------------------------------
    fig, ax = plt.subplots(figsize=(8, 8), subplot_kw=dict(polar=True))  # bigger

    ax.set_theta_zero_location("N")
    ax.set_theta_direction(-1)
    ax.set_xticks([]); ax.set_yticks([])
    ax.spines['polar'].set_visible(False)

    scale = 0.9
    ax.set_aspect('equal')

    # ------------------------------
    # Drawing (BIGGER RIM + ELEMENTS)
    # ------------------------------
    rim_radius = 1.10 * scale
    rim_circle = plt.Circle((0, 0), rim_radius, transform=ax.transData._b,
                            color='#6dbc45', linewidth=26, fill=False,   # thicker rim
                            zorder=5, clip_on=False)
    ax.add_artist(rim_circle)

    ax.add_artist(plt.Circle((0, 0), 1.0 * scale, transform=ax.transData._b,
                             color='#19a94b', zorder=0))
    ax.add_artist(plt.Circle((0, 0), 0.6 * scale, transform=ax.transData._b,
                             color='#4CAF50', zorder=1))
    ax.add_artist(plt.Rectangle((-0.08 * scale / 2, -0.33 * scale / 2),
                                0.08 * scale, 0.33 * scale,
                                transform=ax.transData._b, color='burlywood', zorder=2))

    for angle in np.linspace(0, 2*np.pi, 9):
        ax.plot([angle, angle], [0, 1.0 * scale],
                color='white', linewidth=3, zorder=3)

    # ------------------------------
    # Sector totals + highlight
    # ------------------------------
    sector_runs = [(bd["1s"] + bd["2s"]*2 + bd["3s"]*3 +
                    bd["4s"]*4 + bd["6s"]*6) for bd in breakdown_data]

    total_runs = sum(sector_runs)

    if total_runs > 0:
        max_idx = sector_runs.index(max(sector_runs))
        sector_angles_deg = [112.5, 67.5, 22.5, 337.5,
                             292.5, 247.5, 202.5, 157.5]
        ax.bar(np.deg2rad(sector_angles_deg[max_idx]), 1.0 * scale,
               width=np.radians(45), color='red', alpha=0.25, zorder=1)

    # ------------------------------
    # Fielding labels (BIGGER)
    # ------------------------------
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
                color='white', fontsize=16, fontweight='bold',   # bigger labels
                ha='center', va='center', rotation=rotation_deg,
                rotation_mode='anchor', zorder=6)

    # ------------------------------
    # Runs + Percentage text (BIGGER)
    # ------------------------------
    box_positions = [
        (103.5, 0, 0.70), (67.5, 0, 0.70),
        (22.5, 0, 0.80), (337.5, 0, 0.80),
        (295.5, 0, 0.75), (250.5, 0, 0.70),
        (204.5, 1, 0.59), (155.5, 1, 0.59)
    ]

    for i, (angle_deg, rot, dist) in enumerate(box_positions):
        rad = np.deg2rad(angle_deg)
        r = dist * scale
        runs = sector_runs[i]
        pct = (runs / total_runs * 100) if total_runs > 0 else 0

        ax.text(rad, r,
                f"{runs}\n({pct:.1f}%)",
                color='white', fontsize=19, fontweight='bold',   # bigger text
                ha='center', va='center',
                rotation=0,
                linespacing=1.15)

    # ------------------------------
    # EXPORT (BIG)
    # ------------------------------
    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=260, transparent=True)
    plt.close(fig)
    buf.seek(0)

    return f"data:image/png;base64,{base64.b64encode(buf.read()).decode()}"







def generate_session_pitchmaps(
    ball_by_ball_df, day, inning, session,
    right_pitch_img="tailwick/static/RightHandPitchPad_1.png",
    left_pitch_img="tailwick/static/LeftHandPitchPad_1.png"
):
    """
    Generate Right-hand and Left-hand pitch maps for a given Inn/Day/Session.
    Returns dict with { 'right': base64_img, 'left': base64_img }.
    This version resolves static asset paths using resource_path() so it works inside a PyInstaller bundle.
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

    # ---- Resolve pitch image paths in a PyInstaller-safe way ----
    def _resolve_asset_path(candidate_path):
        """
        If candidate_path is an absolute path and exists -> return it.
        Otherwise try resource_path(candidate_path) (which handles sys._MEIPASS).
        As a last fallback try resource_path(os.path.join('tailwick','static', basename))
        """
        # if absolute and exists, use it (dev or explicit override)
        if os.path.isabs(candidate_path) and os.path.exists(candidate_path):
            return candidate_path

        # try direct resource_path (handles 'tailwick/static/...' relative strings)
        try:
            rp = resource_path(candidate_path)
            if os.path.exists(rp):
                return rp
        except Exception:
            pass

        # fallback: try under tailwick/static with basename
        try:
            base_try = resource_path(os.path.join("tailwick", "static", os.path.basename(candidate_path)))
            if os.path.exists(base_try):
                return base_try
        except Exception:
            pass

        # not found
        return None

    resolved_right = _resolve_asset_path(right_pitch_img)
    resolved_left  = _resolve_asset_path(left_pitch_img)

    # Optional debug prints you can remove after testing
    # (Will help confirm path resolution when run in EXE.)
    print(f"[pitchmaps] resolved_right={resolved_right}, resolved_left={resolved_left}")

    # ---- Call your existing scaled plotting function ----
    right_img = None
    left_img = None

    if not right_df.empty and resolved_right and os.path.exists(resolved_right):
        right_img = plot_pitch_points_scaled(right_df, pitch_image_path=resolved_right)

    if not left_df.empty and resolved_left and os.path.exists(resolved_left):
        left_img = plot_pitch_points_scaled(left_df, pitch_image_path=resolved_left)

    return {"right": right_img, "left": left_img}


import pandas as pd

def generate_line_length_table_new(df, day=None, inning=None, session=None):

    subset = df.copy()

    # ------------------ FILTERS ------------------
    if day is not None:
        subset = subset[subset["scrM_DayNo"] == day]
    if inning is not None:
        subset = subset[subset["scrM_InningNo"] == inning]
    if session is not None:
        subset = subset[subset["scrM_SessionNo"] == session]

    if subset.empty:
        return []



    # ------------------ LINE MAPPING (scrM_PitchX)
    # ORIGINAL FOR WIDTH = 170, HEIGHT = 280
    def map_line(x):
        try:
            x = float(x)
        except:
            return None

        if x < 50:
            return "Outside Off St."
        elif x < 70:
            return "Off/Mid. St."
        elif x < 80:
            return "Off St."
        elif x < 90:
            return "Mid. St."
        elif x < 100:
            return "Leg. St."
        else:
            return "Leg. St."

    subset["LineCategory"] = subset["scrM_PitchX"].apply(map_line)

    # ------------------ LENGTH MAPPING (scrM_PitchY)
    def map_length(y):
        try:
            y = float(y)
        except:
            return None

        if y < 80:
            return "Short Pitch"
        elif y < 120:
            return "Short Of Good Length"
        elif y < 180:
            return "Good Length"
        elif y < 220:
            return "Over Pitch"
        elif y < 250:
            return "Full Length"
        else:
            return "Full Toss"

    subset["LengthCategory"] = subset["scrM_PitchY"].apply(map_length)

    # ------------------ TABLE STRUCTURE ------------------

    line_lengths = [
        "Full Length", "Full Toss", "Good Length",
        "Over Pitch", "Short Of Good Length", "Short Pitch"
    ]

    lines = [
        "Leg. St.", "Mid. St.", "Mid/Leg. St.",
        "Off St.", "Off/Mid. St.", "Outside Off St."
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
    """
    try:
        conn = get_connection()
        query = """
            SELECT trnM_MatchFormat_z
            FROM tblTournaments
            WHERE trnM_Id = ?
        """
        result = pd.read_sql(query, conn, params=[tournament_id])
        conn.close()

        if not result.empty:
            code = int(result.iloc[0]['trnM_MatchFormat_z'])
            if code in [26, 27, 28, 29]:
                return code
            else:
                print(f"⚠️ Unknown match format code: {code}")
                return None
        return None
    except Exception as e:
        print("⚠️ Match format code fetch error:", e)
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

    max_over = max(
        int(innings1_df["scrM_OverNo"].max()) if (not innings1_df.empty) else 0,
        int(innings2_df["scrM_OverNo"].max()) if (not innings2_df.empty) else 0
    )
    if max_over == 0:
        return go.Figure()

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
        return go.Figure()

    overs = list(range(start_over, end_over + 1))
    inn1 = innings1_df[innings1_df["scrM_OverNo"].between(start_over, end_over)]
    inn2 = innings2_df[innings2_df["scrM_OverNo"].between(start_over, end_over)]

    runs1 = inn1.groupby("scrM_OverNo")["scrM_DelRuns"].sum().reindex(overs, fill_value=0)
    runs2 = inn2.groupby("scrM_OverNo")["scrM_DelRuns"].sum().reindex(overs, fill_value=0)

    runs1_vals = runs1.values
    runs2_vals = runs2.values

    x_base = np.array(overs, dtype=float)
    bar_width = 0.35

    # ✔ NEW color
    label_color = "#00C853"

    fig = go.Figure()

    # --- Bars (no change) ---
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

    # --- Wickets ---
    wk1 = inn1.groupby("scrM_OverNo")["scrM_IsWicket"].sum().to_dict() if "scrM_IsWicket" in inn1.columns else {}
    wk2 = inn2.groupby("scrM_OverNo")["scrM_IsWicket"].sum().to_dict() if "scrM_IsWicket" in inn2.columns else {}

    for i, over in enumerate(overs):
        if wk1.get(over, 0) > 0:
            fig.add_trace(go.Scatter(
                x=[over - bar_width / 2],
                y=[float(runs1_vals[i])],
                mode="markers",
                marker=dict(color="red", size=15, symbol="circle"),
                showlegend=False,
            ))
        if wk2.get(over, 0) > 0:
            fig.add_trace(go.Scatter(
                x=[over + bar_width / 2],
                y=[float(runs2_vals[i])],
                mode="markers",
                marker=dict(color="red", size=15, symbol="circle"),
                showlegend=False,
            ))

    # --- Value labels ---
    for i, over in enumerate(overs):
        if runs1_vals[i] > 0:
            fig.add_annotation(
                x=over - bar_width / 2,
                y=runs1_vals[i],
                text=str(int(runs1_vals[i])),
                showarrow=False,
                font=dict(size=18, color=label_color),
                yshift=12
            )
        if runs2_vals[i] > 0:
            fig.add_annotation(
                x=over + bar_width / 2,
                y=runs2_vals[i],
                text=str(int(runs2_vals[i])),
                showarrow=False,
                font=dict(size=18, color=label_color),
                yshift=12
            )

    # Axis scaling
    y_tick_max = int(max(runs1_vals.max(), runs2_vals.max()))
    y_tick_step = max(1, int(np.ceil(y_tick_max / 5)))

    fig.update_layout(
        xaxis=dict(
            title=dict(text="Over Number", font=dict(size=19, color=label_color)),
            tickfont=dict(size=19, color=label_color),
            tickvals=overs,
            tickmode="array",
            showgrid=False,
            zeroline=False
        ),
        yaxis=dict(
            title=dict(text="Runs Scored", font=dict(size=19, color=label_color)),
            tickfont=dict(size=19, color=label_color),
            tickvals=list(range(0, y_tick_max + y_tick_step, y_tick_step)),
            showgrid=False,
            zeroline=False
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            font=dict(size=17, color=label_color)
        ),
        autosize=True,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=30, r=30, t=10, b=40),
        barmode="group",
        bargap=0.15
    )

    return fig









# Run rate (cumulative) chart — phase-aware
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

    # phase ranges
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

    # runs per over
    runs1 = (innings1_df.groupby("scrM_OverNo")["scrM_DelRuns"].sum()
             .reindex(overs, fill_value=0) if not innings1_df.empty else np.zeros(len(overs)))
    runs2 = (innings2_df.groupby("scrM_OverNo")["scrM_DelRuns"].sum()
             .reindex(overs, fill_value=0) if not innings2_df.empty else np.zeros(len(overs)))

    runs1_vals = np.array(runs1)
    runs2_vals = np.array(runs2)

    # cumulative RR
    denom = np.arange(1, len(overs) + 1)
    crr1 = np.cumsum(runs1_vals) / denom if len(denom) else np.array([])
    crr2 = np.cumsum(runs2_vals) / denom if len(denom) else np.array([])

    # ✔ Same bright green for readability
    label_color = "#00C853"

    # TEAM 1
    if len(crr1):
        fig.add_trace(go.Scatter(
            x=overs, y=crr1,
            mode="lines+markers+text",
            name=team1_name,
            marker=dict(color="#002f6c"),
            text=[f"{r:.2f}" for r in crr1],
            textposition="top center",
            textfont=dict(size=18, color=label_color)
        ))

    # TEAM 2
    if len(crr2):
        fig.add_trace(go.Scatter(
            x=overs, y=crr2,
            mode="lines+markers+text",
            name=team2_name,
            marker=dict(color="#FF8C00"),
            text=[f"{r:.2f}" for r in crr2],
            textposition="top center",
            textfont=dict(size=18, color=label_color)
        ))

    # wicket markers
    wk1 = innings1_df.groupby("scrM_OverNo")["scrM_IsWicket"].sum().to_dict() \
         if "scrM_IsWicket" in innings1_df.columns else {}
    wk2 = innings2_df.groupby("scrM_OverNo")["scrM_IsWicket"].sum().to_dict() \
         if "scrM_IsWicket" in innings2_df.columns else {}

    for i, over in enumerate(overs):
        if wk1.get(over, 0) > 0 and len(crr1) > i:
            fig.add_trace(go.Scatter(
                x=[over], y=[float(crr1[i])],
                mode="markers",
                marker=dict(color="red", size=15, symbol="circle"),
                showlegend=False
            ))
        if wk2.get(over, 0) > 0 and len(crr2) > i:
            fig.add_trace(go.Scatter(
                x=[over], y=[float(crr2[i])],
                mode="markers",
                marker=dict(color="red", size=15, symbol="circle"),
                showlegend=False
            ))

    # y max padding
    max_crr = 0
    if len(crr1): max_crr = max(max_crr, float(np.nanmax(crr1)))
    if len(crr2): max_crr = max(max_crr, float(np.nanmax(crr2)))
    y_max = max_crr + 0.5 if max_crr > 0 else 1.0

    # layout
    fig.update_layout(
        xaxis=dict(
            title=dict(text="Over Number", font=dict(size=19, color=label_color)),
            tickvals=overs,
            tickmode="array",
            tickfont=dict(size=19, color=label_color),
            showgrid=False,
            zeroline=False
        ),
        yaxis=dict(
            title=dict(text="Run Rate (Cumulative)", font=dict(size=19, color=label_color)),
            tickfont=dict(size=19, color=label_color),
            range=[0, y_max],
            showgrid=False,
            zeroline=False
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            font=dict(size=17, color=label_color)
        ),

        # ⭐ PREVENT LABELS FROM GETTING CUT ⭐
        height=550,

        autosize=True,
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
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

    # Green text color
    green = "#00C853"

    fig = go.Figure()

    # ---------------- TEAM 1 ----------------
    fig.add_trace(go.Pie(
        labels=labels,
        values=runs_inning1,
        name=f'{team1_name}',
        hole=0.55,
        textinfo='label+value',
        hoverinfo='label+value+percent',
        domain=dict(x=[0, 0.48]),
        marker=dict(colors=colors),
        showlegend=True,

        insidetextfont=dict(size=16, color=None),
        outsidetextfont=dict(size=16, color=green),
        hoverlabel=dict(font_size=16)
    ))

    # Insert team name **inside donut center**
    fig.add_annotation(
        x=0.24, y=0.50,
        text=f"<b>{team1_name}</b>",
        showarrow=False,
        font=dict(size=20, color=green),
        xanchor="center",
        yanchor="middle"
    )

    # ---------------- TEAM 2 ----------------
    fig.add_trace(go.Pie(
        labels=labels,
        values=runs_inning2,
        name=f'{team2_name}',
        hole=0.55,
        textinfo='label+value',
        hoverinfo='label+value+percent',
        domain=dict(x=[0.52, 1]),
        marker=dict(colors=colors),
        showlegend=True,

        insidetextfont=dict(size=16, color=None),
        outsidetextfont=dict(size=16, color=green),
        hoverlabel=dict(font_size=16)
    ))

    # Insert team name **inside donut center**
    fig.add_annotation(
        x=0.76, y=0.50,
        text=f"<b>{team2_name}</b>",
        showarrow=False,
        font=dict(size=20, color=green),
        xanchor="center",
        yanchor="middle"
    )

    # ---------------- LAYOUT ----------------
    fig.update_layout(
        autosize=True,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=10, r=10, t=20, b=10),

        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.05,
            xanchor="center",
            x=0.5,
            font=dict(size=16, color=green)
        )
    )

    return fig








# 4️⃣ Extra Runs Chart (all extras: wide, no-ball, bye, leg-bye, penalty)
def create_extra_runs_comparison_chart(df, team1, team2):
    team1_name, team2_name = safe_team_name(team1), safe_team_name(team2)

    inning1_df = df[df['scrM_InningNo'] == 1]
    inning2_df = df[df['scrM_InningNo'] == 2]

    wide_runs     = [inning1_df['scrM_WideRuns'].sum(),     inning2_df['scrM_WideRuns'].sum()]
    noball_runs   = [inning1_df['scrM_NoBallRuns'].sum(),   inning2_df['scrM_NoBallRuns'].sum()]
    bye_runs      = [inning1_df['scrM_ByeRuns'].sum(),      inning2_df['scrM_ByeRuns'].sum()]
    legbye_runs   = [inning1_df['scrM_LegByeRuns'].sum(),   inning2_df['scrM_LegByeRuns'].sum()]
    penalty_runs  = [inning1_df['scrM_PenaltyRuns'].sum(),  inning2_df['scrM_PenaltyRuns'].sum()]

    # ✔ SAME green theme as runs-per-over chart
    label_color = "#00C853"

    fig = go.Figure()

    # --------------------------- BAR TRACES ---------------------------
    # Inside text font increased from 18 → 20 (+2)
    inside_font = dict(size=20)

    fig.add_trace(go.Bar(
        y=[team1_name, team2_name], x=wide_runs,
        name='Wide Runs', orientation='h',
        marker=dict(color='#002f6c'),
        text=wide_runs, textposition='inside',
        insidetextfont=inside_font
    ))

    fig.add_trace(go.Bar(
        y=[team1_name, team2_name], x=noball_runs,
        name='No Ball Runs', orientation='h',
        marker=dict(color='#228B22'),
        text=noball_runs, textposition='inside',
        insidetextfont=inside_font
    ))

    fig.add_trace(go.Bar(
        y=[team1_name, team2_name], x=bye_runs,
        name='Bye Runs', orientation='h',
        marker=dict(color='#8B008B'),
        text=bye_runs, textposition='inside',
        insidetextfont=inside_font
    ))

    fig.add_trace(go.Bar(
        y=[team1_name, team2_name], x=legbye_runs,
        name='Leg Bye Runs', orientation='h',
        marker=dict(color='#FF8C00'),
        text=legbye_runs, textposition='inside',
        insidetextfont=inside_font
    ))

    fig.add_trace(go.Bar(
        y=[team1_name, team2_name], x=penalty_runs,
        name='Penalty Runs', orientation='h',
        marker=dict(color='#B22222'),
        text=penalty_runs, textposition='inside',
        insidetextfont=inside_font
    ))

    # --------------------------- LAYOUT ---------------------------
    fig.update_layout(
        autosize=True,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        barmode='stack',
        bargap=0.4,
        bargroupgap=0.2,
        margin=dict(l=30, r=20, t=10, b=30),

        xaxis=dict(
            title=dict(text='Runs', font=dict(size=19, color=label_color)),
            tickfont=dict(size=21, color=label_color),
            showgrid=False, zeroline=False
        ),
        yaxis=dict(
            title=dict(text='Teams', font=dict(size=19, color=label_color)),
            tickfont=dict(size=21, color=label_color),
            showgrid=False, zeroline=False
        ),

        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.05,
            xanchor="center",
            x=0.5,
            font=dict(size=19, color=label_color)
        )
    )

    return fig





# 5️⃣ Wagon Area Comparison (batting runs only, exclude extras, dynamic areas)
def create_comparison_bar_chart(df, team1, team2):
    team1_name, team2_name = safe_team_name(team1), safe_team_name(team2)

    inning1_df = df[(df['scrM_InningNo'] == 1) & (df['scrM_BatsmanRuns'].between(0, 6))]
    inning2_df = df[(df['scrM_InningNo'] == 2) & (df['scrM_BatsmanRuns'].between(0, 6))]

    valid_areas = df["scrM_WagonArea_zName"].dropna().unique().tolist()

    data1 = inning1_df.groupby("scrM_WagonArea_zName")["scrM_BatsmanRuns"].sum().reset_index()
    data2 = inning2_df.groupby("scrM_WagonArea_zName")["scrM_BatsmanRuns"].sum().reset_index()

    comparison = pd.merge(data1, data2, on="scrM_WagonArea_zName", how="outer").fillna(0)
    comparison.columns = ["Wagon Area", f"{team1_name}", f"{team2_name}"]

    comparison = comparison[comparison["Wagon Area"].isin(valid_areas)]

    # THEME GREEN
    label_color = "#00C853"

    fig = go.Figure()

    # ------------------------- TEAM 1 -------------------------
    fig.add_trace(go.Bar(
        x=comparison["Wagon Area"],
        y=comparison[f"{team1_name}"],
        name=team1_name,
        marker=dict(color="#002f6c", line=dict(width=0)),
        text=comparison[f"{team1_name}"],
        textposition="outside",
        textfont=dict(size=18, color=label_color)
    ))

    # ------------------------- TEAM 2 -------------------------
    fig.add_trace(go.Bar(
        x=comparison["Wagon Area"],
        y=comparison[f"{team2_name}"],
        name=team2_name,
        marker=dict(color="#FF8C00", line=dict(width=0)),
        text=comparison[f"{team2_name}"],
        textposition="outside",
        textfont=dict(size=18, color=label_color)
    ))

    # ------------------------- LAYOUT FIX (PREVENT RESIZING) -------------------------
    fig.update_layout(
        autosize=True,
        height=650,  # ★★★ prevents shrinking and label overlap ★★★
        barmode="group",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=20, r=20, t=80, b=80),

        xaxis=dict(
            title=dict(text="Area", font=dict(size=19, color=label_color)),
            tickfont=dict(size=19, color=label_color),
            tickangle=-30,
            showgrid=False, zeroline=False, showline=False
        ),
        yaxis=dict(
            title=dict(text="Runs", font=dict(size=19, color=label_color)),
            tickfont=dict(size=19, color=label_color),
            showgrid=False, zeroline=False, showline=False
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.05,
            xanchor="center",
            x=0.5,
            font=dict(size=19, color=label_color)
        )
    )


    return fig





import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import io, base64
import pandas as pd

def generate_team_comparison_radar(team1_name, team2_name, df,
                                   run_filter_team1=None, run_filter_team2=None):
    """
    BIG-SIZE team comparison radar charts (PB vs CSK etc.)
    EXACT visuals of session radar chart:
    - Thick rim
    - Same font sizes
    - Same runs + percentage layout
    - ❌ NO detailed breakdown text (1s, 2s, 4s, 6s)
    """

    import numpy as np
    import matplotlib.pyplot as plt
    import io, base64

    # ---------------------------------------------------------------
    # Helper: normalize run filters
    # ---------------------------------------------------------------
    def normalize_rf(rf):
        if rf is None:
            return None
        if isinstance(rf, (list, tuple, set)):
            return set(int(x) for x in rf)
        s = str(rf)
        if s.lower() == "all":
            return None
        if "," in s:
            return set(int(x.strip()) for x in s.split(",") if x.strip())
        return set([int(s.strip())])

    rf1 = normalize_rf(run_filter_team1)
    rf2 = normalize_rf(run_filter_team2)

    # ---------------------------------------------------------------
    # SECTORS (fixed)
    # ---------------------------------------------------------------
    sectors = [
        "Mid Wicket", "Square Leg", "Fine Leg", "Third Man",
        "Point", "Covers", "Long Off", "Long On"
    ]
    sector_map = {name: i for i, name in enumerate(sectors)}
    sector_angles_deg = [112.5, 67.5, 22.5, 337.5,
                         292.5, 247.5, 202.5, 157.5]

    # ---------------------------------------------------------------
    # Compute per-sector breakdown for a team
    # ---------------------------------------------------------------
    def compute_breakdown(team_name, rf):
        breakdown = [{"1s":0,"2s":0,"3s":0,"4s":0,"6s":0} for _ in sectors]
        # Pick correct batting-team column
        # Determine correct batting team column
        if "scrM_tmMIdBattingName" in df.columns:
            team_col = "scrM_tmMIdBattingName"
        elif "BattingTeam" in df.columns:
            team_col = "BattingTeam"
        elif "scrM_BattingTeam" in df.columns:
            team_col = "scrM_BattingTeam"
        else:
            print("❌ No batting team column found in dataframe")
            return "", ""


        tdf = df[df[team_col] == team_name]

        for _, row in tdf.iterrows():
            sector = (
                str(row.get("scrM_WagonArea_zName"))
                or str(row.get("WagonArea"))
                or str(row.get("wagon_area"))
                or ""
            )

            try:
                runs = int(row.get("scrM_BatsmanRuns",0))
            except:
                runs = 0

            # apply run filter
            if rf is not None and runs not in rf:
                continue

            if sector in sector_map and runs > 0:
                idx = sector_map[sector]
                if runs == 1: breakdown[idx]["1s"] += 1
                elif runs == 2: breakdown[idx]["2s"] += 1
                elif runs == 3: breakdown[idx]["3s"] += 1
                elif runs == 4:
                    breakdown[idx]["4s"] += 1
                elif runs == 6:
                    breakdown[idx]["6s"] += 1


        return breakdown

    t1_bd = compute_breakdown(team1_name, rf1)
    t2_bd = compute_breakdown(team2_name, rf2)

    # ---------------------------------------------------------------
    # Create ONE BIG radar with session-style visuals
    # ---------------------------------------------------------------
    def make_big_radar(bd):
        fig, ax = plt.subplots(figsize=(8,8), subplot_kw=dict(polar=True))

        ax.set_theta_zero_location("N")
        ax.set_theta_direction(-1)
        ax.set_xticks([]); ax.set_yticks([])
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

        # Thick black rim (matching session radar)
        rim_radius = 1.10 * scale
        rim_circle = plt.Circle((0, 0), rim_radius, transform=ax.transData._b,
                                color='#6dbc45', linewidth=26, fill=False,
                                zorder=5, clip_on=False)
        ax.add_artist(rim_circle)

        # Sector lines
        for angle in np.linspace(0, 2*np.pi, 9):
            ax.plot([angle, angle], [0,1.0*scale], color='white', linewidth=3)

        # Compute runs
        sector_runs = [
            bd[i]["1s"] +
            bd[i]["2s"]*2 +
            bd[i]["3s"]*3 +
            bd[i]["4s"]*4 +
            bd[i]["6s"]*6
            for i in range(8)
        ]
        total_runs = sum(sector_runs)

        # Highlight max
        if total_runs > 0:
            max_i = sector_runs.index(max(sector_runs))
            ax.bar(np.deg2rad(sector_angles_deg[max_i]), 1.0*scale,
                   width=np.radians(45), color='red', alpha=0.25)

        # Labels (same placement as session radar with clip_on=False)
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

        # Runs + % (two-line format)
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

        # ❌ REMOVED BREAKDOWN TEXT
        # (No 1s 2s 4s 6s small text)

        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=260, transparent=True)
        plt.close(fig)
        buf.seek(0)
        return base64.b64encode(buf.read()).decode()

    # Return final images
    return make_big_radar(t1_bd), make_big_radar(t2_bd)



# 🥧 Player Contribution Donut
def create_player_contribution_donut(df, team, inning_no, phase_name="Overall"):
    """
    Donut chart: % contribution of each batter in runs scored during a phase.
    Only changes:
    - Outside text: green + size 16
    - Inside text: increased by +2 (size 16)
    - Legend font size increased by +2 (size 18)
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

    # Same palette
    colors = [
        '#002f6c', '#FF8C00', '#004C99', '#0069B3', '#0099E5',
        '#22CCEE', '#FF4477', '#44BB99', '#AA3377', '#EE7733'
    ]

    green = "#00C853"

    fig = go.Figure()

    fig.add_trace(go.Pie(
        labels=labels,
        values=values,
        hole=0.4,
        textinfo='percent',
        hoverinfo='label+value+percent',
        marker=dict(colors=colors),
        showlegend=True,

        # ✔ Inside text font increased by 2
        insidetextfont=dict(
            color=None,
            size=16
        ),

        # ✔ Outside text = green + bigger font
        outsidetextfont=dict(
            color=green,
            size=16
        )
    ))

    # ✔ Legend font size increased by +2 (now 18)
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
            font=dict(color=green, size=18)
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
    Updated according to create_player_contribution_donut():
    - Inside text size = 16
    - Outside text size = 16 & green
    - Legend font size = 18 & green
    """
    team_name = safe_team_name(team)

    # Filter only this inning
    inn_df = df[df["scrM_InningNo"] == inning_no]
    if inn_df.empty:
        return no_data_figure()

    # Dot balls per bowler
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

    # Same palette as batting donut
    colors = [
        '#002f6c', '#FF8C00', '#004C99', '#0069B3', '#0099E5',
        '#22CCEE', '#FF4477', '#44BB99', '#AA3377', '#EE7733'
    ]

    green = "#00C853"

    fig = go.Figure()
    fig.add_trace(go.Pie(
        labels=labels,
        values=values,
        hole=0.4,
        textinfo="percent",
        hoverinfo="label+value+percent",
        marker=dict(colors=colors),
        showlegend=True,

        # ✔ Inside label font (increased by +2)
        insidetextfont=dict(
            color=None,
            size=16
        ),

        # ✔ Outside labels green + bigger
        outsidetextfont=dict(
            color=green,
            size=16
        )
    ))

    # ✔ Legend font updated to match your style
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
            font=dict(
                color=green,
                size=18
            )
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
    
def calculate_maidens(df):
    """
    Return a mapping {bowler_name: maiden_count} computed from the given DataFrame.
    Definition used:
      - Count overs (grouped by scrM_PlayMIdBowlerName + scrM_OverNo)
      - For an over to be a 'maiden' its total runs conceded (batsman runs + wides + no-ball runs + byes + legbyes)
        must equal 0 AND the over must contain 6 legal deliveries (no missing balls).
    Notes:
      - df is expected to contain columns:
        scrM_PlayMIdBowlerName, scrM_OverNo, scrM_BatsmanRuns, scrM_WideRuns, scrM_NoBallRuns,
        scrM_ByeRuns, scrM_LegByeRuns, scrM_IsWideBall, scrM_IsNoBall
    """
    if df is None or df.empty:
        return {}

    # Ensure columns exist and fillna
    cols = [
        "scrM_PlayMIdBowlerName", "scrM_OverNo", "scrM_BatsmanRuns",
        "scrM_WideRuns", "scrM_NoBallRuns", "scrM_ByeRuns", "scrM_LegByeRuns",
        "scrM_IsWideBall", "scrM_IsNoBall"
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = 0

    # Compute total runs conceded per delivery (includes extras)
    df = df.copy()
    df["__runs_conceded"] = (
        df.get("scrM_BatsmanRuns", 0).fillna(0).astype(int) +
        df.get("scrM_WideRuns", 0).fillna(0).astype(int) +
        df.get("scrM_NoBallRuns", 0).fillna(0).astype(int) +
        df.get("scrM_ByeRuns", 0).fillna(0).astype(int) +
        df.get("scrM_LegByeRuns", 0).fillna(0).astype(int)
    )

    # Determine legal deliveries in each over (legal = not wide and not noball)
    df["__is_legal_ball"] = (~(df.get("scrM_IsWideBall", 0).fillna(0).astype(int).astype(bool) |
                              df.get("scrM_IsNoBall", 0).fillna(0).astype(int).astype(bool))).astype(int)

    # Group by bowler + over, sum runs and legal ball count
    grp = df.groupby(["scrM_PlayMIdBowlerName", "scrM_OverNo"], as_index=False).agg(
        runs_in_over = ("__runs_conceded", "sum"),
        legal_balls = ("__is_legal_ball", "sum")
    )

    # A maiden over: runs_in_over == 0 and legal_balls == 6
    grp["is_maiden"] = ((grp["runs_in_over"] == 0) & (grp["legal_balls"] == 6)).astype(int)

    maiden_map = grp.groupby("scrM_PlayMIdBowlerName")["is_maiden"].sum().to_dict()

    # ensure ints (not np.int64)
    maiden_map = {k: int(v) for k,v in maiden_map.items()}

    return maiden_map



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
    # Detect the correct wicket column
    if "scrM_IsBowlerWicket" in df.columns:
        wicket_col = "scrM_IsBowlerWicket"
    elif "scrM_IsWicket" in df.columns:
        wicket_col = "scrM_IsWicket"
    else:
        wicket_col = None

    # Batting stats per bowler type
    batting_stats = (
        df.groupby(["BowlingType", "scrM_PlayMIdStrikerName"])
        .agg(
            Runs=("scrM_BatsmanRuns", "sum"),
            Balls=("IsLegal", "sum"),
            Dots=("scrM_BatsmanRuns", lambda x: (x == 0).sum()),
            Fours=("scrM_BatsmanRuns", lambda x: (x == 4).sum()),
            Sixes=("scrM_BatsmanRuns", lambda x: (x == 6).sum()),
            W=(wicket_col, "sum") if wicket_col else ("scrM_BatsmanRuns", lambda x: 0)
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
        ["Batter", "Runs", "Balls", "SR", "Dots", "Fours", "Sixes", "W"]
    ]

    spin_table = batting_stats[batting_stats["BowlingType"] == "Spin"][
        ["Batter", "Runs", "Balls", "SR", "Dots", "Fours", "Sixes", "W"]
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
    Font sizes increased by +7 total (14 → 17).
    """
    import plotly.graph_objects as go
    import pandas as pd

    if phase_df.empty:
        return no_data_figure()

    extras_cols = ["scrM_ByeRuns", "scrM_LegByeRuns", "scrM_NoBallRuns",
                   "scrM_WideRuns", "scrM_PenaltyRuns"]
    for col in extras_cols:
        if col not in phase_df.columns:
            phase_df[col] = 0

    phase_df["scrM_Extras"] = phase_df[extras_cols].sum(axis=1)
    phase_df["Valid_Ball"] = phase_df["scrM_IsValidBall"] == 1

    # Partnership key
    phase_df["Partnership_Key"] = phase_df.apply(
        lambda row: "_&_".join(sorted([
            str(row["scrM_PlayMIdStrikerName"]),
            str(row["scrM_PlayMIdNonStrikerName"])
        ])),
        axis=1
    )

    # Build partnership list
    partnerships = []
    for _, group in phase_df[phase_df["Valid_Ball"]].groupby("Partnership_Key"):
        striker = group["scrM_PlayMIdStrikerName"].iloc[0]
        non_striker = group["scrM_PlayMIdNonStrikerName"].iloc[0]

        b1 = group["scrM_PlayMIdStrikerName"].iloc[0]
        b2 = group["scrM_PlayMIdNonStrikerName"].iloc[0]
        batter1, batter2 = b1, b2  # do NOT sort


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
    
    phase_df["ball_id"] = (phase_df["scrM_OverNo"] - 1) * 6 + phase_df["scrM_DelNo"]

    order_map = (
        phase_df.groupby("Partnership_Key")["ball_id"]
        .min()
        .sort_values()
    )

    partnerships_df["Appear_Order"] = partnerships_df["Batter1"] + "_" + partnerships_df["Batter2"]
    partnerships_df["Appear_Order"] = partnerships_df["Appear_Order"].map(order_map)

    partnerships_df = partnerships_df.sort_values(by="Appear_Order").reset_index(drop=True)

    if partnerships_df.empty:
        return no_data_figure()

    # Sort
    # partnerships_df = partnerships_df.sort_values(by="Total", ascending=False)

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

    strip_gap = 2.5
    y_positions = [(n-1-i) * strip_gap for i in range(n)]
    strip_thickness = 0.7

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

    # NEW FONT SIZE (increased by 3 more)
    FONT_SIZE = 17

    # Annotations
    for i, row in partnerships_df.iterrows():
        y = y_positions[partnerships_df.index.get_loc(i)]

        # Batter 1
        fig.add_annotation(
            x=-0.8, y=y, xanchor="right", align="center",
            text=f"{row['Batter1']}<br><b>{row['Batter1_Runs']} ({row['Batter1_Balls']})</b>",
            showarrow=False, font=dict(size=FONT_SIZE, color="#FF8C00")
        )

        # Batter 2
        fig.add_annotation(
            x=0.8, y=y, xanchor="left", align="center",
            text=f"{row['Batter2']}<br><b>{row['Batter2_Runs']} ({row['Batter2_Balls']})</b>",
            showarrow=False, font=dict(size=FONT_SIZE, color="#1E90FF")
        )

        # Partnership total
        fig.add_annotation(
            x=0, y=y + 0.6, xanchor="center", align="center",
            text=f"<b>Partnership - {row['Total']} ({row['Balls']})</b>",
            showarrow=False, font=dict(size=FONT_SIZE, color="#808080")
        )

        # Extras
        fig.add_annotation(
            x=0, y=y - 0.6, xanchor="center", align="center",
            text=f"Extras - {row['Extras']}",
            showarrow=False, font=dict(size=FONT_SIZE, color="#32CD32")
        )

    # Layout
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

    FONT_SIZE = 18
    FONT_COLOR = "#19a94b"

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
        textfont=dict(color=FONT_COLOR, size=FONT_SIZE),
        showlegend=False
    )

    fig.update_layout(
        height=600,  # 🔥 ensures enough vertical space
        title=dict(text=title, x=0.5, font=dict(size=FONT_SIZE, color=FONT_COLOR)),
        autosize=True,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',

        margin=dict(l=10, r=10, t=50, b=120),  # 🔥 big bottom margin

        xaxis=dict(
            title=dict(text="Delivery Type", font=dict(color=FONT_COLOR, size=FONT_SIZE)),
            tickfont=dict(color=FONT_COLOR, size=FONT_SIZE),
            tickangle=-35,       # 🔥 rotate labels to avoid overlap/cut
            automargin=True,     # 🔥 Plotly adjusts margins automatically
            showgrid=False,
            zeroline=False
        ),

        yaxis=dict(
            title=dict(text="Percentage (%)", font=dict(color=FONT_COLOR, size=FONT_SIZE)),
            tickfont=dict(color=FONT_COLOR, size=FONT_SIZE),
            automargin=True,
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

    # ================================
    #   NEW FONT SIZE & COLOR
    # ================================
    FONT_SIZE = 18  # 14 + 4
    FONT_COLOR = "#19a94b"

    fig = px.bar(
        data,
        y="Pitch_Area", x="Percentage", orientation="h",
        text=data["Percentage"].apply(lambda x: f"{x:.1f}%"),
        color="Pitch_Area",
        color_discrete_sequence=colors
    )

    fig.update_traces(
        textposition="outside",
        textfont=dict(color=FONT_COLOR, size=FONT_SIZE),
        showlegend=False
    )

    fig.update_layout(
        title=dict(
            text=title,
            x=0.5,
            font=dict(size=FONT_SIZE, color=FONT_COLOR)
        ),
        autosize=True,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        margin=dict(l=10, r=10, t=50, b=10),

        xaxis=dict(
            title=dict(text="Percentage (%)", font=dict(color=FONT_COLOR, size=FONT_SIZE)),
            tickfont=dict(color=FONT_COLOR, size=FONT_SIZE),
            showgrid=False,
            zeroline=False
        ),
        yaxis=dict(
            title=dict(text="Pitch Area", font=dict(color=FONT_COLOR, size=FONT_SIZE)),
            tickfont=dict(color=FONT_COLOR, size=FONT_SIZE),
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
    Fully upgraded: Font size + color applied everywhere.
    """
    FONT_SIZE = 18
    FONT_COLOR = "#19a94b"

    team_data = build_bowling_runs_conceded_summary(df, team_name, inning_no, phase_name)
    if team_data.empty:
        return go.Figure(), pd.DataFrame()

    # ---------- Melt Data ----------
    melted = team_data.melt(
        id_vars=["Bowler"],
        value_vars=["< 6 Runs Overs", "= 6 Runs Overs", "> 6 Runs Overs"]
    )

    # ---------- Chart ----------
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

    # ---------- Bar Label Style ----------
    fig.update_traces(
        texttemplate="%{y}",
        textposition="outside",
        textfont=dict(color=FONT_COLOR, size=FONT_SIZE),
        marker=dict(line=dict(width=0))
    )

    y_max = melted["value"].max()

    # ---------- Layout ----------
    fig.update_layout(
        height=600,
        title=dict(
            text=f"{team_name} – Runs Conceded Breakdown ({phase_name})",
            x=0.5,
            y=0.97,                     # ✅ highest safe value
            xanchor="center",
            yanchor="top",
            font=dict(size=FONT_SIZE, color=FONT_COLOR),
            pad=dict(t=10)              # small internal padding
        ),

        autosize=True,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",

        margin=dict(l=20, r=20, t=120, b=120),  # bigger bottom margin

        xaxis=dict(
            title=dict(text="Bowler", font=dict(color=FONT_COLOR, size=FONT_SIZE)),
            tickfont=dict(color=FONT_COLOR, size=FONT_SIZE),
            tickangle=-35,        # prevent overlapping labels
            automargin=True,
            showgrid=False,
            zeroline=False
        ),

        yaxis=dict(
            title=dict(text="Number of Overs", font=dict(color=FONT_COLOR, size=FONT_SIZE)),
            tickfont=dict(color=FONT_COLOR, size=FONT_SIZE),
            dtick=1,
            automargin=True,
            rangemode="tozero",
            range=[0, y_max + 1],
            showgrid=False,
            zeroline=False
        ),

        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.05,
            xanchor="center",
            x=0.5,
            title_text="",
            font=dict(color=FONT_COLOR, size=FONT_SIZE)
        )
    )

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
    broken down by ball number in the over.
    """
    if team_data.empty:
        print(f"No data to plot for {team_name}. Returning empty figure.")
        return go.Figure()

    # Custom color palette for ball numbers
    custom_colors = {
        "1st": "#002f6c", "2nd": "#FF8C00", "3rd": "#004C99",
        "4th": "#0069B3", "5th": "#0099E5", "6th": "#EE553B"
    }

    FONT_SIZE = 18
    FONT_COLOR = "#19a94b"

    # Melt dataframe for plotting
    melted = team_data.melt(
        id_vars=["Bowler"],
        value_vars=["1st", "2nd", "3rd", "4th", "5th", "6th"]
    )

    # ✅ Stacked bar chart
    fig = px.bar(
        melted,
        x="Bowler",
        y="value",
        color="scrM_BallNo",
        barmode="stack",
        labels={"value": "Boundary Count", "scrM_BallNo": "Ball Number"},
        color_discrete_map=custom_colors
    )

    # Bar labels inside bars
    fig.update_traces(
        texttemplate="%{y}",
        textposition="inside",
        insidetextanchor="middle",
        textfont=dict(color="white", size=FONT_SIZE)
    )

    # Max y for scaling
    y_max = melted["value"].max()

    fig.update_layout(
        height=600,

        # 🔥 SAME TITLE POSITIONING AS RUNS CONCEDED CHART
        title=dict(
            text=f"{team_name} – Boundaries Conceded per Ball",
            x=0.5,
            y=0.97,
            xanchor="center",
            yanchor="top",
            font=dict(size=FONT_SIZE, color=FONT_COLOR),
            pad=dict(t=10)
        ),

        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",

        margin=dict(l=20, r=20, t=120, b=80),

        # X-Axis styling
        xaxis=dict(
            showgrid=False,
            zeroline=False,
            showline=False,
            tickfont=dict(color=FONT_COLOR, size=FONT_SIZE),
            title=dict(text="Bowler", font=dict(color=FONT_COLOR, size=FONT_SIZE)),
        ),

        # Y-Axis styling
        yaxis=dict(
            showgrid=False,
            zeroline=False,
            showline=False,
            tickfont=dict(color=FONT_COLOR, size=FONT_SIZE),
            title=dict(text="Boundary Count", font=dict(color=FONT_COLOR, size=FONT_SIZE)),
            dtick=1,
            rangemode="tozero",
            range=[0, y_max + 1]
        ),

        # Legend styling
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.05,
            xanchor="center",
            x=0.5,
            title_text="",
            font=dict(color=FONT_COLOR, size=FONT_SIZE),
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
    """
    try:
        conn = get_connection()

        query = """
            WITH player_innings AS (
                SELECT
                    s.scrM_PlayMIdStrikerName AS player_name,
                    s.scrM_tmMIdBattingName AS team_short_name,
                    CASE 
                        WHEN s.scrM_StrikerBatterSkill LIKE '%Right%' THEN 'RHB'
                        WHEN s.scrM_StrikerBatterSkill LIKE '%Left%' THEN 'LHB'
                        ELSE 'UNK'
                    END AS batter_skill,
                    s.scrM_InnId,
                    SUM(s.scrM_BatsmanRuns) AS runs_in_inn,
                    COUNT(s.scrM_DelId) AS balls_in_inn,
                    SUM(CASE WHEN s.scrM_BatsmanRuns = 0 THEN 1 ELSE 0 END) AS dots_in_inn,
                    SUM(CASE WHEN s.scrM_IsBoundry = 1 THEN 1 ELSE 0 END) AS fours_in_inn,
                    SUM(CASE WHEN s.scrM_IsSixer = 1 THEN 1 ELSE 0 END) AS sixes_in_inn,
                    MAX(CAST(s.scrM_IsWicket AS INT)) AS got_out
                FROM tblScoreMaster s
                WHERE s.scrM_TrnMId = ?
                  AND s.scrM_IsValidBall = 1
                GROUP BY s.scrM_PlayMIdStrikerName, s.scrM_tmMIdBattingName, 
                         CASE 
                            WHEN s.scrM_StrikerBatterSkill LIKE '%Right%' THEN 'RHB'
                            WHEN s.scrM_StrikerBatterSkill LIKE '%Left%' THEN 'LHB'
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
            ORDER BY runs DESC;
        """

        df = pd.read_sql(query, conn, params=[tournament_id])
        conn.close()
        return df.to_dict(orient="records")

    except Exception as e:
        print("Error fetching players by tournament:", e)
        return []


def get_players_by_team(tournament_id, team_name):
    """
    Fetch aggregated player batting stats for a specific tournament AND team.
    Normalizes scrM_StrikerBatterSkill into RHB/LHB.
    """
    try:
        conn = get_connection()

        query = """
            WITH player_innings AS (
                SELECT
                    s.scrM_PlayMIdStrikerName AS player_name,
                    s.scrM_tmMIdBattingName AS team_short_name,
                    CASE 
                        WHEN s.scrM_StrikerBatterSkill LIKE '%Right%' THEN 'RHB'
                        WHEN s.scrM_StrikerBatterSkill LIKE '%Left%' THEN 'LHB'
                        ELSE 'UNK'
                    END AS batter_skill,
                    s.scrM_InnId,
                    SUM(s.scrM_BatsmanRuns) AS runs_in_inn,
                    COUNT(s.scrM_DelId) AS balls_in_inn,
                    SUM(CASE WHEN s.scrM_BatsmanRuns = 0 THEN 1 ELSE 0 END) AS dots_in_inn,
                    SUM(CASE WHEN s.scrM_IsBoundry = 1 THEN 1 ELSE 0 END) AS fours_in_inn,
                    SUM(CASE WHEN s.scrM_IsSixer = 1 THEN 1 ELSE 0 END) AS sixes_in_inn,
                    MAX(CAST(s.scrM_IsWicket AS INT)) AS got_out
                FROM tblScoreMaster s
                WHERE s.scrM_TrnMId = ?
                  AND s.scrM_tmMIdBattingName = ?
                  AND s.scrM_IsValidBall = 1
                GROUP BY s.scrM_PlayMIdStrikerName, s.scrM_tmMIdBattingName, 
                         CASE 
                            WHEN s.scrM_StrikerBatterSkill LIKE '%Right%' THEN 'RHB'
                            WHEN s.scrM_StrikerBatterSkill LIKE '%Left%' THEN 'LHB'
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
            ORDER BY runs DESC;
        """

        df = pd.read_sql(query, conn, params=[tournament_id, team_name])
        conn.close()
        return df.to_dict(orient="records")

    except Exception as e:
        print("Error fetching players by team:", e)
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
    Works without team selection — all bowlers in the tournament.
    Normalizes scrM_BowlerSkill into PACE/SPIN (Python-side).
    """
    try:
        conn = get_connection()
        query = """
            SELECT
                s.scrM_PlayMIdBowlerName AS player_name,
                s.scrM_tmMIdBowlingName AS team_short_name,
                s.scrM_BowlerSkill,
                s.scrM_InnId,
                SUM(s.scrM_BatsmanRuns + s.scrM_LegByeRuns + s.scrM_IsNoBall + s.scrM_IsWideBall) AS runs_in_inn,
                COUNT(s.scrM_DelId) AS balls_in_inn,
                SUM(CASE WHEN s.scrM_BatsmanRuns = 0 THEN 1 ELSE 0 END) AS dots_in_inn,
                SUM(CAST(s.scrM_IsWicket AS INT)) AS wkts_in_inn
            FROM tblScoreMaster s
            WHERE s.scrM_TrnMId = ?
              AND s.scrM_IsValidBall = 1
            GROUP BY s.scrM_PlayMIdBowlerName, s.scrM_tmMIdBowlingName, s.scrM_BowlerSkill, s.scrM_InnId
        """
        df = pd.read_sql(query, conn, params=[tournament_id])
        conn.close()

        if df.empty:
            return []

        # ✅ Normalize bowler skill
        df["bowler_skill"] = df["scrM_BowlerSkill"].apply(map_bowling_type_1)

        # Aggregate across innings
        agg = df.groupby(["player_name", "team_short_name", "bowler_skill"]).agg(
            innings=("scrM_InnId", "count"),
            runs=("runs_in_inn", "sum"),
            wkts=("wkts_in_inn", "sum"),
            overs=("balls_in_inn", lambda x: round(x.sum() / 6.0, 1)),
            dots=("dots_in_inn", "sum"),
            balls=("balls_in_inn", "sum"),
        ).reset_index()

        # Eco, SR, Avg
        agg["eco"] = agg.apply(
            lambda row: round((row["runs"] * 6.0 / row["balls"]), 2) if row["balls"] > 0 else 0, axis=1
        )
        agg["strike_rate"] = agg.apply(
            lambda row: round((row["balls"] / row["wkts"]), 2) if row["wkts"] > 0 else 0, axis=1
        )
        agg["average"] = agg.apply(
            lambda row: round((row["runs"] / row["wkts"]), 2) if row["wkts"] > 0 else 0, axis=1
        )

        return agg.to_dict(orient="records")

    except Exception as e:
        print("Error fetching bowlers by tournament:", e)
        return []


def get_bowlers_by_team(tournament_id, team_name):
    """
    Fetch aggregated player bowling stats for a specific tournament AND team.
    Normalizes scrM_BowlerSkill into PACE/SPIN (Python-side).
    """
    try:
        conn = get_connection()
        query = """
            SELECT
                s.scrM_PlayMIdBowlerName AS player_name,
                s.scrM_tmMIdBowlingName AS team_short_name,
                s.scrM_BowlerSkill,
                s.scrM_InnId,
                SUM(s.scrM_BatsmanRuns + s.scrM_LegByeRuns + s.scrM_IsNoBall + s.scrM_IsWideBall) AS runs_in_inn,
                COUNT(s.scrM_DelId) AS balls_in_inn,
                SUM(CASE WHEN s.scrM_BatsmanRuns = 0 THEN 1 ELSE 0 END) AS dots_in_inn,
                SUM(CAST(s.scrM_IsWicket AS INT)) AS wkts_in_inn
            FROM tblScoreMaster s
            WHERE s.scrM_TrnMId = ?
              AND s.scrM_tmMIdBowlingName = ?
              AND s.scrM_IsValidBall = 1
            GROUP BY s.scrM_PlayMIdBowlerName, s.scrM_tmMIdBowlingName, s.scrM_BowlerSkill, s.scrM_InnId
        """
        df = pd.read_sql(query, conn, params=[tournament_id, team_name])
        conn.close()

        if df.empty:
            return []

        # ✅ Normalize bowler skill
        df["bowler_skill"] = df["scrM_BowlerSkill"].apply(map_bowling_type_1)

        # Aggregate across innings
        agg = df.groupby(["player_name", "team_short_name", "bowler_skill"]).agg(
            innings=("scrM_InnId", "count"),
            runs=("runs_in_inn", "sum"),
            wkts=("wkts_in_inn", "sum"),
            overs=("balls_in_inn", lambda x: round(x.sum() / 6.0, 1)),
            dots=("dots_in_inn", "sum"),
            balls=("balls_in_inn", "sum"),
        ).reset_index()

        # Eco, SR, Avg
        agg["eco"] = agg.apply(
            lambda row: round((row["runs"] * 6.0 / row["balls"]), 2) if row["balls"] > 0 else 0, axis=1
        )
        agg["strike_rate"] = agg.apply(
            lambda row: round((row["balls"] / row["wkts"]), 2) if row["wkts"] > 0 else 0, axis=1
        )
        agg["average"] = agg.apply(
            lambda row: round((row["runs"] / row["wkts"]), 2) if row["wkts"] > 0 else 0, axis=1
        )

        return agg.to_dict(orient="records")

    except Exception as e:
        print("Error fetching bowlers by team:", e)
        return []

def get_offline_video_path(del_id, row, parent_path):
    """
    Try to resolve the correct offline video path for a delivery.
    row: dict from tblScoreMaster (with scrM_Video1FileName..6FileName)
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
    """
    Fetches the offline parent video path (base directory)
    from tblSettingsMaster.setM_VideoPath in your local DB.
    Returns a normalized Windows path or None if missing.
    """
    import os
    conn = None
    try:
        conn = get_connection()
        if not conn:
            print("❌ Could not connect to local DB.")
            return None

        cursor = conn.cursor()
        cursor.execute("SELECT TOP 1 setM_VideoPath FROM tblSettingsMaster")
        row = cursor.fetchone()
        conn.close()

        if row and row[0]:
            parent_path = row[0].strip()
            parent_path = os.path.normpath(parent_path)
            print(f"📂 Loaded parent video path from tblSettingsMaster: {parent_path}")
            return parent_path
        else:
            print("⚠️ No video path found in tblSettingsMaster.")
            return None

    except Exception as e:
        print(f"⚠️ Error fetching parent video path: {e}")
        return None



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


from flask import Response, request
import os

# utils.py (add / ensure these utilities exist)

import os
from typing import Optional
from flask import Response, request

VIDEO_BASE_PATH: Optional[str] = None

def set_video_base_path(path: str):
    global VIDEO_BASE_PATH
    VIDEO_BASE_PATH = path

def get_video_base_path():
    """
    Return in-memory base path if set and valid, otherwise load from DB (tblSettingsMaster.setM_VideoPath).
    Returns None if nothing valid found.
    """
    global VIDEO_BASE_PATH
    if VIDEO_BASE_PATH and os.path.isdir(VIDEO_BASE_PATH):
        return VIDEO_BASE_PATH

    # Try reading from DB
    try:
        from .apps import get_connection  # import from your app module (adjust if different)
    except Exception:
        try:
            # fallback for other import layout
            from apps import get_connection
        except Exception:
            get_connection = None

    if get_connection:
        try:
            conn = get_connection()
            cur = conn.cursor()
            cur.execute("SELECT TOP 1 setM_VideoPath FROM tblSettingsMaster")
            row = cur.fetchone()
            try:
                cur.close()
                conn.close()
            except:
                pass
            if row and row[0]:
                candidate = row[0]
                if os.path.isdir(candidate):
                    VIDEO_BASE_PATH = candidate
                    print("📁 Loaded VIDEO_BASE_PATH from DB →", VIDEO_BASE_PATH)
                    return VIDEO_BASE_PATH
                else:
                    print("⚠️ VIDEO_BASE_PATH from DB does not exist on disk:", candidate)
        except Exception as e:
            print("⚠️ Error reading VIDEO_BASE_PATH from DB:", e)

    return None

def partial_response(path):
    """
    Streams large files (like MP4s) in partial chunks to support Range requests.
    """
    range_header = request.headers.get('Range', None)
    if not os.path.exists(path):
        return Response("File not found", status=404)

    file_size = os.path.getsize(path)
    start, end = 0, file_size - 1

    if range_header:
        # parse Range: bytes=start-end
        bytes_range = range_header.replace('bytes=', '')
        start_str, sep, end_str = bytes_range.partition('-')
        try:
            if start_str:
                start = int(start_str)
            if end_str:
                end = int(end_str)
        except:
            pass

    with open(path, 'rb') as f:
        f.seek(start)
        data = f.read(end - start + 1)

    rv = Response(data, status=206, mimetype="video/mp4", direct_passthrough=True)
    rv.headers.add('Content-Range', f'bytes {start}-{end}/{file_size}')
    rv.headers.add('Accept-Ranges', 'bytes')
    return rv

def generate_progressive_radar_chart(day_no, inning_no, inning_df, stance=None):
    """
    Generates a progressive radar chart for the multiday dashboard.
    ✅ Uses real data from the given inning_df (filtered by day + inning)
    ✅ Auto-detects stance if not provided
    ✅ Returns None safely if no valid data
    ✅ Keeps same radar design (rim, labels, layout)
    """
    import numpy as np
    import matplotlib.pyplot as plt
    import io, base64

    # === Auto-detect stance if not provided ===
    if stance is None:
        if "scrM_PlayMIdStrikerBatStyle_zName" in inning_df.columns:
            stance_value = inning_df["scrM_PlayMIdStrikerBatStyle_zName"].dropna().mode()
            if not stance_value.empty and "Left" in stance_value.iloc[0]:
                stance = "LHB"
            else:
                stance = "RHB"
        else:
            stance = "RHB"

    # === Radar Labels (fixed 8 directions) ===
    labels = ["Mid Wicket", "Square Leg", "Fine Leg", "Third Man",
              "Point", "Covers", "Long Off", "Long On"]

    # === Map DB shot zones to radar zones ===
    zone_map = {
        "Mid Wicket": "Mid Wicket", "Deep Mid Wicket": "Mid Wicket",
        "Square Leg": "Square Leg", "Backward Square Leg": "Square Leg",
        "Fine Leg": "Fine Leg", "Deep Fine Leg": "Fine Leg",
        "Third Man": "Third Man", "Deep Third Man": "Third Man",
        "Point": "Point", "Backward Point": "Point",
        "Covers": "Covers", "Extra Cover": "Covers",
        "Long Off": "Long Off", "Mid Off": "Long Off",
        "Long On": "Long On", "Mid On": "Long On"
    }

    # === Filter for valid balls only ===
    valid_df = inning_df.copy()
    valid_df = valid_df[valid_df.get("scrM_IsValidBall", 1) == 1]
    valid_df = valid_df[valid_df.get("scrM_BatsmanRuns", 0) >= 0]

    # ✅ Prefer WagonArea if available, else fallback to BatPitchArea
    if "scrM_WagonArea_zName" in valid_df.columns and valid_df["scrM_WagonArea_zName"].notna().any():
        valid_df["RadarZone"] = valid_df["scrM_WagonArea_zName"].map(zone_map)
    elif "scrM_BatPitchArea_zName" in valid_df.columns:
        valid_df["RadarZone"] = valid_df["scrM_BatPitchArea_zName"].map(zone_map)
    else:
        return None


    # === Aggregate real scoring data ===
    breakdown_data = [{ "1s":0, "2s":0, "3s":0, "4s":0, "6s":0 } for _ in labels]
    for _, row in valid_df.iterrows():
        zone = row["RadarZone"]
        runs = int(row["scrM_BatsmanRuns"])
        if zone in labels and runs in [1, 2, 3, 4, 6]:
            idx = labels.index(zone)
            breakdown_data[idx][f"{runs}s"] += 1

    # === Calculate sector totals ===
    sector_runs = [(bd["1s"]*1)+(bd["2s"]*2)+(bd["3s"]*3)+(bd["4s"]*4)+(bd["6s"]*6) for bd in breakdown_data]
    total_sector_runs = sum(sector_runs)
    total_actual_runs = int(valid_df["scrM_BatsmanRuns"].sum())

    if total_actual_runs == 0:
        return None  # avoid blank radar

    # === Normalize if mismatch ===
    if total_sector_runs != total_actual_runs and total_sector_runs > 0:
        scale_factor = total_actual_runs / total_sector_runs
        sector_runs = [r * scale_factor for r in sector_runs]

    # === Polar setup ===
    num_vars = len(labels)
    angles = np.linspace(0, 2 * np.pi, num_vars, endpoint=False).tolist()
    angles += angles[:1]

    fig, ax = plt.subplots(figsize=(3.0, 3.0), subplot_kw=dict(polar=True))
    ax.set_theta_zero_location("N")
    ax.set_theta_direction(-1)
    ax.set_frame_on(False)
    ax.set_xticks([])
    ax.set_yticks([])
    ax.spines['polar'].set_visible(False)
    scale = 1.05
    ax.set_aspect('equal')

    # === Background Layers ===
    rim_radius = 1.03 * scale
    rim_circle = plt.Circle((0, 0), rim_radius, transform=ax.transData._b,
                            color='#6dbc45', linewidth=10, fill=False, zorder=5, clip_on=False)
    ax.add_artist(rim_circle)

    outer_circle = plt.Circle((0, 0), 1.0 * scale, transform=ax.transData._b, color='#19a94b', zorder=0)
    inner_circle = plt.Circle((0, 0), 0.6 * scale, transform=ax.transData._b, color='#4CAF50', zorder=1)
    pitch = plt.Rectangle((-0.035, -0.15), 0.07, 0.3, transform=ax.transData._b, color='burlywood', zorder=2)
    ax.add_artist(outer_circle)
    ax.add_artist(inner_circle)
    ax.add_artist(pitch)

    # === Sector lines ===
    for angle in np.linspace(0, 2 * np.pi, 9):
        ax.plot([angle, angle], [0, 1.0 * scale], color='white', linewidth=0.5, zorder=3)

    # === Orientation ===
    rhb_sector_angles_deg = [112.5, 67.5, 22.5, 337.5, 292.5, 247.5, 202.5, 157.5]
    if stance == "LHB":
        swap_map = {0:5,1:4,2:7,3:6,4:1,5:0,6:2,7:3}
        sector_runs = [sector_runs[swap_map[i]] for i in range(8)]
        breakdown_data = [breakdown_data[swap_map[i]] for i in range(8)]
    sector_angles_deg = rhb_sector_angles_deg

    # === Highlight Max Sector ===
    if total_sector_runs > 0:
        max_idx = int(np.argmax(sector_runs))
        max_angle = np.deg2rad(sector_angles_deg[max_idx])
        ax.bar(max_angle, 1.0 * scale, width=np.radians(45), color='red', alpha=0.25, zorder=1)

    # === Fielding Labels ===
    if stance == "LHB":
        position_labels = [
            ("Covers", 112.5, -110, -0.005), ("Point", 67.5, -70, -0.005),
            ("Third Man", 22.5, -25, -0.005), ("Fine Leg", 337.5, 20, -0.01),
            ("Square Leg", 292.5, 70, -0.015), ("Mid Wicket", 247.5, 110, -0.005),
            ("Long On", 202.5, 155, -0.01), ("Long Off", 157.5, 200, -0.005)
        ]
    else:
        position_labels = [
            ("Mid Wicket", 112.5, -110, -0.005), ("Square Leg", 67.5, -70, -0.005),
            ("Fine Leg", 22.5, -25, -0.005), ("Third Man", 337.5, 20, -0.01),
            ("Point", 292.5, 70, -0.015), ("Covers", 247.5, 110, -0.005),
            ("Long Off", 202.5, 155, -0.01), ("Long On", 157.5, 200, -0.005)
        ]

    for text, ang, rot, offset in position_labels:
        ax.text(np.deg2rad(ang), rim_radius + offset, text,
                color='white', fontsize=6, fontweight='medium',
                ha='center', va='center', rotation=rot, rotation_mode='anchor', zorder=6)

    # === Runs + % Boxes ===
    total_runs = sum(sector_runs)
    box_positions = [
        (103.5, 0, 0.70), (67.5, 0, 0.70), (22.5, 0, 0.80), (337.5, 0, 0.80),
        (292.5, 0, 0.75), (250.5, 0, 0.70), (204.5, 1, 0.59), (155.5, 1, 0.59)
    ]
    for i, (ang, rot, dist) in enumerate(box_positions):
        rad = np.deg2rad(ang)
        r = dist * scale
        runs = sector_runs[i]
        pct = (runs / total_runs * 100) if total_runs > 0 else 0
        label = f"{int(runs)} ({pct:.1f}%)"
        ax.text(rad, r, label, color='white', fontsize=7,
                ha='center', va='center', rotation=rot,
                bbox=dict(facecolor='black', alpha=0.6, boxstyle='round,pad=0.3'), zorder=10)

    # === Breakdown (1s, 2s, 4s, 6s) Text ===
    detail_positions = [
        (116.5, 0, 0.68), (78.5, 0, 0.68), (29.5, 0, 0.70), (330.5, 0, 0.70),
        (283.5, 0, 0.68), (239.5, 0, 0.72), (200.5, 1, 0.72), (163.5, 1, 0.70)
    ]
    for i, (ang, rot, dist) in enumerate(detail_positions):
        bd = breakdown_data[i]
        text = f"1s:{bd['1s']}  2s:{bd['2s']}\n4s:{bd['4s']}  6s:{bd['6s']}"
        ax.text(np.deg2rad(ang), dist * scale, text,
                color='white', fontsize=6, ha='center', va='center',
                rotation=rot, rotation_mode='anchor', zorder=11)

    # === Title ===
    ax.text(0, -1.5 * scale, f"Day {day_no} - Inning {inning_no}",
            ha='center', va='center', color='white', fontsize=8.5, fontweight='bold')

    # === Export as base64 ===
    buf = io.BytesIO()
    plt.tight_layout(pad=0.3)
    plt.savefig(buf, format="png", bbox_inches='tight', dpi=130, transparent=True)
    plt.close(fig)
    buf.seek(0)
    encoded = base64.b64encode(buf.read()).decode("utf-8")
    return f"data:image/png;base64,{encoded}"


def generate_pitchmap_for_day_inning(day_no, inning_no, inning_df, stance=None):
    """
    Generates a pitch map image for the given day + inning.
    PyInstaller-safe version:
    - Uses resource_path() for pitch pad + ball icons
    - Fully compatible with bundled EXE
    """
    import os, io, base64
    import matplotlib.pyplot as plt
    import matplotlib.image as mpimg
    from matplotlib.offsetbox import AnnotationBbox, OffsetImage
    import pandas as pd

    # === Safety filter for valid balls ===
    valid_df = inning_df.copy()
    valid_df = valid_df[
        (valid_df.get("scrM_IsValidBall", 1) == 1) &
        (valid_df["scrM_BatsmanRuns"].notna()) &
        (valid_df["scrM_PitchX"].notna()) &
        (valid_df["scrM_PitchY"].notna())
    ]

    if valid_df.empty:
        print(f"⚠️ No pitch map data for Day {day_no} Inning {inning_no}")
        return None

    # === Resolve pitch pad path using resource_path (EXE-safe) ===
    try:
        base_static = resource_path("tailwick/static")
    except:
        base_static = os.path.join(os.path.dirname(__file__), "tailwick", "static")

    if stance == "LHB":
        pitch_image_path = os.path.join(base_static, "LeftHandPitchPad_1.png")
    else:
        pitch_image_path = os.path.join(base_static, "RightHandPitchPad_1.png")

    if not os.path.exists(pitch_image_path):
        print("⚠️ Pitch pad image not found:", pitch_image_path)
        return None
    else:
        print(f"✅ Using pitch image: {pitch_image_path}")

    # === Ball images (EXE-safe paths) ===
    ball_images = {
        "W": os.path.join(base_static, "RedBall_Resized.png"),
        0:   os.path.join(base_static, "MaroonBall_Resized.png"),
        1:   os.path.join(base_static, "BlueBall_Resized.png"),
        2:   os.path.join(base_static, "PinkBall_Resized.png"),
        3:   os.path.join(base_static, "YellowBall_Resized.png"),
        4:   os.path.join(base_static, "OrangeBall_Resized.png"),
        6:   os.path.join(base_static, "GreenBall_Resized.png"),
    }

    # === Load pitch + ball icons ===
    pitch_img = mpimg.imread(pitch_image_path)
    loaded_imgs = {}
    for key, path in ball_images.items():
        if os.path.exists(path):
            loaded_imgs[key] = mpimg.imread(path)
        else:
            print(f"⚠️ Missing ball image: {path}")

    # === Reference image for uniform scaling ===
    reference_img = loaded_imgs.get("W")

    def scaled_offset_image(img, target_size=14):
        if reference_img is not None:
            rh, rw = reference_img.shape[0], reference_img.shape[1]
            scale = target_size / max(rh, rw)
        else:
            h, w = img.shape[0], img.shape[1]
            scale = target_size / max(h, w)
        return OffsetImage(img, zoom=scale)

    # === Setup Matplotlib ===
    output_width, output_height, dpi = 260, 400, 100
    fig, ax = plt.subplots(figsize=(output_width / dpi, output_height / dpi), dpi=dpi)

    img_h, img_w = pitch_img.shape[0], pitch_img.shape[1]
    db_width, db_height = 157, 272
    scale_x = img_w / db_width
    scale_y = img_h / db_height

    ax.imshow(pitch_img)
    ax.axis("off")

    # === Plot each ball ===
    for _, row in valid_df.iterrows():
        x = row["scrM_PitchX"] * scale_x
        y = row["scrM_PitchY"] * scale_y

        run_val = int(row["scrM_BatsmanRuns"])
        is_wicket = bool(row["scrM_IsWicket"])

        # determine correct ball icon
        if is_wicket and "W" in loaded_imgs:
            ball_img = loaded_imgs["W"]
        elif run_val in loaded_imgs:
            ball_img = loaded_imgs[run_val]
        else:
            ball_img = None

        if ball_img is not None:
            imgbox = scaled_offset_image(ball_img, target_size=14)
            ax.add_artist(AnnotationBbox(imgbox, (x, y), frameon=False, pad=0, zorder=5))
        else:
            ax.scatter(x, y, s=14, c="#000000", zorder=5)

    # === Export as base64 ===
    buf = io.BytesIO()
    plt.tight_layout(pad=0)
    plt.savefig(buf, format="png", dpi=dpi, bbox_inches="tight", pad_inches=0, transparent=True)
    plt.close(fig)
    encoded = base64.b64encode(buf.getvalue()).decode("utf-8")
    return f"data:image/png;base64,{encoded}"


def generate_line_length_heatmap_for_day_inning(day_no, inning_no, inning_df, dark_mode=False):
    """
    Generates an HTML Line & Length heatmap grid for a given day + inning.
    Supports LIGHT and DARK modes automatically.
    """

    import pandas as pd
    import numpy as np

    if inning_df.empty or 'scrM_PitchX' not in inning_df.columns or 'scrM_PitchY' not in inning_df.columns:
        return "<div class='text-center italic'>No pitch data available.</div>"

    # --- Data cleanup ---
    df = inning_df.copy()
    df['scrM_PitchX'] = pd.to_numeric(df['scrM_PitchX'], errors='coerce')
    df['scrM_PitchY'] = pd.to_numeric(df['scrM_PitchY'], errors='coerce')
    df = df.dropna(subset=['scrM_PitchX', 'scrM_PitchY'])

    # --- Define zones ---
    line_bins = [-float('inf'), 50, 70, 80, 84, 88, 95, float('inf')]
    line_labels = ['Way Outside Off', 'Outside Off', 'Just Outside Off',
                   'Off Stump', 'Middle Stump', 'Leg Stump', 'Outside Leg']

    length_bins = [-float('inf'), 80, 120, 180, 220, 240, 250, float('inf')]
    length_labels = ['Short Pitch', 'Short of Good', 'Good Length',
                     'Overpitch', 'Full Length', 'Yorker', 'Fulltoss']

    df['LineZone'] = pd.cut(df['scrM_PitchX'], bins=line_bins, labels=line_labels, right=False)
    df['LengthZone'] = pd.cut(df['scrM_PitchY'], bins=length_bins, labels=length_labels, right=False)
    df['fours'] = (df.get('scrM_IsBoundry', 0) == 1).astype(int)
    df['sixes'] = (df.get('scrM_IsSixer', 0) == 1).astype(int)
    df['boundaries'] = df['fours'] + df['sixes']

    # --- Aggregate summary ---
    summary = df.groupby(['LengthZone', 'LineZone']).agg(
        balls=('scrM_PitchX', 'count'),
        runs=('scrM_BatsmanRuns', 'sum'),
        boundaries=('boundaries', 'sum'),
        wickets=('scrM_IsWicket', 'sum')
    ).reset_index()

    pivot = summary.pivot(index='LengthZone', columns='LineZone', values='balls').fillna(0)
    max_balls = pivot.to_numpy().max() if len(pivot) > 0 else 1

    # === THEME COLORS ===
    if dark_mode:
        bg_header = "#1e293b"      # dark slate
        bg_cell = "#0f172a"        # deep navy
        border = "#334155"         # slate border
        text_main = "#f1f5f9"      # light text
        text_dim = "#94a3b8"
    else:
        bg_header = "#f1f5f9"
        bg_cell = "#ffffff"
        border = "#cbd5e1"
        text_main = "#0f172a"
        text_dim = "#64748b"

    # --- Build HTML ---
    html = f"""
    <div style="overflow-x:auto;">
      <table style="border-collapse: collapse; margin:auto; font-size:11px; text-align:center; color:{text_main};">
        <thead>
          <tr>
            <th style="background:{bg_header}; border:1px solid {border}; padding:4px; position:sticky; left:0; z-index:2;">
              Length \\ Line
            </th>
    """

    for line in line_labels:
        html += f"""
            <th style="background:{bg_header}; border:1px solid {border}; padding:4px;">{line}</th>
        """

    html += "</tr></thead><tbody>"

    # --- Fill rows ---
    for length in length_labels:
        html += f"""
        <tr>
            <th style="background:{bg_header}; border:1px solid {border}; padding:4px;
                       text-align:left; position:sticky; left:0; z-index:1;">
                {length}
            </th>
        """

        for line in line_labels:
            zone_row = summary[(summary['LengthZone'] == length) &
                               (summary['LineZone'] == line)]

            if not zone_row.empty:
                balls = int(zone_row.iloc[0]['balls'])
                runs = int(zone_row.iloc[0]['runs'])
                bnd = int(zone_row.iloc[0]['boundaries'])
                wkts = int(zone_row.iloc[0]['wickets'])

                # --- Heatmap color intensity ---
                ratio = balls / max_balls if max_balls > 0 else 0

                if dark_mode:
                    # dark mode → red-based dark scale
                    r = int(60 + ratio * 180)
                    g = int(20)
                    b = int(20)
                    cell_color = f"rgb({r},{g},{b})"
                else:
                    # light mode → classic red heat scale
                    r = 255
                    g = int(255 - ratio * 140)
                    b = int(255 - ratio * 140)
                    cell_color = f"rgb({r},{g},{b})"

                html += f"""
                <td style="border:1px solid {border}; background:{cell_color}; padding:4px; min-width:65px;">
                    <div>{balls}b</div>
                    <div>{runs}r</div>
                    <div>{bnd}b,{wkts}w</div>
                </td>
                """
            else:
                html += f"<td style='border:1px solid {border}; background:{bg_cell}; padding:4px; color:{text_dim};'>-</td>"

        html += "</tr>"

    html += "</tbody></table></div>"
    return html

# ============================================================
#   SESSION-WISE RADAR CHART
#   (Uses your existing day+inning radar generator internally)
# ============================================================

def generate_progressive_radar_chart_session(day_no, inning_no, session_no, session_df):
    """
    Generates the progressive radar chart for a specific SESSION.
    This simply re-uses your existing radar chart function but
    with session_df instead of inning_df.
    """
    try:
        return generate_progressive_radar_chart(day_no, inning_no, session_df)
    except Exception as e:
        print(f"⚠️ Radar Session Error: {e}")
        return None

# ============================================================
#   SESSION-WISE PITCH MAP
#   (Uses your existing day+inning pitchmap generator)
# ============================================================

def generate_pitchmap_for_session(day_no, inning_no, session_no, session_df):
    """
    Generates the pitch map for a specific SESSION.
    Re-uses your existing pitch map function.
    """
    try:
        return generate_pitchmap_for_day_inning(day_no, inning_no, session_df)
    except Exception as e:
        print(f"⚠️ Pitchmap Session Error: {e}")
        return None

# ============================================================
#   SESSION-WISE LINE & LENGTH HEATMAP
#   (Uses your existing day+inning heatmap generator)
# ============================================================

def generate_line_length_heatmap_for_session(day_no, inning_no, session_no, session_df):
    """
    Generates the Line & Length heatmap for a specific SESSION.
    Re-uses your existing heatmap generator.
    """
    try:
        # Default dark_mode = False (same as your original)
        return generate_line_length_heatmap_for_day_inning(
            day_no,
            inning_no,
            session_df,
            dark_mode=False
        )
    except Exception as e:
        print(f"⚠️ Heatmap Session Error: {e}")
        return "<div>Error generating session heatmap.</div>"


def get_team_inning_distribution(tournament_id, team_name, matches):
    """
    For the given tournament, team and list of matches, return:
    - count_batting_first: number of matches where this team batted first
    - count_batting_second: number of matches where this team batted second

    Logic:
      1) For each match + team, find the earliest inning number where that team batted.
      2) Within a match, sort teams by that earliest inning number.
      3) The team with rank 1 = 'batted first', others = 'batted second'.
    """
    import pandas as pd

    if not tournament_id or not team_name or not matches:
        return 0, 0

    try:
        conn = get_connection()
        placeholders = ",".join("?" for _ in matches)

        query = f"""
            SELECT 
                scrM_MatchName,
                scrM_InningNo,
                scrM_tmMIdBattingName
            FROM tblScoreMaster
            WHERE scrM_TrnMId = ?
              AND scrM_MatchName IN ({placeholders})
              AND scrM_tmMIdBattingName IS NOT NULL
        """

        params = [int(tournament_id)] + list(matches)
        df = pd.read_sql(query, conn, params=params)
        conn.close()

        if df.empty:
            return 0, 0

        # Earliest inning per (match, team)
        df_min = (
            df.groupby(["scrM_MatchName", "scrM_tmMIdBattingName"])["scrM_InningNo"]
              .min()
              .reset_index()
        )

        # Rank teams within each match by earliest inning (1 = batted first)
        df_min = df_min.sort_values(["scrM_MatchName", "scrM_InningNo"])
        df_min["bat_order"] = (
            df_min.groupby("scrM_MatchName")["scrM_InningNo"]
                  .rank(method="dense")
                  .astype(int)
        )

        # Only rows for the selected team
        team_rows = df_min[df_min["scrM_tmMIdBattingName"] == team_name]

        if team_rows.empty:
            return 0, 0

        count_batting_first = int((team_rows["bat_order"] == 1).sum())
        count_batting_second = int((team_rows["bat_order"] > 1).sum())

        return count_batting_first, count_batting_second

    except Exception as e:
        print("❌ get_team_inning_distribution error:", e)
        return 0, 0

def get_powerplay_stats(trn_id, team, matches):
    if not trn_id or not team or not matches:
        return None
    
    try:
        conn = get_connection()
        placeholders = ",".join("?" for _ in matches)

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
            FROM tblScoreMaster
            WHERE scrM_TrnMId = ?
              AND scrM_MatchName IN ({placeholders})
              AND scrM_tmMIdBattingName = ?
              AND scrM_IsValidBall = 1
        """

        params = [trn_id] + list(matches) + [team]
        df = pd.read_sql(query, conn, params=params)
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
        placeholders = ",".join("?" for _ in matches)

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
            FROM tblScoreMaster
            WHERE scrM_TrnMId = ?
              AND scrM_MatchName IN ({placeholders})
              AND scrM_tmMIdBowlingName = ?    -- 🔥 bowling filter
              AND scrM_IsValidBall = 1
        """

        params = [trn_id] + list(matches) + [team]
        df = pd.read_sql(query, conn, params=params)
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
    placeholders = ",".join("?" for _ in matches)

    query = f"""
        SELECT scrM_MatchName, MIN(scrM_InningNo) AS first_inning
        FROM tblScoreMaster
        WHERE scrM_TrnMId = ?
          AND scrM_tmMIdBowlingName = ?
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






















    

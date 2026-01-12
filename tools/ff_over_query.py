import os, sys
sys.path.insert(0, os.getcwd())
from tailwick.utils import get_connection
import pandas as pd

match_name = "MCA_vs_ASSAM _03_Jan_2026"
teams = ["ASSAM", "MCA"]

conn = get_connection()
try:
    query = """
    SELECT scrM_OverNo, scrM_DelNo, scrM_PlayMIdStrikerName AS Batter,
           scrM_PlayMIdBowlerName AS Bowler, scrM_DelRuns, scrM_IsWicket,
           scrM_WideRuns, scrM_NoBallRuns, scrM_FFRunsTarget,
           scrM_tmMIdBattingName, scrM_tmMIdBowlingName
    FROM tblscoremaster
    WHERE scrM_MatchName = %s AND scrM_IsFFOver = 1
      AND (scrM_tmMIdBattingName = %s OR scrM_tmMIdBowlingName = %s)
    ORDER BY scrM_OverNo, scrM_DelNo
    """
    df = pd.read_sql(query, conn, params=(match_name, teams[0], teams[0]))
    # Also check for the other team
    df2 = pd.read_sql(query, conn, params=(match_name, teams[1], teams[1]))
    df = pd.concat([df, df2], ignore_index=True)
    if df.empty:
        print('NO_ROWS')
    else:
        print(f"ROWS: {len(df)}")
        print(df.to_string(index=False))
finally:
    conn.close()

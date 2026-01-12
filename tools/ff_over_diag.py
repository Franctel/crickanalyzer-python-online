from tailwick.utils import get_connection
import pandas as pd

conn = get_connection()
try:
    query = "SELECT DISTINCT scrM_MatchName, scrM_tmMIdBattingName, scrM_tmMIdBowlingName, scrM_FFRunsTarget FROM tblscoremaster WHERE scrM_IsFFOver = 1 ORDER BY scrM_MatchName LIMIT 500"
    df = pd.read_sql(query, conn)
    if df.empty:
        print('NO_ROWS')
    else:
        print(df.to_string(index=False))
finally:
    conn.close()

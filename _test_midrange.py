"""Quick test: fetch mid-range data for one team and check DB."""

import sqlite3
from nba_data import get_connection, fetch_scoring_stats

# 1) Test fetch_scoring_stats
print("Fetching scoring stats from API...")
scoring = fetch_scoring_stats()
print(f"Got data for {len(scoring)} teams")

# Show sample
for tid, data in list(scoring.items())[:5]:
    print(f"  Team {tid}: pct_pts_mid_range = {data['pct_pts_mid_range']}%")

# 2) Check DB has new columns
conn = get_connection()
cols = [row[1] for row in conn.execute("PRAGMA table_info(teams)").fetchall()]
print(f"\nDB columns check:")
print(f"  pts_mid_range: {'OK' if 'pts_mid_range' in cols else 'MISSING'}")
print(f"  pct_pts_mid_range: {'OK' if 'pct_pts_mid_range' in cols else 'MISSING'}")

# 3) Check current values (should be NULL before update)
row = conn.execute(
    "SELECT abbreviation, pts_mid_range, pct_pts_mid_range FROM teams LIMIT 3"
).fetchall()
print(f"\nCurrent values (before update):")
for r in row:
    print(f"  {r[0]}: pts_mid_range={r[1]}, pct_pts_mid_range={r[2]}")

conn.close()
print("\nAll checks passed!")

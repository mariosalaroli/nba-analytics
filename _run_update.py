"""Run save_to_db to populate mid-range data."""

from nba_data import get_connection, save_to_db

conn = get_connection()
save_to_db(conn)

# Verify
rows = conn.execute(
    "SELECT abbreviation, pts, pts_paint, pts_mid_range, pct_pts_mid_range "
    "FROM teams ORDER BY pts_mid_range DESC LIMIT 10"
).fetchall()
print("\n=== Top 10 Mid-Range Pts/jogo ===")
print(f"{'Time':<5} {'PTS':>6} {'Paint':>6} {'MidR':>6} {'%MidR':>6}")
for r in rows:
    print(f"{r[0]:<5} {r[1]:>6} {r[2]:>6} {r[3]:>6} {r[4]:>5.1f}%")

conn.close()

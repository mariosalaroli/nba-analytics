from nba_data import get_connection

c = get_connection()
rows = c.execute(
    "SELECT g1.matchup, g1.pts, g2.pts as opp_pts "
    "FROM games g1 JOIN games g2 ON g1.game_id=g2.game_id AND g1.team_abbr != g2.team_abbr "
    "WHERE g1.team_abbr='LAL' ORDER BY g1.date DESC LIMIT 5"
).fetchall()
for r in rows:
    d = dict(r)
    print(f"{d['matchup']}  {d['pts']} x {d['opp_pts']}")
c.close()

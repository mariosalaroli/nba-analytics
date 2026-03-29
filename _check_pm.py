from nba_data import get_connection

c = get_connection()
rows = c.execute(
    "SELECT matchup, pts, plus_minus FROM games WHERE team_abbr='LAL' ORDER BY date DESC LIMIT 5"
).fetchall()
for r in rows:
    d = dict(r)
    opp = d["pts"] - d["plus_minus"]
    print(f"{d['matchup']}  pts={d['pts']}  +/-={d['plus_minus']}  opp={opp}")
c.close()

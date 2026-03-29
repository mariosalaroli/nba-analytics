from nba_data import get_connection

c = get_connection()
# Contar quantos game_ids tem apenas 1 lado (sem adversário)
rows = c.execute(
    "SELECT g1.game_id, g1.team_abbr, g1.matchup "
    "FROM games g1 LEFT JOIN games g2 ON g1.game_id=g2.game_id AND g1.team_abbr != g2.team_abbr "
    "WHERE g2.game_id IS NULL "
    "ORDER BY g1.date DESC LIMIT 10"
).fetchall()
print(f"Jogos sem adversário no banco: {len(rows)}")
for r in rows:
    print(dict(r))
c.close()

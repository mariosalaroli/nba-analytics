import sqlite3

conn = sqlite3.connect("data/nba.db")

# Check if there are any April games
print("Games with APR in date:")
rows = conn.execute(
    "SELECT date, COUNT(*) FROM games WHERE date LIKE '%APR%' GROUP BY date ORDER BY date"
).fetchall()
for r in rows:
    print(f"  {r[0]}: {r[1]} jogos")

if not rows:
    print("  NENHUM jogo de abril encontrado!")

# All distinct months
print("\nDistinct months in games:")
rows = conn.execute(
    "SELECT SUBSTR(date, 1, 3) as month, COUNT(*) FROM games GROUP BY month"
).fetchall()
for r in rows:
    print(f"  {r[0]}: {r[1]} jogos")

# Check total unique dates
print(
    f"\nTotal unique game dates: {conn.execute('SELECT COUNT(DISTINCT date) FROM games').fetchone()[0]}"
)

# N=15 per team - show a sample
print("\nGames per team (sample):")
rows = conn.execute(
    "SELECT team_abbr, COUNT(*) FROM games GROUP BY team_abbr ORDER BY COUNT(*) LIMIT 5"
).fetchall()
for r in rows:
    print(f"  {r[0]}: {r[1]} jogos")

conn.close()

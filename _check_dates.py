import sqlite3

conn = sqlite3.connect("data/nba.db")
print("Max date:", conn.execute("SELECT MAX(date) FROM games").fetchone()[0])
print(
    "Last update:",
    conn.execute("SELECT value FROM meta WHERE key='last_update'").fetchone()[0],
)
print("Total games:", conn.execute("SELECT COUNT(*) FROM games").fetchone()[0])

print("\nLast 10 game dates:")
rows = conn.execute(
    "SELECT date, COUNT(*) as cnt FROM games GROUP BY date ORDER BY date DESC LIMIT 10"
).fetchall()
for r in rows:
    print(f"  {r[0]}: {r[1]} jogos")

conn.close()

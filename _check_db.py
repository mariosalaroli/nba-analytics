import sqlite3

conn = sqlite3.connect("data/nba.db")

print("=== Tabelas ===")
for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'"):
    print(f"  {r[0]}")

print("\n=== Times (tabela teams) ===")
for r in conn.execute(
    "SELECT abbreviation, name, w, l FROM teams ORDER BY abbreviation"
):
    print(f"  {r[0]} - {r[1]} ({r[2]}-{r[3]})")

print("\n=== Meta ===")
for r in conn.execute("SELECT key, value FROM meta"):
    print(f"  {r[0]} = {r[1]}")

print(f"\n=== Total jogos (tabela games) ===")
r = conn.execute("SELECT COUNT(*) FROM games").fetchone()
print(f"  {r[0]} registros")

conn.close()

import sqlite3

conn = sqlite3.connect("data/nba.db")
cur = conn.cursor()

# List tables
tables = [
    r[0]
    for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
]
print("Tables:", tables)

for t in tables:
    cols = [r[1] for r in cur.execute(f"PRAGMA table_info({t})").fetchall()]
    print(f"\n{t}: {cols}")

# Check if we have paint/3pt columns in teams_advanced or similar
for t in tables:
    if "team" in t.lower() or "advanced" in t.lower():
        row = cur.execute(f"SELECT * FROM {t} LIMIT 1").fetchone()
        cols = [d[0] for d in cur.description]
        print(f"\n--- {t} sample ---")
        for c, v in zip(cols, row):
            print(f"  {c}: {v}")

conn.close()

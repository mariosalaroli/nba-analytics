"""Patch nba_data.py to add mid-range columns in SQL statements."""

import re

with open("nba_data.py", "r", encoding="utf-8") as f:
    content = f.read()

# 1) In save_to_db: update INSERT INTO teams (find via "print" context)
# The save_to_db function uses print("ok") at the end, force_update uses yield

# Update the INSERT column list - both occurrences
old_cols = "pts_2nd_chance, pts_fb, pts_paint)"
new_cols = "pts_2nd_chance, pts_fb, pts_paint,\n                    pts_mid_range, pct_pts_mid_range)"
content = content.replace(old_cols, new_cols)

# Update VALUES placeholders - both occurrences
old_vals = """                        ?,?,?,?,?,?,?,?,?,
                        ?,?,?)"""
new_vals = """                        ?,?,?,?,?,?,?,?,?,
                        ?,?,?,?,?)"""
content = content.replace(old_vals, new_vals)

# Update ON CONFLICT SET - both occurrences
old_conflict = """pts_2nd_chance=excluded.pts_2nd_chance,
                    pts_fb=excluded.pts_fb, pts_paint=excluded.pts_paint
            \"\"\","""
new_conflict = """pts_2nd_chance=excluded.pts_2nd_chance,
                    pts_fb=excluded.pts_fb, pts_paint=excluded.pts_paint,
                    pts_mid_range=excluded.pts_mid_range,
                    pct_pts_mid_range=excluded.pct_pts_mid_range
            \"\"\","""
content = content.replace(old_conflict, new_conflict)

# Update parameter tuples - both occurrences
old_params = """stats.get("pts_2nd_chance"),
                    stats.get("pts_fb"),
                    stats.get("pts_paint"),
                ),
            )"""
new_params = """stats.get("pts_2nd_chance"),
                    stats.get("pts_fb"),
                    stats.get("pts_paint"),
                    stats.get("pts_mid_range"),
                    stats.get("pct_pts_mid_range"),
                ),
            )"""
content = content.replace(old_params, new_params)

# 2) In force_update: add scoring fetch + calc (find via "yield" context)
old_force_misc = """    yield "Buscando stats diversas..."
    misc_map = fetch_misc_stats()

    for i, team in enumerate(all_teams, 1):"""
new_force_misc = """    yield "Buscando stats diversas..."
    misc_map = fetch_misc_stats()

    yield "Buscando stats de scoring (mid-range)..."
    scoring_map = fetch_scoring_stats()

    for i, team in enumerate(all_teams, 1):"""
content = content.replace(old_force_misc, new_force_misc)

# In force_update: add scoring_map merge + calc (find via "yield" context)
old_force_stats = '''            misc = misc_map.get(tid, {})
            stats.update(misc)
            games = fetch_last_games(tid, n=15)
            st = standings_map.get(tid, {})

            w = int(st.get("WINS", 0))
            l = int(st.get("LOSSES", 0))
            diff = round(stats["pts"] - stats.get("opp_pts", 0), 1)
            diff_str = f"{'+'if diff >= 0 else ''}{diff}"

            conn.execute(
                """
                INSERT INTO teams (abbreviation, id, name, city, nickname,'''
# This still appears twice. Let me try with the yield before it
old_force_update_block = """        yield f"[{i}/{total}] Baixando {abbr} — {team[\'nickname\']}..."

        try:
            stats = fetch_team_stats(tid)
            adv = advanced_map.get(tid, {})
            stats.update(adv)
            opp = opponent_map.get(tid, {})
            stats.update(opp)
            misc = misc_map.get(tid, {})
            stats.update(misc)
            games = fetch_last_games(tid, n=15)"""
new_force_update_block = """        yield f"[{i}/{total}] Baixando {abbr} — {team[\'nickname\']}..."

        try:
            stats = fetch_team_stats(tid)
            adv = advanced_map.get(tid, {})
            stats.update(adv)
            opp = opponent_map.get(tid, {})
            stats.update(opp)
            misc = misc_map.get(tid, {})
            stats.update(misc)
            scoring = scoring_map.get(tid, {})
            stats.update(scoring)
            # Calcular pts_mid_range a partir da % e pts/jogo
            pct_mr = stats.get("pct_pts_mid_range") or 0
            pts_total = stats.get("pts") or 0
            stats["pts_mid_range"] = round(pts_total * pct_mr / 100, 1)
            games = fetch_last_games(tid, n=15)"""
content = content.replace(old_force_update_block, new_force_update_block)

with open("nba_data.py", "w", encoding="utf-8") as f:
    f.write(content)

# Verify
count_mid = content.count("pts_mid_range")
count_pct = content.count("pct_pts_mid_range")
print(f"pts_mid_range appears {count_mid} times")
print(f"pct_pts_mid_range appears {count_pct} times")
print("Patch applied successfully!")

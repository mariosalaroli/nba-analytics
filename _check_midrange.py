"""Check available mid-range data from NBA API."""

import time
from nba_api.stats.endpoints import LeagueDashTeamStats

SEASON = "2025-26"

# Try Scoring measure type
print("=== LeagueDashTeamStats (Scoring) ===")
d = LeagueDashTeamStats(
    season=SEASON,
    per_mode_detailed="PerGame",
    measure_type_detailed_defense="Scoring",
    timeout=60,
)
df = d.get_data_frames()[0]
print("Columns:", list(df.columns))
print()

# Show mid-range related columns
mr_cols = [c for c in df.columns if "MR" in c or "MID" in c.upper() or "2PT" in c]
print("Mid-range related columns:", mr_cols)
print()

# Show sample data (first 3 teams)
if mr_cols:
    print(df[["TEAM_NAME"] + mr_cols].head(5).to_string(index=False))
else:
    print("No mid-range columns found in Scoring measure type")
    print()
    # Show all columns with sample
    print(df.head(2).to_string(index=False))

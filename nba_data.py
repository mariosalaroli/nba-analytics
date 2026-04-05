"""
nba_data.py - Coleta e armazenamento de dados NBA em SQLite
Atualiza automaticamente no primeiro uso de cada dia.
"""

import time
import json
import sqlite3
import urllib.request
import pandas as pd
from datetime import datetime, date, timezone
from pathlib import Path
from nba_api.stats.static import teams as nba_teams_static
from nba_api.stats.endpoints import (
    TeamDashboardByGeneralSplits,
    TeamGameLog,
    LeagueStandings,
    TeamEstimatedMetrics,
    LeagueDashPlayerStats,
    LeagueDashTeamStats,
    PlayerGameLog,
    BoxScoreTraditionalV3,
)

SEASON = "2025-26"
# Streamlit Cloud monta o repo como read-only; usar /tmp para o banco
import os as _os

DB_PATH = Path("/tmp/nba.db") if _os.path.exists("/mount/src") else Path("data/nba.db")
SLEEP = 0.8
API_TIMEOUT = 60
API_RETRIES = 3


def _api_call(endpoint_cls, **kwargs):
    """Chama endpoint da NBA API com retry e timeout."""
    for attempt in range(1, API_RETRIES + 1):
        try:
            time.sleep(SLEEP)
            return endpoint_cls(timeout=API_TIMEOUT, **kwargs)
        except Exception as e:
            if attempt < API_RETRIES:
                wait = attempt * 5
                print(f"  Tentativa {attempt} falhou ({e}), aguardando {wait}s...")
                time.sleep(wait)
            else:
                raise


def _f(row, col, pct=False):
    """Extrai float de uma row, retorna None se inválido."""
    try:
        v = float(row[col])
        return round(v * 100, 1) if pct else round(v, 1)
    except (TypeError, ValueError, KeyError):
        return None


def _i(row, col):
    """Extrai int de uma row, retorna None se inválido."""
    try:
        return int(row[col])
    except (TypeError, ValueError, KeyError):
        return None


def _parse_game_date(raw: str) -> str:
    """Converte data da NBA API (ex: 'MAR 30, 2026') para ISO 'YYYY-MM-DD'."""
    try:
        return datetime.strptime(raw, "%b %d, %Y").strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return str(raw)


def init_db(conn: sqlite3.Connection):
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS meta (
            key   TEXT PRIMARY KEY,
            value TEXT
        );
        CREATE TABLE IF NOT EXISTS teams (
            abbreviation TEXT PRIMARY KEY,
            id           INTEGER,
            name         TEXT,
            city         TEXT,
            nickname     TEXT,
            conference   TEXT,
            division     TEXT,
            w            INTEGER,
            l            INTEGER,
            pct          REAL,
            conf_rank    INTEGER,
            home_record  TEXT,
            road_record  TEXT,
            last10       TEXT,
            streak       TEXT,
            diff         TEXT,
            pts          REAL,
            ast          REAL,
            reb          REAL,
            oreb         REAL,
            dreb         REAL,
            stl          REAL,
            blk          REAL,
            tov          REAL,
            pf           REAL,
            plus_minus   REAL,
            fgm          REAL,
            fga          REAL,
            fg_pct       REAL,
            fg3m         REAL,
            fg3a         REAL,
            fg3_pct      REAL,
            ftm          REAL,
            fta          REAL,
            ft_pct       REAL,
            opp_pts      REAL,
            off_rating   REAL,
            def_rating   REAL,
            net_rating   REAL,
            pace         REAL,
            ast_ratio    REAL,
            oreb_pct     REAL,
            dreb_pct     REAL,
            reb_pct      REAL,
            tov_pct      REAL,
            opp_fgm      REAL,
            opp_fga      REAL,
            opp_fg_pct   REAL,
            opp_fg3m     REAL,
            opp_fg3a     REAL,
            opp_fg3_pct  REAL,
            opp_oreb     REAL,
            opp_dreb     REAL,
            opp_reb      REAL,
            pts_2nd_chance REAL,
            pts_fb       REAL,
            pts_paint    REAL,
            pts_mid_range    REAL,
            pct_pts_mid_range REAL
        );
        CREATE TABLE IF NOT EXISTS games (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id   TEXT,
            team_abbr TEXT,
            date      TEXT,
            matchup   TEXT,
            wl        TEXT,
            pts       INTEGER,
            oreb      INTEGER,
            dreb      INTEGER,
            reb       INTEGER,
            ast       INTEGER,
            stl       INTEGER,
            blk       INTEGER,
            tov       INTEGER,
            pf        INTEGER,
            fg_pct    REAL,
            fg3_pct   REAL,
            ft_pct    REAL,
            UNIQUE(team_abbr, date, matchup)
        );
        CREATE TABLE IF NOT EXISTS injuries (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            team_abbr  TEXT,
            player     TEXT,
            status     TEXT,
            comment    TEXT,
            detail     TEXT,
            updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS players (
            player_id      INTEGER PRIMARY KEY,
            player_name    TEXT,
            team_id        INTEGER,
            team_abbr      TEXT,
            age            REAL,
            gp             INTEGER,
            w              INTEGER,
            l              INTEGER,
            min            REAL,
            pts            REAL,
            ast            REAL,
            reb            REAL,
            oreb           REAL,
            dreb           REAL,
            stl            REAL,
            blk            REAL,
            tov            REAL,
            pf             REAL,
            fgm            REAL,
            fga            REAL,
            fg_pct         REAL,
            fg3m           REAL,
            fg3a           REAL,
            fg3_pct        REAL,
            ftm            REAL,
            fta            REAL,
            ft_pct         REAL,
            plus_minus     REAL,
            dd2            INTEGER,
            td3            INTEGER,
            fantasy_pts    REAL,
            off_rating     REAL,
            def_rating     REAL,
            net_rating     REAL,
            ast_pct        REAL,
            ast_to         REAL,
            ast_ratio      REAL,
            oreb_pct       REAL,
            dreb_pct       REAL,
            reb_pct        REAL,
            tov_pct        REAL,
            efg_pct        REAL,
            ts_pct         REAL,
            usg_pct        REAL,
            pace           REAL,
            pie            REAL
        );
    """
    )
    conn.commit()

    # Migrar colunas novas (SQLite não altera CREATE TABLE IF NOT EXISTS)
    _migrate_columns(
        conn,
        "teams",
        {
            "pts_mid_range": "REAL",
            "pct_pts_mid_range": "REAL",
        },
    )


def _migrate_columns(conn: sqlite3.Connection, table: str, columns: dict):
    """Adiciona colunas que ainda não existem na tabela."""
    existing = {
        row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
    }
    for col, col_type in columns.items():
        if col not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")
    conn.commit()


def _build_espn_team_map() -> dict:
    """Mapeia ESPN displayName -> abbreviation NBA."""
    mapping = {}
    for t in nba_teams_static.get_teams():
        mapping[t["full_name"]] = t["abbreviation"]
    # ESPN pode usar nomes ligeiramente diferentes
    mapping["LA Clippers"] = "LAC"
    return mapping


_ESPN_TEAM_MAP = _build_espn_team_map()


def fetch_injuries() -> list[dict]:
    """Busca injury report da ESPN e retorna lista de dicts prontos para o BD."""
    url = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/injuries"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    resp = urllib.request.urlopen(req, timeout=30)
    data = json.loads(resp.read())
    rows = []
    for team in data.get("injuries", []):
        espn_name = team.get("displayName", "")
        abbr = _ESPN_TEAM_MAP.get(espn_name)
        if not abbr:
            continue
        for inj in team.get("injuries", []):
            ath = inj.get("athlete", {})
            rows.append(
                {
                    "team_abbr": abbr,
                    "player": ath.get("displayName", ""),
                    "status": inj.get("status", ""),
                    "comment": inj.get("shortComment", ""),
                    "detail": inj.get("longComment", ""),
                    "updated_at": inj.get("date", ""),
                }
            )
    return rows


def save_injuries_to_db(conn: sqlite3.Connection):
    """Baixa injuries da ESPN e grava no BD (replace completo)."""
    rows = fetch_injuries()
    conn.execute("DELETE FROM injuries")
    for r in rows:
        conn.execute(
            "INSERT INTO injuries (team_abbr, player, status, comment, detail, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                r["team_abbr"],
                r["player"],
                r["status"],
                r["comment"],
                r["detail"],
                r["updated_at"],
            ),
        )
    conn.commit()
    return len(rows)


def get_all_teams() -> list[dict]:
    return nba_teams_static.get_teams()


def fetch_team_stats(team_id: int) -> dict:
    dash = _api_call(
        TeamDashboardByGeneralSplits,
        team_id=team_id,
        per_mode_detailed="PerGame",
        season=SEASON,
        season_type_all_star="Regular Season",
    )
    df = dash.get_data_frames()[0]
    row = df.iloc[0]
    return {
        "pts": round(float(row["PTS"]), 1),
        "ast": round(float(row["AST"]), 1),
        "reb": round(float(row["REB"]), 1),
        "oreb": round(float(row["OREB"]), 1),
        "dreb": round(float(row["DREB"]), 1),
        "stl": round(float(row["STL"]), 1),
        "blk": round(float(row["BLK"]), 1),
        "tov": round(float(row["TOV"]), 1),
        "pf": round(float(row["PF"]), 1),
        "plus_minus": round(float(row["PLUS_MINUS"]), 1),
        "fgm": round(float(row["FGM"]), 1),
        "fga": round(float(row["FGA"]), 1),
        "fg_pct": round(float(row["FG_PCT"]) * 100, 1),
        "fg3m": round(float(row["FG3M"]), 1),
        "fg3a": round(float(row["FG3A"]), 1),
        "fg3_pct": round(float(row["FG3_PCT"]) * 100, 1),
        "ftm": round(float(row["FTM"]), 1),
        "fta": round(float(row["FTA"]), 1),
        "ft_pct": round(float(row["FT_PCT"]) * 100, 1),
        "opp_pts": round(float(row["OPP_PTS"]) if "OPP_PTS" in row else 0, 1),
    }


def fetch_standings() -> pd.DataFrame:
    standings = _api_call(LeagueStandings, season=SEASON)
    df = standings.get_data_frames()[0]
    return df[
        [
            "TeamID",
            "TeamName",
            "Conference",
            "Division",
            "WINS",
            "LOSSES",
            "WinPCT",
            "PlayoffRank",
            "DivisionRank",
            "HOME",
            "ROAD",
            "L10",
            "CurrentStreak",
            "strCurrentStreak",
        ]
    ]


def fetch_last_games(team_id: int, n: int = 10) -> list[dict]:
    log = _api_call(
        TeamGameLog,
        team_id=team_id,
        season=SEASON,
        season_type_all_star="Regular Season",
    )
    df = log.get_data_frames()[0].head(n)
    games = []
    for _, row in df.iterrows():
        games.append(
            {
                "game_id": str(row["Game_ID"]),
                "date": _parse_game_date(str(row["GAME_DATE"])),
                "matchup": str(row["MATCHUP"]),
                "wl": str(row["WL"]),
                "pts": int(row["PTS"]),
                "oreb": int(row.get("OREB", 0)),
                "dreb": int(row.get("DREB", 0)),
                "reb": int(row["REB"]),
                "ast": int(row["AST"]),
                "stl": int(row.get("STL", 0)),
                "blk": int(row.get("BLK", 0)),
                "tov": int(row.get("TOV", 0)),
                "pf": int(row.get("PF", 0)),
                "fg_pct": round(float(row["FG_PCT"]) * 100, 1),
                "fg3_pct": round(float(row.get("FG3_PCT", 0)) * 100, 1),
                "ft_pct": round(float(row.get("FT_PCT", 0)) * 100, 1),
            }
        )
    return games


def fetch_advanced_metrics() -> dict:
    """Busca métricas avançadas estimadas para todos os times."""
    metrics = _api_call(
        TeamEstimatedMetrics, season=SEASON, season_type="Regular Season"
    )
    df = metrics.get_data_frames()[0]
    result = {}
    for _, row in df.iterrows():
        result[int(row["TEAM_ID"])] = {
            "off_rating": round(float(row["E_OFF_RATING"]), 1),
            "def_rating": round(float(row["E_DEF_RATING"]), 1),
            "net_rating": round(float(row["E_NET_RATING"]), 1),
            "pace": round(float(row["E_PACE"]), 1),
            "ast_ratio": round(float(row["E_AST_RATIO"]), 1),
            "oreb_pct": round(float(row["E_OREB_PCT"]) * 100, 1),
            "dreb_pct": round(float(row["E_DREB_PCT"]) * 100, 1),
            "reb_pct": round(float(row["E_REB_PCT"]) * 100, 1),
            "tov_pct": round(float(row["E_TM_TOV_PCT"]) * 100, 1),
        }
    return result


def fetch_opponent_stats() -> dict:
    """Busca stats dos adversários (por jogo) para todos os times."""
    d = _api_call(
        LeagueDashTeamStats,
        season=SEASON,
        per_mode_detailed="PerGame",
        measure_type_detailed_defense="Opponent",
    )
    df = d.get_data_frames()[0]
    result = {}
    for _, row in df.iterrows():
        result[int(row["TEAM_ID"])] = {
            "opp_pts": round(float(row["OPP_PTS"]), 1),
            "opp_fgm": round(float(row["OPP_FGM"]), 1),
            "opp_fga": round(float(row["OPP_FGA"]), 1),
            "opp_fg_pct": round(float(row["OPP_FG_PCT"]) * 100, 1),
            "opp_fg3m": round(float(row["OPP_FG3M"]), 1),
            "opp_fg3a": round(float(row["OPP_FG3A"]), 1),
            "opp_fg3_pct": round(float(row["OPP_FG3_PCT"]) * 100, 1),
            "opp_oreb": round(float(row["OPP_OREB"]), 1),
            "opp_dreb": round(float(row["OPP_DREB"]), 1),
            "opp_reb": round(float(row["OPP_REB"]), 1),
        }
    return result


def fetch_misc_stats() -> dict:
    """Busca stats misc (second chance, fast break, paint) para todos os times."""
    d = _api_call(
        LeagueDashTeamStats,
        season=SEASON,
        per_mode_detailed="PerGame",
        measure_type_detailed_defense="Misc",
    )
    df = d.get_data_frames()[0]
    result = {}
    for _, row in df.iterrows():
        result[int(row["TEAM_ID"])] = {
            "pts_2nd_chance": round(float(row["PTS_2ND_CHANCE"]), 1),
            "pts_fb": round(float(row["PTS_FB"]), 1),
            "pts_paint": round(float(row["PTS_PAINT"]), 1),
        }
    return result


def fetch_scoring_stats() -> dict:
    """Busca stats de scoring (mid-range, etc.) para todos os times."""
    d = _api_call(
        LeagueDashTeamStats,
        season=SEASON,
        per_mode_detailed="PerGame",
        measure_type_detailed_defense="Scoring",
    )
    df = d.get_data_frames()[0]
    result = {}
    for _, row in df.iterrows():
        result[int(row["TEAM_ID"])] = {
            "pct_pts_mid_range": round(float(row["PCT_PTS_2PT_MR"]) * 100, 1),
        }
    return result


def save_to_db(conn: sqlite3.Connection):
    all_teams = get_all_teams()
    total = len(all_teams)
    print(f"Buscando dados de {total} times para {SEASON}...")

    standings_df = fetch_standings()
    standings_map = {
        int(row["TeamID"]): row.to_dict() for _, row in standings_df.iterrows()
    }

    print("  Buscando métricas avançadas...", end=" ", flush=True)
    advanced_map = fetch_advanced_metrics()
    print("ok")

    print("  Buscando stats dos adversários...", end=" ", flush=True)
    opponent_map = fetch_opponent_stats()
    print("ok")

    print("  Buscando stats misc...", end=" ", flush=True)
    misc_map = fetch_misc_stats()
    print("ok")

    print("  Buscando stats de scoring (mid-range)...", end=" ", flush=True)
    scoring_map = fetch_scoring_stats()
    print("ok")

    for i, team in enumerate(all_teams, 1):
        tid = team["id"]
        abbr = team["abbreviation"]
        print(f"  [{i}/{total}] {abbr}...", end=" ", flush=True)

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
            games = fetch_last_games(tid, n=15)
            st = standings_map.get(tid, {})

            w = int(st.get("WINS", 0))
            l = int(st.get("LOSSES", 0))
            diff = round(stats["pts"] - stats.get("opp_pts", 0), 1)
            diff_str = f"{'+'if diff >= 0 else ''}{diff}"

            conn.execute(
                """
                INSERT INTO teams (abbreviation, id, name, city, nickname,
                    conference, division, w, l, pct, conf_rank,
                    home_record, road_record, last10, streak, diff,
                    pts, ast, reb, oreb, dreb, stl, blk, tov, pf, plus_minus,
                    fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct,
                    opp_pts, off_rating, def_rating, net_rating, pace,
                    ast_ratio, oreb_pct, dreb_pct, reb_pct, tov_pct,
                    opp_fgm, opp_fga, opp_fg_pct, opp_fg3m, opp_fg3a,
                    opp_fg3_pct, opp_oreb, opp_dreb, opp_reb,
                    pts_2nd_chance, pts_fb, pts_paint,
                    pts_mid_range, pct_pts_mid_range)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                        ?,?,?,?,?,?,?,?,?,?,
                        ?,?,?,?,?,?,?,?,?,
                        ?,?,?,?,?)
                ON CONFLICT(abbreviation) DO UPDATE SET
                    id=excluded.id, name=excluded.name, city=excluded.city,
                    nickname=excluded.nickname, conference=excluded.conference,
                    division=excluded.division, w=excluded.w, l=excluded.l,
                    pct=excluded.pct, conf_rank=excluded.conf_rank,
                    home_record=excluded.home_record, road_record=excluded.road_record,
                    last10=excluded.last10, streak=excluded.streak, diff=excluded.diff,
                    pts=excluded.pts, ast=excluded.ast, reb=excluded.reb,
                    oreb=excluded.oreb, dreb=excluded.dreb, stl=excluded.stl,
                    blk=excluded.blk, tov=excluded.tov, pf=excluded.pf,
                    plus_minus=excluded.plus_minus,
                    fgm=excluded.fgm, fga=excluded.fga, fg_pct=excluded.fg_pct,
                    fg3m=excluded.fg3m, fg3a=excluded.fg3a, fg3_pct=excluded.fg3_pct,
                    ftm=excluded.ftm, fta=excluded.fta, ft_pct=excluded.ft_pct,
                    opp_pts=excluded.opp_pts,
                    off_rating=excluded.off_rating, def_rating=excluded.def_rating,
                    net_rating=excluded.net_rating, pace=excluded.pace,
                    ast_ratio=excluded.ast_ratio, oreb_pct=excluded.oreb_pct,
                    dreb_pct=excluded.dreb_pct, reb_pct=excluded.reb_pct,
                    tov_pct=excluded.tov_pct,
                    opp_fgm=excluded.opp_fgm, opp_fga=excluded.opp_fga,
                    opp_fg_pct=excluded.opp_fg_pct, opp_fg3m=excluded.opp_fg3m,
                    opp_fg3a=excluded.opp_fg3a, opp_fg3_pct=excluded.opp_fg3_pct,
                    opp_oreb=excluded.opp_oreb, opp_dreb=excluded.opp_dreb,
                    opp_reb=excluded.opp_reb,
                    pts_2nd_chance=excluded.pts_2nd_chance,
                    pts_fb=excluded.pts_fb, pts_paint=excluded.pts_paint,
                    pts_mid_range=excluded.pts_mid_range,
                    pct_pts_mid_range=excluded.pct_pts_mid_range
            """,
                (
                    abbr,
                    tid,
                    team["full_name"],
                    team["city"],
                    team["nickname"],
                    st.get("Conference", ""),
                    st.get("Division", ""),
                    w,
                    l,
                    round(w / (w + l), 3) if (w + l) > 0 else 0,
                    int(st.get("PlayoffRank", 0)),
                    st.get("HOME", ""),
                    st.get("ROAD", ""),
                    st.get("L10", ""),
                    st.get("strCurrentStreak", ""),
                    diff_str,
                    stats.get("pts"),
                    stats.get("ast"),
                    stats.get("reb"),
                    stats.get("oreb"),
                    stats.get("dreb"),
                    stats.get("stl"),
                    stats.get("blk"),
                    stats.get("tov"),
                    stats.get("pf"),
                    stats.get("plus_minus"),
                    stats.get("fgm"),
                    stats.get("fga"),
                    stats.get("fg_pct"),
                    stats.get("fg3m"),
                    stats.get("fg3a"),
                    stats.get("fg3_pct"),
                    stats.get("ftm"),
                    stats.get("fta"),
                    stats.get("ft_pct"),
                    stats.get("opp_pts"),
                    stats.get("off_rating"),
                    stats.get("def_rating"),
                    stats.get("net_rating"),
                    stats.get("pace"),
                    stats.get("ast_ratio"),
                    stats.get("oreb_pct"),
                    stats.get("dreb_pct"),
                    stats.get("reb_pct"),
                    stats.get("tov_pct"),
                    stats.get("opp_fgm"),
                    stats.get("opp_fga"),
                    stats.get("opp_fg_pct"),
                    stats.get("opp_fg3m"),
                    stats.get("opp_fg3a"),
                    stats.get("opp_fg3_pct"),
                    stats.get("opp_oreb"),
                    stats.get("opp_dreb"),
                    stats.get("opp_reb"),
                    stats.get("pts_2nd_chance"),
                    stats.get("pts_fb"),
                    stats.get("pts_paint"),
                    stats.get("pts_mid_range"),
                    stats.get("pct_pts_mid_range"),
                ),
            )

            conn.execute("DELETE FROM games WHERE team_abbr = ?", (abbr,))
            for g in games:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO games
                    (game_id, team_abbr, date, matchup, wl, pts, oreb, dreb, reb, ast,
                     stl, blk, tov, pf, fg_pct, fg3_pct, ft_pct)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        g["game_id"],
                        abbr,
                        g["date"],
                        g["matchup"],
                        g["wl"],
                        g["pts"],
                        g["oreb"],
                        g["dreb"],
                        g["reb"],
                        g["ast"],
                        g["stl"],
                        g["blk"],
                        g["tov"],
                        g["pf"],
                        g["fg_pct"],
                        g["fg3_pct"],
                        g["ft_pct"],
                    ),
                )

            conn.commit()
            print("ok")
        except Exception as e:
            print(f"ERRO: {e}")

    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO meta (key, value) VALUES ('last_update', ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (now,),
    )
    conn.execute(
        "INSERT INTO meta (key, value) VALUES ('season', ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (SEASON,),
    )
    conn.commit()
    print(f"\nDados salvos em {DB_PATH} - {now}")

    print("Buscando injury report (ESPN)...", end=" ", flush=True)
    try:
        n = save_injuries_to_db(conn)
        print(f"ok ({n} jogadores)")
    except Exception as e:
        print(f"ERRO: {e}")


def save_players_to_db(conn: sqlite3.Connection):
    """Busca stats base + avançadas de todos os jogadores e grava no SQLite."""
    print("Buscando dados de jogadores...")

    # Stats base (per game)
    base = _api_call(
        LeagueDashPlayerStats,
        season=SEASON,
        per_mode_detailed="PerGame",
        season_type_all_star="Regular Season",
    ).get_data_frames()[0]
    print(f"  Stats base: {len(base)} jogadores")

    # Stats avançadas
    adv = _api_call(
        LeagueDashPlayerStats,
        season=SEASON,
        per_mode_detailed="PerGame",
        measure_type_detailed_defense="Advanced",
        season_type_all_star="Regular Season",
    ).get_data_frames()[0]
    print(f"  Stats avançadas: {len(adv)} jogadores")

    # Merge por PLAYER_ID
    adv_map = {}
    for _, row in adv.iterrows():
        adv_map[int(row["PLAYER_ID"])] = row

    conn.execute("DELETE FROM players")

    count = 0
    for _, row in base.iterrows():
        pid = int(row["PLAYER_ID"])
        a = adv_map.get(pid)

        conn.execute(
            """INSERT OR REPLACE INTO players (
                player_id, player_name, team_id, team_abbr, age, gp, w, l,
                min, pts, ast, reb, oreb, dreb, stl, blk, tov, pf,
                fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct,
                plus_minus, dd2, td3, fantasy_pts,
                off_rating, def_rating, net_rating,
                ast_pct, ast_to, ast_ratio, oreb_pct, dreb_pct, reb_pct,
                tov_pct, efg_pct, ts_pct, usg_pct, pace, pie
            ) VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            )""",
            (
                pid,
                str(row["PLAYER_NAME"]),
                int(row["TEAM_ID"]),
                str(row["TEAM_ABBREVIATION"]),
                _f(row, "AGE"),
                _i(row, "GP"),
                _i(row, "W"),
                _i(row, "L"),
                _f(row, "MIN"),
                _f(row, "PTS"),
                _f(row, "AST"),
                _f(row, "REB"),
                _f(row, "OREB"),
                _f(row, "DREB"),
                _f(row, "STL"),
                _f(row, "BLK"),
                _f(row, "TOV"),
                _f(row, "PF"),
                _f(row, "FGM"),
                _f(row, "FGA"),
                _f(row, "FG_PCT", pct=True),
                _f(row, "FG3M"),
                _f(row, "FG3A"),
                _f(row, "FG3_PCT", pct=True),
                _f(row, "FTM"),
                _f(row, "FTA"),
                _f(row, "FT_PCT", pct=True),
                _f(row, "PLUS_MINUS"),
                _i(row, "DD2"),
                _i(row, "TD3"),
                _f(row, "NBA_FANTASY_PTS"),
                _f(a, "OFF_RATING") if a is not None else None,
                _f(a, "DEF_RATING") if a is not None else None,
                _f(a, "NET_RATING") if a is not None else None,
                _f(a, "AST_PCT", pct=True) if a is not None else None,
                _f(a, "AST_TO") if a is not None else None,
                _f(a, "AST_RATIO") if a is not None else None,
                _f(a, "OREB_PCT", pct=True) if a is not None else None,
                _f(a, "DREB_PCT", pct=True) if a is not None else None,
                _f(a, "REB_PCT", pct=True) if a is not None else None,
                _f(a, "E_TOV_PCT") if a is not None else None,
                _f(a, "EFG_PCT", pct=True) if a is not None else None,
                _f(a, "TS_PCT", pct=True) if a is not None else None,
                _f(a, "USG_PCT", pct=True) if a is not None else None,
                _f(a, "PACE") if a is not None else None,
                _f(a, "PIE", pct=True) if a is not None else None,
            ),
        )
        count += 1

    conn.commit()
    print(f"  {count} jogadores salvos na tabela 'players'")


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    init_db(conn)
    return conn


def needs_update(conn: sqlite3.Connection) -> bool:
    row = conn.execute("SELECT value FROM meta WHERE key='last_update'").fetchone()
    if row is None:
        return True
    last = datetime.fromisoformat(row["value"]).date()
    return last < date.today()


def load_all_teams(conn: sqlite3.Connection) -> dict:
    rows = conn.execute("SELECT * FROM teams ORDER BY abbreviation").fetchall()
    teams = {}
    for r in rows:
        abbr = r["abbreviation"]
        games_rows = conn.execute(
            "SELECT game_id, date, matchup, wl, pts, oreb, dreb, reb, ast, stl, blk, "
            "tov, pf, fg_pct, fg3_pct, ft_pct FROM games "
            "WHERE team_abbr = ? ORDER BY date DESC",
            (abbr,),
        ).fetchall()
        last_games = [dict(g) for g in games_rows]
        d = dict(r)
        d["last_games"] = last_games
        teams[abbr] = d
    return teams


def load_season(conn: sqlite3.Connection) -> str:
    row = conn.execute("SELECT value FROM meta WHERE key='season'").fetchone()
    return row["value"] if row else SEASON


def load_players_list() -> list[dict]:
    """Retorna lista de jogadores com id, nome e time para o seletor."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT player_id, player_name, team_abbr FROM players ORDER BY player_name"
    ).fetchall()
    result = [
        {
            "player_id": r["player_id"],
            "player_name": r["player_name"],
            "team_abbr": r["team_abbr"],
        }
        for r in rows
    ]
    conn.close()
    return result


def load_player_stats(player_id: int) -> dict | None:
    """Carrega stats completas de um jogador do banco."""
    conn = get_connection()
    row = conn.execute(
        "SELECT * FROM players WHERE player_id = ?", (player_id,)
    ).fetchone()
    conn.close()
    if row is None:
        return None
    return dict(row)


def load_all_players() -> list[dict]:
    """Retorna lista de dicts com stats de todos os jogadores."""
    conn = get_connection()
    rows = conn.execute("SELECT * FROM players ORDER BY pts DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def load_injuries(team_abbr: str | None = None) -> list[dict]:
    """Carrega injuries do BD. Se team_abbr, filtra por time."""
    conn = get_connection()
    if team_abbr:
        rows = conn.execute(
            "SELECT * FROM injuries WHERE team_abbr = ? ORDER BY status, player",
            (team_abbr,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM injuries ORDER BY team_abbr, status, player"
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def fetch_player_game_log(player_id: int, n: int = 10) -> list[dict]:
    """Busca os últimos N jogos de um jogador via API (on-demand)."""
    time.sleep(SLEEP)
    log = PlayerGameLog(
        player_id=player_id,
        season=SEASON,
        season_type_all_star="Regular Season",
    )
    df = log.get_data_frames()[0].head(n)
    games = []
    for _, row in df.iterrows():
        games.append(
            {
                "date": _parse_game_date(str(row["GAME_DATE"])),
                "matchup": str(row["MATCHUP"]),
                "wl": str(row["WL"]),
                "min": int(row["MIN"]) if pd.notna(row["MIN"]) else 0,
                "pts": int(row["PTS"]),
                "ast": int(row["AST"]),
                "reb": int(row["REB"]),
                "oreb": int(row["OREB"]),
                "dreb": int(row["DREB"]),
                "stl": int(row["STL"]),
                "blk": int(row["BLK"]),
                "tov": int(row["TOV"]),
                "fg3m": int(row["FG3M"]),
                "fg3a": int(row["FG3A"]),
                "fgm": int(row["FGM"]),
                "fga": int(row["FGA"]),
                "fg_pct": (
                    round(float(row["FG_PCT"]) * 100, 1)
                    if pd.notna(row["FG_PCT"])
                    else 0
                ),
                "fg3_pct": (
                    round(float(row["FG3_PCT"]) * 100, 1)
                    if pd.notna(row["FG3_PCT"])
                    else 0
                ),
                "ftm": int(row["FTM"]),
                "fta": int(row["FTA"]),
                "ft_pct": (
                    round(float(row["FT_PCT"]) * 100, 1)
                    if pd.notna(row["FT_PCT"])
                    else 0
                ),
                "plus_minus": (
                    int(row["PLUS_MINUS"]) if pd.notna(row["PLUS_MINUS"]) else 0
                ),
            }
        )
    return games


def fetch_head_to_head(team_id: int, opponent_abbr: str) -> list[dict]:
    """Busca jogos entre dois times na temporada atual via API (on-demand)."""
    time.sleep(SLEEP)
    log = TeamGameLog(
        team_id=team_id,
        season=SEASON,
        season_type_all_star="Regular Season",
    )
    df = log.get_data_frames()[0]
    df = df[df["MATCHUP"].str.contains(opponent_abbr)]
    games = []
    for _, row in df.iterrows():
        games.append(
            {
                "game_id": str(row["Game_ID"]),
                "date": _parse_game_date(str(row["GAME_DATE"])),
                "matchup": str(row["MATCHUP"]),
                "wl": str(row["WL"]),
                "pts": int(row["PTS"]),
                "ast": int(row["AST"]),
                "reb": int(row["REB"]),
                "stl": int(row["STL"]),
                "blk": int(row["BLK"]),
                "fgm": int(row["FGM"]),
                "fga": int(row["FGA"]),
                "fg_pct": round(float(row["FG_PCT"]) * 100, 1),
                "fg3m": int(row["FG3M"]),
                "fg3a": int(row["FG3A"]),
                "fg3_pct": round(float(row["FG3_PCT"]) * 100, 1),
                "oreb": int(row["OREB"]),
                "dreb": int(row["DREB"]),
            }
        )
    return games


def fetch_game_details(game_id: str) -> dict:
    """Busca box score completo de um jogo: stats dos dois times e jogadores."""
    time.sleep(SLEEP)
    bs = BoxScoreTraditionalV3(game_id=game_id)
    df = bs.get_data_frames()[0]

    teams_in_game = df["teamId"].unique()
    result = {"teams": [], "players": {}}

    for tid in teams_in_game:
        df_team = df[df["teamId"] == tid]
        # Buscar abreviação do time
        team_abbr = str(df_team.iloc[0].get("teamTricode", ""))
        team_city = str(df_team.iloc[0].get("teamCity", ""))
        team_name = str(df_team.iloc[0].get("teamName", ""))

        # Totais do time (somar jogadores)
        t_pts = t_reb = t_ast = t_stl = t_blk = t_tov = 0
        t_fgm = t_fga = t_fg3m = t_fg3a = t_ftm = t_fta = t_oreb = t_dreb = 0
        players = []

        for _, row in df_team.iterrows():
            pts = int(row["points"]) if pd.notna(row["points"]) else 0
            reb = int(row["reboundsTotal"]) if pd.notna(row["reboundsTotal"]) else 0
            ast = int(row["assists"]) if pd.notna(row["assists"]) else 0
            stl = int(row["steals"]) if pd.notna(row["steals"]) else 0
            blk = int(row["blocks"]) if pd.notna(row["blocks"]) else 0
            tov = int(row["turnovers"]) if pd.notna(row["turnovers"]) else 0
            fgm = int(row["fieldGoalsMade"]) if pd.notna(row["fieldGoalsMade"]) else 0
            fga = (
                int(row["fieldGoalsAttempted"])
                if pd.notna(row["fieldGoalsAttempted"])
                else 0
            )
            fg3m = (
                int(row["threePointersMade"])
                if pd.notna(row["threePointersMade"])
                else 0
            )
            fg3a = (
                int(row["threePointersAttempted"])
                if pd.notna(row["threePointersAttempted"])
                else 0
            )
            ftm = int(row["freeThrowsMade"]) if pd.notna(row["freeThrowsMade"]) else 0
            fta = (
                int(row["freeThrowsAttempted"])
                if pd.notna(row["freeThrowsAttempted"])
                else 0
            )
            oreb = (
                int(row["reboundsOffensive"])
                if pd.notna(row.get("reboundsOffensive"))
                else 0
            )
            dreb = (
                int(row["reboundsDefensive"])
                if pd.notna(row.get("reboundsDefensive"))
                else 0
            )

            mins_str = str(row["minutes"]) if pd.notna(row["minutes"]) else "0"
            try:
                parts = mins_str.split(":")
                mins_val = int(parts[0])
            except (ValueError, IndexError):
                mins_val = 0

            t_pts += pts
            t_reb += reb
            t_ast += ast
            t_stl += stl
            t_blk += blk
            t_tov += tov
            t_fgm += fgm
            t_fga += fga
            t_fg3m += fg3m
            t_fg3a += fg3a
            t_ftm += ftm
            t_fta += fta
            t_oreb += oreb
            t_dreb += dreb

            name = f"{row['firstName']} {row['familyName']}"
            players.append(
                {
                    "name": name,
                    "min": mins_val,
                    "pts": pts,
                    "reb": reb,
                    "ast": ast,
                    "stl": stl,
                    "blk": blk,
                    "tov": tov,
                    "fg": f"{fgm}/{fga}",
                    "fg3m": fg3m,
                    "fg3": f"{fg3m}/{fg3a}",
                    "ft": f"{ftm}/{fta}",
                }
            )

        team_stats = {
            "team_id": int(tid),
            "abbr": team_abbr,
            "city": team_city,
            "name": team_name,
            "pts": t_pts,
            "reb": t_reb,
            "ast": t_ast,
            "stl": t_stl,
            "blk": t_blk,
            "tov": t_tov,
            "oreb": t_oreb,
            "dreb": t_dreb,
            "fg": f"{t_fgm}/{t_fga}",
            "fg_pct": round(t_fgm / t_fga * 100, 1) if t_fga > 0 else 0,
            "fg3": f"{t_fg3m}/{t_fg3a}",
            "fg3_pct": round(t_fg3m / t_fg3a * 100, 1) if t_fg3a > 0 else 0,
            "ft": f"{t_ftm}/{t_fta}",
            "ft_pct": round(t_ftm / t_fta * 100, 1) if t_fta > 0 else 0,
        }

        # Destaques: agrupar por jogador, não repetir nomes
        # Buscar médias da temporada para jogadores de stl/blk
        conn_hl = get_connection()
        season_avgs = {}
        for p in players:
            row_avg = conn_hl.execute(
                "SELECT pts, reb, fg3m FROM players WHERE player_name = ?",
                (p["name"],),
            ).fetchone()
            if row_avg:
                season_avgs[p["name"]] = {
                    "pts": float(row_avg["pts"]) if row_avg["pts"] else 0,
                    "reb": float(row_avg["reb"]) if row_avg["reb"] else 0,
                    "fg3m": float(row_avg["fg3m"]) if row_avg["fg3m"] else 0,
                }
        conn_hl.close()

        player_highlights = {}  # name -> set of stat keys

        # Líder em pontos
        top_pts = sorted(players, key=lambda x: x["pts"], reverse=True)
        if top_pts:
            player_highlights.setdefault(top_pts[0]["name"], set()).add("pts")

        # Líder em rebotes
        top_reb = sorted(players, key=lambda x: x["reb"], reverse=True)
        if top_reb:
            player_highlights.setdefault(top_reb[0]["name"], set()).add("reb")

        # Líder em assistências
        top_ast = sorted(players, key=lambda x: x["ast"], reverse=True)
        if top_ast:
            player_highlights.setdefault(top_ast[0]["name"], set()).add("ast")

        # Jogadores com stl >= 2, blk >= 2, ou boas bolas de 3
        for p in players:
            name = p["name"]
            fg3m_game = p.get("fg3m", 0)
            savg = season_avgs.get(name, {})

            has_extra = False
            if fg3m_game >= 3 or (fg3m_game >= 2 and fg3m_game > savg.get("fg3m", 0)):
                player_highlights.setdefault(name, set()).add("3pm")
                has_extra = True
            if p["stl"] >= 2:
                player_highlights.setdefault(name, set()).add("stl")
                has_extra = True
            if p["blk"] >= 2:
                player_highlights.setdefault(name, set()).add("blk")
                has_extra = True
            # Para jogadores com extras, adicionar pts/reb se acima da média
            if has_extra:
                if savg and p["pts"] > savg.get("pts", 0):
                    player_highlights[name].add("pts")
                if savg and p["reb"] > savg.get("reb", 0):
                    player_highlights[name].add("reb")

        # Para todo jogador com destaque, garantir pts >= 20 e reb >= 5
        # Ordem fixa: pts, reb, ast, stl, blk, 3PM
        stat_order = ["pts", "reb", "ast", "stl", "blk", "3pm"]
        highlights = []
        for name, stat_keys in player_highlights.items():
            p_data = next((p for p in players if p["name"] == name), None)
            if not p_data:
                continue
            if p_data["pts"] >= 20:
                stat_keys.add("pts")
            if p_data["reb"] >= 5:
                stat_keys.add("reb")
            parts = []
            for key in stat_order:
                if key not in stat_keys:
                    continue
                if key == "pts":
                    parts.append(f"{p_data['pts']} pts")
                elif key == "reb":
                    parts.append(f"{p_data['reb']} reb")
                elif key == "ast":
                    parts.append(f"{p_data['ast']} ast")
                elif key == "stl":
                    parts.append(f"{p_data['stl']} stl")
                elif key == "blk":
                    parts.append(f"{p_data['blk']} blk")
                elif key == "3pm":
                    parts.append(f"{p_data.get('fg3m', 0)} 3PM")
            highlights.append({"name": name, "stats": ", ".join(parts)})

        team_stats["highlights"] = highlights
        result["teams"].append(team_stats)
        result["players"][team_abbr] = players

    return result


def fetch_h2h_player_stats(game_ids: list[str], team_id: int) -> list[dict]:
    """Busca estatísticas de jogadores nos confrontos diretos (box scores)."""
    from collections import defaultdict

    totals = defaultdict(
        lambda: {
            "name": "",
            "gp": 0,
            "min": 0,
            "pts": 0,
            "reb": 0,
            "ast": 0,
            "stl": 0,
            "blk": 0,
            "tov": 0,
            "fgm": 0,
            "fga": 0,
            "fg3m": 0,
            "fg3a": 0,
            "ftm": 0,
            "fta": 0,
            "plus_minus": 0,
        }
    )

    for gid in game_ids:
        time.sleep(SLEEP)
        bs = BoxScoreTraditionalV3(game_id=gid)
        df = bs.get_data_frames()[0]
        df_team = df[df["teamId"] == team_id]
        for _, row in df_team.iterrows():
            pid = int(row["personId"])
            p = totals[pid]
            p["name"] = f"{row['firstName']} {row['familyName']}"
            mins_str = str(row["minutes"]) if pd.notna(row["minutes"]) else "0"
            try:
                parts = mins_str.split(":")
                mins_val = int(parts[0]) + (int(parts[1]) / 60 if len(parts) > 1 else 0)
            except (ValueError, IndexError):
                mins_val = 0
            p["gp"] += 1
            p["min"] += mins_val
            p["pts"] += int(row["points"]) if pd.notna(row["points"]) else 0
            p["reb"] += (
                int(row["reboundsTotal"]) if pd.notna(row["reboundsTotal"]) else 0
            )
            p["ast"] += int(row["assists"]) if pd.notna(row["assists"]) else 0
            p["stl"] += int(row["steals"]) if pd.notna(row["steals"]) else 0
            p["blk"] += int(row["blocks"]) if pd.notna(row["blocks"]) else 0
            p["tov"] += int(row["turnovers"]) if pd.notna(row["turnovers"]) else 0
            p["fgm"] += (
                int(row["fieldGoalsMade"]) if pd.notna(row["fieldGoalsMade"]) else 0
            )
            p["fga"] += (
                int(row["fieldGoalsAttempted"])
                if pd.notna(row["fieldGoalsAttempted"])
                else 0
            )
            p["fg3m"] += (
                int(row["threePointersMade"])
                if pd.notna(row["threePointersMade"])
                else 0
            )
            p["fg3a"] += (
                int(row["threePointersAttempted"])
                if pd.notna(row["threePointersAttempted"])
                else 0
            )
            p["ftm"] += (
                int(row["freeThrowsMade"]) if pd.notna(row["freeThrowsMade"]) else 0
            )
            p["fta"] += (
                int(row["freeThrowsAttempted"])
                if pd.notna(row["freeThrowsAttempted"])
                else 0
            )
            p["plus_minus"] += (
                int(row["plusMinusPoints"]) if pd.notna(row["plusMinusPoints"]) else 0
            )

    result = []
    for pid, p in totals.items():
        gp = p["gp"]
        if gp == 0:
            continue
        result.append(
            {
                "jogador": p["name"],
                "jogos": gp,
                "min": round(p["min"] / gp, 1),
                "pts": round(p["pts"] / gp, 1),
                "reb": round(p["reb"] / gp, 1),
                "ast": round(p["ast"] / gp, 1),
                "stl": round(p["stl"] / gp, 1),
                "blk": round(p["blk"] / gp, 1),
                "tov": round(p["tov"] / gp, 1),
                "fg": f"{p['fgm']}/{p['fga']}",
                "fg_pct": round(p["fgm"] / p["fga"] * 100, 1) if p["fga"] > 0 else 0.0,
                "fg3": f"{p['fg3m']}/{p['fg3a']}",
                "fg3_pct": (
                    round(p["fg3m"] / p["fg3a"] * 100, 1) if p["fg3a"] > 0 else 0.0
                ),
                "ft": f"{p['ftm']}/{p['fta']}",
                "ft_pct": round(p["ftm"] / p["fta"] * 100, 1) if p["fta"] > 0 else 0.0,
                "+/-": p["plus_minus"],
            }
        )
    result.sort(key=lambda x: x["pts"], reverse=True)
    return result


def ensure_fresh_data() -> dict:
    """Garante dados atualizados. Atualiza no primeiro uso do dia."""
    conn = get_connection()
    if needs_update(conn):
        print("Dados desatualizados - iniciando atualizacao...")
        save_to_db(conn)
        save_players_to_db(conn)
    season = load_season(conn)
    teams = load_all_teams(conn)
    conn.close()
    return {"season": season, "teams": teams}


def force_update():
    """Força atualização completa. Gera mensagens de progresso via yield."""
    conn = get_connection()

    all_teams = get_all_teams()
    total = len(all_teams)

    yield f"Buscando classificação ({SEASON})..."
    standings_df = fetch_standings()
    standings_map = {
        int(row["TeamID"]): row.to_dict() for _, row in standings_df.iterrows()
    }

    yield "Buscando métricas avançadas..."
    advanced_map = fetch_advanced_metrics()

    yield "Buscando stats dos adversários..."
    opponent_map = fetch_opponent_stats()

    yield "Buscando stats diversas..."
    misc_map = fetch_misc_stats()

    yield "Buscando stats de scoring (mid-range)..."
    scoring_map = fetch_scoring_stats()

    for i, team in enumerate(all_teams, 1):
        tid = team["id"]
        abbr = team["abbreviation"]
        yield f"[{i}/{total}] Baixando {abbr} — {team['nickname']}..."

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
            games = fetch_last_games(tid, n=15)
            st = standings_map.get(tid, {})

            w = int(st.get("WINS", 0))
            l = int(st.get("LOSSES", 0))
            diff = round(stats["pts"] - stats.get("opp_pts", 0), 1)
            diff_str = f"{'+'if diff >= 0 else ''}{diff}"

            conn.execute(
                """
                INSERT INTO teams (abbreviation, id, name, city, nickname,
                    conference, division, w, l, pct, conf_rank,
                    home_record, road_record, last10, streak, diff,
                    pts, ast, reb, oreb, dreb, stl, blk, tov, pf, plus_minus,
                    fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct,
                    opp_pts, off_rating, def_rating, net_rating, pace,
                    ast_ratio, oreb_pct, dreb_pct, reb_pct, tov_pct,
                    opp_fgm, opp_fga, opp_fg_pct, opp_fg3m, opp_fg3a,
                    opp_fg3_pct, opp_oreb, opp_dreb, opp_reb,
                    pts_2nd_chance, pts_fb, pts_paint,
                    pts_mid_range, pct_pts_mid_range)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                        ?,?,?,?,?,?,?,?,?,?,
                        ?,?,?,?,?,?,?,?,?,
                        ?,?,?,?,?)
                ON CONFLICT(abbreviation) DO UPDATE SET
                    id=excluded.id, name=excluded.name, city=excluded.city,
                    nickname=excluded.nickname, conference=excluded.conference,
                    division=excluded.division, w=excluded.w, l=excluded.l,
                    pct=excluded.pct, conf_rank=excluded.conf_rank,
                    home_record=excluded.home_record, road_record=excluded.road_record,
                    last10=excluded.last10, streak=excluded.streak, diff=excluded.diff,
                    pts=excluded.pts, ast=excluded.ast, reb=excluded.reb,
                    oreb=excluded.oreb, dreb=excluded.dreb, stl=excluded.stl,
                    blk=excluded.blk, tov=excluded.tov, pf=excluded.pf,
                    plus_minus=excluded.plus_minus,
                    fgm=excluded.fgm, fga=excluded.fga, fg_pct=excluded.fg_pct,
                    fg3m=excluded.fg3m, fg3a=excluded.fg3a, fg3_pct=excluded.fg3_pct,
                    ftm=excluded.ftm, fta=excluded.fta, ft_pct=excluded.ft_pct,
                    opp_pts=excluded.opp_pts,
                    off_rating=excluded.off_rating, def_rating=excluded.def_rating,
                    net_rating=excluded.net_rating, pace=excluded.pace,
                    ast_ratio=excluded.ast_ratio, oreb_pct=excluded.oreb_pct,
                    dreb_pct=excluded.dreb_pct, reb_pct=excluded.reb_pct,
                    tov_pct=excluded.tov_pct,
                    opp_fgm=excluded.opp_fgm, opp_fga=excluded.opp_fga,
                    opp_fg_pct=excluded.opp_fg_pct, opp_fg3m=excluded.opp_fg3m,
                    opp_fg3a=excluded.opp_fg3a, opp_fg3_pct=excluded.opp_fg3_pct,
                    opp_oreb=excluded.opp_oreb, opp_dreb=excluded.opp_dreb,
                    opp_reb=excluded.opp_reb,
                    pts_2nd_chance=excluded.pts_2nd_chance,
                    pts_fb=excluded.pts_fb, pts_paint=excluded.pts_paint,
                    pts_mid_range=excluded.pts_mid_range,
                    pct_pts_mid_range=excluded.pct_pts_mid_range
            """,
                (
                    abbr,
                    tid,
                    team["full_name"],
                    team["city"],
                    team["nickname"],
                    st.get("Conference", ""),
                    st.get("Division", ""),
                    w,
                    l,
                    round(w / (w + l), 3) if (w + l) > 0 else 0,
                    int(st.get("PlayoffRank", 0)),
                    st.get("HOME", ""),
                    st.get("ROAD", ""),
                    st.get("L10", ""),
                    st.get("strCurrentStreak", ""),
                    diff_str,
                    stats.get("pts"),
                    stats.get("ast"),
                    stats.get("reb"),
                    stats.get("oreb"),
                    stats.get("dreb"),
                    stats.get("stl"),
                    stats.get("blk"),
                    stats.get("tov"),
                    stats.get("pf"),
                    stats.get("plus_minus"),
                    stats.get("fgm"),
                    stats.get("fga"),
                    stats.get("fg_pct"),
                    stats.get("fg3m"),
                    stats.get("fg3a"),
                    stats.get("fg3_pct"),
                    stats.get("ftm"),
                    stats.get("fta"),
                    stats.get("ft_pct"),
                    stats.get("opp_pts"),
                    stats.get("off_rating"),
                    stats.get("def_rating"),
                    stats.get("net_rating"),
                    stats.get("pace"),
                    stats.get("ast_ratio"),
                    stats.get("oreb_pct"),
                    stats.get("dreb_pct"),
                    stats.get("reb_pct"),
                    stats.get("tov_pct"),
                    stats.get("opp_fgm"),
                    stats.get("opp_fga"),
                    stats.get("opp_fg_pct"),
                    stats.get("opp_fg3m"),
                    stats.get("opp_fg3a"),
                    stats.get("opp_fg3_pct"),
                    stats.get("opp_oreb"),
                    stats.get("opp_dreb"),
                    stats.get("opp_reb"),
                    stats.get("pts_2nd_chance"),
                    stats.get("pts_fb"),
                    stats.get("pts_paint"),
                    stats.get("pts_mid_range"),
                    stats.get("pct_pts_mid_range"),
                ),
            )

            conn.execute("DELETE FROM games WHERE team_abbr = ?", (abbr,))
            for g in games:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO games
                    (game_id, team_abbr, date, matchup, wl, pts, oreb, dreb, reb, ast,
                     stl, blk, tov, pf, fg_pct, fg3_pct, ft_pct)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        g["game_id"],
                        abbr,
                        g["date"],
                        g["matchup"],
                        g["wl"],
                        g["pts"],
                        g["oreb"],
                        g["dreb"],
                        g["reb"],
                        g["ast"],
                        g["stl"],
                        g["blk"],
                        g["tov"],
                        g["pf"],
                        g["fg_pct"],
                        g["fg3_pct"],
                        g["ft_pct"],
                    ),
                )

            conn.commit()
        except Exception as e:
            yield f"⚠️ Erro em {abbr}: {e}"

    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO meta (key, value) VALUES ('last_update', ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (now,),
    )
    conn.execute(
        "INSERT INTO meta (key, value) VALUES ('season', ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (SEASON,),
    )
    conn.commit()

    yield "Baixando injury report (ESPN)..."
    try:
        n = save_injuries_to_db(conn)
        yield f"Injuries: {n} jogadores registrados"
    except Exception as e:
        yield f"⚠️ Erro ao baixar injuries: {e}"

    yield "Baixando dados de jogadores (base)..."
    save_players_to_db(conn)

    yield "✅ Atualização concluída!"
    conn.close()


def get_last_update() -> str | None:
    """Retorna a data/hora da última atualização."""
    conn = get_connection()
    row = conn.execute("SELECT value FROM meta WHERE key='last_update'").fetchone()
    conn.close()
    return row["value"] if row else None


def get_last_game_date() -> str | None:
    """Retorna a data do jogo mais recente disponível nos dados."""
    conn = get_connection()
    row = conn.execute("SELECT MAX(date) FROM games").fetchone()
    conn.close()
    return row[0] if row and row[0] else None


if __name__ == "__main__":
    conn = get_connection()
    save_to_db(conn)
    save_players_to_db(conn)
    conn.close()

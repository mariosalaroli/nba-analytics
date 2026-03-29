"""
app.py — Painel NBA com Streamlit
Uso:  streamlit run app.py
"""

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go
import plotly.express as px
from pathlib import Path
from nba_data import (
    ensure_fresh_data,
    force_update,
    get_last_update,
    load_players_list,
    load_player_stats,
    fetch_player_game_log,
    fetch_head_to_head,
    fetch_h2h_player_stats,
    load_all_players,
    get_connection,
    needs_update,
    save_to_db,
    save_players_to_db,
    load_season,
    load_all_teams,
)

# ─── Configuração da página ───────────────────────────────────────────────────
st.set_page_config(
    page_title="NBA Stats",
    page_icon="🏀",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── CSS customizado ──────────────────────────────────────────────────────────
st.markdown(
    """
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=DM+Sans:wght@400;500;600&display=swap');

html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }

header[data-testid="stHeader"] { display: none !important; }
.block-container { padding: 1rem 2rem 2rem; max-width: 1400px; }

/* Sidebar */
section[data-testid="stSidebar"] { background: #0d0d0d; }
section[data-testid="stSidebar"] * { color: #ccc !important; }
section[data-testid="stSidebar"] .stSelectbox label,
section[data-testid="stSidebar"] h1,
section[data-testid="stSidebar"] h2,
section[data-testid="stSidebar"] h3 { color: #fff !important; }
.sidebar-team-badge {
    border-radius: 10px; padding: 0.6rem 1rem; margin: 0.3rem 0 0.2rem; text-align: center;
}
.sidebar-team-badge .team-name {
    color: #fff !important; font-weight: 700 !important; font-size: 1.15rem !important;
    letter-spacing: 0.02em; margin: 0; text-shadow: 0 1px 3px rgba(0,0,0,0.4);
}

/* Metric cards */
div[data-testid="metric-container"] {
    background: #f7f7f5;
    border: 1px solid #ebebeb;
    border-radius: 10px;
    padding: 1rem 1.2rem;
}
div[data-testid="metric-container"] label { font-size: 11px !important; color: #888 !important; letter-spacing: 0.05em; }
div[data-testid="metric-container"] [data-testid="stMetricValue"] { font-size: 1.6rem !important; font-weight: 600 !important; color: #111 !important; }
div[data-testid="metric-container"] [data-testid="stMetricDelta"] { font-size: 12px !important; }

/* Tabs */
.stTabs [data-baseweb="tab-list"] { gap: 4px; border-bottom: 1px solid #ebebeb; }
.stTabs [data-baseweb="tab"] { padding: 8px 16px; font-size: 13px; font-weight: 500; }
.stTabs [aria-selected="true"] { color: #111 !important; border-bottom: 2px solid #111 !important; }

/* Dataframe */
.dataframe { font-family: 'DM Mono', monospace !important; font-size: 12px !important; }

/* Seção header */
.section-header {
    font-size: 11px;
    font-weight: 600;
    color: #888;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    margin-bottom: 0.5rem;
    padding-bottom: 6px;
    border-bottom: 1px solid #ebebeb;
}

/* Streak chip */
.streak-chip {
    display: inline-block;
    padding: 2px 7px;
    border-radius: 4px;
    font-size: 11px;
    font-weight: 600;
    margin: 1px;
    font-family: 'DM Mono', monospace;
}
.chip-w { background: #e8f5e9; color: #2e7d32; }
.chip-l { background: #ffebee; color: #c62828; }

/* Record badge */
.record-badge {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 6px;
    font-size: 12px;
    font-weight: 600;
    font-family: 'DM Mono', monospace;
}

.diff-pos { color: #2e7d32; font-weight: 600; font-family: 'DM Mono', monospace; }
.diff-neg { color: #c62828; font-weight: 600; font-family: 'DM Mono', monospace; }
</style>
""",
    unsafe_allow_html=True,
)


# ─── Carregamento de dados ────────────────────────────────────────────────────


@st.cache_data(ttl=3600)
def load_cache() -> dict:
    conn = get_connection()
    if needs_update(conn):
        conn.close()
        status = st.status("🏀 Baixando dados da NBA...", expanded=True)
        status.write("Buscando dados dos times...")
        conn = get_connection()
        save_to_db(conn)
        status.write("Buscando dados dos jogadores...")
        save_players_to_db(conn)
        status.update(label="✅ Dados carregados!", state="complete")
    season = load_season(conn)
    teams = load_all_teams(conn)
    conn.close()
    return {"season": season, "teams": teams}


def get_team_color(abbr: str) -> str:
    colors = {
        "ATL": "#E03A3E",
        "BOS": "#007A33",
        "BKN": "#777777",
        "CHA": "#00788C",
        "CHI": "#CE1141",
        "CLE": "#6F263D",
        "DAL": "#00538C",
        "DEN": "#FEC524",
        "DET": "#C8102E",
        "GSW": "#1D428A",
        "HOU": "#CE1141",
        "IND": "#FDBB30",
        "LAC": "#C8102E",
        "LAL": "#552583",
        "MEM": "#5D76A9",
        "MIA": "#98002E",
        "MIL": "#00471B",
        "MIN": "#236192",
        "NOP": "#C8102E",
        "NYK": "#F58426",
        "OKC": "#007AC1",
        "ORL": "#0077C0",
        "PHI": "#006BB6",
        "PHX": "#E56020",
        "POR": "#E03A3E",
        "SAC": "#5A2D81",
        "SAS": "#C4CED4",
        "TOR": "#CE1141",
        "UTA": "#F9A01B",
        "WAS": "#E31837",
    }
    return colors.get(abbr, "#333333")


# ─── Sidebar ──────────────────────────────────────────────────────────────────


def render_sidebar(cache: dict) -> tuple[dict, str]:
    with st.sidebar:
        st.markdown("### 🏀 NBA Stats")
        st.markdown(f"*Temporada {cache['season']}*")
        st.divider()

        teams_available = list(cache["teams"].keys())
        default_team_idx = (
            teams_available.index("CLE") if "CLE" in teams_available else 0
        )
        selected_abbr = st.selectbox(
            "Time",
            options=teams_available,
            index=default_team_idx,
            format_func=lambda a: f"{a} — {cache['teams'][a]['nickname']}",
            key="sidebar_team",
        )
        team_data = cache["teams"][selected_abbr]
        tc = get_team_color(selected_abbr)
        st.markdown(
            f'<div class="sidebar-team-badge" style="background:{tc}; border:1px solid {tc};">'
            f'<p class="team-name">{selected_abbr} — {team_data["nickname"]}</p></div>',
            unsafe_allow_html=True,
        )

        st.divider()
        st.markdown("**Navegação**")

        pages = [
            "Visão geral",
            "Comparativo da Liga",
            "Últimos jogos",
            "Confronto direto",
            "Jogadores",
        ]
        # Suporte a navegação programática
        if "_navigate_to" in st.session_state:
            nav = st.session_state.pop("_navigate_to")
            if nav in pages:
                st.session_state["sidebar_page"] = nav

        page = st.radio(
            "Seção",
            options=pages,
            label_visibility="collapsed",
            key="sidebar_page",
        )

        if "_note" in cache:
            st.divider()
            st.caption(f"ℹ️ {cache['_note']}")

        st.divider()
        if st.button("🔄 Atualizar dados", use_container_width=True):
            status = st.status("📡 Atualizando banco de dados...", expanded=True)
            for msg in force_update():
                status.write(msg)
            status.update(label="✅ Atualização concluída!", state="complete")
            # Recarrega cache
            from nba_data import get_connection, load_season, load_all_teams

            _conn = get_connection()
            st.session_state["cache"] = {
                "season": load_season(_conn),
                "teams": load_all_teams(_conn),
            }
            _conn.close()
            import time

            time.sleep(1.5)
            st.rerun()
        last_upd = get_last_update()
        if last_upd:
            from datetime import datetime as _dt

            try:
                dt = _dt.fromisoformat(last_upd)
                st.caption(f"Última atualização: {dt.strftime('%d/%m/%Y %H:%M')}")
            except Exception:
                st.caption(f"Última atualização: {last_upd}")

    return cache["teams"][selected_abbr], page


# ─── Helpers de visualização ──────────────────────────────────────────────────


def render_streak_chips(games: list[dict]) -> str:
    chips = []
    for g in reversed(games):
        cls = "chip-w" if g["wl"] == "W" else "chip-l"
        chips.append(f'<span class="streak-chip {cls}">{g["wl"]}</span>')
    return " ".join(chips)


def stat_bar_chart(
    team: dict,
    stat_key: str,
    label: str,
    all_teams: dict,
    lower_is_better: bool = False,
) -> go.Figure:
    vals = {
        abbr: t[stat_key]
        for abbr, t in all_teams.items()
        if t.get(stat_key) is not None
    }
    df = pd.DataFrame(list(vals.items()), columns=["Time", label])
    df = df.sort_values(label, ascending=lower_is_better).reset_index(drop=True)

    # Posição real do time selecionado (1-based, empates = mesma posição)
    abbr_sel = team["abbreviation"]
    if lower_is_better:
        df["_rank"] = df[label].rank(method="min", ascending=True).astype(int)
    else:
        df["_rank"] = df[label].rank(method="min", ascending=False).astype(int)
    real_rank = (
        df.loc[df["Time"] == abbr_sel, "_rank"].values[0]
        if abbr_sel in df["Time"].values
        else None
    )
    df = df.drop(columns=["_rank"])

    # Top 10 (ou 9 + time selecionado se não estiver no top 10)
    in_top = abbr_sel in df.head(10)["Time"].values
    if in_top:
        top = df.head(10).copy()
        # Adicionar posição ao label do time selecionado
        top["Time"] = top["Time"].apply(
            lambda x: f"{x} ({real_rank}º)" if x == abbr_sel else x
        )
    else:
        top = df.head(9).copy()
        if real_rank is not None:
            sel_row = df[df["Time"] == abbr_sel].copy()
            sel_row["Time"] = sel_row["Time"].apply(lambda x: f"{x} ({real_rank}º)")
            top = pd.concat([top, sel_row], ignore_index=True)

    top = top.sort_values(label, ascending=not lower_is_better).reset_index(drop=True)

    colors = [
        get_team_color(abbr_sel) if abbr_sel in t else "#9e9e9e" for t in top["Time"]
    ]

    fig = go.Figure(
        go.Bar(
            x=top[label],
            y=top["Time"],
            orientation="h",
            marker_color=colors,
            text=top[label],
            textposition="inside",
            textfont=dict(size=10, family="DM Mono", color="white"),
            insidetextanchor="end",
        )
    )
    fig.update_layout(
        height=max(200, len(top) * 28),
        margin=dict(l=0, r=40, t=10, b=10),
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis=dict(
            showgrid=True, gridcolor="#f0f0f0", zeroline=False, showticklabels=False
        ),
        yaxis=dict(tickfont=dict(size=11, family="DM Mono")),
        showlegend=False,
    )
    return fig


def last_games_chart(games: list[dict], team_color: str) -> go.Figure:
    df = pd.DataFrame(games)
    df = df.iloc[::-1].reset_index(drop=True)
    df["jogo"] = df["matchup"].str.replace(r"^[A-Z]+ ", "", regex=True)
    df["cor"] = df["wl"].map({"W": "#2e7d32", "L": "#c62828"})

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=list(range(len(df))),
            y=df["pts"],
            marker_color=df["cor"],
            text=df["pts"],
            textposition="inside",
            textfont=dict(size=10, family="DM Mono", color="white"),
            hovertemplate="<b>%{customdata[0]}</b><br>Pts: %{y}<br>Ast: %{customdata[1]}<br>Reb: %{customdata[2]}<br>FG%: %{customdata[3]}%<extra></extra>",
            customdata=df[["matchup", "ast", "reb", "fg_pct"]].values,
            name="Pontos",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=list(range(len(df))),
            y=df["pts"].rolling(3, min_periods=1).mean(),
            mode="lines",
            line=dict(color=team_color, width=2, dash="dot"),
            name="Média móvel",
            hoverinfo="skip",
        )
    )

    fig.update_layout(
        height=280,
        margin=dict(l=0, r=0, t=20, b=40),
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis=dict(
            tickvals=list(range(len(df))),
            ticktext=df["date"].tolist(),
            tickfont=dict(size=10, family="DM Mono"),
            gridcolor="#f5f5f5",
        ),
        yaxis=dict(
            title="Pontos",
            gridcolor="#f5f5f5",
            tickfont=dict(size=10),
        ),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
        showlegend=True,
    )
    return fig


def radar_chart(team: dict, all_teams: dict) -> go.Figure:
    # Ofensivas na meia-lua superior, defensivas na inferior
    stats_keys = ["ast", "off_rating", "pts", "reb", "def_rating", "blk", "stl"]
    labels = [
        "Assist.",
        "Off Rtg",
        "Pontos",
        "Rebotes",
        "Def Rtg",
        "Bloqueios",
        "Roubos",
    ]

    def normalize(key):
        vals = [t[key] for t in all_teams.values()]
        mn, mx = min(vals), max(vals)
        return [(v - mn) / (mx - mn + 1e-9) * 10 for v in vals]

    team_vals = []
    for key in stats_keys:
        all_vals = [t[key] for t in all_teams.values()]
        mn, mx = min(all_vals), max(all_vals)
        v = team[key]
        team_vals.append((v - mn) / (mx - mn + 1e-9) * 10)

    color = get_team_color(team["abbreviation"])
    # Convert hex color to rgba for fillcolor (Plotly doesn't support 8-char hex)
    r, g, b = int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)
    fill_rgba = f"rgba({r},{g},{b},0.2)"

    fig = go.Figure(
        go.Scatterpolar(
            r=team_vals + [team_vals[0]],
            theta=labels + [labels[0]],
            fill="toself",
            fillcolor=fill_rgba,
            line=dict(color=color, width=2),
            name=team["nickname"],
        )
    )
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 10],
                showticklabels=False,
                gridcolor="#eee",
                showline=False,
            ),
            angularaxis=dict(tickfont=dict(size=12, family="DM Sans")),
            bgcolor="white",
            gridshape="circular",
        ),
        showlegend=False,
        height=300,
        margin=dict(l=40, r=40, t=20, b=20),
        paper_bgcolor="white",
    )
    return fig


# ─── Páginas ──────────────────────────────────────────────────────────────────


def page_overview(team: dict, all_teams: dict):
    color = get_team_color(team["abbreviation"])

    # Header do time
    streak_html = render_streak_chips(team["last_games"][:7])

    st.markdown(
        f"""
    <div style="display:flex; align-items:center; gap:16px; margin-bottom:1.2rem;">
        <div style="width:52px;height:52px;border-radius:50%;background:{color};display:flex;
                    align-items:center;justify-content:center;color:white;font-weight:700;font-size:14px;
                    font-family:'DM Mono',monospace;">
            {team["abbreviation"]}
        </div>
        <div>
            <div style="font-size:22px;font-weight:600;color:#111;line-height:1.1;">{team["name"]}</div>
            <div style="font-size:12px;color:#888;">{team["conference"]}ern Conference · {team["division"]} Division · #{team["conf_rank"]}</div>
        </div>
        <div style="margin-left:auto;text-align:right;">
            <div style="font-size:28px;font-weight:700;font-family:'DM Mono',monospace;color:#111;">
                {team["w"]}-{team["l"]}
            </div>
            <div style="font-size:12px;color:#888;">{team["pct"]:.3f} · #{team["conf_rank"]} {team["conference"]}</div>
        </div>
    </div>
    """,
        unsafe_allow_html=True,
    )

    # Métricas principais
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Pontos / jogo", f"{team['pts']}")
    c2.metric("Assistências / jogo", f"{team['ast']}")
    c3.metric("Rebotes / jogo", f"{team['reb']}")
    c4.metric("FG%", f"{team['fg_pct']}%")
    c5.metric("3P%", f"{team['fg3_pct']}%")

    # Métricas avançadas
    a1, a2, a3, a4, a5 = st.columns(5)
    a1.metric("Off Rating", f"{team.get('off_rating', '—')}")
    a2.metric("Def Rating", f"{team.get('def_rating', '—')}")
    a3.metric("Net Rating", f"{team.get('net_rating', '—')}")
    a4.metric("Pace", f"{team.get('pace', '—')}")
    a5.metric("+/−", f"{team.get('plus_minus', '—')}")

    st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

    col_left, col_right = st.columns([1, 1])

    with col_left:
        st.markdown(
            '<div class="section-header">Radar ofensivo/defensivo</div>',
            unsafe_allow_html=True,
        )
        st.plotly_chart(
            radar_chart(team, all_teams),
            use_container_width=True,
            config={"displayModeBar": False},
        )

    with col_right:
        st.markdown(
            '<div class="section-header">Últimos 7 jogos</div>', unsafe_allow_html=True
        )
        st.markdown(
            f"<div style='margin:8px 0 12px;'>{streak_html}</div>",
            unsafe_allow_html=True,
        )
        games_data = []
        for g in team["last_games"][:7]:
            games_data.append(
                {
                    "Data": g["date"],
                    "Jogo": g["matchup"],
                    "W/L": g["wl"],
                    "PTS": g["pts"],
                    "REB": g["reb"],
                    "AST": g["ast"],
                    "STL": g.get("stl", 0),
                    "BLK": g.get("blk", 0),
                    "TOV": g.get("tov", 0),
                    "FG%": g["fg_pct"],
                    "3P%": g.get("fg3_pct", 0),
                    "FT%": g.get("ft_pct", 0),
                    "+/−": g.get("plus_minus", 0),
                }
            )
        if games_data:
            df_games = pd.DataFrame(games_data)
            st.dataframe(df_games, use_container_width=True, hide_index=True)

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
        rec_cols = st.columns(3)
        rec_cols[0].markdown(f"🏠 **Casa**\n\n`{team['home_record']}`")
        rec_cols[1].markdown(f"✈️ **Fora**\n\n`{team['road_record']}`")
        rec_cols[2].markdown(f"📅 **Últimos 10**\n\n`{team['last10']}`")


def page_stats(team: dict, all_teams: dict):
    # ── Pontos e Assistências (lado a lado) ──
    st.markdown(
        '<div class="section-header">Comparativo da Liga — posição entre os 30 times</div>',
        unsafe_allow_html=True,
    )
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(
            '<div class="section-header">Pontos por jogo</div>',
            unsafe_allow_html=True,
        )
        st.plotly_chart(
            stat_bar_chart(team, "pts", "Pontos / jogo", all_teams),
            use_container_width=True,
            config={"displayModeBar": False},
        )
    with c2:
        st.markdown(
            '<div class="section-header">Assistências por jogo</div>',
            unsafe_allow_html=True,
        )
        st.plotly_chart(
            stat_bar_chart(team, "ast", "Assistências / jogo", all_teams),
            use_container_width=True,
            config={"displayModeBar": False},
        )

    # ── Rebotes e Second Chance ──
    c3, c4 = st.columns(2)
    with c3:
        st.markdown(
            '<div class="section-header">Rebotes por jogo</div>', unsafe_allow_html=True
        )
        st.plotly_chart(
            stat_bar_chart(team, "reb", "Rebotes / jogo", all_teams),
            use_container_width=True,
            config={"displayModeBar": False},
        )
    with c4:
        st.markdown(
            '<div class="section-header">Pts de segunda chance / jogo</div>',
            unsafe_allow_html=True,
        )
        st.plotly_chart(
            stat_bar_chart(team, "pts_2nd_chance", "2nd Chance Pts", all_teams),
            use_container_width=True,
            config={"displayModeBar": False},
        )

    # ── Roubos e Bloqueios ──
    c5, c6 = st.columns(2)
    with c5:
        st.markdown(
            '<div class="section-header">Roubos de bola por jogo</div>',
            unsafe_allow_html=True,
        )
        st.plotly_chart(
            stat_bar_chart(team, "stl", "Roubos / jogo", all_teams),
            use_container_width=True,
            config={"displayModeBar": False},
        )
    with c6:
        st.markdown(
            '<div class="section-header">Bloqueios por jogo</div>',
            unsafe_allow_html=True,
        )
        st.plotly_chart(
            stat_bar_chart(team, "blk", "Bloqueios / jogo", all_teams),
            use_container_width=True,
            config={"displayModeBar": False},
        )

    # ── Estatísticas avançadas ──
    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    st.markdown(
        '<div class="section-header">Métricas avançadas — comparação com a liga</div>',
        unsafe_allow_html=True,
    )

    ac1, ac2 = st.columns(2)
    with ac1:
        st.markdown(
            '<div class="section-header">Offensive Rating</div>',
            unsafe_allow_html=True,
        )
        st.plotly_chart(
            stat_bar_chart(team, "off_rating", "Off Rating", all_teams),
            use_container_width=True,
            config={"displayModeBar": False},
        )
    with ac2:
        st.markdown(
            '<div class="section-header">Defensive Rating</div>',
            unsafe_allow_html=True,
        )
        st.plotly_chart(
            stat_bar_chart(
                team, "def_rating", "Def Rating", all_teams, lower_is_better=True
            ),
            use_container_width=True,
            config={"displayModeBar": False},
        )

    ac3, ac4 = st.columns(2)
    with ac3:
        st.markdown(
            '<div class="section-header">Net Rating</div>',
            unsafe_allow_html=True,
        )
        st.plotly_chart(
            stat_bar_chart(team, "net_rating", "Net Rating", all_teams),
            use_container_width=True,
            config={"displayModeBar": False},
        )
    with ac4:
        st.markdown(
            '<div class="section-header">Pace</div>',
            unsafe_allow_html=True,
        )
        st.plotly_chart(
            stat_bar_chart(team, "pace", "Pace", all_teams),
            use_container_width=True,
            config={"displayModeBar": False},
        )

    # ── Arremessos ──
    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    st.markdown(
        '<div class="section-header">Eficiência de arremesso</div>',
        unsafe_allow_html=True,
    )
    sc1, sc2, sc3 = st.columns(3)
    with sc1:
        st.markdown(
            '<div class="section-header">Field Goal %</div>',
            unsafe_allow_html=True,
        )
        st.plotly_chart(
            stat_bar_chart(team, "fg_pct", "FG%", all_teams),
            use_container_width=True,
            config={"displayModeBar": False},
        )
    with sc2:
        st.markdown(
            '<div class="section-header">3-Point %</div>',
            unsafe_allow_html=True,
        )
        st.plotly_chart(
            stat_bar_chart(team, "fg3_pct", "3P%", all_teams),
            use_container_width=True,
            config={"displayModeBar": False},
        )
    with sc3:
        st.markdown(
            '<div class="section-header">Free Throw %</div>',
            unsafe_allow_html=True,
        )
        st.plotly_chart(
            stat_bar_chart(team, "ft_pct", "FT%", all_teams),
            use_container_width=True,
            config={"displayModeBar": False},
        )

    # ── Volume de 3 pontos ──
    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
    st.markdown(
        '<div class="section-header">Volume de 3 pontos</div>',
        unsafe_allow_html=True,
    )

    # Calcular frequência de 3P (% dos arremessos que são de 3)
    for t in all_teams.values():
        fga = t.get("fga") or 1
        t["fg3_freq"] = round((t.get("fg3a") or 0) / fga * 100, 1)

    tp1, tp2, tp3 = st.columns(3)
    with tp1:
        st.markdown(
            '<div class="section-header">3PTA (tentativas / jogo)</div>',
            unsafe_allow_html=True,
        )
        st.plotly_chart(
            stat_bar_chart(team, "fg3a", "3PTA", all_teams),
            use_container_width=True,
            config={"displayModeBar": False},
        )
    with tp2:
        st.markdown(
            '<div class="section-header">3PTM (convertidas / jogo)</div>',
            unsafe_allow_html=True,
        )
        st.plotly_chart(
            stat_bar_chart(team, "fg3m", "3PTM", all_teams),
            use_container_width=True,
            config={"displayModeBar": False},
        )
    with tp3:
        st.markdown(
            '<div class="section-header">3P Freq% (% arremessos de 3)</div>',
            unsafe_allow_html=True,
        )
        st.plotly_chart(
            stat_bar_chart(team, "fg3_freq", "3P Freq%", all_teams),
            use_container_width=True,
            config={"displayModeBar": False},
        )

    # ── Tabela geral ──
    st.markdown(
        '<div class="section-header">Tabela geral — todos os times</div>',
        unsafe_allow_html=True,
    )
    rows = []
    for abbr, t in all_teams.items():
        rows.append(
            {
                "Time": t["name"],
                "Conf": t["conference"][0] if t["conference"] else "",
                "W": t["w"],
                "L": t["l"],
                "W%": f"{t['pct']:.3f}",
                "Pts": t["pts"],
                "Ast": t["ast"],
                "Reb": t["reb"],
                "Stl": t["stl"],
                "Blk": t["blk"],
                "Tov": t["tov"],
                "FG%": t["fg_pct"],
                "3P%": t["fg3_pct"],
                "FT%": t["ft_pct"],
                "ORtg": t.get("off_rating", ""),
                "DRtg": t.get("def_rating", ""),
                "NetRtg": t.get("net_rating", ""),
                "Pace": t.get("pace", ""),
                "+/−": t.get("plus_minus", ""),
            }
        )
    df = pd.DataFrame(rows).sort_values("W", ascending=False).reset_index(drop=True)
    st.dataframe(df, use_container_width=True, hide_index=True)


def page_games(team: dict):
    st.markdown(
        '<div class="section-header">Pontuação nos últimos 10 jogos</div>',
        unsafe_allow_html=True,
    )
    color = get_team_color(team["abbreviation"])
    st.plotly_chart(
        last_games_chart(team["last_games"], color),
        use_container_width=True,
        config={"displayModeBar": False},
    )

    st.markdown(
        '<div class="section-header">Log completo</div>', unsafe_allow_html=True
    )
    df = pd.DataFrame(team["last_games"])
    df.columns = ["Data", "Confronto", "R", "Pts", "Ast", "Reb", "FG%"]
    df["R"] = df["R"].map({"W": "✅ V", "L": "❌ D"})
    st.dataframe(df, use_container_width=True, hide_index=True)


def _go_to_player(player_name: str):
    """Navega para a página Jogadores com o jogador selecionado."""
    players = _get_players_list()
    idx = next(
        (i for i, p in enumerate(players) if p["player_name"] == player_name),
        None,
    )
    if idx is not None:
        st.session_state["_player_idx"] = idx
        st.session_state["_navigate_to"] = "Jogadores"
        st.rerun()


def page_comparison(all_teams: dict):
    st.markdown(
        '<div class="section-header">Comparar dois times</div>', unsafe_allow_html=True
    )

    teams_list = list(all_teams.keys())

    # Persistir seleções
    if "_compare_a" not in st.session_state:
        st.session_state["_compare_a"] = teams_list[0]
    if "_compare_b" not in st.session_state:
        st.session_state["_compare_b"] = (
            teams_list[1] if len(teams_list) > 1 else teams_list[0]
        )

    idx_a = (
        teams_list.index(st.session_state["_compare_a"])
        if st.session_state["_compare_a"] in teams_list
        else 0
    )
    idx_b = (
        teams_list.index(st.session_state["_compare_b"])
        if st.session_state["_compare_b"] in teams_list
        else 1
    )

    c1, c2 = st.columns(2)
    with c1:
        a = st.selectbox(
            "Time A",
            teams_list,
            index=idx_a,
            format_func=lambda x: all_teams[x]["name"],
            key="compare_team_a",
        )
    with c2:
        b = st.selectbox(
            "Time B",
            teams_list,
            index=idx_b,
            format_func=lambda x: all_teams[x]["name"],
            key="compare_team_b",
        )

    st.session_state["_compare_a"] = a
    st.session_state["_compare_b"] = b

    ta, tb = all_teams[a], all_teams[b]
    ca, cb = get_team_color(a), get_team_color(b)

    stats_keys = [
        "pts",
        "ast",
        "reb",
        "stl",
        "blk",
        "tov",
        "fg_pct",
        "fg3_pct",
        "ft_pct",
        "off_rating",
        "def_rating",
        "net_rating",
        "pace",
        "plus_minus",
    ]
    labels = [
        "Pts/j",
        "Ast/j",
        "Reb/j",
        "Stl/j",
        "Blk/j",
        "Tov/j",
        "FG%",
        "3P%",
        "FT%",
        "ORtg",
        "DRtg",
        "NetRtg",
        "Pace",
        "+/−",
    ]

    fig = go.Figure()
    vals_a = [ta[k] for k in stats_keys]
    vals_b = [tb[k] for k in stats_keys]

    fig.add_trace(
        go.Bar(
            name=ta["nickname"],
            x=labels,
            y=vals_a,
            marker_color=ca,
            text=[f"{v:.1f}" for v in vals_a],
            textposition="inside",
            textfont=dict(size=10, family="DM Mono", color="white"),
        )
    )
    fig.add_trace(
        go.Bar(
            name=tb["nickname"],
            x=labels,
            y=vals_b,
            marker_color=cb,
            text=[f"{v:.1f}" for v in vals_b],
            textposition="inside",
            textfont=dict(size=10, family="DM Mono", color="white"),
        )
    )

    fig.update_layout(
        barmode="group",
        height=320,
        margin=dict(l=0, r=0, t=30, b=10),
        plot_bgcolor="white",
        paper_bgcolor="white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        yaxis=dict(showgrid=True, gridcolor="#f5f5f5", showticklabels=False),
        xaxis=dict(tickfont=dict(size=12, family="DM Mono")),
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    # Tabela resumo
    lower_is_better = {"def_rating", "tov"}
    comp_rows = []
    for key, label in zip(stats_keys, labels):
        va, vb = ta[key], tb[key]
        if key in lower_is_better:
            melhor = ta["nickname"] if va < vb else (tb["nickname"] if vb < va else "—")
        else:
            melhor = ta["nickname"] if va > vb else (tb["nickname"] if vb > va else "—")
        comp_rows.append(
            {
                "Estatística": label,
                ta["nickname"]: va,
                tb["nickname"]: vb,
                "Vantagem": melhor,
            }
        )
    df_comp = pd.DataFrame(comp_rows)
    st.dataframe(df_comp, use_container_width=True, hide_index=True)

    # Confrontos diretos na temporada
    st.markdown("---")
    st.subheader("Confrontos diretos na temporada")

    h2h_games = fetch_head_to_head(ta["id"], b)
    if not h2h_games:
        st.info("Nenhum confronto direto encontrado nesta temporada.")
    else:
        wins_a = sum(1 for g in h2h_games if g["wl"] == "W")
        wins_b = len(h2h_games) - wins_a
        st.markdown(
            f"**{ta['nickname']}** {wins_a} × {wins_b} **{tb['nickname']}**  "
            f"&nbsp;&nbsp;({len(h2h_games)} jogo{'s' if len(h2h_games) > 1 else ''})"
        )

        h2h_rows = []
        for g in h2h_games:
            h2h_rows.append(
                {
                    "Data": g["date"],
                    "Confronto": g["matchup"],
                    "R": g["wl"],
                    "Pts": g["pts"],
                    "Reb": g["reb"],
                    "Ast": g["ast"],
                    "Stl": g["stl"],
                    "Blk": g["blk"],
                    "FG": f"{g['fgm']}/{g['fga']}",
                    "FG%": g["fg_pct"],
                    "3P": f"{g['fg3m']}/{g['fg3a']}",
                    "3P%": g["fg3_pct"],
                    "OREB": g["oreb"],
                    "DREB": g["dreb"],
                    "Erros FG": g["fga"] - g["fgm"],
                }
            )
        df_h2h = pd.DataFrame(h2h_rows)
        st.dataframe(df_h2h, use_container_width=True, hide_index=True)

        # Estatísticas dos jogadores nos confrontos diretos
        game_ids = [g["game_id"] for g in h2h_games]

        col_a, col_b = st.columns(2)

        # Carregar médias da temporada para comparação
        all_players = load_all_players()
        season_avg = {}
        for p in all_players:
            season_avg[p["player_name"]] = p

        def _color_h2h(ps_list):
            """Retorna DataFrame estilizado comparando h2h vs temporada."""
            df = pd.DataFrame(ps_list)
            df.columns = [
                "Jogador",
                "J",
                "Min",
                "Pts",
                "Reb",
                "Ast",
                "Stl",
                "Blk",
                "Tov",
                "FG",
                "FG%",
                "3P",
                "3P%",
                "FT",
                "FT%",
                "+/−",
            ]
            # Mapas: coluna display -> (chave h2h, chave season, lower_is_better)
            compare_map = {
                "Min": ("min", "min", False),
                "Pts": ("pts", "pts", False),
                "Reb": ("reb", "reb", False),
                "Ast": ("ast", "ast", False),
                "Stl": ("stl", "stl", False),
                "Blk": ("blk", "blk", False),
                "Tov": ("tov", "tov", True),
                "FG%": ("fg_pct", "fg_pct", False),
                "3P%": ("fg3_pct", "fg3_pct", False),
                "FT%": ("ft_pct", "ft_pct", False),
            }

            def _style_row(row):
                jogador = row["Jogador"]
                savg = season_avg.get(jogador, {})
                styles = [""] * len(row)
                for i, col in enumerate(row.index):
                    if col not in compare_map or not savg:
                        continue
                    h2h_key, season_key, lower = compare_map[col]
                    h2h_val = (
                        ps_list[row.name][h2h_key] if row.name < len(ps_list) else None
                    )
                    s_val = savg.get(season_key)
                    if h2h_val is None or s_val is None:
                        continue
                    if lower:
                        color = "#2e7d32" if h2h_val <= s_val else "#c62828"
                    else:
                        color = "#2e7d32" if h2h_val >= s_val else "#c62828"
                    styles[i] = f"color: {color}; font-weight: 600"
                return styles

            return df.style.apply(_style_row, axis=1).format(
                {
                    "J": "{:.0f}",
                    "Min": "{:.1f}",
                    "Pts": "{:.1f}",
                    "Reb": "{:.1f}",
                    "Ast": "{:.1f}",
                    "Stl": "{:.1f}",
                    "Blk": "{:.1f}",
                    "Tov": "{:.1f}",
                    "FG%": "{:.1f}",
                    "3P%": "{:.1f}",
                    "FT%": "{:.1f}",
                    "+/−": "{:.0f}",
                }
            )

        st.markdown(
            '<div style="font-size:12px;color:#888;margin-bottom:8px;">'
            '🟢 <span style="color:#2e7d32;font-weight:600;">Verde</span> = acima ou igual à média da temporada &nbsp;&nbsp; '
            '🔴 <span style="color:#c62828;font-weight:600;">Vermelho</span> = abaixo da média da temporada'
            "</div>",
            unsafe_allow_html=True,
        )

        with col_a:
            st.markdown(f"**{ta['nickname']}** — Médias nos confrontos")
            ps_a = fetch_h2h_player_stats(game_ids, ta["id"])
            if ps_a:
                styled_a = _color_h2h(ps_a)
                sel_a = st.dataframe(
                    styled_a,
                    use_container_width=True,
                    hide_index=True,
                    on_select="rerun",
                    selection_mode="single-row",
                    key="h2h_ps_a",
                )
                if sel_a and sel_a.selection and sel_a.selection.rows:
                    _go_to_player(ps_a[sel_a.selection.rows[0]]["jogador"])

        with col_b:
            st.markdown(f"**{tb['nickname']}** — Médias nos confrontos")
            ps_b = fetch_h2h_player_stats(game_ids, tb["id"])
            if ps_b:
                styled_b = _color_h2h(ps_b)
                sel_b = st.dataframe(
                    styled_b,
                    use_container_width=True,
                    hide_index=True,
                    on_select="rerun",
                    selection_mode="single-row",
                    key="h2h_ps_b",
                )
                if sel_b and sel_b.selection and sel_b.selection.rows:
                    _go_to_player(ps_b[sel_b.selection.rows[0]]["jogador"])


# ─── Página Jogadores ─────────────────────────────────────────────────────────


@st.cache_data(ttl=3600)
def _get_players_list():
    return load_players_list()


@st.cache_data(ttl=300)
def _get_player_game_log(player_id: int):
    return fetch_player_game_log(player_id, n=10)


def page_players():
    st.markdown(
        '<div class="section-header">Estatísticas de Jogador</div>',
        unsafe_allow_html=True,
    )

    players = _get_players_list()
    if not players:
        st.warning("Nenhum jogador encontrado no banco.")
        return

    # Seletor com busca por nome
    player_names = [f"{p['player_name']} ({p['team_abbr']})" for p in players]
    default_player_idx = next(
        (i for i, p in enumerate(players) if "Harden" in p["player_name"]), 0
    )

    # Persistir seleção
    if "_player_idx" not in st.session_state:
        st.session_state["_player_idx"] = default_player_idx
    saved_idx = st.session_state["_player_idx"]
    if saved_idx >= len(players):
        saved_idx = default_player_idx

    selected_idx = st.selectbox(
        "Buscar jogador",
        options=range(len(players)),
        format_func=lambda i: player_names[i],
        index=saved_idx,
        key="player_select",
    )
    st.session_state["_player_idx"] = selected_idx
    player_info = players[selected_idx]
    pid = player_info["player_id"]
    pstats = load_player_stats(pid)

    if pstats is None:
        st.error("Jogador não encontrado.")
        return

    # ── Header do jogador ──
    color = get_team_color(pstats["team_abbr"])
    r, g, b = int(color[1:3], 16), int(color[3:5], 16), int(color[5:7], 16)

    st.markdown(
        f"""
    <div style="display:flex;align-items:center;gap:16px;margin-bottom:1.2rem;">
        <div style="width:52px;height:52px;border-radius:50%;background:{color};display:flex;
                    align-items:center;justify-content:center;color:white;font-weight:700;font-size:12px;
                    font-family:'DM Mono',monospace;">
            {pstats['team_abbr']}
        </div>
        <div>
            <div style="font-size:22px;font-weight:600;color:#111;">{pstats['player_name']}</div>
            <div style="font-size:12px;color:#888;">{pstats['team_abbr']} · {pstats['age']:.0f} anos · {pstats['gp']} jogos</div>
        </div>
    </div>
    """,
        unsafe_allow_html=True,
    )

    # ── Métricas principais (apostas) ──
    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("PTS", f"{pstats['pts']}")
    m2.metric("REB", f"{pstats['reb']}")
    m3.metric("AST", f"{pstats['ast']}")
    m4.metric("STL", f"{pstats['stl']}")
    m5.metric("BLK", f"{pstats['blk']}")
    m6.metric("3PM", f"{pstats['fg3m']}")

    # ── Métricas avançadas ──
    a1, a2, a3, a4, a5, a6 = st.columns(6)
    a1.metric("MIN", f"{pstats['min']}")
    a2.metric("FG%", f"{pstats['fg_pct']}%" if pstats["fg_pct"] else "—")
    a3.metric("3P%", f"{pstats['fg3_pct']}%" if pstats["fg3_pct"] else "—")
    a4.metric("FT%", f"{pstats['ft_pct']}%" if pstats["ft_pct"] else "—")
    a5.metric("TS%", f"{pstats['ts_pct']}%" if pstats.get("ts_pct") else "—")
    a6.metric("+/−", f"{pstats['plus_minus']}")

    b1, b2, b3, b4, b5, b6 = st.columns(6)
    b1.metric(
        "USG%", f"{pstats.get('usg_pct', '—')}%" if pstats.get("usg_pct") else "—"
    )
    b2.metric("ORtg", f"{pstats.get('off_rating', '—')}")
    b3.metric("DRtg", f"{pstats.get('def_rating', '—')}")
    b4.metric("NetRtg", f"{pstats.get('net_rating', '—')}")
    b5.metric("TOV", f"{pstats['tov']}")
    b6.metric("PIE", f"{pstats.get('pie', '—')}%" if pstats.get("pie") else "—")

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    # ── Últimos jogos ──
    st.markdown(
        '<div class="section-header">Últimos jogos — linhas de aposta</div>',
        unsafe_allow_html=True,
    )

    with st.spinner("Buscando últimos jogos..."):
        game_log = _get_player_game_log(pid)

    if not game_log:
        st.info("Sem jogos recentes.")
        return

    last5 = game_log[:5]
    last5_rev = list(reversed(last5))

    # Tabela resumo dos últimos 5
    df_log = pd.DataFrame(last5)
    df_display = df_log[
        [
            "date",
            "matchup",
            "wl",
            "min",
            "pts",
            "reb",
            "ast",
            "stl",
            "blk",
            "fg3m",
            "tov",
            "fgm",
            "fga",
            "fg_pct",
            "ftm",
            "fta",
            "plus_minus",
        ]
    ].copy()
    df_display.columns = [
        "Data",
        "Jogo",
        "R",
        "MIN",
        "PTS",
        "REB",
        "AST",
        "STL",
        "BLK",
        "3PM",
        "TOV",
        "FGM",
        "FGA",
        "FG%",
        "FTM",
        "FTA",
        "+/−",
    ]
    df_display["R"] = df_display["R"].map({"W": "✅ V", "L": "❌ D"})
    st.dataframe(df_display, use_container_width=True, hide_index=True)

    # Médias dos últimos 5
    if last5:
        avg = {
            "PTS": round(sum(g["pts"] for g in last5) / len(last5), 1),
            "REB": round(sum(g["reb"] for g in last5) / len(last5), 1),
            "AST": round(sum(g["ast"] for g in last5) / len(last5), 1),
            "STL": round(sum(g["stl"] for g in last5) / len(last5), 1),
            "BLK": round(sum(g["blk"] for g in last5) / len(last5), 1),
            "3PM": round(sum(g["fg3m"] for g in last5) / len(last5), 1),
            "TOV": round(sum(g["tov"] for g in last5) / len(last5), 1),
            "MIN": round(sum(g["min"] for g in last5) / len(last5), 1),
        }
        st.markdown(
            '<div class="section-header">Médias dos últimos 5 jogos</div>',
            unsafe_allow_html=True,
        )
        av1, av2, av3, av4, av5, av6, av7, av8 = st.columns(8)
        av1.metric("PTS", avg["PTS"])
        av2.metric("REB", avg["REB"])
        av3.metric("AST", avg["AST"])
        av4.metric("STL", avg["STL"])
        av5.metric("BLK", avg["BLK"])
        av6.metric("3PM", avg["3PM"])
        av7.metric("TOV", avg["TOV"])
        av8.metric("MIN", avg["MIN"])

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    # ── Gráficos de tendência (últimos 10 jogos) ──
    all_games_rev = list(reversed(game_log))
    dates = [g["date"] for g in all_games_rev]

    def trend_chart(values, title, avg_val, season_avg):
        fig, ax = plt.subplots(figsize=(6, 2.8))
        palette = ["#2e7d32" if v >= avg_val else "#c62828" for v in values]
        bars = ax.bar(
            range(len(values)),
            values,
            color=palette,
            width=0.65,
            edgecolor="white",
            linewidth=0.5,
            zorder=3,
        )
        for bar, v in zip(bars, values):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + 0.15,
                str(v),
                ha="center",
                va="bottom",
                fontsize=8,
                fontfamily="monospace",
                fontweight="bold",
                color="#333",
            )

        ax.axhline(
            season_avg,
            color="#1565C0",
            linewidth=2,
            linestyle="--",
            zorder=4,
            label=f"x̄: {season_avg}",
        )
        if avg_val != season_avg:
            ax.axhline(
                avg_val,
                color="#E65100",
                linewidth=2,
                linestyle=":",
                zorder=4,
                label=f"L5: {avg_val}",
            )

        ax.set_xticks(range(len(dates)))
        ax.set_xticklabels(
            dates, fontsize=7, fontfamily="monospace", rotation=35, ha="right"
        )
        ax.set_title(title, fontsize=11, fontweight="600", pad=8)
        ax.set_ylim(0, max(values) * 1.25 if max(values) > 0 else 1)
        ax.spines[["top", "right"]].set_visible(False)
        ax.spines[["left", "bottom"]].set_color("#ddd")
        ax.tick_params(axis="y", labelsize=8, colors="#888")
        ax.tick_params(axis="x", colors="#888")
        ax.set_facecolor("white")
        fig.patch.set_facecolor("white")
        ax.grid(axis="y", color="#f0f0f0", linewidth=0.5, zorder=0)
        ax.legend(
            fontsize=8,
            loc="upper left",
            framealpha=0.9,
            edgecolor="#ddd",
            fancybox=True,
        )
        fig.tight_layout()
        return fig

    st.markdown(
        '<div class="section-header">Tendência — últimos 10 jogos</div>',
        unsafe_allow_html=True,
    )

    tc1, tc2 = st.columns(2)
    with tc1:
        pts_vals = [g["pts"] for g in all_games_rev]
        avg5_pts = round(sum(g["pts"] for g in last5) / max(len(last5), 1), 1)
        st.pyplot(
            trend_chart(pts_vals, "Pontos", avg5_pts, pstats["pts"]),
        )
    with tc2:
        reb_vals = [g["reb"] for g in all_games_rev]
        avg5_reb = round(sum(g["reb"] for g in last5) / max(len(last5), 1), 1)
        st.pyplot(
            trend_chart(reb_vals, "Rebotes", avg5_reb, pstats["reb"]),
        )

    tc3, tc4 = st.columns(2)
    with tc3:
        ast_vals = [g["ast"] for g in all_games_rev]
        avg5_ast = round(sum(g["ast"] for g in last5) / max(len(last5), 1), 1)
        st.pyplot(
            trend_chart(ast_vals, "Assistências", avg5_ast, pstats["ast"]),
        )
    with tc4:
        fg3_vals = [g["fg3m"] for g in all_games_rev]
        avg5_fg3 = round(sum(g["fg3m"] for g in last5) / max(len(last5), 1), 1)
        st.pyplot(
            trend_chart(fg3_vals, "Bolas de 3", avg5_fg3, pstats["fg3m"]),
        )

    tc5, tc6 = st.columns(2)
    with tc5:
        stl_vals = [g["stl"] for g in all_games_rev]
        avg5_stl = round(sum(g["stl"] for g in last5) / max(len(last5), 1), 1)
        st.pyplot(
            trend_chart(stl_vals, "Roubos", avg5_stl, pstats["stl"]),
        )
    with tc6:
        blk_vals = [g["blk"] for g in all_games_rev]
        avg5_blk = round(sum(g["blk"] for g in last5) / max(len(last5), 1), 1)
        st.pyplot(
            trend_chart(blk_vals, "Bloqueios", avg5_blk, pstats["blk"]),
        )

    # ── Tabela geral de jogadores ──
    st.markdown("<div style='height:24px'></div>", unsafe_allow_html=True)
    st.markdown(
        '<div class="section-header">Tabela geral — todos os jogadores</div>',
        unsafe_allow_html=True,
    )

    @st.cache_data(ttl=3600)
    def _all_players_df():
        all_p = load_all_players()
        rows = []
        for p in all_p:
            rows.append(
                {
                    "Jogador": p["player_name"],
                    "Time": p["team_abbr"],
                    "Idade": p.get("age"),
                    "GP": p["gp"],
                    "MIN": p["min"],
                    "PTS": p["pts"],
                    "REB": p["reb"],
                    "AST": p["ast"],
                    "STL": p["stl"],
                    "BLK": p["blk"],
                    "TOV": p["tov"],
                    "3PM": p["fg3m"],
                    "FG%": p["fg_pct"],
                    "3P%": p["fg3_pct"],
                    "FT%": p["ft_pct"],
                    "+/−": p["plus_minus"],
                    "TS%": p.get("ts_pct"),
                    "USG%": p.get("usg_pct"),
                    "ORtg": p.get("off_rating"),
                    "DRtg": p.get("def_rating"),
                    "NetRtg": p.get("net_rating"),
                    "PIE": p.get("pie"),
                }
            )
        return pd.DataFrame(rows)

    df_all = _all_players_df()
    st.dataframe(df_all, use_container_width=True, hide_index=True, height=600)


# ─── Main ─────────────────────────────────────────────────────────────────────


def main():
    cache = load_cache()
    team, page = render_sidebar(cache)

    if page == "Visão geral":
        page_overview(team, cache["teams"])
    elif page == "Comparativo da Liga":
        page_stats(team, cache["teams"])
    elif page == "Últimos jogos":
        page_games(team)
    elif page == "Confronto direto":
        page_comparison(cache["teams"])
    elif page == "Jogadores":
        page_players()


if __name__ == "__main__":
    main()

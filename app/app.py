"""
app.py
Oil Flow Map — Global Crude Oil Supply & Risk Intelligence
Streamlit application entry point.

Run from project root:
    streamlit run app/app.py
"""

import os
from datetime import timedelta

import streamlit as st
import pandas as pd
import pydeck as pdk
import plotly.graph_objects as go
from dotenv import load_dotenv
from neo4j import GraphDatabase

load_dotenv()

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Oil Flow Map",
    page_icon="🛢️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    /* Tab highlight colour — arc blue */
    .stTabs [data-baseweb="tab-highlight"] {
        background-color: #3d9eff;
    }
    .stTabs [data-baseweb="tab"] {
        color: rgba(255,255,255,0.6);
    }
    .stTabs [aria-selected="true"] {
        color: #3d9eff;
    }
    /* Remove top padding */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
</style>
""", unsafe_allow_html=True)

# ── Neo4j connection ──────────────────────────────────────────────────────────
@st.cache_resource
def get_driver():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(os.getenv("NEO4J_USERNAME"), os.getenv("NEO4J_PASSWORD")),
    )


# ── Data loaders ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_chokepoints():
    driver = get_driver()
    with driver.session() as session:
        result = session.run("""
            MATCH (c:Chokepoint)
            RETURN c.id                          AS id,
                   c.name                        AS name,
                   c.lat                         AS lat,
                   c.lon                         AS lon,
                   c.risk_score                  AS risk_score,
                   c.static_vulnerability_score  AS static_vulnerability_score,
                   c.sentiment_score             AS sentiment_score,
                   c.flow_mbpd                   AS flow_mbpd,
                   c.instability                 AS instability
        """)
        return pd.DataFrame([dict(r) for r in result])


@st.cache_data(ttl=3600)
def load_pipelines():
    driver = get_driver()
    with driver.session() as session:
        result = session.run("""
            MATCH (p:Pipeline)
            RETURN p.id                          AS id,
                   p.name                        AS name,
                   p.lat                         AS lat,
                   p.lon                         AS lon,
                   p.risk_score                  AS risk_score,
                   p.static_vulnerability_score  AS static_vulnerability_score,
                   p.sentiment_score             AS sentiment_score,
                   p.instability                 AS instability
        """)
        return pd.DataFrame([dict(r) for r in result])


@st.cache_data(ttl=3600)
def load_flows(year=2024):
    driver = get_driver()
    with driver.session() as session:
        result = session.run("""
            MATCH (from:Region)-[f:FLOW]->(to:Region)
            WHERE f.year = $year
            RETURN from.id   AS from_id,
                   from.name AS from_name,
                   from.lat  AS from_lat,
                   from.lon  AS from_lon,
                   to.id     AS to_id,
                   to.name   AS to_name,
                   to.lat    AS to_lat,
                   to.lon    AS to_lon,
                   f.volume_mt AS volume_mt
        """, year=year)
        return pd.DataFrame([dict(r) for r in result])


@st.cache_data(ttl=3600)
def load_regions():
    driver = get_driver()
    with driver.session() as session:
        result = session.run("""
            MATCH (r:Region)
            RETURN r.id   AS id,
                   r.name AS name,
                   r.lat  AS lat,
                   r.lon  AS lon
        """)
        return pd.DataFrame([dict(r) for r in result])


@st.cache_data(ttl=3600)
def load_price_forecast():
    driver = get_driver()
    with driver.session() as session:
        result = session.run("""
            MATCH (pf:PriceForecast)
            RETURN pf.date           AS date,
                   pf.price_forecast AS price_forecast,
                   pf.price_lower    AS price_lower,
                   pf.price_upper    AS price_upper
            ORDER BY pf.date
        """)
        return pd.DataFrame([dict(r) for r in result])


@st.cache_data(ttl=3600)
def load_prices():
    df = pd.read_csv("data/raw/eia_prices_raw.csv", parse_dates=["period"])
    df = df[df["series"] == "RBRTE"].copy()
    df = df.rename(columns={"period": "date", "value": "price"})
    df = df[["date", "price"]].sort_values("date").reset_index(drop=True)
    return df


# ── Helper functions ──────────────────────────────────────────────────────────
def risk_to_color(risk_score, alpha=200):
    if risk_score is None:
        return [150, 150, 150, alpha]
    try:
        risk_score = float(risk_score)
    except (ValueError, TypeError):
        return [150, 150, 150, alpha]
    r = int(min(255, risk_score * 2 * 255))
    g = int(min(255, (1 - risk_score) * 2 * 255))
    return [r, g, 30, alpha]


def arc_color(volume_mt, max_vol):
    intensity = int(80 + (volume_mt / max_vol) * 175)
    return [30, intensity, 255, 180]


def build_region_stats(flows_df, regions_df):
    exports = flows_df.groupby("from_id")["volume_mt"].sum().reset_index()
    exports.columns = ["id", "total_exports_mt"]
    imports = flows_df.groupby("to_id")["volume_mt"].sum().reset_index()
    imports.columns = ["id", "total_imports_mt"]

    region_stats = regions_df.merge(exports, on="id", how="left")
    region_stats = region_stats.merge(imports, on="id", how="left")
    region_stats["total_exports_mt"] = region_stats["total_exports_mt"].fillna(0)
    region_stats["total_imports_mt"] = region_stats["total_imports_mt"].fillna(0)

    region_stats["tooltip"] = (
        "<br/>Exports: " + region_stats["total_exports_mt"].round(1).astype(str) + " Mt<br/>" +
        "Imports: " + region_stats["total_imports_mt"].round(1).astype(str) + " Mt"
    )
    region_stats["name"]         = "Region: " + region_stats["name"]
    region_stats["risk_pct"]     = ""
    region_stats["from_name"]    = ""
    region_stats["to_name"]      = ""
    region_stats["volume_label"] = ""
    return region_stats


def build_price_chart(prices_df, forecast_df):
    cutoff = prices_df["date"].max() - timedelta(days=180)
    hist   = prices_df[prices_df["date"] >= cutoff].copy()
    fcast  = forecast_df.copy()
    fcast["date"] = pd.to_datetime(fcast["date"])

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=pd.concat([fcast["date"], fcast["date"].iloc[::-1]]),
        y=pd.concat([fcast["price_upper"], fcast["price_lower"].iloc[::-1]]),
        fill="toself",
        fillcolor="rgba(61, 158, 255, 0.15)",
        line=dict(color="rgba(255,255,255,0)"),
        name="80% confidence",
        hoverinfo="skip",
    ))

    fig.add_trace(go.Scatter(
        x=fcast["date"],
        y=fcast["price_forecast"],
        mode="lines",
        line=dict(color="#3d9eff", width=1.5, dash="dot"),
        name="Forecast",
        hovertemplate="%{x|%b %d, %Y}:  $%{y:.1f}<extra></extra>",
        hoverlabel=dict(
            bgcolor="#1a1a2e",
            font=dict(color="white", size=12),
            bordercolor="#3d9eff",
        ),
    ))

    fig.add_trace(go.Scatter(
        x=hist["date"],
        y=hist["price"],
        mode="lines",
        line=dict(color="#ffffff", width=1.5),
        name="Brent (actual)",
        hovertemplate="%{x|%b %d, %Y}:  $%{y:.1f}<extra></extra>",
        hoverlabel=dict(
            bgcolor="#1a1a2e",
            font=dict(color="white", size=12),
            bordercolor="#3d9eff",
        ),
    ))

    latest = hist.iloc[-1]
    fig.add_trace(go.Scatter(
        x=[latest["date"]],
        y=[latest["price"]],
        mode="markers+text",
        marker=dict(color="#ffffff", size=8),
        text=[f"  ${latest['price']:.1f}"],
        textposition="middle right",
        textfont=dict(color="white", size=11),
        name="Current",
        showlegend=False,
        hoverinfo="skip",
    ))

    fig.update_layout(
        height=200,
        margin=dict(l=0, r=10, t=30, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="white", size=11),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
            font=dict(size=10),
        ),
        xaxis=dict(showgrid=False, color="rgba(255,255,255,0.4)"),
        yaxis=dict(
            showgrid=True,
            gridcolor="rgba(255,255,255,0.08)",
            color="rgba(255,255,255,0.4)",
            tickprefix="$",
        ),
        hoverlabel=dict(
            bgcolor="#1a1a2e",
            font_size=12,
            font_color="white",
            bordercolor="#3d9eff",
        ),
    )
    return fig


# ── Load base data ────────────────────────────────────────────────────────────
with st.spinner("Loading data..."):
    chokepoints = load_chokepoints()
    pipelines   = load_pipelines()
    regions     = load_regions()
    forecast    = load_price_forecast()
    prices      = load_prices()

# ── Main layout columns ───────────────────────────────────────────────────────
map_col, panel_col = st.columns([2, 1], gap="large")

with map_col:
    # Title and legend
    st.markdown("#### Global Crude Oil Flows & Strategic Vulnerability")
    st.markdown("<p style='text-align: center; color: rgba(255,255,255,0.5); font-size: 0.85rem;'>● Chokepoints — Strategic Vulnerability Index &nbsp;&nbsp; ○ Pipelines — land infrastructure &nbsp;&nbsp; ◎ Regions — hover for trade volumes</p>", unsafe_allow_html=True)

    # Controls directly above map
    ctrl1, ctrl2, ctrl3 = st.columns(3)
    with ctrl1:
        selected_year = st.selectbox(
            "Flow year",
            options=[2021, 2022, 2023, 2024],
            index=3,
        )
    with ctrl2:
        region_options = ["— None —"] + sorted(regions["name"].dropna().tolist())
        selected_region = st.selectbox(
            "Show flows for region",
            options=region_options,
            index=0,
        )
    with ctrl3:
        flow_direction = st.selectbox(
            "Flow direction",
            options=["Both", "Imports only", "Exports only"],
        )

    # ── Load and filter flows ─────────────────────────────────────────────────
    flows_year   = load_flows(selected_year)
    region_stats = build_region_stats(flows_year, regions)

    show_arcs = selected_region != "— None —"
    if show_arcs:
        region_name = selected_region
        if flow_direction == "Imports only":
            direction_filter = flows_year["to_name"] == region_name
        elif flow_direction == "Exports only":
            direction_filter = flows_year["from_name"] == region_name
        else:
            direction_filter = (
                (flows_year["from_name"] == region_name) |
                (flows_year["to_name"]   == region_name)
            )
        flows_display = flows_year[
            (flows_year["volume_mt"] >= st.session_state.get("min_flow", 25)) & direction_filter
        ].copy()
    else:
        flows_display = pd.DataFrame()

    # ── Prepare map data ──────────────────────────────────────────────────────
    chk_map = chokepoints.copy()
    chk_map["color"]        = chk_map["risk_score"].apply(risk_to_color)
    chk_map["risk_pct"]     = "<br/>Strategic Vulnerability Index: " + (chk_map["risk_score"] * 100).round(1).astype(str) + "%"
    chk_map["radius"]       = (chk_map["flow_mbpd"].fillna(0) * 15000 + 80000).astype(int)
    chk_map["from_name"]    = ""
    chk_map["to_name"]      = ""
    chk_map["volume_label"] = ""
    chk_map["tooltip"]      = ""

    pipe_map = pipelines.copy()
    pipe_map["color"]        = [[40, 40, 40, 30]] * len(pipe_map)
    pipe_map["risk_pct"]     = ""
    pipe_map["from_name"]    = ""
    pipe_map["to_name"]      = ""
    pipe_map["volume_label"] = ""
    pipe_map["tooltip"]      = ""

    if show_arcs and not flows_display.empty:
        max_vol = flows_display["volume_mt"].max()
        flows_display["color"]        = flows_display["volume_mt"].apply(lambda v: arc_color(v, max_vol))
        flows_display["width"]        = (flows_display["volume_mt"] / max_vol * 6 + 1).round(1)
        flows_display["volume_label"] = flows_display["volume_mt"].round(1).astype(str) + " Mt"
        flows_display["risk_pct"]     = ""
        flows_display["tooltip"]      = ""
        flows_display["name"]         = ""
        flows_display["to_name"]      = " → " + flows_display["to_name"] + ":"

    # ── Pydeck layers ─────────────────────────────────────────────────────────
    region_layer = pdk.Layer(
        "ScatterplotLayer",
        data=region_stats,
        get_position=["lon", "lat"],
        get_fill_color=[100, 140, 200, 60],
        get_line_color=[100, 140, 200, 150],
        get_radius=120000,
        pickable=True,
        filled=True,
        stroked=True,
        line_width_min_pixels=1,
    )

    pipeline_layer = pdk.Layer(
        "ScatterplotLayer",
        data=pipe_map,
        get_position=["lon", "lat"],
        get_fill_color=[40, 40, 40, 30],
        get_line_color=[200, 200, 200, 220],
        get_radius=40000,
        pickable=True,
        filled=True,
        stroked=True,
        line_width_min_pixels=2,
    )

    chokepoint_layer = pdk.Layer(
        "ScatterplotLayer",
        data=chk_map,
        get_position=["lon", "lat"],
        get_fill_color="color",
        get_radius="radius",
        pickable=True,
        opacity=0.85,
        stroked=True,
        get_line_color=[255, 255, 255, 80],
        line_width_min_pixels=1,
    )

    layers = [region_layer, pipeline_layer, chokepoint_layer]

    if show_arcs and not flows_display.empty:
        arc_layer = pdk.Layer(
            "ArcLayer",
            data=flows_display,
            get_source_position=["from_lon", "from_lat"],
            get_target_position=["to_lon", "to_lat"],
            get_source_color="color",
            get_target_color="color",
            get_width="width",
            get_height=0.1,
            pickable=True,
            auto_highlight=True,
        )
        layers = [arc_layer, region_layer, pipeline_layer, chokepoint_layer]

    if show_arcs and not region_stats.empty:
        region_row = region_stats[region_stats["name"] == "Region: " + selected_region]
        if not region_row.empty:
            centre_lat = float(region_row.iloc[0]["lat"])
            centre_lon = float(region_row.iloc[0]["lon"])
            centre_zoom = 2.0
        else:
            centre_lat, centre_lon, centre_zoom = 30.0, 50.0, 2.0
    else:
        centre_lat, centre_lon, centre_zoom = 30.0, 50.0, 2.0

    view_state = pdk.ViewState(
        latitude=centre_lat,
        longitude=centre_lon,
        zoom=centre_zoom,
        pitch=0,
    )

    tooltip = {
        "html": "<b>{name}</b>{risk_pct}{tooltip}<b>{from_name}</b>{to_name}<br/>{volume_label}",
        "style": {
            "backgroundColor": "#1a1a2e",
            "color": "white",
            "fontSize": "13px",
            "padding": "8px",
            "borderRadius": "4px",
        },
    }

    deck = pdk.Deck(
        layers=layers,
        initial_view_state=view_state,
        tooltip=tooltip,
        map_style="https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
    )

    st.pydeck_chart(deck, use_container_width=True, height=360)

    min_flow = st.select_slider(
        "Min flow (Mt/year)",
        options=[0, 25, 50, 75, 100],
        value=st.session_state.get("min_flow", 0),
        key="min_flow",
    )

with panel_col:
    # Header levelled to dropdowns
    st.markdown("#### Market & Trade")
    st.caption("Brent crude front-month futures:<br/>6 month history + 90-day forecast", unsafe_allow_html=True)

    # Price chart — levelled to map top
    fig = build_price_chart(prices, forecast)
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

    st.markdown("<hr style='margin: 0.5rem 0; border-color: rgba(255,255,255,0.1);'>", unsafe_allow_html=True)

    # Trade tabs — levelled to slider bottom

    exports_agg = flows_year.groupby("from_name")["volume_mt"].sum().reset_index()
    exports_agg.columns = ["Region", "Volume (Mt)"]
    exports_agg = exports_agg.sort_values("Volume (Mt)", ascending=False).head(5)
    exports_agg["Volume (Mt)"] = exports_agg["Volume (Mt)"].round(1)

    imports_agg = flows_year.groupby("to_name")["volume_mt"].sum().reset_index()
    imports_agg.columns = ["Region", "Volume (Mt)"]
    imports_agg = imports_agg.sort_values("Volume (Mt)", ascending=False).head(5)
    imports_agg["Volume (Mt)"] = imports_agg["Volume (Mt)"].round(1)

    tab_exp, tab_imp = st.tabs([f"Top Exporters {selected_year}", f"Top Importers {selected_year}"])

    with tab_exp:
        st.dataframe(
            exports_agg.reset_index(drop=True),
            use_container_width=True,
            hide_index=True,
        )

    with tab_imp:
        st.dataframe(
            imports_agg.reset_index(drop=True),
            use_container_width=True,
            hide_index=True,
        )

# ── Methodology expander ──────────────────────────────────────────────────────
with st.expander("Data & Methodology"):
    st.markdown("""
**Strategic Vulnerability Index** — weighted composite of annual crude oil transit volume (Mt, 80%) 
and regional political instability (World Bank WGI, inverted, 20%), adjusted daily by news sentiment (20% weight). 
Measures consequence severity if disruption occurs, not probability of disruption.

**Crude Oil Flows** — bilateral crude trade volumes (Mt/year) from Energy Institute / BP Statistical Review 2021–2024. Regional aggregates.

**Price Forecast** — Brent front-month futures (Yahoo Finance). Facebook Prophet baseline under stable market conditions. Cannot predict geopolitical shocks.

**Data Sources:** Energy Institute Statistical Review · BP Statistical Review · World Bank WGI · Yahoo Finance · NewsData.io
    """)
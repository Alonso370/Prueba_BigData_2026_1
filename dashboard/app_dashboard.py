import streamlit as st
import pandas as pd
import plotly.express as px
from pymongo import MongoClient
from cassandra.cluster import Cluster
from neo4j import GraphDatabase

# Configuración de la página de Streamlit
st.set_page_config(page_title="Spotify Big Data Analytics - UTEC", layout="wide")
st.title("Spotify Music Analytics Dashboard (2015–2025)")
st.caption("Dashboard conectado a MongoDB, Cassandra y Neo4j")
st.markdown("---")


# =========================
# FUNCIONES AUXILIARES
# =========================
def format_number(value, decimals=0):
    """Formatea números para tarjetas KPI sin romper si el dato viene vacío."""
    try:
        if value is None:
            return "0"
        value = float(value)
        if decimals == 0:
            return f"{value:,.0f}"
        return f"{value:,.{decimals}f}"
    except Exception:
        return "0"


def format_percent(value, decimals=2):
    """Formatea valores porcentuales guardados como 0-100."""
    return f"{format_number(value, decimals)}%"


def prepare_cassandra_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza tipos y deriva columnas útiles para gráficos dinámicos."""
    if df.empty:
        return df

    prepared = df.copy()
    prepared["stream_count"] = pd.to_numeric(prepared["stream_count"], errors="coerce").fillna(0)
    prepared["popularity"] = pd.to_numeric(prepared["popularity"], errors="coerce").fillna(0)
    prepared["release_year"] = (
        prepared["release_date"]
        .astype(str)
        .str.extract(r"(\d{4})", expand=False)
        .pipe(pd.to_numeric, errors="coerce")
    )
    return prepared


def apply_stream_filters(
    df: pd.DataFrame,
    countries: list[str],
    genres: list[str],
    pop_range: tuple[int, int],
    min_streams: int,
    year_range: tuple[int, int] | None,
) -> pd.DataFrame:
    """Aplica filtros del sidebar sobre el dataset de Cassandra."""
    if df.empty:
        return df

    filtered = df.copy()

    if countries:
        filtered = filtered[filtered["country"].isin(countries)]
    if genres:
        filtered = filtered[filtered["genre"].isin(genres)]

    filtered = filtered[
        (filtered["popularity"] >= pop_range[0])
        & (filtered["popularity"] <= pop_range[1])
        & (filtered["stream_count"] >= min_streams)
    ]

    if year_range is not None:
        filtered = filtered[
            filtered["release_year"].between(year_range[0], year_range[1], inclusive="both")
        ]

    return filtered


def plotly_layout(title: str) -> dict:
    """Layout consistente para todos los gráficos Plotly."""
    return dict(
        title=title,
        template="plotly_dark",
        margin=dict(l=40, r=20, t=50, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )


# =========================
# EXTRACCIÓN DE KPIS DESDE MONGODB
# =========================
@st.cache_data(ttl=10)
def fetch_current_kpis():
    """Lee el documento actualizado por el DAG en MongoDB.

    El DAG actualiza siempre el mismo documento:
    spotify_db.kpis / _id = spotify_kpis_current
    """
    try:
        client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=2000)
        db = client["spotify_db"]
        kpis = db["kpis"].find_one({"_id": "spotify_kpis_current"})
        client.close()
        return kpis
    except Exception as e:
        st.sidebar.error(f"Error leyendo KPIs desde MongoDB: {e}")
        return None


# =========================
# EXTRACCIÓN DE DATOS PARA GRÁFICOS
# =========================
@st.cache_data(ttl=10)
def fetch_cassandra_data():
    """Extrae datos estructurados desde Cassandra para visualizaciones."""
    try:
        cluster = Cluster(["127.0.0.1"], port=9042)
        session = cluster.connect("spotify_analytics")

        query = """
            SELECT country, genre, stream_count, popularity,
                   track_name, artist_name, release_date
            FROM streams_by_country_genre;
        """
        rows = session.execute(query)
        df = prepare_cassandra_df(pd.DataFrame(list(rows)))
        cluster.shutdown()
        return df
    except Exception as e:
        st.sidebar.error(f"Error Cassandra: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=10)
def fetch_neo4j_top_artists(limit: int = 10):
    """Consulta el grafo para artistas con más canciones relacionadas."""
    try:
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password123"))
        query = """
        MATCH (a:Artist)-[:PERFORMS]->(t:Track)
        WHERE a.name IS NOT NULL
          AND a.name <> ''
          AND a.name <> 'Unknown Artist'
        RETURN a.name AS artist, count(t) AS total_tracks
        ORDER BY total_tracks DESC
        LIMIT $limit
        """
        with driver.session() as session:
            result = session.run(query, limit=int(limit))
            df = pd.DataFrame([dict(record) for record in result])
        driver.close()
        return df
    except Exception as e:
        st.sidebar.error(f"Error Neo4j: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=10)
def fetch_neo4j_genre_network(limit: int = 15):
    """Relaciones artista-género para visualización de red."""
    try:
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password123"))
        query = """
        MATCH (a:Artist)-[:PERFORMS]->(t:Track)-[:BELONGS_TO_GENRE]->(g:Genre)
        WHERE a.name IS NOT NULL AND a.name <> 'Unknown Artist'
        RETURN a.name AS artist, g.name AS genre, count(t) AS tracks
        ORDER BY tracks DESC
        LIMIT $limit
        """
        with driver.session() as session:
            result = session.run(query, limit=int(limit))
            df = pd.DataFrame([dict(record) for record in result])
        driver.close()
        return df
    except Exception as e:
        st.sidebar.error(f"Error Neo4j (red): {e}")
        return pd.DataFrame()


# =========================
# SIDEBAR — FILTROS DINÁMICOS
# =========================
with st.sidebar:
    st.header("Controles")
    if st.button("Actualizar datos", icon=":material/refresh:", width="stretch"):
        st.cache_data.clear()
        st.rerun()

    st.divider()
    st.subheader("Filtros analíticos")

    df_cassandra_raw = fetch_cassandra_data()

    if df_cassandra_raw.empty:
        st.info("Sin datos en Cassandra. Ejecuta el pipeline para habilitar filtros.")
        selected_countries = []
        selected_genres = []
        pop_range = (0, 100)
        min_streams = 0
        year_range = None
        top_n = 10
        chart_metric = "Streams"
    else:
        all_countries = sorted(df_cassandra_raw["country"].dropna().unique())
        all_genres = sorted(df_cassandra_raw["genre"].dropna().unique())
        pop_min = int(df_cassandra_raw["popularity"].min())
        pop_max = int(df_cassandra_raw["popularity"].max())
        stream_min = int(df_cassandra_raw["stream_count"].min())
        stream_max = int(df_cassandra_raw["stream_count"].max())
        valid_years = df_cassandra_raw["release_year"].dropna()
        has_years = not valid_years.empty

        selected_countries = st.multiselect(
            "Países",
            options=all_countries,
            placeholder="Todos los países",
        )
        selected_genres = st.multiselect(
            "Géneros",
            options=all_genres,
            placeholder="Todos los géneros",
        )
        pop_range = st.slider(
            "Rango de popularidad",
            min_value=pop_min,
            max_value=pop_max,
            value=(pop_min, pop_max),
        )
        min_streams = st.slider(
            "Streams mínimos",
            min_value=stream_min,
            max_value=stream_max,
            value=stream_min,
            step=max(1, (stream_max - stream_min) // 100 or 1),
        )

        if has_years:
            year_min = int(valid_years.min())
            year_max = int(valid_years.max())
            year_range = st.slider(
                "Año de lanzamiento",
                min_value=year_min,
                max_value=year_max,
                value=(year_min, year_max),
            )
        else:
            year_range = None

        top_n = st.slider("Top N en rankings", min_value=5, max_value=25, value=10, step=1)
        chart_metric = st.segmented_control(
            "Métrica principal",
            options=["Streams", "Popularidad"],
            default="Streams",
        )

    st.divider()
    st.subheader("Neo4j")
    neo4j_limit = st.slider("Top artistas en grafo", min_value=5, max_value=30, value=10, step=1)


# =========================
# CARGA DE DATOS
# =========================
kpis = fetch_current_kpis()
df_cassandra = apply_stream_filters(
    df_cassandra_raw,
    selected_countries,
    selected_genres,
    pop_range,
    min_streams,
    year_range,
)
df_neo4j = fetch_neo4j_top_artists(neo4j_limit)
df_neo4j_network = fetch_neo4j_genre_network(neo4j_limit * 2)


# =========================
# SECCIÓN PRINCIPAL DE KPIS
# =========================
st.subheader("KPIs ejecutivos generados automáticamente por Airflow")

if not kpis:
    st.warning(
        "Todavía no existe el documento de KPIs en MongoDB. "
        "Ejecuta el DAG `pipeline_spotify_multimodelo` y verifica que la tarea "
        "`generate_kpis` haya finalizado correctamente."
    )
else:
    fecha_actualizacion = kpis.get("fecha_actualizacion", "Sin fecha")
    total_canciones = kpis.get("total_canciones_unicas", 0)
    total_streams = kpis.get("total_streams", 0)

    st.caption(
        f"Última actualización: {fecha_actualizacion} | "
        f"Canciones únicas analizadas: {format_number(total_canciones)} | "
        f"Streams acumulados: {format_number(total_streams)}"
    )

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            label="Concentración Top 10 Artistas",
            value=format_percent(
                kpis.get("kpi_01_concentracion_streams_top10_artistas_pct"),
                2,
            ),
        )
        st.caption(f"Streams Top 10: {format_number(kpis.get('streams_top10_artistas'))}")

    with col2:
        st.metric(
            label="Participación del Género Líder",
            value=format_percent(
                kpis.get("kpi_02_participacion_genero_lider_pct"),
                2,
            ),
        )
        st.caption(f"Género líder: {kpis.get('genero_lider_nombre', 'Sin datos')}")

    with col3:
        st.metric(
            label="Participación del País Líder",
            value=format_percent(
                kpis.get("kpi_03_participacion_pais_lider_pct"),
                2,
            ),
        )
        st.caption(f"País líder: {kpis.get('pais_lider_nombre', 'Sin datos')}")

    col4, col5, col6 = st.columns(3)

    with col4:
        st.metric(
            label="Canciones Exitosas",
            value=format_percent(kpis.get("kpi_04_porcentaje_canciones_exitosas"), 2),
        )
        st.caption("Popularity ≥ 80")

    with col5:
        st.metric(
            label="Canciones Populares",
            value=format_percent(kpis.get("kpi_05_porcentaje_canciones_populares"), 2),
        )
        st.caption("Más de 100 millones de streams")

    with col6:
        st.metric(
            label="Edad Promedio Top 100",
            value=f"{format_number(kpis.get('kpi_06_edad_promedio_top100_mas_escuchado'), 2)} años",
        )
        st.caption("Top 100 canciones con más streams")

    with st.expander("Ver detalle técnico del documento KPI"):
        st.json(
            {
                "_id": kpis.get("_id"),
                "fecha_actualizacion": str(kpis.get("fecha_actualizacion")),
                "total_canciones_procesadas": kpis.get("total_canciones_procesadas"),
                "total_canciones_unicas": kpis.get("total_canciones_unicas"),
                "total_streams": kpis.get("total_streams"),
                "kpi_01_concentracion_streams_top10_artistas_pct": kpis.get(
                    "kpi_01_concentracion_streams_top10_artistas_pct"
                ),
                "streams_top10_artistas": kpis.get("streams_top10_artistas"),
                "top10_artistas": kpis.get("top10_artistas"),
                "kpi_02_participacion_genero_lider_pct": kpis.get(
                    "kpi_02_participacion_genero_lider_pct"
                ),
                "genero_lider_nombre": kpis.get("genero_lider_nombre"),
                "streams_genero_lider": kpis.get("streams_genero_lider"),
                "kpi_03_participacion_pais_lider_pct": kpis.get(
                    "kpi_03_participacion_pais_lider_pct"
                ),
                "pais_lider_nombre": kpis.get("pais_lider_nombre"),
                "streams_pais_lider": kpis.get("streams_pais_lider"),
                "kpi_04_porcentaje_canciones_exitosas": kpis.get(
                    "kpi_04_porcentaje_canciones_exitosas"
                ),
                "canciones_exitosas": kpis.get("canciones_exitosas"),
                "kpi_05_porcentaje_canciones_populares": kpis.get(
                    "kpi_05_porcentaje_canciones_populares"
                ),
                "canciones_populares": kpis.get("canciones_populares"),
                "kpi_06_edad_promedio_top100_mas_escuchado": kpis.get(
                    "kpi_06_edad_promedio_top100_mas_escuchado"
                ),
                "fecha_referencia_edad": kpis.get("fecha_referencia_edad"),
            }
        )

st.markdown("---")


# =========================
# GRÁFICOS ANALÍTICOS DINÁMICOS
# =========================
@st.fragment
def render_dynamic_charts(df: pd.DataFrame, metric: str, n: int):
    st.subheader("Visualizaciones analíticas")

    if df.empty:
        st.info(
            "No hay registros con los filtros actuales. "
            "Amplía países, géneros o reduce el umbral de streams."
        )
        return

    metric_col = "stream_count" if metric == "Streams" else "popularity"
    metric_label = "Reproducciones" if metric == "Streams" else "Popularidad"
    agg_fn = "sum" if metric == "Streams" else "mean"

    st.caption(
        f"{len(df):,} registros | "
        f"{df['country'].nunique()} países | "
        f"{df['genre'].nunique()} géneros"
    )

    tab_rankings, tab_distribution, tab_trends, tab_detail = st.tabs(
        ["Rankings", "Distribución", "Tendencias", "Detalle"]
    )

    with tab_rankings:
        left_col, right_col = st.columns(2)

        with left_col:
            with st.container(border=True):
                df_country = (
                    df.groupby("country", as_index=False)[metric_col]
                    .agg(agg_fn)
                    .sort_values(metric_col, ascending=False)
                    .head(n)
                )
                fig_country = px.bar(
                    df_country,
                    x="country",
                    y=metric_col,
                    color=metric_col,
                    color_continuous_scale="Greens",
                    labels={metric_col: metric_label, "country": "País"},
                )
                fig_country.update_layout(**plotly_layout(f"Top {n} países por {metric_label.lower()}"))
                fig_country.update_coloraxes(showscale=False)
                st.plotly_chart(fig_country, width="stretch")

        with right_col:
            with st.container(border=True):
                df_genre = (
                    df.groupby("genre", as_index=False)[metric_col]
                    .agg(agg_fn)
                    .sort_values(metric_col, ascending=False)
                    .head(n)
                )
                fig_genre = px.bar(
                    df_genre,
                    x="genre",
                    y=metric_col,
                    color=metric_col,
                    color_continuous_scale="Purples",
                    labels={metric_col: metric_label, "genre": "Género"},
                )
                fig_genre.update_layout(**plotly_layout(f"Top {n} géneros por {metric_label.lower()}"))
                fig_genre.update_coloraxes(showscale=False)
                st.plotly_chart(fig_genre, width="stretch")

        with st.container(border=True):
            df_artist = (
                df.groupby("artist_name", as_index=False)["stream_count"]
                .sum()
                .sort_values("stream_count", ascending=False)
                .head(n)
            )
            fig_artist = px.bar(
                df_artist,
                x="stream_count",
                y="artist_name",
                orientation="h",
                color="stream_count",
                color_continuous_scale="Blues",
                labels={"stream_count": "Reproducciones", "artist_name": "Artista"},
            )
            fig_artist.update_layout(**plotly_layout(f"Top {n} artistas por reproducciones"))
            fig_artist.update_coloraxes(showscale=False)
            st.plotly_chart(fig_artist, width="stretch")

    with tab_distribution:
        left_col, right_col = st.columns(2)

        with left_col:
            with st.container(border=True):
                fig_box = px.box(
                    df,
                    x="genre",
                    y="popularity",
                    color="genre",
                    points="outliers",
                    labels={"genre": "Género", "popularity": "Popularidad"},
                )
                fig_box.update_layout(**plotly_layout("Dispersión de popularidad por género"))
                fig_box.update_xaxes(tickangle=-35)
                st.plotly_chart(fig_box, width="stretch")

        with right_col:
            with st.container(border=True):
                fig_scatter = px.scatter(
                    df,
                    x="popularity",
                    y="stream_count",
                    color="genre",
                    size="popularity",
                    hover_data=["track_name", "artist_name", "country"],
                    labels={
                        "popularity": "Popularidad",
                        "stream_count": "Reproducciones",
                        "genre": "Género",
                    },
                    size_max=18,
                )
                fig_scatter.update_layout(**plotly_layout("Popularidad vs reproducciones"))
                st.plotly_chart(fig_scatter, width="stretch")

        with st.container(border=True):
            df_treemap = (
                df.groupby(["country", "genre"], as_index=False)["stream_count"]
                .sum()
                .sort_values("stream_count", ascending=False)
            )
            fig_treemap = px.treemap(
                df_treemap,
                path=["country", "genre"],
                values="stream_count",
                color="stream_count",
                color_continuous_scale="Tealgrn",
                labels={"stream_count": "Reproducciones"},
            )
            fig_treemap.update_layout(**plotly_layout("Mapa jerárquico país → género"))
            st.plotly_chart(fig_treemap, width="stretch")

    with tab_trends:
        if df["release_year"].notna().any():
            left_col, right_col = st.columns(2)

            with left_col:
                with st.container(border=True):
                    df_year = (
                        df.dropna(subset=["release_year"])
                        .groupby("release_year", as_index=False)
                        .agg(
                            stream_count=("stream_count", "sum"),
                            popularity=("popularity", "mean"),
                            tracks=("track_name", "count"),
                        )
                        .sort_values("release_year")
                    )
                    fig_year = px.line(
                        df_year,
                        x="release_year",
                        y="stream_count",
                        markers=True,
                        labels={
                            "release_year": "Año",
                            "stream_count": "Reproducciones totales",
                        },
                    )
                    fig_year.update_layout(**plotly_layout("Evolución de streams por año de lanzamiento"))
                    st.plotly_chart(fig_year, width="stretch")

            with right_col:
                with st.container(border=True):
                    fig_pop_year = px.area(
                        df_year,
                        x="release_year",
                        y="popularity",
                        labels={
                            "release_year": "Año",
                            "popularity": "Popularidad promedio",
                        },
                    )
                    fig_pop_year.update_layout(
                        **plotly_layout("Popularidad promedio por año de lanzamiento")
                    )
                    st.plotly_chart(fig_pop_year, width="stretch")
        else:
            st.info("No hay fechas de lanzamiento válidas para construir tendencias temporales.")

    with tab_detail:
        sort_col = st.selectbox(
            "Ordenar tabla por",
            options=["stream_count", "popularity", "release_year", "track_name"],
            format_func=lambda c: {
                "stream_count": "Reproducciones",
                "popularity": "Popularidad",
                "release_year": "Año",
                "track_name": "Canción",
            }[c],
        )
        st.dataframe(
            df.sort_values(sort_col, ascending=False)[
                [
                    "track_name",
                    "artist_name",
                    "country",
                    "genre",
                    "stream_count",
                    "popularity",
                    "release_date",
                ]
            ],
            width="stretch",
            hide_index=True,
        )


render_dynamic_charts(df_cassandra, chart_metric, top_n)

st.markdown("---")


# =========================
# ANÁLISIS DE RED NEO4J
# =========================
@st.fragment
def render_neo4j_section(df_artists: pd.DataFrame, df_network: pd.DataFrame, limit: int):
    st.subheader("Análisis de relaciones en Neo4j")

    if df_artists.empty:
        st.warning(
            "Aún no hay suficientes relaciones cargadas en Neo4j para procesar el análisis de red."
        )
        return

    left_col, right_col = st.columns(2)

    with left_col:
        with st.container(border=True):
            fig_bar = px.bar(
                df_artists,
                x="total_tracks",
                y="artist",
                orientation="h",
                color="total_tracks",
                color_continuous_scale="Oranges",
                labels={
                    "total_tracks": "Canciones enlazadas",
                    "artist": "Artista",
                },
            )
            fig_bar.update_layout(**plotly_layout(f"Top {limit} artistas en el grafo"))
            fig_bar.update_coloraxes(showscale=False)
            st.plotly_chart(fig_bar, width="stretch")

    with right_col:
        with st.container(border=True):
            if not df_network.empty:
                fig_sunburst = px.sunburst(
                    df_network,
                    path=["genre", "artist"],
                    values="tracks",
                    color="tracks",
                    color_continuous_scale="Sunset",
                    labels={"tracks": "Canciones", "genre": "Género", "artist": "Artista"},
                )
                fig_sunburst.update_layout(**plotly_layout("Composición artista-género"))
                st.plotly_chart(fig_sunburst, width="stretch")
            else:
                st.info("Sin datos de relaciones artista-género en Neo4j.")

    if not df_network.empty:
        with st.container(border=True):
            pivot = df_network.pivot_table(
                index="artist",
                columns="genre",
                values="tracks",
                aggfunc="sum",
                fill_value=0,
            )
            fig_heatmap = px.imshow(
                pivot,
                aspect="auto",
                color_continuous_scale="YlOrRd",
                labels=dict(x="Género", y="Artista", color="Canciones"),
            )
            fig_heatmap.update_layout(**plotly_layout("Heatmap artista × género"))
            st.plotly_chart(fig_heatmap, width="stretch")


render_neo4j_section(df_neo4j, df_neo4j_network, neo4j_limit)

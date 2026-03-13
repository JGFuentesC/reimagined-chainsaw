# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "streamlit",
#     "pandas",
#     "sqlalchemy",
#     "psycopg2-binary",
#     "plotly",
#     "folium",
#     "streamlit-folium",
#     "h3"
# ]
# ///
"""Dashboard Streamlit para el Cubo de Inteligencia PYME."""
import os

import folium
import h3
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine
from streamlit_folium import folium_static

st.set_page_config(
    page_title="PYME Analytics Cube - Dr. Fuentes",
    layout="wide",
    page_icon="📊",
)

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600&display=swap');

    html, body, [class*="st-"] { font-family: 'Outfit', sans-serif; }
    .main { background-color: #f0f2f6; }
    .stMetric {
        background: white;
        padding: 1.5rem;
        border-radius: 15px;
        box-shadow: 0 10px 25px rgba(0,0,0,0.05);
        border-bottom: 4px solid #4f46e5;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
        background-color: transparent;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        background-color: white;
        border-radius: 10px 10px 0 0;
        padding: 10px 20px;
        font-weight: 600;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

DB_URI = os.getenv("DATABASE_URL")

if not DB_URI:
    st.error(
        "❌ Variable DATABASE_URL no configurada. "
        "Copie olap/.env.example → olap/.env y complete sus credenciales."
    )
    st.stop()

engine = create_engine(DB_URI)

st.title("📊 Cubo de Inteligencia PYME")
st.markdown("### Una visión científica y estratégica para el Dr. Fuentes")


@st.cache_data(ttl=300)
def query(sql: str) -> pd.DataFrame:
    """Ejecuta una consulta SQL y cachea el resultado por 5 minutos."""
    return pd.read_sql(sql, engine)


# --- KPIs ---
col1, col2, col3, col4 = st.columns(4)
try:
    total_pymes = query("SELECT COUNT(*) FROM fact_sme_metrics").iloc[0, 0]
    ventas_avg = query("SELECT AVG(ventas_promedio_diarias) FROM fact_sme_metrics").iloc[0, 0]
    total_emp = query("SELECT SUM(num_empleados) FROM fact_sme_metrics").iloc[0, 0]
    antiguedad = query("SELECT AVG(antiguedad_negocio) FROM fact_sme_metrics").iloc[0, 0]

    col1.metric("PYMES Registradas", f"{total_pymes:,}")
    col2.metric("Ticket Promedio Diario", f"${ventas_avg:,.2f}")
    col3.metric("Fuerza Laboral Base", f"{total_emp:,}")
    col4.metric("Madurez Comercial", f"{antiguedad:.1f} años")
except Exception as exc:
    st.error(f"Error cargando KPIs: {exc}")

st.markdown("---")

tab1, tab2, tab3 = st.tabs(
    ["🗺️ Mapa Geográfico H3", "🏢 Análisis de Giros", "🎯 Metas y Rentabilidad"]
)

with tab1:
    st.markdown("#### Inteligencia Territorial: Densidad Hexagonal")
    try:
        geo_data = query("SELECT * FROM mv_ventas_por_giro_h3")
        m = folium.Map(location=[19.4326, -99.1332], zoom_start=11, tiles="cartodbpositron")
        max_v = geo_data["promedio_ventas"].max() or 1

        for _, row in geo_data.iterrows():
            boundary = [[p[0], p[1]] for p in h3.cell_to_boundary(row["h3_hex_id"])]
            opacity = 0.4 if row["promedio_ventas"] < (max_v / 2) else 0.8
            folium.Polygon(
                locations=boundary,
                fill=True,
                fill_color="#4f46e5",
                fill_opacity=opacity,
                color="white",
                weight=1,
                tooltip=(
                    f"<b>{row['giro_negocio']}</b><br>"
                    f"Ventas: ${row['promedio_ventas']:,.0f}<br>"
                    f"Pymes: {row['cantidad_pymes']}"
                ),
            ).add_to(m)

        folium_static(m, width=1100, height=600)
    except Exception as exc:
        st.error(f"Error cargando mapa: {exc}")

with tab2:
    st.markdown("#### Desempeño por Sector Económico")
    try:
        giro_data = query(
            """
            SELECT
                n.giro_negocio,
                AVG(f.ventas_promedio_diarias) AS ventas,
                SUM(f.num_empleados) AS empleados,
                COUNT(*) AS pymes
            FROM fact_sme_metrics f
            JOIN dim_negocio n ON f.id_negocio_dim = n.id_negocio_dim
            GROUP BY 1
            ORDER BY ventas DESC
            """
        )
        fig = px.bar(
            giro_data,
            x="giro_negocio",
            y="ventas",
            color="empleados",
            title="Ventas Promedio vs Capacidad de Empleo",
            template="plotly_white",
            labels={"giro_negocio": "Giro Comercial", "ventas": "Ventas Promedio ($)"},
            color_continuous_scale="Viridis",
        )
        st.plotly_chart(fig, use_container_width=True)
    except Exception as exc:
        st.error(f"Error cargando datos de giro: {exc}")

with tab3:
    st.markdown("#### Correlación: Aspiración vs Supervivencia")
    try:
        deseo_data = query("SELECT * FROM mv_deseos_rentabilidad")
        fig_bubble = px.scatter(
            deseo_data,
            x="antiguedad_promedio",
            y="promedio_ventas",
            size="cantidad_pymes",
            color="categoria_deseo",
            hover_name="categoria_deseo",
            size_max=70,
            title="Matriz de Ambiciones: ¿Qué metas logran mayor éxito financiero?",
            labels={
                "antiguedad_promedio": "Años de Estabilidad",
                "promedio_ventas": "Éxito en Ventas ($)",
            },
            template="plotly_white",
        )
        st.plotly_chart(fig_bubble, use_container_width=True)
    except Exception as exc:
        st.error(f"Error cargando datos de psicología: {exc}")

st.sidebar.markdown("### Configuración")
st.sidebar.info(
    "El cubo PostgreSQL opera sobre el puerto configurado en DATABASE_URL. "
    "Los datos han sido normalizados con clustering H3 (resolución 8)."
)
st.sidebar.caption("v2.0.0 - Dr. Fuentes Research Group")

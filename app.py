import streamlit as st
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# Configuración de la página
st.set_page_config(
    page_title="Credit Card BI Dashboard",
    page_icon="💳",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilo personalizado
st.markdown("""
    <style>
    .main {
        background-color: #0e1117;
    }
    .stMetric {
        background-color: #1e2130;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    </style>
    """, unsafe_allow_html=True)

# Título y descripción
st.title("💳 Credit Card Transactions BI Dashboard")
st.markdown("Visualización del cubo agregado de datos (24.4M de transacciones procesadas)")

# Carga de datos
@st.cache_data
def load_data():
    path = Path("data/bi_cube.parquet")
    if not path.exists():
        return None
    return pl.read_parquet(path)

df = load_data()

if df is None:
    st.error("❌ El archivo Parquet no fue encontrado. Por favor corre el script ETL primero.")
    st.stop()

# --- SIDEBAR ---
st.sidebar.header("Filtros Globales")

# Obtener valores únicos y filtrar nulos para el ordenamiento
years = sorted([y for y in df["Year"].unique().to_list() if y is not None])
selected_years = st.sidebar.multiselect("Años", years, default=years)

states = sorted([str(s) for s in df["Merchant State"].unique().to_list() if s is not None])
selected_states = st.sidebar.multiselect("Estados", states, default=states[:5] if states else [])

# Filtrar dataframe
mask = (df["Year"].is_in(selected_years)) & (df["Merchant State"].is_in(selected_states))
df_filtered = df.filter(mask)

# --- KPIs PRINCIPALES ---
col1, col2, col3, col4 = st.columns(4)

total_trans = df_filtered["total_transactions"].sum()
total_amt = df_filtered["total_amount"].sum()
fraud_cnt = df_filtered.filter(pl.col("is_fraud") == True)["total_transactions"].sum()
fraud_rate = (fraud_cnt / total_trans * 100) if total_trans > 0 else 0

with col1:
    st.metric("Total Transacciones", f"{total_trans:,}")
with col2:
    st.metric("Monto Total", f"${total_amt:,.0f}")
with col3:
    st.metric("Casos de Fraude", f"{fraud_cnt:,}")
with col4:
    st.metric("Tasa de Fraude", f"{fraud_rate:.2f}%")

st.markdown("---")

# --- GRÁFICOS ---
row2_col1, row2_col2 = st.columns(2)

with row2_col1:
    st.subheader("Evolución Temporal de Ventas")
    temp_df = df_filtered.group_by(["Year", "Month"]).agg(pl.col("total_amount").sum()).sort(["Year", "Month"])
    # Crear etiqueta de fecha para el eje X
    temp_df = temp_df.with_columns(
        pl.format("{}-{}", "Year", "Month").alias("period")
    )
    fig_line = px.line(temp_df.to_pandas(), x="period", y="total_amount", 
                      title="Monto por Mes", markers=True,
                      color_discrete_sequence=["#1f77b4"])
    fig_line.update_layout(template="plotly_dark")
    st.plotly_chart(fig_line, use_container_width=True)

with row2_col2:
    st.subheader("Monto por Tipo de Transacción")
    type_df = df_filtered.group_by("Use Chip").agg(pl.col("total_amount").sum())
    fig_pie = px.pie(type_df.to_pandas(), values="total_amount", names="Use Chip", 
                    hole=0.4, title="Distribución por Método",
                    color_discrete_sequence=px.colors.qualitative.Pastel)
    fig_pie.update_layout(template="plotly_dark")
    st.plotly_chart(fig_pie, use_container_width=True)

# --- SEGUNDA FILA ---
row3_col1, row3_col2 = st.columns(2)

with row3_col1:
    st.subheader("Top 10 Categorías (MCC)")
    mcc_df = df_filtered.group_by("MCC").agg(pl.col("total_amount").sum()).sort("total_amount", descending=True).head(10)
    fig_bar = px.bar(mcc_df.to_pandas(), x="MCC", y="total_amount", 
                    title="Monto por Categoría de Comerciante",
                    color="total_amount", color_continuous_scale="Viridis")
    fig_bar.update_layout(template="plotly_dark")
    st.plotly_chart(fig_bar, use_container_width=True)

with row3_col2:
    st.subheader("Fraude por Estado (Top)")
    fraud_state = df_filtered.filter(pl.col("is_fraud") == True).group_by("Merchant State").agg(
        pl.col("total_transactions").sum().alias("fraud_count")
    ).sort("fraud_count", descending=True).head(10)
    fig_fraud = px.bar(fraud_state.to_pandas(), y="Merchant State", x="fraud_count", 
                      orientation='h', title="Estados con más Fraudes Detectados",
                      color="fraud_count", color_continuous_scale="Reds")
    fig_fraud.update_layout(template="plotly_dark", yaxis={'categoryorder':'total ascending'})
    st.plotly_chart(fig_fraud, use_container_width=True)

# --- DATABLA FINAL ---
st.subheader("Vista Previa del Cubo (Datos Filtrados)")
st.dataframe(df_filtered.head(100).to_pandas(), use_container_width=True)

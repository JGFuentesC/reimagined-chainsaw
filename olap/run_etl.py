# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pandas",
#     "h3",
#     "sqlalchemy",
#     "psycopg2-binary"
# ]
# ///
"""ETL: sme_mx.csv → Star Schema OLAP en PostgreSQL."""
import os

import h3
import pandas as pd
from sqlalchemy import create_engine, text

# Threshold mínimo de representación por categoría (5 %)
CATEGORY_THRESHOLD = 0.05

# Columnas requeridas sin valores nulos para el análisis
REQUIRED_COLS = [
    "id",
    "edadEmprendedor",
    "ventasPromedioDiarias",
    "latitud",
    "longitud",
    "horaApertura",
    "horaCierre",
    "numEmpleados",
    "antiguedadNegocio",
]

# Columnas categóricas a normalizar por el umbral del 5 %
COLS_TO_GROUP = [
    "escolaridadEmprendedor",
    "estadoCivil",
    "giroNegocio",
    "registroVentas",
    "registroContabilidad",
    "tiempoCreditoProveedores",
    "Categoría",
    "Respuesta Original",
]

DIM_NEGOCIO_COLS = [
    "giroNegocio",
    "registroVentas",
    "registroContabilidad",
    "altaSAT",
    "usaCredito",
    "tiempoCreditoProveedores",
]

H3_RESOLUTION = 8


def map_rare_categories(series: pd.Series, threshold: float = CATEGORY_THRESHOLD) -> pd.Series:
    """Colapsa categorías con representación < threshold a 'OTROS'."""
    counts = series.value_counts(normalize=True, dropna=False)
    frequent = counts[counts >= threshold].index
    return series.apply(lambda x: x if x in frequent else "OTROS")


def parse_hours(val) -> float | None:
    """Convierte un valor de hora a float; devuelve None si falla."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def calc_hours(row: pd.Series) -> float:
    """Calcula horas diarias de operación considerando turnos nocturnos."""
    h_open = row["horaAperturaNum"]
    h_close = row["horaCierreNum"]
    if h_close < h_open:
        return (h_close + 24) - h_open
    return h_close - h_open


def load_and_clean(file_path: str) -> pd.DataFrame:
    """Carga el CSV y aplica limpieza de missings y normalización categórica."""
    df = pd.read_csv(file_path)
    print(f"Total registros originales: {len(df)}")

    df = df.dropna(subset=REQUIRED_COLS).copy()
    print(f"Total registros limpios de NAs críticos: {len(df)}")

    df["horaAperturaNum"] = df["horaApertura"].apply(parse_hours)
    df["horaCierreNum"] = df["horaCierre"].apply(parse_hours)
    df = df.dropna(subset=["horaAperturaNum", "horaCierreNum"]).copy()
    df["horas_operacion_diarias"] = df.apply(calc_hours, axis=1)

    print("📊 Agrupando categorías menores al 5 % en 'OTROS'...")
    for col in COLS_TO_GROUP:
        if col in df.columns:
            df[col] = df[col].fillna("DESCONOCIDO")
            df[col] = map_rare_categories(df[col])

    print("🗺️  Transformando coordenadas a hexágonos H3 (res=8)...")
    df["h3_hex_id"] = df.apply(
        lambda row: h3.latlng_to_cell(row["latitud"], row["longitud"], H3_RESOLUTION),
        axis=1,
    )
    return df


def build_dimensions(df: pd.DataFrame):
    """Construye las cuatro dimensiones del Star Schema."""
    dim_emprendedor = df[
        [
            "id",
            "edadEmprendedor",
            "sexoEmprendedor",
            "escolaridadEmprendedor",
            "estadoCivil",
            "dependientesEconomicos",
            "familiaAyuda",
        ]
    ].copy()
    dim_emprendedor.columns = [
        "id_emprendedor",
        "edad_emprendedor",
        "sexo_emprendedor",
        "escolaridad_emprendedor",
        "estado_civil",
        "dependientes_economicos",
        "familia_ayuda",
    ]

    dim_geografia = df[["h3_hex_id"]].drop_duplicates().reset_index(drop=True)
    dim_geografia["id_geografia_dim"] = dim_geografia.index + 1
    dim_geografia["latitud_centroide"] = dim_geografia["h3_hex_id"].apply(
        lambda h: h3.cell_to_latlng(h)[0]
    )
    dim_geografia["longitud_centroide"] = dim_geografia["h3_hex_id"].apply(
        lambda h: h3.cell_to_latlng(h)[1]
    )

    dim_negocio = df[DIM_NEGOCIO_COLS].drop_duplicates().reset_index(drop=True)
    dim_negocio["id_negocio_dim"] = dim_negocio.index + 1
    dim_negocio.columns = [
        "giro_negocio",
        "registro_ventas",
        "registro_contabilidad",
        "alta_sat",
        "usa_credito",
        "tiempo_credito_proveedores",
        "id_negocio_dim",
    ]

    dim_deseo = (
        df[["Respuesta Original", "Categoría"]].drop_duplicates().reset_index(drop=True)
    )
    dim_deseo["id_deseo_dim"] = dim_deseo.index + 1
    dim_deseo.columns = ["deseo_original", "categoria_deseo", "id_deseo_dim"]

    return dim_emprendedor, dim_geografia, dim_negocio, dim_deseo


def build_fact(df: pd.DataFrame, dim_geografia, dim_negocio, dim_deseo) -> pd.DataFrame:
    """Une las dimensiones para producir la Fact Table con surrogate keys."""
    fact = df.copy()
    fact = pd.merge(fact, dim_geografia, on="h3_hex_id", how="left")
    fact = pd.merge(
        fact,
        dim_negocio,
        left_on=DIM_NEGOCIO_COLS,
        right_on=[
            "giro_negocio",
            "registro_ventas",
            "registro_contabilidad",
            "alta_sat",
            "usa_credito",
            "tiempo_credito_proveedores",
        ],
        how="left",
    )
    fact = pd.merge(
        fact,
        dim_deseo,
        left_on=["Respuesta Original", "Categoría"],
        right_on=["deseo_original", "categoria_deseo"],
        how="left",
    )

    fact_metrics = fact[
        [
            "id",
            "id_negocio_dim",
            "id_geografia_dim",
            "id_deseo_dim",
            "ventasPromedioDiarias",
            "numEmpleados",
            "antiguedadNegocio",
            "horas_operacion_diarias",
        ]
    ].copy()
    fact_metrics.columns = [
        "id_emprendedor",
        "id_negocio_dim",
        "id_geografia_dim",
        "id_deseo_dim",
        "ventas_promedio_diarias",
        "num_empleados",
        "antiguedad_negocio",
        "horas_operacion_diarias",
    ]
    return fact_metrics


def build_molap_flat(df: pd.DataFrame, dim_negocio, dim_deseo, dim_geografia) -> pd.DataFrame:
    """
    Genera la tabla desnormalizada MOLAP (un solo CSV plano).
    Une todas las dimensiones sobre el dataset limpio.
    """
    molap = df.copy()
    molap = pd.merge(molap, dim_geografia, on="h3_hex_id", how="left")
    molap = pd.merge(
        molap,
        dim_negocio,
        left_on=DIM_NEGOCIO_COLS,
        right_on=[
            "giro_negocio",
            "registro_ventas",
            "registro_contabilidad",
            "alta_sat",
            "usa_credito",
            "tiempo_credito_proveedores",
        ],
        how="left",
    )
    molap = pd.merge(
        molap,
        dim_deseo,
        left_on=["Respuesta Original", "Categoría"],
        right_on=["deseo_original", "categoria_deseo"],
        how="left",
    )

    # Seleccionar y renombrar columnas relevantes para la tabla plana
    molap_flat = molap[
        [
            "id",
            "edadEmprendedor",
            "sexoEmprendedor",
            "escolaridadEmprendedor",
            "estadoCivil",
            "dependientesEconomicos",
            "familiaAyuda",
            "giro_negocio",
            "registro_ventas",
            "registro_contabilidad",
            "alta_sat",
            "usa_credito",
            "tiempo_credito_proveedores",
            "deseo_original",
            "categoria_deseo",
            "h3_hex_id",
            "latitud_centroide",
            "longitud_centroide",
            "ventasPromedioDiarias",
            "numEmpleados",
            "antiguedadNegocio",
            "horas_operacion_diarias",
        ]
    ].copy()

    molap_flat.columns = [
        "id_emprendedor",
        "edad",
        "sexo",
        "escolaridad",
        "estado_civil",
        "dependientes_economicos",
        "familia_ayuda",
        "giro_negocio",
        "registro_ventas",
        "registro_contabilidad",
        "alta_sat",
        "usa_credito",
        "tiempo_credito_proveedores",
        "deseo_negocio",
        "categoria_deseo",
        "h3_hex_id",
        "latitud_centroide",
        "longitud_centroide",
        "ventas_promedio_diarias",
        "num_empleados",
        "antiguedad_negocio",
        "horas_operacion_diarias",
    ]
    return molap_flat


MV_VENTAS_GIRO_H3 = """
    DROP MATERIALIZED VIEW IF EXISTS mv_ventas_por_giro_h3;
    CREATE MATERIALIZED VIEW mv_ventas_por_giro_h3 AS
    SELECT
        g.h3_hex_id,
        n.giro_negocio,
        COUNT(f.id_emprendedor) AS cantidad_pymes,
        AVG(f.ventas_promedio_diarias) AS promedio_ventas,
        SUM(f.num_empleados) AS suma_empleados
    FROM fact_sme_metrics f
    JOIN dim_geografia_h3 g ON f.id_geografia_dim = g.id_geografia_dim
    JOIN dim_negocio n ON f.id_negocio_dim = n.id_negocio_dim
    GROUP BY 1, 2;
"""

MV_DESEOS_RENTABILIDAD = """
    DROP MATERIALIZED VIEW IF EXISTS mv_deseos_rentabilidad;
    CREATE MATERIALIZED VIEW mv_deseos_rentabilidad AS
    SELECT
        d.categoria_deseo,
        COUNT(f.id_emprendedor) AS cantidad_pymes,
        AVG(f.ventas_promedio_diarias) AS promedio_ventas,
        MAX(f.ventas_promedio_diarias) AS max_ventas,
        AVG(f.antiguedad_negocio) AS antiguedad_promedio
    FROM fact_sme_metrics f
    JOIN dim_deseo d ON f.id_deseo_dim = d.id_deseo_dim
    GROUP BY 1;
"""


def main() -> None:
    """Punto de entrada principal del ETL."""
    print("🚀 Iniciando extracción de sme_mx.csv...")
    root = os.path.dirname(os.path.dirname(__file__))
    file_path = os.path.join(root, "data", "sme_mx.csv")
    out_dir = os.path.dirname(__file__)

    if not os.path.exists(file_path):
        print(f"❌ Error: Archivo {file_path} no encontrado.")
        return

    df = load_and_clean(file_path)
    dim_emprendedor, dim_geografia, dim_negocio, dim_deseo = build_dimensions(df)
    fact_metrics = build_fact(df, dim_geografia, dim_negocio, dim_deseo)
    molap_flat = build_molap_flat(df, dim_negocio, dim_deseo, dim_geografia)

    print("✅ Modelado Star Schema + MOLAP finalizado en memoria.")

    # Exportar MOLAP flat
    molap_path = os.path.join(out_dir, "sme_molap_flat.csv")
    molap_flat.to_csv(molap_path, index=False)
    print(f"📄 MOLAP exportado → {molap_path} ({len(molap_flat)} filas)")

    db_uri = os.getenv("DATABASE_URL")
    if not db_uri:
        print(
            "⚠️  No se detectó DATABASE_URL. "
            "Exporte las variables de .env y vuelva a ejecutar para cargar a PostgreSQL."
        )
        # Exportar CSVs ROLAP como fallback
        for name, frame in [
            ("dim_emprendedor", dim_emprendedor),
            ("dim_geografia", dim_geografia),
            ("dim_negocio", dim_negocio),
            ("dim_deseo", dim_deseo),
            ("fact_sme_metrics", fact_metrics),
        ]:
            frame.to_csv(os.path.join(out_dir, f"{name}.csv"), index=False)
        return

    try:
        engine = create_engine(db_uri)
        host_info = db_uri.split("@")[-1]
        print(f"💾 Cargando Star Schema en: {host_info}...")

        # Eliminar MVs primero para evitar errores de dependencia al recrear tablas
        with engine.begin() as conn:
            conn.execute(text("DROP MATERIALIZED VIEW IF EXISTS mv_ventas_por_giro_h3 CASCADE;"))
            conn.execute(text("DROP MATERIALIZED VIEW IF EXISTS mv_deseos_rentabilidad CASCADE;"))

        dim_emprendedor.to_sql("dim_emprendedor", engine, if_exists="replace", index=False)
        dim_geografia.to_sql("dim_geografia_h3", engine, if_exists="replace", index=False)
        dim_negocio.to_sql("dim_negocio", engine, if_exists="replace", index=False)
        dim_deseo.to_sql("dim_deseo", engine, if_exists="replace", index=False)
        fact_metrics.to_sql("fact_sme_metrics", engine, if_exists="replace", index=False)
        print("💾 Guardando Fact Table ... ✓")

        print("🛠️  Creando Vistas Materializadas...")
        with engine.begin() as conn:
            conn.execute(text(MV_VENTAS_GIRO_H3))
            conn.execute(text(MV_DESEOS_RENTABILIDAD))

        print("🎉 Proceso OLAP completado exitosamente en PostgreSQL.")
    except Exception as exc:
        print(f"❌ Error crítico subiendo a la DB: {exc}")


if __name__ == "__main__":
    main()

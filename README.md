# 💳 Reimagined Chainsaw: Credit Card BI Suite

Este repositorio contiene una suite de ingeniería de datos y visualización diseñada para procesar y analizar grandes volúmenes de transacciones de tarjetas de crédito (~24.4M de registros) de forma eficiente.

## 🚀 Arquitectura del Proyecto

El sistema está dividido en dos capas principales:

1.  **Capa ETL (Ingeniería)**: Un pipeline ultra-optimizado que utiliza el motor de **Polars Streaming** para transformar datos crudos en un cubo agregado Parquet.
2.  **Capa de Negocio (BI Dashboard)**: Un tablero interactivo modular construido con **Streamlit** y **Plotly** para el descubrimiento de insights.

## 🛠️ Tecnologías Utilizadas

*   **Procesamiento**: [Polars](https://pola.rs/) (Streaming API & Out-of-Core Processing)
*   **Validación**: [Pydantic V2](https://docs.pydantic.dev/)
*   **Visualización**: [Streamlit](https://streamlit.io/) & [Plotly](https://plotly.com/python/)
*   **Gestor de Paquetes**: [uv](https://github.com/astral-sh/uv)
*   **Formato de Datos**: Apache Parquet

## 📦 Instalación y Configuración

El proyecto utiliza `uv` para una gestión de dependencias ultra-rápida.

```bash
# Sincronizar entorno y dependencias
uv sync
```

## ⚙️ Cómo Ejecutar

### 1. Construir el Cubo de Datos (ETL)
Procesa el archivo `data/credit_card.csv` y genera un cubo agregado optimizado para BI.

```bash
uv run etl/build_bi_cube.py
```
*Tiempo estimado: ~13 segundos para 24.4M de registros.*

### 2. Lanzar el Tablero de BI
Inicia el dashboard interactivo para explorar los datos agregados.

```bash
uv run streamlit run app.py
```

## 📊 Capacidades del Dashboard
*   **KPIs Globales**: Transacciones, Montos, Casos de Fraude y Tasas de Error.
*   **Análisis Temporal**: Tendencias de ventas mes a mes.
*   **Segmentación Geográfica**: Mapa de fraude por estado.
*   **Análisis de Canal**: Comparativa entre Chip, Swipe y Online Transactions.

## 🛡️ Seguridad y Robustez
*   **Manejo de Memoria**: El pipeline nunca carga el dataset completo en RAM, permitiendo su ejecución en hardware estándar.
*   **Calidad de Datos**: Limpieza automática de símbolos de moneda, manejo de nulos y normalización de estados/ciudades.

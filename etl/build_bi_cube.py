#!/usr/bin/env python3
"""
ETL Script: Credit Card Transactions to BI Cube (Optimized)
==========================================================
Versión optimizada para memoria (Out-of-Core) que utiliza Polars Streaming.
Procesa ~24M+ de registros eficientemente sin colapsar la RAM.
"""

import polars as pl
from pydantic import BaseModel, Field, ConfigDict
from typing import Literal
from pathlib import Path
from datetime import datetime
import logging
import sys

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("etl_process.log")
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURACIÓN Y VALIDACIÓN (PYDANTIC)
# ============================================================================

class ETLConfig(BaseModel):
    """Configuración robusta para el proceso ETL"""
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    input_file: Path = Field(default=Path("data/credit_card.csv"))
    output_cube: Path = Field(default=Path("data/bi_cube.parquet"))
    output_summaries_dir: Path = Field(default=Path("data/summaries"))
    compression: Literal["zstd", "snappy", "lz4"] = Field(default="zstd")
    streaming: bool = Field(default=True)

# ============================================================================
# PIPELINE DE TRANSFORMACIÓN (LAZY)
# ============================================================================

def build_optimized_pipeline(config: ETLConfig) -> pl.LazyFrame:
    """
    Define el grafo de ejecución perezoso (LazyFrame) con optimizaciones
    de memoria y tipos de datos.
    """
    logger.info("Definiendo pipeline de transformación...")

    # 1. Escaneo inteligente (Solo columnas necesarias y tipos optimizados)
    # Seleccionamos solo las dimensiones y métricas base
    columns_to_keep = [
        "User", "Card", "Year", "Month", "Day", "Amount", 
        "Use Chip", "Merchant City", "Merchant State", "MCC", 
        "Errors?", "Is Fraud?"
    ]

    lf = pl.scan_csv(
        config.input_file,
        ignore_errors=True,
        cache=False, # Desactivamos cache para ahorrar RAM
    ).select(columns_to_keep)

    # 2. Downcasting y Tipado Eficiente
    logger.info("Aplicando optimización de tipos (Downcasting)...")
    lf = lf.with_columns([
        # Temporales a Int16/Int8
        pl.col("Year").cast(pl.Int16),
        pl.col("Month").cast(pl.Int8),
        pl.col("Day").cast(pl.Int8),
        pl.col("MCC").cast(pl.Int16),
        pl.col("User").cast(pl.Int32),
        pl.col("Card").cast(pl.Int8),
        
        # Categorical para strings repetitivos (Ahorra muchísima RAM)
        pl.col("Use Chip").cast(pl.Categorical),
        pl.col("Merchant State").cast(pl.Categorical),
        pl.col("Merchant City").cast(pl.Categorical),
        
        # Limpieza de Amount (Float32 es suficiente para BI y consume 50% menos que Float64)
        pl.col("Amount")
        .str.replace_all(r"[\$,]", "")
        .cast(pl.Float32)
        .alias("amount_clean")
    ])

    # 3. Limpieza y Flags
    lf = lf.with_columns([
        # Flags Booleanos (Más compactos que strings)
        (pl.col("Is Fraud?").str.to_lowercase() == "yes").alias("is_fraud"),
        pl.col("Errors?").is_not_null().alias("has_error"),
        
        # Dimensiones temporales calculadas
        pl.date(pl.col("Year"), pl.col("Month"), pl.col("Day")).alias("date")
    ])

    # 4. Agregación (Cubo BI)
    logger.info("Preparando agregaciones para el cubo...")
    cube_lf = lf.group_by([
        "Year", 
        "Month", 
        "Merchant State", 
        "Use Chip", 
        "MCC",
        "is_fraud"
    ]).agg([
        pl.len().alias("total_transactions"),
        pl.col("amount_clean").sum().alias("total_amount"),
        pl.col("amount_clean").mean().alias("avg_amount"),
        pl.col("has_error").sum().alias("error_count"),
        pl.col("User").n_unique().alias("unique_users"),
    ])

    return cube_lf

# ============================================================================
# EJECUCIÓN
# ============================================================================

def main():
    start_time = datetime.now()
    config = ETLConfig()
    
    logger.info("🚀 Iniciando proceso ETL Ultra-Optimizado")
    
    try:
        # Construir pipeline
        pipeline = build_optimized_pipeline(config)
        
        # Crear directorios
        config.output_summaries_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"⏳ Ejecutando pipeline en modo STREAMING={config.streaming}...")
        
        # Paso Final: Ejecución y escritura directa (Sink)
        # sink_parquet es el más eficiente en memoria si el grafo es compatible
        # Si falla, usamos collect(streaming=True).write_parquet()
        try:
            logger.info("Intentando sink_parquet para máxima eficiencia...")
            pipeline.sink_parquet(
                config.output_cube,
                compression=config.compression
            )
        except Exception as e:
            logger.warning(f"sink_parquet no disponible para este grafo: {e}. Usando collect(streaming=True).")
            result = pipeline.collect(streaming=True)
            result.write_parquet(config.output_cube, compression=config.compression)

        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("-" * 40)
        logger.info(f"✅ FINALIZADO EXITOSAMENTE")
        logger.info(f"⏱️ Tiempo total: {duration}")
        logger.info(f"📦 Destino: {config.output_cube}")
        
        # Verificar tamaño del resultado
        file_size = config.output_cube.stat().st_size / (1024 * 1024)
        logger.info(f"📊 Tamaño del cubo Parquet: {file_size:.2f} MB")
        
    except Exception as e:
        logger.error(f"❌ Error crítico en el ETL: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()

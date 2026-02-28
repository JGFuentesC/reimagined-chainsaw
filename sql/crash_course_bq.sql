/*
  =============================================================================
  BIGQUERY CRASH COURSE - SQL PARA ANÁLISIS DE DATOS
  =============================================================================
  Este script contiene los conceptos fundamentales de SQL en BigQuery, 
  desde consultas básicas hasta funciones de ventana y manejo de tablas particionadas.
  
  Autor: Dr. Fuentes
  Propósito: Educativo / Repaso para estudiantes
  =============================================================================
*/

-------------------------------------------------------------------------------
-- 1. CONSULTA BÁSICA: AGREGACIÓN Y ORDENAMIENTO
-- Aprendemos: SELECT, SUM, GROUP BY, ORDER BY, LIMIT
-------------------------------------------------------------------------------
-- Esta consulta suma la cantidad de nombres registrados por estado en EE.UU.
SELECT 
    state, 
    SUM(number) AS total_count
FROM 
    `bigquery-public-data.usa_names.usa_1910_current`
GROUP BY 
    state
ORDER BY 
    total_count DESC
LIMIT 50;


-------------------------------------------------------------------------------
-- 2. CTEs (Common Table Expressions) Y JOINS
-- Aprendemos: WITH, FULL OUTER JOIN, HAVING
-------------------------------------------------------------------------------
-- Las CTEs (bloques WITH) ayudan a organizar consultas complejas en pasos lógicos.
WITH
  -- Paso A: Filtrar nombres populares en Texas (TX)
  namesTX AS (
    SELECT
      name, 
      SUM(number) AS total_tx
    FROM 
      `bigquery-public-data.usa_names.usa_1910_current`
    WHERE 
      state = 'TX'
    GROUP BY 
      1 -- Agrupa por la primera columna (name)
    HAVING 
      total_tx > 100000 -- Filtro después de la agregación
    ORDER BY 
      2 DESC
    LIMIT 10
  ),
  
  -- Paso B: Filtrar nombres populares en Pennsylvania (PA)
  namesPA AS (
    SELECT
      name, 
      SUM(number) AS total_pa
    FROM 
      `bigquery-public-data.usa_names.usa_1910_current`
    WHERE 
      state = 'PA'
    GROUP BY 
      1
    HAVING 
      total_pa > 100000
    ORDER BY 
      2 DESC
    LIMIT 10
  )

-- Paso Final: Unir ambos resultados para comparar
-- Un FULL OUTER JOIN mantiene registros de ambas tablas, incluso si no hay coincidencia.
SELECT 
    * 
FROM 
    namesTX 
FULL OUTER JOIN 
    namesPA USING (name);


-------------------------------------------------------------------------------
-- 3. FUNCIONES DE FECHA, STRINGS Y TABLAS WILDCARD
-- Aprendemos: PARSE_DATE, EXTRACT, UPPER, COALESCE, _TABLE_SUFFIX
-------------------------------------------------------------------------------
-- BigQuery permite usar '*' para consultar múltiples tablas particionadas.
SELECT 
    event_date,
    -- Convierte string 'YYYYMMDD' a formato fecha y extrae el mes
    EXTRACT(MONTH FROM PARSE_DATE('%Y%m%d', event_date)) AS mes,
    -- Pone el texto en mayúsculas
    UPPER(event_name) AS evento_gritando,
    -- Manejo de nulos: si country es NULL, devuelve 'Desconocido'
    COALESCE(geo.country, 'Desconocido') AS pais 
FROM 
    `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE 
    -- Filtra específicamente por el sufijo de la tabla (las fechas)
    _TABLE_SUFFIX BETWEEN '20210101' AND '20210131';


-------------------------------------------------------------------------------
-- 4. CREACIÓN DE TABLAS Y FILTRADO AVANZADO
-- Aprendemos: CREATE OR REPLACE TABLE
-------------------------------------------------------------------------------
-- Creamos una tabla persistente a partir de un resultado de consulta.
CREATE OR REPLACE TABLE `prueba.taxis` AS
SELECT 
    trip_start_timestamp, 
    trip_miles, 
    trip_total
FROM 
    `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE 
    taxi_id = '4923b6c4d0e73f1cb052c0b7833dc1e76d9b31ad5c20c75d9280e48d1a4e8d45eadc81877953654f6169cf534c6be7e355461e7ad094b8e7d9c7cd055cfbfc72';


-------------------------------------------------------------------------------
-- 5. FUNCIONES DE VENTANA (WINDOW FUNCTIONS)
-- Aprendemos: DATE_TRUNC, ROW_NUMBER, LAG, OVER
-------------------------------------------------------------------------------
-- Las funciones de ventana operan sobre un conjunto de filas relacionadas.
WITH
  agg AS (
    SELECT
      -- Redondea la fecha al primer día del mes
      DATE_TRUNC(trip_start_timestamp, MONTH) AS mes_original,
      ROUND(SUM(trip_total), 0) AS total_dinero,
      ROUND(SUM(trip_miles * 1.6), 0) AS total_km -- Conversión a km
    FROM
      `prueba.taxis`
    GROUP BY 
      1
    ORDER BY 
      1
  )

SELECT
  * EXCEPT(mes_original), -- Selecciona todo menos la columna original
  CAST(mes_original AS DATE) AS mes,
  -- Genera un número secuencial basado en el orden de los meses
  ROW_NUMBER() OVER (ORDER BY mes_original) AS num_mes_historia,
  -- LAG obtiene el valor de la fila anterior. Útil para calcular crecimiento (%).
  (total_dinero / LAG(total_dinero, 1) OVER (ORDER BY mes_original)) - 1 AS pct_crecimiento_dinero
FROM 
    agg;

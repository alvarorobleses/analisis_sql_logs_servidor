import duckdb
con = duckdb.connect()
con.execute("""
    CREATE TABLE logs AS
    SELECT * FROM read_json_auto('data/logs_access_logs.json')
""")
print('Total de filas:', con.execute("SELECT COUNT(*) FROM logs").fetchone()[0])

#Estructura de la tabla
print('\nColumnas:')
for col in con.execute('DESCRIBE logs').fetchall():
    print(f' {col[0]}: {col[1]}')

#Ver primeras filas
print('\nPrimeras 3 filas')
print(con.execute('SELECT * FROM logs LIMIT 3').fetchdf())

# Paso 4: Exploración Inicial (Query 1)
#¿Cuántos registros? ¿Qué período cubren?
print(con.execute("""
SELECT
    COUNT(*) as total_requests,
    MIN(timestamp) primera_request,
    MAX(timestamp) as ultima_request,
    COUNT(DISTINCT user_id) as usuarios_unicos,
    COUNT(DISTINCT endpoint) as endpoints_unicos
FROM logs;
""").fetchdf())

# Paso 5: Endpoints más usados (Query 2)
# ¿Qué endpoints reciben más tráfico?
print(con.execute("""
SELECT
    endpoint,
    COUNT(*) as hits,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM logs), 2) as porcentaje
FROM logs
GROUP BY endpoint
ORDER BY hits DESC
LIMIT 10;
""").fetchdf())

# Paso 6: Análisis de errores (Query 3)
# ¿Qué endpoints tienen más errores 500?
print(con.execute("""
SELECT 
    endpoint,
    COUNT(*) as total_errors,
    COUNT(DISTINCT user_id) as usuarios_afectados,
    ROUND(AVG(response_time_ms), 2) as avg_response_time
FROM logs
WHERE status_code >= 500
GROUP BY endpoint
ORDER BY total_errors DESC
LIMIT 10;
""").fetchdf())

#Paso 7: Performance por endpoint (Query 4)
#¿Qué endpoints son más lentos?
print('\nEndpoints más lentos:')
print(con.execute("""
SELECT
    endpoint,
    COUNT(*) as requests,
    ROUND(AVG(response_time_ms), 2) as avg_time,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY response_time_ms), 2) AS p50,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY response_time_ms), 2) AS p95,
    MAX(response_time_ms) AS max_time
FROM logs
WHERE status_code < 500 --Solo requests exitosas
GROUP BY endpoint
HAVING COUNT(*) > 50 -- Solo endpoints con suficiente tráfico
ORDER BY p95 DESC                  
LIMIT 10;
""").fetchdf())

# Paso 8: Tendencia horaria
# ¿A qué hora hay más tráfico?
print('\nHoras con más tráfico:')
print(con.execute("""
SELECT
    EXTRACT(HOUR FROM timestamp) as hora,
    COUNT(*) as requests,
    ROUND(AVG(response_time_ms), 2) AS avg_response_time,
    SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) AS errors
FROM logs
GROUP BY EXTRACT(HOUR FROM timestamp)
ORDER BY hora; --Buscar correlaciones entre tráficos altos y tiempos de respuestas altos.
""").fetchdf())

# Paso 9: Window Functions - Ranking
# Top 3 requests más lentas por endpoint
print('\nRequests más lentas por endpoint')
print(con.execute("""
WITH ranked AS(
    SELECT
        endpoint,
        timestamp,
        response_time_ms,
        user_id,
        ROW_NUMBER() OVER(PARTITION BY endpoint
            ORDER BY response_time_ms DESC) AS rank
    FROM logs
    WHERE status_code < 500)
SELECT * FROM ranked
WHERE rank <= 3
ORDER BY endpoint, rank;
""").fetchdf())

# Paso 7: Comparación con periodo anterior (LAG)
# ¿Cómo cambia el tráfico día a día?
print('\nCambio del tráfico día a día')
print(con.execute("""
WITH daily_stats AS(
    SELECT 
        DATE(timestamp) as fecha,
        COUNT(*) as requests,
        ROUND(AVG(response_time_ms), 2) AS avg_time
    FROM logs
    GROUP BY DATE(timestamp)                 
)
SELECT 
    fecha,
    requests,
    LAG(requests) OVER (ORDER BY fecha) AS requests_dia_anterior,
    requests - LAG(requests) OVER (ORDER BY fecha) as diferencia,
    ROUND((requests - LAG(requests) OVER (ORDER BY fecha)) * 100.0 / LAG(requests) OVER (ORDER BY fecha), 2) AS cambio_porcentual
    FROM daily_stats
    ORDER BY fecha;
""").fetchdf())

# Recomendaciones
# Revisar primero /api/cart, /api/checkout y /api/payments, porque concentran muchos errores y tiempos de respuesta muy altos en fallos.
# Optimizar /api/products y /api/search, ya que tienen los p95 más altos entre los endpoints más usados.
# Analizar qué ocurre a las 15:00, 17:00 y 23:00, porque en esas horas aumentan bastante la latencia y los errores.
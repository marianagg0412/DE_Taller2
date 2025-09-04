
# Taller 2 - Data Engineering

Este repositorio contiene los ejercicios y scripts desarrollados para el Taller 2 de Ingeniería de Datos. El taller abarca scraping, análisis, integración y modelado de datos deportivos, así como ejercicios prácticos de pandas y manejo de datos.

## Contenido

### 1. Scraping y Análisis de MercadoLibre
- **Archivo:** `punto1.py`
- Se implementó un scraper en Python usando Selenium y BeautifulSoup para extraer productos de MercadoLibre Colombia.
- Los datos extraídos (título, precio, calificación, url, etc.) se almacenan en MongoDB.
- Incluye funciones de análisis: conteo de productos por palabra clave, análisis de precios, productos mejor calificados, etc.

### 2. Extracción y Modelado de Datos Deportivos (API-SPORTS)
- **Archivos:** `punto2.py`, `punto2.ipynb`, `punto2-etl.py`
- Se extrajeron datos de tres deportes usando la API de API-SPORTS:
   - Fútbol (Premier League)
   - Baloncesto (NBA)
   - Fórmula 1
- Los datos se almacenan en diferentes colecciones de MongoDB (`sports_db`).
- Se diseñó un modelo estrella (star schema) para cada deporte y se generaron los DDLs para PostgreSQL.
- Se implementó un proceso ETL en Python para migrar los datos de MongoDB a PostgreSQL, poblando las tablas del modelo estrella.

#### Modelo Estrella (Star Schema)
- **Fútbol:** Tablas de hechos y dimensiones para partidos, equipos, ligas, fechas, estadios, árbitros.
- **Baloncesto:** Tablas de hechos y dimensiones para juegos, equipos, jugadores, ligas, fechas.
- **Fórmula 1:** Tablas de hechos y dimensiones para resultados de carreras, pilotos, equipos, circuitos, carreras.

#### DDLs
- Los scripts de creación de tablas (DDL) para cada modelo están incluidos en `punto2.ipynb` y pueden ejecutarse directamente en PostgreSQL.

#### ETL
- El script `punto2-etl.py` realiza la extracción de datos desde MongoDB y la carga en PostgreSQL, asegurando la integridad referencial y la actualización de dimensiones.

### 3. Ejercicios de Pandas y Procesamiento de Datos
- **Archivo:** `punto3_6.ipynb`
- Comparación de formatos de almacenamiento (CSV, Excel, Parquet) en tiempos de lectura y escritura.
- Explicación y ejemplos de multiprocessing vs. multithreading en Python para procesamiento de datos.
- Ejercicios prácticos de pandas: manejo de fechas, joins, concat, merge, manejo de nulos, funciones de ventana (rolling, expanding), crosstab, pivot, melt, etc.

---

## Requisitos
- Python 3.8+
- MongoDB
- PostgreSQL
- Paquetes: `selenium`, `beautifulsoup4`, `pymongo`, `psycopg2-binary`, `pandas`, `numpy`, `webdriver-manager`, `python-dotenv`, etc.

## Ejecución
1. Configura las variables de entorno en el archivo `.env` para las conexiones a MongoDB, API-SPORTS y PostgreSQL.
2. Ejecuta los scripts de scraping y extracción (`punto1.py`, `punto2.py`).
3. Ejecuta el ETL para poblar el modelo estrella en PostgreSQL (`punto2-etl.py`).
4. Consulta y analiza los datos usando los notebooks y scripts incluidos.

---

**Autor:** Mariana González G
**Fecha:** Septiembre 2025

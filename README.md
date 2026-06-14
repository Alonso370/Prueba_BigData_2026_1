# 🎵 Sistema Multimodelo de Monitoreo y Analítica en Tiempo Real
## Spotify Data Pipeline | Big Data UTEC

<div align="center">

**Proyecto Grupal II** | Facultad de Computación  
**Universidad de Ingeniería y Tecnología (UTEC)**  
📅 **Fecha de Entrega:** 06 de Julio de 2026

![Status](https://img.shields.io/badge/status-active-brightgreen) ![Python](https://img.shields.io/badge/python-3.10+-blue) ![Docker](https://img.shields.io/badge/docker-20.10+-2496ED)

</div>

---

## 📋 Tabla de Contenidos

- [Descripción del Proyecto](#-descripción-del-proyecto)
- [Arquitectura del Sistema](#-arquitectura-del-sistema)
- [Stack Tecnológico](#-stack-tecnológico)
- [Requisitos Previos](#-requisitos-previos)
- [Estructura de Directorios](#-estructura-de-directorios)
- [Guía de Despliegue Paso a Paso](#-guía-de-despliegue-paso-a-paso)
- [Validación y Monitoreo](#-validación-y-monitoreo)
- [Troubleshooting](#-troubleshooting)
- [Referencias y Documentación](#-referencias-y-documentación)

---

## 🎯 Descripción del Proyecto

Este proyecto implementa un **pipeline de procesamiento de datos en tiempo real** utilizando el dataset **Spotify Music Analytics Dataset (2015–2025)** (+50,000 registros). La solución simula un flujo continuo de eventos analíticos de música, distribuyendo los datos mediante una arquitectura **multimodelo NoSQL** que aprovecha las fortalezas técnicas de cada paradigma:

- **MongoDB**: Almacenamiento documental flexible para auditoría y análisis exploratorio
- **Apache Cassandra**: Base de datos columnar optimizada para series temporales masivas
- **Neo4j**: Grafos para modelar relaciones estructurales entre Artistas, Canciones y Géneros

La orquestación se realiza mediante **Apache Airflow** ejecutado en Docker, garantizando procesamiento batch optimizado cada 2 minutos. Finalmente, un **dashboard interactivo** en Streamlit visualiza KPIs en tiempo real.

### Objetivos Técnicos

✅ Diseñar e implementar un ETL robusto capaz de procesar >50,000 registros en modo streaming  
✅ Demostrar sinergia entre paradigmas NoSQL mediante consultas especializadas  
✅ Orquestar tareas complejas con tolerancia a fallos mediante Airflow  
✅ Exponer métricas operacionales en tiempo real via Streamlit + Plotly  

---

## 🏗️ Arquitectura del Sistema

### Flujo de Datos: "Estructura 1 - Productor en Streaming"

```
┌──────────────────┐
│  stream_generator │  (Simula ingesta continua)
│   (Python)       │  Lee CSV cada 20s → JSON chunks
└────────┬─────────┘
         │
         ▼
    ┌─────────────┐
    │  data/queue │  (Buffer temporal)
    │ (JSON files)│
    └────────┬────┘
             │
             ▼
    ┌──────────────────┐
    │   Apache Airflow │  (Orquestador — cada 2 minutos)
    │  (Scheduler)     │
    └────────┬─────────┘
             │
   ┌─────────┴──────────┬──────────────────┐
   │                    │                  │
   ▼                    ▼                  ▼
┌─────────┐        ┌──────────┐      ┌────────────┐
│ MongoDB │        │ Cassandra│      │   Neo4j    │
│(Auditoría)      │(Series   │      │ (Grafos)   │
│Documental)      │Temporales)       │ Relaciones)
└─────────┘        └──────────┘      └────────────┘
   │                    │                  │
   └─────────┬──────────┴──────────────────┘
             │
             ▼
    ┌──────────────────┐
    │  Streamlit       │  (Visualización)
    │  Dashboard       │  Consultas en vivo
    └──────────────────┘
```

### Etapas del Pipeline

#### 1️⃣ **Ingesta (Productor)**
- **Componente**: `producer/stream_generator.py`
- **Frecuencia**: Cada 20 segundos
- **Operación**: Lee el CSV maestro en chunks de 100 filas, serializa a JSON, deposita en `data/queue/`

#### 2️⃣ **Extracción y Validación (DAG - Tarea: `extract_and_validate`)**
- **Frecuencia**: Cada 2 minutos (Scheduler de Airflow)
- **Lógica**:
  - Succión atómica de todos los JSON en `data/queue/`
  - Unificación en un único DataFrame con Pandas
  - Limpieza: eliminación de nulos, validación de tipos, deduplicación
  - Almacenamiento en XCom (memoria compartida de Airflow)

#### 3️⃣ **Persistencia Multimodelo (DAG - Tareas Paralelas)**

**Tarea `load_to_mongodb`**
- Inserción masiva (bulk insert) de documentos JSON en colección `spotify_events`
- Índices automáticos en campos frecuentes: `track_id`, `artist_id`, `timestamp`

**Tarea `load_to_cassandra`**
- Insert en tabla `time_series_events` con clave de partición: `country`
- Clustering order: `timestamp DESC` (optimiza rangos temporales)
- Estrategia de compresión LZ4

**Tarea `load_to_neo4j`**
- Consultas MERGE para crear/actualizar nodos y relaciones:
  - Nodos: `Artist`, `Track`, `Genre`, `Country`
  - Relaciones: `PERFORMED_BY`, `BELONGS_TO`, `PLAYED_IN`
- Índices nativos en propiedades de identidad

#### 4️⃣ **Limpieza (DAG - Tarea: `clean_up_files`)**
- Traslado de archivos JSON procesados de `data/queue/` a `data/processed/`
- Prepara la cola para el siguiente ciclo

---

## 💾 Stack Tecnológico

| Capa | Tecnología | Versión | Propósito |
|------|-----------|---------|----------|
| **Orquestación** | Apache Airflow | 2.7.0+ | Scheduling + DAG orchestration (SequentialExecutor) |
| **Almacenamiento** | MongoDB | 7.0+ | Base de datos documental (JSON flexible) |
| | Apache Cassandra | 4.1+ | Base de datos columnar (series temporales) |
| | Neo4j | 5.x | Base de datos de grafos (relaciones estructurales) |
| **Procesamiento** | Python | 3.10+ | Lógica ETL, validación de datos |
| | Pandas | 2.0+ | Transformación y limpieza de datos |
| **Visualización** | Streamlit | 1.28+ | Frontend interactivo |
| | Plotly Express | 5.17+ | Gráficos interactivos |
| **Contenedorización** | Docker & Docker Compose | 20.10+ | Infraestructura reproducible |

---

## 🔧 Requisitos Previos

### Requerimientos del Sistema

- **Docker Desktop**: [Descargar](https://www.docker.com/products/docker-desktop) (v20.10+)
  - Configurar **al menos 6GB de RAM** asignada a Docker
  - Mínimo 10GB de espacio en disco disponible

- **Python**: 3.10 o superior
  ```bash
  python --version  # Verificar
  ```

- **pip**: Gestor de paquetes de Python
  ```bash
  pip --version  # Verificar
  ```

- **Git** (opcional, para clonar el repositorio)
  ```bash
  git --version  # Verificar
  ```

### Dependencias Python Locales

Serán instaladas en la Sección "Paso 4" mediante:
```bash
pip install -r requirements.txt
```

---

## 📂 Estructura de Directorios

```
proyectobigdata/
├── airflow/
│   ├── dags/
│   │   └── dag_streaming_pipeline.py    # DAG orquestador (ETL batch optimizado)
│   └── docker-compose.yaml              # Infraestructura Docker (Mongo, Cassandra, Neo4j, Airflow)
├── dashboard/
│   └── app_dashboard.py                 # Frontend interactivo en Streamlit
├── data/
│   ├── raw/
│   │   └── spotify_dataset.csv          # Dataset maestro descargado
│   ├── queue/                           # Buffer temporal monitoreado por Airflow (JSONs entrantes)
│   └── processed/                       # Histórico de JSONs procesados con éxito
├── producer/
│   └── stream_generator.py              # Script productor (simula ingesta continua)
├── scripts/
│   ├── init_cassandra.cql               # Creación de Keyspace y tablas
│   ├── init_mongo.js                    # Creación de colecciones e índices
│   └── init_neo4j.cypher                # Constraints e índices de red
├── requirements.txt                     # Dependencias Python
├── mio.md                               # Notas internas del equipo
└── README.md                            # Esta documentación
```

---

## 🚀 Guía de Despliegue Paso a Paso

### ✅ Paso 1: Preparación del Dataset

1. **Obtener el Dataset**
   - Descargar el archivo `spotify_dataset.csv` desde la fuente acordada del equipo
   - Verificar que contiene **al menos 50,000 registros** con columnas como: `track_id`, `artist_id`, `artist_name`, `track_name`, `genre`, `country`, `play_count`, etc.

2. **Colocar en el Directorio Correcto**
   ```bash
   cp /ruta/a/spotify_dataset.csv ./data/raw/
   ```

3. **Verificar**
   ```bash
   ls -lh ./data/raw/spotify_dataset.csv
   ```

---

### ✅ Paso 2: Levantar la Infraestructura Docker

1. **Navegar a la Carpeta de Airflow**
   ```bash
   cd ./airflow
   ```

2. **Descargar Imágenes e Iniciar Contenedores**
   ```bash
   docker compose up -d
   ```
   
   Esta acción crea y arranca:
   - `airflow-webserver` (puerto 8080)
   - `airflow-scheduler` (orquestador)
   - `mongodb` (puerto 27017)
   - `cassandra` (puerto 9042)
   - `neo4j` (puerto 7474 UI, 7687 Bolt)
   - `postgres` (metadatos de Airflow, puerto 5432)

3. **Verificar que Todos los Contenedores Estén Corriendo**
   ```bash
   docker compose ps
   ```
   
   Esperado: Todos con estado `Up`

4. **Esperar a que Airflow Esté Listo (~30-60 segundos)**
   ```bash
   docker compose logs airflow-webserver | grep "Application startup complete"
   ```

---

### ✅ Paso 3: Inicializar las Bases de Datos

#### 3a. Inicializar MongoDB

1. **Acceder al Contenedor**
   ```bash
   docker exec -it mongodb mongosh
   ```

2. **Ejecutar el Script de Inicialización**
   ```javascript
   // Crear base de datos
   use spotify_analytics

   // Crear colección con validación
   db.createCollection("spotify_events", {
     validator: {
       $jsonSchema: {
         bsonType: "object",
         required: ["track_id", "artist_id", "timestamp"],
         properties: {
           track_id: { bsonType: "string" },
           artist_id: { bsonType: "string" },
           artist_name: { bsonType: "string" },
           track_name: { bsonType: "string" },
           genre: { bsonType: "string" },
           country: { bsonType: "string" },
           play_count: { bsonType: "int" },
           timestamp: { bsonType: "date" }
         }
       }
     }
   })

   // Crear índices
   db.spotify_events.createIndex({ track_id: 1 })
   db.spotify_events.createIndex({ artist_id: 1 })
   db.spotify_events.createIndex({ timestamp: -1 })
   db.spotify_events.createIndex({ country: 1 })

   // Salir
   exit
   ```

#### 3b. Inicializar Cassandra

1. **Copiar el Script CQL**
   ```bash
   docker cp ../scripts/init_cassandra.cql cassandra:/tmp/
   ```

2. **Ejecutar el Script en el Contenedor**
   ```bash
   docker exec -it cassandra cqlsh -f /tmp/init_cassandra.cql
   ```

3. **Verificar** (opcional)
   ```bash
   docker exec -it cassandra cqlsh -e "DESCRIBE KEYSPACE spotify_keyspace;"
   ```

**Contenido esperado del archivo `init_cassandra.cql`:**
```sql
CREATE KEYSPACE IF NOT EXISTS spotify_keyspace
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE spotify_keyspace;

CREATE TABLE IF NOT EXISTS time_series_events (
    country TEXT,
    timestamp TIMESTAMP,
    track_id TEXT,
    artist_id TEXT,
    artist_name TEXT,
    track_name TEXT,
    genre TEXT,
    play_count INT,
    PRIMARY KEY ((country), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
AND compression = {'sstable_compression': 'LZ4Compressor'};

CREATE INDEX idx_artist_id ON time_series_events(artist_id);
CREATE INDEX idx_track_id ON time_series_events(track_id);
```

#### 3c. Inicializar Neo4j

1. **Acceder a la Interfaz Web**
   - Abrir en el navegador: `http://localhost:7474`
   - Credenciales por defecto:
     - **Usuario**: `neo4j`
     - **Contraseña**: `password123` (o la configurada en `docker-compose.yaml`)

2. **Ejecutar el Script Cypher**
   - Copiar el contenido del archivo `scripts/init_neo4j.cypher`
   - Pegarlo en la consola de Neo4j (panel derecho)
   - Presionar Ctrl+Enter para ejecutar

**Contenido esperado del archivo `init_neo4j.cypher`:**
```cypher
// Crear constraints de unicidad
CREATE CONSTRAINT artist_id_unique IF NOT EXISTS FOR (a:Artist) REQUIRE a.id IS UNIQUE;
CREATE CONSTRAINT track_id_unique IF NOT EXISTS FOR (t:Track) REQUIRE t.id IS UNIQUE;
CREATE CONSTRAINT genre_unique IF NOT EXISTS FOR (g:Genre) REQUIRE g.name IS UNIQUE;
CREATE CONSTRAINT country_unique IF NOT EXISTS FOR (c:Country) REQUIRE c.code IS UNIQUE;

// Crear índices de búsqueda de texto
CREATE INDEX artist_name_idx IF NOT EXISTS FOR (a:Artist) ON (a.name);
CREATE INDEX track_name_idx IF NOT EXISTS FOR (t:Track) ON (t.name);

// Crear índices compuestos para queries frecuentes
CREATE INDEX country_timestamp_idx IF NOT EXISTS FOR (e:Event) ON (e.country, e.timestamp);
```

3. **Verificar**
   ```cypher
   SHOW CONSTRAINTS;
   SHOW INDEXES;
   ```

---

### ✅ Paso 4: Ejecutar el Simulador de Streaming

1. **Instalar Dependencias Locales** (una sola vez)
   ```bash
   cd .. # Volver al raíz del proyecto
   pip install -r requirements.txt
   ```

2. **Iniciar el Productor de Eventos**
   - Abrir una **nueva terminal** en la raíz del proyecto
   ```bash
   python producer/stream_generator.py
   ```
   
   Esperado: Verás logs como:
   ```
   [INFO] Stream generator started. Reading from data/raw/spotify_dataset.csv
   [INFO] Generated chunk #1 → data/queue/spotify_events_20260613_223100_0.json
   [INFO] Generated chunk #2 → data/queue/spotify_events_20260613_223120_1.json
   ...
   ```

3. **Dejar Ejecutándose**
   - El script continuará indefinidamente generando JSON cada 20 segundos
   - Para detener: Presionar `Ctrl+C`

---

### ✅ Paso 5: Monitoreo en Airflow

1. **Acceder al Panel Web de Airflow**
   - Abrir: `http://localhost:8080`
   - Credenciales:
     - **Usuario**: `airflow`
     - **Contraseña**: `airflow`

2. **Activar el DAG**
   - En la lista de DAGs, buscar: `pipeline_streaming`
   - Hacer clic en el **botón de encendido** (toggle) en la columna izquierda
   - El DAG ahora se ejecutará automáticamente cada 2 minutos

3. **Monitorear Ejecuciones**
   - Observar la columna "Last Run" para ver timestamp de ejecuciones recientes
   - Hacer clic en una ejecución para ver detalles de tareas individuales
   - Los logs de cada tarea están disponibles en la vista de detalles

4. **Validar Éxito de Tareas**
   - Todos deben mostrar estado **Green** (✓ success)
   - En caso de error (rojo), hacer clic en la tarea para ver logs detallados

---

### ✅ Paso 6: Lanzar el Dashboard Interactivo

1. **Instalar Dependencias del Dashboard** (si no se hizo en Paso 4)
   ```bash
   pip install -r requirements.txt
   ```

2. **Ejecutar Streamlit**
   - Abrir una **nueva terminal** en la raíz
   ```bash
   streamlit run dashboard/app_dashboard.py
   ```
   
   Esperado:
   ```
   ...
     You can now view your Streamlit app in your browser.
   
     Local URL: http://localhost:8501
     Network URL: http://xxx.xxx.x.x:8501
   ```

3. **Acceder al Dashboard**
   - Abrir en el navegador: `http://localhost:8501`
   - Interactuar con gráficos y filtros
   - El dashboard actualiza automáticamente cada 5 segundos desde las BD NoSQL

---

## ✔️ Validación y Monitoreo

### Checklist de Verificación

Después de completar todos los pasos, validar:

- [ ] **Docker**: Todos los contenedores en estado `Up`
  ```bash
  docker compose ps
  ```

- [ ] **MongoDB**: Conectado y con colección inicializada
  ```bash
  docker exec -it mongodb mongosh --eval "db.spotify_analytics.spotify_events.countDocuments()"
  ```

- [ ] **Cassandra**: Keyspace y tabla creados
  ```bash
  docker exec -it cassandra cqlsh -e "SELECT COUNT(*) FROM spotify_keyspace.time_series_events;"
  ```

- [ ] **Neo4j**: Constraints creados
  ```bash
  # En http://localhost:7474 ejecutar:
  SHOW CONSTRAINTS;
  ```

- [ ] **Airflow**: DAG activo y ejecutándose
  - Verificar en `http://localhost:8080/dags`

- [ ] **Stream Generator**: Generando archivos JSON
  ```bash
  ls -lrt data/queue/ | tail -5  # Ver últimos archivos generados
  ```

- [ ] **Dashboard**: Accesible en `http://localhost:8501`

### Métricas Operacionales

Acceder a través del dashboard para verificar:

- **Volumen Diario**: Total de eventos procesados en últimas 24h
- **Top Artistas**: Ranking de artistas más reproducidos
- **Distribución Geográfica**: Heatmap de plays por país
- **Tendencias Temporales**: Series de tiempo de actividad
- **Salud del Pipeline**: Tasa de éxito/error de tareas Airflow

---

## 🐛 Troubleshooting

### Problema: Docker Compose No Inicia

**Síntomas**: `docker compose up -d` falla con errores de conexión

**Solución**:
1. Verificar que Docker Desktop está corriendo: `docker ps`
2. Limpiar contenedores previos:
   ```bash
   docker compose down -v
   docker system prune
   ```
3. Reintentar: `docker compose up -d`
4. Verificar logs: `docker compose logs`

---

### Problema: Airflow No Reconoce el DAG

**Síntomas**: El DAG no aparece en `http://localhost:8080`

**Solución**:
1. Verificar que el archivo está en la ruta correcta:
   ```bash
   ls -la ./airflow/dags/dag_streaming_pipeline.py
   ```
2. Validar sintaxis Python:
   ```bash
   python -m py_compile ./airflow/dags/dag_streaming_pipeline.py
   ```
3. Reiniciar el scheduler de Airflow:
   ```bash
   docker compose restart airflow-scheduler
   ```
4. Revisar logs:
   ```bash
   docker compose logs airflow-scheduler | grep -i "dag"
   ```

---

### Problema: MongoDB o Cassandra Sin Datos

**Síntomas**: Las bases de datos están vacías aunque el pipeline ha corrido

**Solución**:
1. Verificar que el productor está generando archivos:
   ```bash
   ls -la data/queue/
   ```
   - Si está vacío: el script productor no se ejecutó (Paso 4)
   - Si hay archivos: el problema está en el DAG

2. Revisar logs del DAG en Airflow:
   - Hacer clic en la tarea `load_to_mongodb` y ver "Logs"
   - Buscar mensajes de error (credenciales, conectividad, etc.)

3. Verificar conectividad desde contenedor a BD:
   ```bash
   docker exec -it airflow-webserver python -c \
     "from pymongo import MongoClient; MongoClient('mongodb:27017').admin.command('ping')"
   ```

---

### Problema: Streamlit No Se Conecta a Bases de Datos

**Síntomas**: Dashboard muestra errores de conexión

**Solución**:
1. Verificar que los contenedores están activos:
   ```bash
   docker compose ps | grep -E "mongodb|cassandra|neo4j"
   ```

2. Revisar `app_dashboard.py`:
   - Verificar URIs de conexión (hosts deben ser `localhost` o IP de máquina)
   - Confirmar puertos correctos: MongoDB (27017), Cassandra (9042), Neo4j (7687)

3. Reiniciar Streamlit:
   ```bash
   # En la terminal de Streamlit: Ctrl+C
   streamlit run dashboard/app_dashboard.py
   ```

---

### Problema: Alto Uso de CPU/Memoria

**Síntomas**: Docker consume recursos excesivos

**Solución**:
1. Revisar qué contenedor consume más:
   ```bash
   docker stats
   ```

2. Si Cassandra: Aumentar heap size en `docker-compose.yaml`:
   ```yaml
   environment:
     - "MAX_HEAP_SIZE=2G"
     - "HEAP_NEWSIZE=512M"
   ```

3. Si Airflow: Reducir frecuencia del DAG de 2 a 5 minutos:
   ```python
   schedule_interval='*/5 * * * *'  # En dag_streaming_pipeline.py
   ```

---

## 📚 Referencias y Documentación

### Documentación Oficial

- [Apache Airflow Docs](https://airflow.apache.org/docs/)
- [MongoDB Documentation](https://docs.mongodb.com/)
- [Apache Cassandra Docs](https://cassandra.apache.org/doc/latest/)
- [Neo4j Developer Guide](https://neo4j.com/developer/)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [Docker Compose Reference](https://docs.docker.com/compose/compose-file/)

### Recursos del Proyecto

- **Análisis Exploratorio**: `analisis.ipynb` (Jupyter Notebook con EDA inicial)
- **Configuración Local**: `requirements.txt` (Dependencias Python)
- **Inicializadores de BD**: `scripts/` (CQL, MongoDB JS, Cypher)

### Contacto y Soporte

Para preguntas o issues durante el despliegue:

1. **Revisar esta guía** (sección Troubleshooting)
2. **Consultar logs de contenedores**: `docker compose logs [nombre_servicio]`
3. **Contactar al Equipo de Desarrollo**: [Email/Slack del equipo]

---

## 📝 Changelog

| Versión | Fecha | Cambios |
|---------|-------|---------|
| 1.0.0 | 2026-06-14 | Documentación inicial completa |

---

<div align="center">

**Proyecto Final de Big Data** | UTEC Facultad de Computación  
© 2026 — Todos los derechos reservados

</div>

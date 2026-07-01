from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd

# Drivers de conexión
from pymongo import MongoClient, ReplaceOne
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from neo4j import GraphDatabase

default_args = {
    'owner': 'utec_bigdata',
    'start_date': datetime(2026, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

with DAG(
    'pipeline_spotify_multimodelo',
    default_args=default_args,
    description='Pipeline ETL incremental para analítica de Spotify',
    schedule_interval=timedelta(minutes=2), # Revisa la carpeta cada 2 minutos
    catchup=False,
) as dag:

    # RUTAS DE INTERCAMBIO DE ARCHIVOS
    QUEUE_DIR = "/opt/airflow/data/queue"
    PROCESSED_DIR = "/opt/airflow/data/processed"
    

    def extract_and_process_batch(**kwargs):
        """Busca TODOS los archivos en la cola, los unifica, los limpia y los pasa a las BDs"""
        os.makedirs(QUEUE_DIR, exist_ok=True)
        os.makedirs(PROCESSED_DIR, exist_ok=True)

        # 1. Buscar TODOS los archivos JSON en la cola
        files = sorted(f for f in os.listdir(QUEUE_DIR) if f.endswith('.json'))
        if not files:
            print(f"No se encontraron nuevos archivos en la cola: {QUEUE_DIR}")
            return None

        print(f"¡Atención! Se encontraron {len(files)} archivos acumulados. Procesando todos juntos...")

        all_records = []
        
        # 2. Leer, limpiar y acumular el contenido de cada archivo
        for target_file in files:
            file_path = os.path.join(QUEUE_DIR, target_file)
            try:
                df = pd.read_json(file_path)
                
                # Limpieza de datos
                df['track_name'] = df['track_name'].fillna("Unknown Track")
                df['artist_name'] = df['artist_name'].fillna("Unknown Artist")
                df['genre'] = df['genre'].fillna("Unknown")
                df['country'] = df['country'].fillna("Global")
                df['stream_count'] = df['stream_count'].fillna(0).astype(int)
                df['popularity'] = df['popularity'].fillna(0).astype(int)
                df = df.dropna(subset=['track_id'])
                df = df.where(pd.notnull(df), None)
                
                # Unir a la lista maestra de registros
                all_records.extend(df.to_dict(orient="records"))
            except Exception as e:
                print(f"Error leyendo el archivo {target_file}: {e}")

        # Guardar la megamuestra de datos y la lista de archivos procesados en XCom
        kwargs['ti'].xcom_push(key='spotify_data', value=all_records)
        kwargs['ti'].xcom_push(key='processed_files', value=files)
        print(f"Total de registros listos para insertar en las 3 BDs: {len(all_records)}")

    def clean_up_file(**kwargs):
        """Mueve TODOS los archivos procesados a la carpeta 'processed'"""
        ti = kwargs['ti']
        files = ti.xcom_pull(key='processed_files', task_ids='extract_and_validate')
        if not files: return

        # Mover cada uno de los archivos que leímos
        for target_file in files:
            src = os.path.join(QUEUE_DIR, target_file)
            dst = os.path.join(PROCESSED_DIR, target_file)
            if os.path.exists(src):
                os.rename(src, dst)
        
        print(f"Éxito: Se limpió la cola moviendo {len(files)} archivos a 'processed'.")
    
    
    def load_to_mongo(**kwargs):
        """Guarda los JSONs crudos tal cual en MongoDB"""
        ti = kwargs['ti']
        data = ti.xcom_pull(key='spotify_data', task_ids='extract_and_validate')
        if not data: return

        # Conectar a MongoDB usando el nombre del contenedor como Host
        client = MongoClient("mongodb://mongodb:27017/")
        db = client["spotify_db"]
        collection = db["raw_events"]

        # Carga idempotente para que reintentos del DAG no dupliquen documentos.
        operations = []
        for row in data:
            track_id = str(row.get('track_id') or '').strip()
            if not track_id:
                continue
            document = dict(row)
            document['_id'] = track_id
            operations.append(ReplaceOne({'_id': track_id}, document, upsert=True))

        if operations:
            collection.bulk_write(operations, ordered=False)
        client.close()
        print(f"Éxito: {len(operations)} documentos guardados/actualizados en MongoDB.")

    def load_to_cassandra(**kwargs):
        """Transforma e inserta los datos en la tabla columnar de Cassandra"""
        ti = kwargs['ti']
        data = ti.xcom_pull(key='spotify_data', task_ids='extract_and_validate')
        if not data: return

        # Conectar a Cassandra usando el nombre del contenedor como Host
        cluster = Cluster(
            ['cassandra'],
            port=9042,
            protocol_version=5,
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='datacenter1')
        )
        session = cluster.connect()

        # Preparar la consulta de inserción (Usa las columnas exactas de tu CSV)
        query = """
            INSERT INTO spotify_analytics.streams_by_country_genre
            (country, genre, stream_count, popularity, track_id, track_name, artist_name, release_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        prepared = session.prepare(query)

        # Insertar registro por registro de forma eficiente
        for row in data:
            session.execute(prepared, (
                str(row.get('country', 'Global')),
                str(row.get('genre', 'Unknown')),
                int(row.get('stream_count', 0)),
                int(row.get('popularity', 0)),
                str(row.get('track_id', '')),
                str(row.get('track_name', '')),
                str(row.get('artist_name', '')),
                str(row.get('release_date', ''))
            ))
        
        cluster.shutdown()
        print(f"Éxito: {len(data)} registros indexados en Cassandra.")

    def load_to_neo4j(**kwargs):
        """Construye el grafo de relaciones en Neo4j (Artista -> Cancion -> Genero)"""
        ti = kwargs['ti']
        data = ti.xcom_pull(key='spotify_data', task_ids='extract_and_validate')
        if not data: return

        # Conexión al contenedor de Neo4j
        driver = GraphDatabase.driver("bolt://neo4j:7687", auth=("neo4j", "password123"))

        # Consulta Cypher por lote para fusionar nodos y relaciones sin duplicados
        cypher_query = """
        UNWIND $rows AS row
        MERGE (a:Artist {name: row.artist_name})
        MERGE (g:Genre {name: row.genre})
        MERGE (t:Track {id: row.track_id})
        SET t.name = row.track_name,
            t.popularity = row.popularity,
            t.stream_count = row.stream_count,
            t.release_date = row.release_date,
            t.duration_ms = row.duration_ms
        MERGE (a)-[:PERFORMS]->(t)
        MERGE (t)-[:BELONGS_TO_GENRE]->(g)
        """

        rows = []
        for row in data:
            track_id = str(row.get('track_id') or '').strip()
            if not track_id:
                continue

            rows.append({
                'artist_name': str(row.get('artist_name') or 'Unknown Artist'),
                'genre': str(row.get('genre') or 'Unknown'),
                'track_id': track_id,
                'track_name': str(row.get('track_name') or 'Unknown Track'),
                'release_date': str(row.get('release_date') or ''),
                'duration_ms': int(row.get('duration_ms') or 0),
                'popularity': int(row.get('popularity') or 0),
                'stream_count': int(row.get('stream_count') or 0),
            })

        with driver.session() as session:
            session.execute_write(lambda tx: tx.run(cypher_query, rows=rows).consume())

        driver.close()
        print(f"Éxito: {len(rows)} registros vinculados en el grafo de Neo4j.")


    def generate_kpis(**kwargs):
        """Calcula KPIs ejecutivos y actualiza un único documento en MongoDB.

        Documento destino:
        - Base: spotify_db
        - Colección: kpis
        - Documento: _id = spotify_kpis_current

        Nota:
        En Streamlit los porcentajes se guardan como 0-100.
        En Power BI las medidas DAX devuelven 0-1 y se formatean como porcentaje.
        """

        client = MongoClient("mongodb://mongodb:27017/")
        db = client["spotify_db"]
        raw_events = db["raw_events"]
        kpis = db["kpis"]

        def safe_float(value):
            try:
                if value is None:
                    return 0.0
                return float(value)
            except Exception:
                return 0.0

        def safe_pct(numerator, denominator):
            denominator = safe_float(denominator)
            if denominator == 0:
                return 0
            return round((safe_float(numerator) / denominator) * 100, 2)

        def parse_release_date(value):
            try:
                if value is None:
                    return None

                text_value = str(value).strip()

                if not text_value:
                    return None

                # Caso: release_date viene como año, por ejemplo "2018"
                if text_value.isdigit() and len(text_value) == 4:
                    year = int(text_value)
                    if 1900 <= year <= 2100:
                        return datetime(year, 1, 1)

                parsed_date = pd.to_datetime(text_value, errors="coerce")

                if pd.isna(parsed_date):
                    return None

                return parsed_date.to_pydatetime().replace(tzinfo=None)

            except Exception:
                return None

        # =========================
        # Base general
        # =========================
        total_canciones_procesadas = raw_events.count_documents({})

        total_streams_result = list(
            raw_events.aggregate(
                [
                    {
                        "$group": {
                            "_id": None,
                            "total_streams": {"$sum": "$stream_count"},
                        }
                    }
                ],
                allowDiskUse=True,
            )
        )

        total_streams = (
            safe_float(total_streams_result[0].get("total_streams"))
            if total_streams_result
            else 0
        )

        # =========================
        # KPI 1:
        # Concentración de Streams del Top 10 Artistas (%)
        # =========================
        top10_artistas = list(
            raw_events.aggregate(
                [
                    {
                        "$match": {
                            "artist_name": {
                                "$nin": [None, "", "Unknown Artist"]
                            }
                        }
                    },
                    {
                        "$group": {
                            "_id": "$artist_name",
                            "streams_artista": {"$sum": "$stream_count"},
                        }
                    },
                    {"$sort": {"streams_artista": -1}},
                    {"$limit": 10},
                    {
                        "$project": {
                            "_id": 0,
                            "artista": "$_id",
                            "streams_artista": 1,
                        }
                    },
                ],
                allowDiskUse=True,
            )
        )

        streams_top10_artistas = sum(
            safe_float(item.get("streams_artista")) for item in top10_artistas
        )

        concentracion_top10_artistas_pct = safe_pct(
            streams_top10_artistas,
            total_streams,
        )

        # =========================
        # KPI 2:
        # Participación del Género Líder (%)
        # =========================
        genero_lider_result = list(
            raw_events.aggregate(
                [
                    {
                        "$match": {
                            "genre": {"$nin": [None, "", "Unknown"]}
                        }
                    },
                    {
                        "$group": {
                            "_id": "$genre",
                            "streams_genero": {"$sum": "$stream_count"},
                        }
                    },
                    {"$sort": {"streams_genero": -1}},
                    {"$limit": 1},
                    {
                        "$project": {
                            "_id": 0,
                            "genero": "$_id",
                            "streams_genero": 1,
                        }
                    },
                ],
                allowDiskUse=True,
            )
        )

        genero_lider_nombre = None
        streams_genero_lider = 0

        if genero_lider_result:
            genero_lider_nombre = genero_lider_result[0].get("genero")
            streams_genero_lider = safe_float(
                genero_lider_result[0].get("streams_genero")
            )

        participacion_genero_lider_pct = safe_pct(
            streams_genero_lider,
            total_streams,
        )

        # =========================
        # KPI 3:
        # Participación del País Líder (%)
        # =========================
        pais_lider_result = list(
            raw_events.aggregate(
                [
                    {
                        "$match": {
                            "country": {"$nin": [None, "", "Global"]}
                        }
                    },
                    {
                        "$group": {
                            "_id": "$country",
                            "streams_pais": {"$sum": "$stream_count"},
                        }
                    },
                    {"$sort": {"streams_pais": -1}},
                    {"$limit": 1},
                    {
                        "$project": {
                            "_id": 0,
                            "pais": "$_id",
                            "streams_pais": 1,
                        }
                    },
                ],
                allowDiskUse=True,
            )
        )

        pais_lider_nombre = None
        streams_pais_lider = 0

        if pais_lider_result:
            pais_lider_nombre = pais_lider_result[0].get("pais")
            streams_pais_lider = safe_float(
                pais_lider_result[0].get("streams_pais")
            )

        participacion_pais_lider_pct = safe_pct(
            streams_pais_lider,
            total_streams,
        )

        # =========================
        # KPIs a nivel canción
        # Agrupamos por track_id para evitar contar varias veces una misma canción.
        # =========================
        canciones = list(
            raw_events.aggregate(
                [
                    {
                        "$match": {
                            "track_id": {"$nin": [None, ""]}
                        }
                    },
                    {
                        "$group": {
                            "_id": "$track_id",
                            "track_name": {"$first": "$track_name"},
                            "artist_name": {"$first": "$artist_name"},
                            "stream_count": {"$sum": "$stream_count"},
                            "popularity": {"$max": "$popularity"},
                            "release_date": {"$first": "$release_date"},
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "track_id": "$_id",
                            "track_name": 1,
                            "artist_name": 1,
                            "stream_count": 1,
                            "popularity": 1,
                            "release_date": 1,
                        }
                    },
                ],
                allowDiskUse=True,
            )
        )

        total_canciones_unicas = len(canciones)

        # =========================
        # KPI 4:
        # Porcentaje de Canciones Exitosas
        # Popularity >= 80
        # =========================
        canciones_exitosas = sum(
            1 for song in canciones
            if safe_float(song.get("popularity")) >= 80
        )

        porcentaje_canciones_exitosas = safe_pct(
            canciones_exitosas,
            total_canciones_unicas,
        )

        # =========================
        # KPI 5:
        # Porcentaje de Canciones Populares
        # stream_count > 100,000,000
        # =========================
        canciones_populares = sum(
            1 for song in canciones
            if safe_float(song.get("stream_count")) > 100_000_000
        )

        porcentaje_canciones_populares = safe_pct(
            canciones_populares,
            total_canciones_unicas,
        )

        # =========================
        # KPI 6:
        # Edad Promedio del Top 100 más Escuchado
        # =========================
        fecha_referencia = datetime.now()

        top100_canciones = sorted(
            canciones,
            key=lambda song: safe_float(song.get("stream_count")),
            reverse=True,
        )[:100]

        edades_top100 = []

        for song in top100_canciones:
            release_date = parse_release_date(song.get("release_date"))

            if release_date is None:
                continue

            edad_anios = (fecha_referencia - release_date).days / 365.25

            if edad_anios >= 0:
                edades_top100.append(edad_anios)

        edad_promedio_top100 = (
            round(sum(edades_top100) / len(edades_top100), 2)
            if edades_top100
            else 0
        )

        # =========================
        # Documento final para Streamlit
        # =========================
        kpi_document = {
            "fecha_actualizacion": datetime.now(),

            "total_canciones_procesadas": total_canciones_procesadas,
            "total_canciones_unicas": total_canciones_unicas,
            "total_streams": total_streams,

            "kpi_01_concentracion_streams_top10_artistas_pct": concentracion_top10_artistas_pct,
            "kpi_02_participacion_genero_lider_pct": participacion_genero_lider_pct,
            "kpi_03_participacion_pais_lider_pct": participacion_pais_lider_pct,
            "kpi_04_porcentaje_canciones_exitosas": porcentaje_canciones_exitosas,
            "kpi_05_porcentaje_canciones_populares": porcentaje_canciones_populares,
            "kpi_06_edad_promedio_top100_mas_escuchado": edad_promedio_top100,

            "streams_top10_artistas": streams_top10_artistas,
            "top10_artistas": top10_artistas,

            "genero_lider_nombre": genero_lider_nombre,
            "streams_genero_lider": streams_genero_lider,

            "pais_lider_nombre": pais_lider_nombre,
            "streams_pais_lider": streams_pais_lider,

            "canciones_exitosas": canciones_exitosas,
            "canciones_populares": canciones_populares,

            "fecha_referencia_edad": fecha_referencia.isoformat(),
        }

        kpis.update_one(
            {"_id": "spotify_kpis_current"},
            {
                "$set": kpi_document,
                "$unset": {
                    "kpi_01_reproduccion_promedio_canciones": "",
                    "kpi_02_popularidad_promedio_generos": "",
                    "kpi_03_popularidad_promedio_paises": "",
                    "kpi_04_numero_artistas": "",
                    "kpi_05_total_reproducciones": "",
                    "kpi_06_promedio_canciones_por_artista": "",
                    "numero_generos_analizados": "",
                    "numero_paises_analizados": "",
                    "kpi_02_popularidad_segun_genero": "",
                    "kpi_03_popularidad_segun_pais": "",
                    "kpi_05_genero_con_mas_reproducciones": "",
                    "kpi_06_artista_con_mas_reproducciones": "",
                },
            },
            upsert=True,
        )

        client.close()

        print("Éxito: KPIs ejecutivos actualizados correctamente en MongoDB.")
        print(kpi_document)

    # TAREAS DE AIRFLOW
    task_extract = PythonOperator(
        task_id='extract_and_validate',
        python_callable=extract_and_process_batch,
        provide_context=True
    )

    task_mongo = PythonOperator(
        task_id='load_to_mongodb',
        python_callable=load_to_mongo,
        provide_context=True
    )

    task_cassandra = PythonOperator(
        task_id='load_to_cassandra',
        python_callable=load_to_cassandra,
        provide_context=True
    )

    task_cleanup = PythonOperator(
        task_id='clean_up_file',
        python_callable=clean_up_file,
        provide_context=True
    )

    task_neo4j = PythonOperator(
        task_id='load_to_neo4j',
        python_callable=load_to_neo4j,
        provide_context=True
    )

    task_kpis = PythonOperator(
        task_id='generate_kpis',
        python_callable=generate_kpis,
        provide_context=True
    )

    # FLUJO DEL PIPELINE
    # Primero se cargan las 3 bases de datos, luego se actualizan KPIs y finalmente se limpia la cola.
    task_extract >> [task_mongo, task_cassandra, task_neo4j] >> task_kpis >> task_cleanup

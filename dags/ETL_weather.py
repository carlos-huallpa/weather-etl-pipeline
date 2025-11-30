"""
Nombre: ETL Datos clima
Descripción: Obtiene datos metereológicos utilizando hooks para conectarse con Open Meteo y S3
Autor: Carlos H
Fecha: 27-11-25
"""

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import pandas as pd
import os
import logging

# ============================================================================
# CONFIGURACIÓN Y CONSTANTES
# ============================================================================

# Variables de entorno
API_CONN_ID = "open_meteo_api"
AWS_CONN_ID = "aws_default"
S3_BUCKET = "weather-r4-pr"

# Constantes
RAW_DIR = "/opt/airflow/dags/data/raw"
PROCESSED_DIR = "/opt/airflow/dags/data/processed"
FECHA = datetime.now().strftime("%Y-%m-%d")
LONGITUDE = '-64.3489782'
LATITUDE = '-33.1237585'
logger = logging.getLogger(__name__)

# Parámetros del DAG
DEFAULT_ARGS = {
    "owner": "carlos_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ============================================================================
# DEFINICIÓN DE FUNCIONES
# ============================================================================

# Extracción 
def extract():
    """Extrae datos de la fuente via hooks"""
    try:
        os.makedirs(RAW_DIR, exist_ok=True)
        
        http_hook = HttpHook(http_conn_id = API_CONN_ID, method = "GET")
        endpoint = (f"v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}"
            "&daily=temperature_2m_max,temperature_2m_min,precipitation_sum"
            "&timezone=auto&past_days=1&forecast_days=0")
        response = http_hook.run(endpoint)
        if response.status_code != 200:
                raise Exception(f"API returned status {response.status_code}")
        data = response.json()
        df_daily = pd.DataFrame(data["daily"])
        df = pd.DataFrame({
            "date": df_daily["time"],
            "temp_max": df_daily["temperature_2m_max"],
            "temp_min": df_daily["temperature_2m_min"],
            "precip": df_daily["precipitation_sum"],
        })
        df.to_csv(f"{RAW_DIR}/raw_{FECHA}.csv", index=False)
        logger.info(f"Extraídos {len(df)} registros exitosamente")
    except Exception as e:
        logger.error(f"Error en extracción: {str(e)}")
        raise

# Validación y Tranformación
def transform():
    """Transforma los datos."""
    try:
        os.makedirs(PROCESSED_DIR, exist_ok=True)
        
        df = pd.read_csv(f"{RAW_DIR}/raw_{FECHA}.csv")

        # Validación de datos esperados
        required_columns = ['date', 'temp_max', 'temp_min', 'precip']
        missing_cols = set(required_columns) - set(df.columns)
        if missing_cols:
            raise ValueError(f"Columnas faltantes: {missing_cols}")
        
        # Validación de nulls críticos
        if df[['temp_max', 'temp_min']].isnull().any().any():
            raise ValueError("Temperaturas tienen valores nulos")

        # Validación lógica de temperaturas
        invalid = df[df['temp_max'] < df['temp_min']]
        if not invalid.empty:
            raise ValueError(f"{len(invalid)} registros con temp_max < temp_min")
        
        # Calculo de parametros: Amplitud térmica, estrés térmico, helada
        
        df["temp_range"] = df["temp_max"] - df["temp_min"]
        df["heat_stress"] = (df["temp_max"] >= 32).astype(int)
        df["frost"] = (df["temp_min"] < 0).astype(int)
        
        df.to_csv(f"{PROCESSED_DIR}/processed_{FECHA}.csv", index=False)
        logger.info(f"Transformados {len(df)} registros exitosamente")
    except Exception as e:
            logger.error(f"Error en tranformación: {str(e)}")
            raise

# Carga de datos
def load():
    """Carga datos procesados a S3."""
    try:
        file = f"{PROCESSED_DIR}/processed_{FECHA}.csv"
        s3_hook = S3Hook(AWS_CONN_ID)
        KEY = f"processed-data/processed_{FECHA}.csv"

        s3_hook.load_file(
            filename= file,
            key= KEY,
            bucket_name= S3_BUCKET,
            replace = True
            )

        logger.info(f"Cargados registros a S3 exitosamente")
    except Exception as e:
        logger.error(f"Error cargando datos a S3: {str(e)}")
        raise    

# ============================================================================
# DEFINICIÓN DEL DAG
# ============================================================================

with DAG(
    dag_id="weather_etl_pipeline",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    description="ETL diario: Open-Meteo API >> Transformación >> S3",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["weather", "data-engineering", "pipeline"],
    max_active_runs=1,
) as dag:

    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    transform_task = PythonOperator(task_id="transform", python_callable=transform)
    load_task = PythonOperator(task_id="load", python_callable=load)

    extract_task >> transform_task >> load_task
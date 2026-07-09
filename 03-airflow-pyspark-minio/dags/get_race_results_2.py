import json
import pathlib
import datetime as dt
from os import environ as env

import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from include.s3_client import S3Client


def _is_api_available(**context):
    """Verifica que la API de F1 responda correctamente."""
    url = "https://f1api.dev/api/current/last/race"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        print("API is available.")
    except requests_exceptions.RequestException as e:
        raise ConnectionError(f"Error connecting to API: {e}")


def _save_raw_race_results(**context):
    """Guarda el JSON original retornado por la API sin transformaciones."""
    input_path = context["templates_dict"]["input_path"]
    output_path = context["templates_dict"]["output_path"]
    load_date = context["templates_dict"]["load_date"]

    pathlib.Path(output_path).mkdir(parents=True, exist_ok=True)

    try:
        with open(input_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        output_file = f"{output_path}/race_results_raw_{load_date}.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(raw_data, f, indent=2, ensure_ascii=False)

        print(f"JSON original guardado en {output_file}")

        try:
            s3_client = S3Client()
            bucket_name = env.get("BUCKET_NAME")
            if not bucket_name:
                raise ValueError("BUCKET_NAME no está configurado en las variables de entorno")

            s3_client.create_bucket(bucket_name)
            object_name = f"raw_race_results/{load_date}/race_results.json"
            s3_client.upload_file(output_file, bucket_name, object_name)
            print(f"Archivo subido exitosamente a s3://{bucket_name}/{object_name}")

        except (ConnectionError, PermissionError, FileNotFoundError, ValueError) as e:
            print(f"Error al subir archivo a MinIO: {str(e)}")
            raise
        except Exception as e:
            print(f"Error inesperado al subir archivo: {str(e)}")
            raise

    except FileNotFoundError as e:
        print(f"Error: No se encontró el archivo de entrada: {str(e)}")
        raise
    except json.JSONDecodeError as e:
        print(f"Error: Fallo al parsear el archivo JSON: {str(e)}")
        raise
    except Exception as e:
        print(f"Error inesperado al procesar los resultados: {str(e)}")
        raise


params = {"execution_date": ""}

default_args = {
    "owner": "data_engineer",
    "start_date": dt.datetime(2026, 3, 6),
    "end_date": dt.datetime(2026, 12, 10),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=1),
}

with DAG(
    dag_id="get_race_results_2",
    schedule_interval=None,
    default_args=default_args,
    params=params,
    tags=["ingestion", "bronze", "race_results"],
    catchup=False,
):
    is_api_available = PythonOperator(
        task_id="is_api_available",
        python_callable=_is_api_available,
    )

    fetch_race_results = BashOperator(
        task_id="fetch_race_results",
        bash_command=(
            "curl -o /tmp/race_results_{{ yesterday_ds_nodash }}.json "
            "-L 'https://f1api.dev/api/current/last/race'"
        ),
    )

    save_raw_race_results = PythonOperator(
        task_id="save_raw_race_results",
        python_callable=_save_raw_race_results,
        templates_dict={
            "input_path": "/tmp/race_results_{{ yesterday_ds_nodash }}.json",
            "output_path": "/tmp/data/{{ yesterday_ds }}",
            "load_date": "{{ yesterday_ds_nodash }}",
        },
    )

    notify = BashOperator(
        task_id="notify",
        bash_command='echo "There are now $(ls /tmp/data/ | wc -l) files."',
    )

    is_api_available >> fetch_race_results >> save_raw_race_results >> notify

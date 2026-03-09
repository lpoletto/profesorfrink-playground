import json
import pathlib
import datetime as dt
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def _is_api_available(**context):
    """Función para verificar la disponibilidad de la API de F1."""
    year = context["templates_dict"]["year"]
    round_num = 1  # Verificar la primera carrera de la temporada
    url = f"https://f1api.dev/api/{year}/{round_num}/race"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status() # Lanza error si no es 200 OK
        print(f"URL {url}")
        print("API is available.")
    except requests_exceptions.RequestException as e:
        raise ConnectionError(f"Error connecting to API: {e}")


def _get_race_results(output_path, **context):
    """Función para parsear y descargar los resultados de la carrera de F1."""
    # Crear el directorio si no existe
    input_path = context["templates_dict"]["input_path"]
    load_date = context["templates_dict"]["load_date"]
    pathlib.Path(output_path).mkdir(parents=True, exist_ok=True)

    # Descargar los resultados de la carrera
    with open(input_path, "r", encoding="utf-8") as f:
        raw_data = json.load(f)

        # Metadata de la carrera
        race_id = raw_data["races"]["raceId"]
        race_name = raw_data["races"]["raceName"]
        race_season = raw_data["season"]
        race_round = raw_data["races"]["round"]
        race_date = raw_data["races"]["date"]
        circuit_id = raw_data["races"]["circuit"]["circuitId"]
        circuit_name = raw_data["races"]["circuit"]["circuitName"]

        # Resultados de la carrera
        race_results = []
        for result in raw_data["races"]["results"]:
            record = {
                "race_id": race_id,
                "race_name": race_name,
                "race_season": race_season,
                "rece_round": race_round,
                "race_date": race_date,
                "circuit_id": circuit_id,
                "circuit_name": circuit_name,

                "driver_id": result["driver"]["driverId"],
                "driver_name": result["driver"]["name"] + " " + result["driver"]["surname"],
                "team_id": result["team"]["teamId"],
                "team_name": result["team"]["teamName"],
                "position": result["position"],
                "grid_position": result["grid"],
                "points": result["points"],
                "time": result["time"],
                "fastest_lap_time": result["fastLap"] if result.get("fastLap") else None,
                "retired": result["retired"] if result.get("retired") else None,
            }
            race_results.append(record)
        # Guardar los resultados en un archivo JSON
        output_file = f"{output_path}/race_results_{load_date}.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(race_results, f, indent=2, ensure_ascii=False)
        print(f"{len(race_results)} registros guardados en {output_file}")


# Argumentos por defecto del DAG
params = {"execution_date": ""}

default_args = {
    "owner": "data_engineer",
    "start_date": dt.datetime(2026, 3, 6),
    "end_date": dt.datetime(2026, 12, 10),
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=1),
}

with DAG(
    dag_id="fetch_all_reace_results",
    # schedule_interval="0 0 * * MON", # Ejecutar los lunes a medianoche
    catchup=False,
    default_args=default_args,
    params= params,
    tags=["ingestion", "bronze", "reace_results"],
):
    is_api_available = PythonOperator(
        task_id="is_api_available",
        python_callable= _is_api_available,
        templates_dict={
            "year": "{{yesterday_ds_nodash[:4]}}",
        },
    )

    fetch_race_results = BashOperator(
        task_id="fetch_race_results",
        bash_command=(
            "curl -o /tmp/race_results_{{ yesterday_ds_nodash }}.json "
            "-L 'https://f1api.dev/api/{{ yesterday_ds_nodash[:4] }}/1/race'"
        ),
    )

    get_race_results = PythonOperator(
        task_id="get_race_results",
        python_callable=_get_race_results,
        templates_dict={
            "input_path": "/tmp/race_results_{{ yesterday_ds_nodash}}.json",
            "load_date": "{{ yesterday_ds_nodash }}",
        },
        op_kwargs={
            "output_path": "/tmp/data"
        },
    )

    notify = BashOperator(
        task_id="notify",
        bash_command='echo "There are now $(ls /tmp/data/ | wc -l) files."',
    )

    is_api_available >> fetch_race_results >> get_race_results >> notify
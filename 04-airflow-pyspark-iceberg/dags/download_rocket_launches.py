import json
import pathlib
import datetime
import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def _get_pictures():
    """Función para parsear y descargar todas las imágenes de los cohetes."""
    # Crear el directorio si no existe
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)

    # Descargar todas las imágenes en launches.json
    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]
        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                # Obtener solo el nombre del archivo seleccionando todo después del último /. Por ejemplo: https://host/RocketImages/Electron.jpg_1440.jpg -> Electron.jpg_1440.jpg
                image_filename = image_url.split("/")[-1]
                target_file = f"/tmp/images/{image_filename}"
                with open(target_file, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded {image_url} to {target_file}.")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")


with DAG(
    dag_id="download_rocket_launches",
    start_date=airflow.utils.dates.days_ago(14), # fecha de inicio de la primera ejecución del dag
    schedule_interval="@daily", # Intervalo que deberia ejecutarse el dag
):
    download_launches = BashOperator(
        task_id="download_launches",
        bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    )

    get_pictures = PythonOperator(
        task_id="get_pictures",
        python_callable=_get_pictures,
    )

    notify = BashOperator(
        task_id="notify",
        bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images."',
    )

    download_launches >> get_pictures >> notify

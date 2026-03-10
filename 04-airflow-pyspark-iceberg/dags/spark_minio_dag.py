from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    'spark_csv_to_minio',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['spark', 'minio']
) as dag:

    submit_spark_job = SparkSubmitOperator(
        task_id='submit_csv_to_minio_job',
        application='./include/scripts/spark_job.py',
        conn_id='spark_default',
        # Usamos la variable JARS_PATH configurada en airflow_settings.yaml
        jars=Variable.get('JARS_PATH'),
        name='airflow_csv_to_minio',
        verbose=True
    )
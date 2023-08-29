from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

image_python = "sua-imagem:tag"

dag = DAG(
    "datapipeline-nyc",
    schedule_interval=None,  # Defina o agendamento conforme necessário
    start_date=datetime(2023, 8, 19),  # Defina a data de início
    catchup=False,  # Impede a execução retroativa das tarefas
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
    },
)

with dag:
    extraction_api_from_bucket = DockerOperator(
        task_id="extraction_api_from_bucket",
        image=image_python,
        command="python -m scripts.extraction_api_from_bucket",
    )

    loading_bucket_bq_raw_data = DockerOperator(
        task_id="loading_bucket_bq_raw_data",
        image=image_python,
        command="python -m scripts.loading_bucket_bq_raw_data",
    )

    # Tarefa para executar os scripts DBT
    process_dbt = DockerOperator(
        task_id="process_dbt",
        image=image_python,
        command=(
            "bash -c 'pip install dbt-bigquery && "
            "dbt run --select tag:nyc_test --profiles-dir /app/nyc_dbt/ --project-dir /app/nyc_dbt/ --target dev'"
        ),
    )

    extraction_api_from_bucket >> loading_bucket_bq_raw_data >> process_dbt
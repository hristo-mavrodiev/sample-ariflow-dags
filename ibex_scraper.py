
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='ibex_scraper',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2025,7,25),
    catchup=False,
) as dag:

    run_ibex_job = KubernetesPodOperator(
        task_id='run_ibex_container',
        name='ibex-job',
        namespace='airflow',
        image='hristomavrodiev/ibex:latest',
        cmds=["python", "main.py"],
        env_vars={
            'AWS_ACCESS_KEY_ID': '{{ var.value.AWS_ACCESS_KEY_ID }}',
            'AWS_SECRET_ACCESS_KEY': '{{ var.value.AWS_SECRET_ACCESS_KEY }}',
            'AWS_DEFAULT_REGION': 'eu-central-1'
        },
        get_logs=True,
        is_delete_operator_pod=True,
    )

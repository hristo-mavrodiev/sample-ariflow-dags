from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
with DAG(
    "tutorial_hm_data_intervals",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="A simple tutorial DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 10, 10),
    catchup=False,
    tags=["hm"],
) as dag:

    # t1, t2 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id="print_date1",
        bash_command="echo {{ ds_nodash }}",
    )

    t2 = BashOperator(
        task_id="print_date2",
        bash_command="echo {{ ts }}",
    )

    t3 = BashOperator(
        task_id="print_date3",
        bash_command="echo {{ logical_date }}",
    )

    t4 = BashOperator(
        task_id="print_date4",
        bash_command="echo {{ data_interval_start }}",
    )
    t5 = BashOperator(
        task_id="print_date5",
        bash_command="echo {{ data_interval_end }}",
    )
    t6 = BashOperator(
        task_id="print_date6",
        bash_command="echo {{ next_ds_nodash }}",
    )
    t1 >> t2 >> t3 >> t4 >> t5 >> t6

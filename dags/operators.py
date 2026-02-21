from datetime import datetime
from airflow.sdk import dag
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor

@dag(
    dag_id="simple_bash_dag",
    start_date=datetime(2026,2,21),
    schedule="@daily",
    catchup=False,
    tags=["operatos"]
)

def bash_operators():
    #defining tasks
    list_dag=BashOperator(
        task_id="task1_listing_the_dags",
        bash_command="echo 'Files in dags dir: ' && ls -lh /opt/airflow/dags",
    )

    #File sensor
    wait_for_csv=FileSensor(
        task_id="wait_for_files_to_appear",
        filepath="/opt/airflow/dags/sample.csv",
        fs_conn_id="fs_default",
        poke_interval=30,
        timeout=300,
        mode="poke"
    )

    #as of now all the tasks of this DAG are independent
    #To make dependent, that is adding dependencies bw tasks (UPSTREAM AND DOWNSTREAM)
    wait_for_csv >> list_dag

dags=bash_operators()

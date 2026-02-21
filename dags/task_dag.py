from datetime import datetime
from airflow.sdk import dag, task

@dag(
    schedule="@daily",
    start_date=datetime(2025, 2, 18),
    catchup=False,
    tags=["demo","task_dag"]
)


def task_dag_first():

    @task
    def starting():
        print("Independent DAG gets started...")
    
    @task
    def extract():
        data=["apple", "banana", "melons"]
        return data
    
    @task
    def transform(data):
        transformed_data=[i.upper() for i in data]
        #raise("TRansfroed exception handled")
        return transformed_data
    
    @task 
    def load(data):
        for i in data:
            print(f"Loaded item: {i}")

    starting()
    load(transform(extract()))

dag=task_dag_first()

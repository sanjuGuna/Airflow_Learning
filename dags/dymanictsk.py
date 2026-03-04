from airflow.sdk import dag, task
from datetime import datetime
from airflow.utils.task_group import TaskGroup
@dag(
    schedule=None,
    start_date=datetime(2026,3,3),
    tags=["task_group"]
)

def dytask():
    
    @task
    def extracted_files():
        return ["file1.csv", "file2.csv", "file3.csv", "file4.csv"]
    
    @task
    def process(file):
        print(f"Processing {file}")
    
    ex=extracted_files()
    process.expand(file=ex)
dag=dytask()
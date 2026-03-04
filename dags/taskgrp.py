from airflow.sdk import dag, task
from datetime import datetime
from airflow.utils.task_group import TaskGroup
@dag(
    schedule=None,
    start_date=datetime(2026,3,3),
    tags=["task_group"]
)

def taskgrp():#grouping of similar tasks to organise and strucuting
    with TaskGroup("Extracting_group") as extract:
        @task
        def extract_user():
            print("Extracting users")
    
        @task
        def extract_user_playlist():
            raise Exception("Error while extracing playlist")
            print("extracting user playlist")
        
        extract_user()
        extract_user_playlist()
    
    @task
    def transform():
        print("Transfromed")
    
    extract >> transform()

dag=taskgrp()
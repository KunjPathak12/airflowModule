from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
string = "My First DAG"
def printTask():
    print(string)

with DAG(dag_id="myDag",\
         start_date=datetime(2023,2,27),\
         schedule="@hourly",\
         catchup=False) as dag:
    printing = PythonOperator(task_id="1",python_callable=printTask)

printing
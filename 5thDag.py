from airflow import *
from airflow.operators.python import *
from airflow.operators.bash import *
from datetime import *


defaults = {
    "owner":"Kunj Pathak",
    "retries":3,
    "retry_delay": timedelta(minutes=3)
}

def getter(ti):
    ti.xcom_push(key="Task 1", value="ran")

def getter1(ti):
    ti.xcom_push(key="Task 2", value="running parallely with Task 3")

def getter2(ti):
    ti.xcom_push(key="Task 3", value="running parallely with Task 2")

def setter(ti):
    val1 = ti.xcom_pull(task_ids="get",key="Task 1")
    val2 = ti.xcom_pull(task_ids="get1",key="Task 2")
    val3 = ti.xcom_pull(task_ids="get2",key="Task 3")
    print("Task1 {} successfully. And, Task2, Task3 are {} and {] respectively.".format(val1,val2,val3))

with DAG(default_args=defaults,\
         schedule="* * * * *",\
         start_date=datetime(2023,3,3),\
         dag_id="5thDag")as dag:

    task0 = PythonOperator(task_id="get",python_callable=getter)
    task1 = PythonOperator(task_id="get1",python_callable=getter1)
    task2 = PythonOperator(task_id="get2",python_callable=getter2)
    task3 = BashOperator(task_id="echoing", bash_command="echo All the task are completed successfully.")

task0 >> [task1,task2] >> task3
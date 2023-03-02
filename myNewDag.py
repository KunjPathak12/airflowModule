from airflow import DAG
from airflow.operators.python import *
from datetime import *

def printTaskWParam(fname,mname,lname):
    print("Hello i am {} {} {}this is a DAG with params.".format(fname,mname,lname))


def printTaskWOParam():
    string = "Just for the sake of creating the dependencies"
    print(string)

defaults = {
    "owner" : "Kunj Pathak",
    "retries" : 5,
    "retry_delay" : timedelta(minutes=5)
}

with DAG(dag_id="myNewDAG",\
         start_date=datetime(2023,2,28),\
         schedule="@daily",\
         catchup=False,\
         default_args=defaults)as dag:
    task0 = PythonOperator(task_id="printWithParam",\
                           python_callable=printTaskWOParam,\
                           op_kwargs={"fname":"Kunj", "mname":"Hiteshkumar","lname":"Pathak"})

    task1 = PythonOperator(task_id="normalPrint",\
                           python_callable=printTaskWOParam)
task1.set_upstream(task0)
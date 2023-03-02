from airflow import DAG
from airflow.operators.python import *
from datetime import *


def setter(ti):
    ti.xcom_push(key="fname", value="Kunj")
    ti.xcom_push(key="mname", value="Hiteshkumar")
    ti.xcom_push(key="lname", value="Pathak")

def getter(ti):
    fname = ti.xcom_pull(task_ids="setter", key="fname")
    mname = ti.xcom_pull(task_ids="setter", key="mname")
    lname = ti.xcom_pull(task_ids="setter", key="lname")
    print("Hello i am {} {} {}this is a DAG with params.".format(fname,mname,lname))


defaults = {
    "owner" : "Kunj Pathak",
    "retries" : 5,
    "retry_delay" : timedelta(minutes=5)
}
with DAG(dag_id="3rdDag",\
         start_date=datetime(2023,2,28),\
         schedule="@hourly",\
         catchup=False,\
         default_args=defaults,\
         description="dag created using xcom for getting \
         and setting return value")as dag:
    task0 = PythonOperator(task_id="setVal",\
                           python_callable=setter)

    task1 = PythonOperator(task_id="getVal",\
                           python_callable=getter)
task0 >> task1


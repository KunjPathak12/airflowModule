from airflow.operators.python import *
from airflow.operators.bash import *
from datetime import *
from airflow import *



defaults = {
    "owner":"Kunj Pathak",
    "retries":5,
    "retry_delay": timedelta(minutes=5)
}


def setter(ti):
    ti.xcom_push(key="position", value="intern")
    ti.xcom_push(key="task",value="Airflow Interview")
    ti.xcom_push(key="i_name", value="sr.DE")

def getter(ti):
    position = ti.xcom_pull(task_ids="setter",key="position")
    task     = ti.xcom_pull(task_ids="setter",key="task")
    i_name   = ti.xcom_pull(task_ids="setter",key="i_name")
    print("Interview on the following topic of {}, for the position of {} will be taken by one of our {}".format(task,position,i_name))



with DAG(dag_id="4thDag",\
         start_date= datetime(2023,3,2),\
         default_args=defaults,\
         # with cron syntax
         schedule="0 14-15 * 3 3-5",\
         catchup= True, \
         description="dag created using xcom  also scheduled with cron syntax\
         for getting and setting return value"
         ) as dag:
    task0 = PythonOperator(task_id="pushVal",\
                           python_callable=setter)
    task1 = PythonOperator(task_id="getVal",\
                           python_callable=getter)
    task2 = BashOperator(task_id="echoContent",\
                         bash_command="echo successfully value pushed and fetched")

task0 >> [task1,task2]
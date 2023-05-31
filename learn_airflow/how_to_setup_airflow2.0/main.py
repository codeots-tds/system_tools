import pandas as pd

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


"""1. Establish DAG Settings Object"""
default_args = {
    'owner': 'CodeOTS',
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    #timedelta = duration/difference between two dates or times.
    #timedelta object here represents a duration of 5minutes waittime before another retry if a task fails.
}

"""2. Create a dag object"""
dag = DAG(
    dag_id = 'my_dag', #name of dag
    default_args = default_args, #default args in step 1
    schedule_interval='0 0 * * *' #runs daily at midnight
)

"""3. Tasks"""
task1 = BashOperator(
    task_id = 'task1', #name of task
    bash_command='echo "Hello, Airflow!!!!" >>>>>>>>>>>>>>>',
    # bash_command='echo "Hello, Airflow!!!!"',
    dag = dag
)

task2 = BashOperator(
    task_id = 'task2',
    bash_command= 'echo "Running task 2" <<<<?????????',
    # bash_command= 'echo "Running task 2"',
    dag=dag
)


"""4. Define task dependencies"""
task1 >> task2
#here task one needs to be executed before task 2

"""
                            Other Notes:
When you start the Airflow Scheduler using the airflow scheduler, the DAGs will get scanned
and execute your DAG in the order based on the tasks.
"""
"""5. create airflow user credentials"""
"""
airflow users create \ --username \ --firstname \ --lastname \ --role Admin \ --email
"""
"""6. turn load examples off in airflow.cfg file"""

"""7. Can't see Dags for task 1 & 2 in Airflow UI:

This is because the airflow dags folder parameter needs to be adjusted with path 
where your dags are located. This might be where docker-airflow comes in handy.
"""





from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


def print_hello():
    print("Hello from Airflow!")
    return "Hello World"


with DAG(
    dag_id="hello_world",
    description="A simple hello world DAG",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example"],
) as dag:

    # Task 1: Print hello using Python
    task_hello = PythonOperator(
        task_id="say_hello",
        python_callable=print_hello,
    )

    # Task 2: Print date using Bash
    task_date = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

    # Task 3: Print goodbye
    task_goodbye = BashOperator(
        task_id="say_goodbye",
        bash_command='echo "Goodbye from Airflow!"',
    )

    # Define task order: hello -> date -> goodbye
    task_hello >> task_date >> task_goodbye

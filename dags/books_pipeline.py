from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def _transform_books():
    pass


default_args = {"owner": "muhfadhil"}

with DAG(
    dag_id="books_pipeline",
    description="A pipeline to extract data from gutendex API and store the extracted date to postgres db",
    default_args=default_args,
) as dag:
    get_books = BashOperator(
        task_id="get_books",
        bash_command="""
            curl -o /opt/airflow/dags/books.json 'https://gutendex.com/books/'
        """,
    )

    transform_books = PythonOperator(
        task_id="transform_books",
        python_callable=_transform_books,
    )

    get_books >> transform_books

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from payment_etl_logic import to_sql_db


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 13),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'payment_update',
    default_args=default_args,
    description='My fee payment ETL DAG',
    schedule_interval='@daily',
)

run_etl = PythonOperator(
    task_id='run_etl',
    python_callable=to_sql_db,
    dag=dag
)

run_etl
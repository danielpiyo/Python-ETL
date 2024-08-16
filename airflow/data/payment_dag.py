from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
# from payment_etl_logic import main

import pandas as pd
import sqlalchemy as db
import pymysql

payment_table = 'data/Block.xlsx'
engine = db.create_engine("mysql+pymysql://root@localhost:3306/customers?charset=utf8mb4")

def main():
    payments = pd.read_excel(payment_table, sheet_name='data')

    payments = payments[['admissionNo', 'studentName', 'balance']]

    payments.to_sql('payments', engine, if_exists='append', index=False)

    print("ETL Script Exacuted succesfully")


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
    python_callable=main,
    dag=dag
)

run_etl
import pandas as pd
import sqlalchemy as db
import pymysql

payment_table = '/opt/airflow/data/MarketingCampaignsData.csv'


def to_sql_db():
    payments = pd.read_csv(payment_table)  
    engine = db.create_engine("mysql+pymysql://root@localhost:3306/customers?charset=utf8mb4")
    payments.to_sql('payments', engine, if_exists='replace', index=False)

    print("ETL Script Exacuted succesfully")

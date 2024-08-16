import pandas as pd
from sqlalchemy import create_engine
import pymysql


def to_sql_db():
    local_ip = 'http://172.16.5.42'
    user = 'root'
    password = ''
    db = 'customers'
    port = '3306'

    connection_string = f"mysql+pymysql://{user}@{local_ip}:{port}/{db}?charset=utf8mb4"
    engine = create_engine(connection_string)

    payment_table = '/opt/airflow/data/MarketingCampaignsData.csv'
    payments = pd.read_csv(payment_table)  
   
    payments.to_sql('payments', engine, if_exists='replace', index=False)

    print("ETL Script Exacuted succesfully")

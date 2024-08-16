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
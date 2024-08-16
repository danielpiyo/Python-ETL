FROM apache/airflow:latest

USER root
RUN apt-get update && \
    apt-get -y install git && \    
    apt-get clean
   

USER airflow
RUN pip install pandas && \
    pip install sqlalchemy && \
    pip install pymysql
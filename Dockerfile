#Use the official Airflow image
FROM apache/airflow:2.9.3

USER root

# Install git and other necessary tools
RUN apt-get update && apt-get install -y git && apt-get clean
#Create directory for dbt config


USER airflow


ENV AIRFLOW_HOME=/opt/airflow
ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
#give permission to access logs
RUN chown -R airflow: /opt/airflow/logs 

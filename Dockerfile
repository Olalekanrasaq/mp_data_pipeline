FROM apache/airflow:3.0.6-python3.10

USER root
RUN apt-get update && apt-get install -y \
    default-jre \
    && apt-get clean
USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dags /opt/airflow/dags

ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=/opt/airflow/dags

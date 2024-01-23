FROM apache/airflow:2.0.0-python3.8

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get update -qq && apt-get install vim wget -qqq

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
FROM apache/airflow:latest-python3.8
USER root

ARG AIRFLOW_HOME=/opt/airflow
ADD dags /opt/airflow/dags
ENV PYTHONPATH=/opt/airflow/dags:$PYTHONPATH

USER airflow
RUN pip install --upgrade pip


COPY ./requirements.txt .
RUN pip3 install -r requirements.txt

USER ${AIRFLOW_UID}





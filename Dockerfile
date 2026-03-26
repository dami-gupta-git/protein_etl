FROM apache/airflow:2.10.5-python3.11
USER root

# Install Java (required for PySpark)
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

ARG AIRFLOW_HOME=/opt/airflow
ADD dags /opt/airflow/dags
ENV PYTHONPATH=/opt/airflow/dags:$PYTHONPATH

USER airflow

COPY ./requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

USER ${AIRFLOW_UID}





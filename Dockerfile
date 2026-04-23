FROM apache/airflow:3.1.8

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

WORKDIR /opt/airflow


COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY ingestion_layer/ /opt/airflow/ingestion_layer/
COPY dbt/ /opt/airflow/dbt/
COPY airflow/dags/ /opt/airflow/dags/
COPY utils/ /opt/airflow/utils/


ENV PYTHONPATH=/opt/airflow

CMD ["airflow", "standalone"]
FROM apache/airflow:3.1.8

USER airflow

RUN pip install --no-cache-dir \
    "dbt-core==1.11.7" \
    "dbt-snowflake==1.11.3"
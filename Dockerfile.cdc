FROM classic_airflow_runtime
USER airflow
COPY requirements.txt /tmp/classic-cdc-requirements.txt
RUN python -m venv --without-pip /home/airflow/classic-cdc && python -m pip --python /home/airflow/classic-cdc/bin/python install --no-index --find-links=/tmp/classic-wheels -r /tmp/classic-cdc-requirements.txt && python -m pip --python /home/airflow/classic-cdc/bin/python check
ENV PATH="/home/airflow/classic-cdc/bin:$PATH" PYTHONPATH=/app PYTHONUNBUFFERED=1
RUN python -c "import pyodbc, confluent_kafka; assert 'ODBC Driver 18 for SQL Server' in pyodbc.drivers()"
WORKDIR /app
ENTRYPOINT []
COPY --chown=airflow:root scripts/ /app/
COPY sql/ /classic-sql/
RUN test -r /app/local_accounts.py && test -r /app/common/connections.py

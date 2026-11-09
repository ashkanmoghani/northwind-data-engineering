FROM apache/airflow:2.10.5-python3.10
USER root
RUN set -eu; if ! odbcinst -q -d -n 'ODBC Driver 18 for SQL Server' >/dev/null 2>&1; then apt-get update; apt-get install -y --no-install-recommends curl ca-certificates unixodbc; curl -fsSL "https://packages.microsoft.com/config/debian/$(. /etc/os-release; echo "$VERSION_ID")/packages-microsoft-prod.deb" -o /tmp/ms.deb; dpkg -i /tmp/ms.deb; apt-get update; ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18; rm -rf /var/lib/apt/lists/* /tmp/ms.deb; fi
USER airflow
COPY airflow/requirements.txt /tmp/classic-requirements.txt
COPY wheels/ /tmp/classic-wheels/
RUN cd /tmp/classic-wheels && sha256sum -c SHA256SUMS
RUN pip install --no-index --find-links=/tmp/classic-wheels --no-cache-dir -r /tmp/classic-requirements.txt && pip check && python -c "import airflow,pyodbc,docker; assert airflow.__version__=='2.10.5' and 'ODBC Driver 18 for SQL Server' in pyodbc.drivers()"
COPY --chown=airflow:root scripts/ /opt/airflow/scripts/
COPY --chown=airflow:root airflow/dags/ /opt/airflow/dags/
COPY airflow/init.py /opt/classic-init.py

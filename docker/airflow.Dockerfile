# Airflow con dbt-sqlserver y las dependencias del batch para correr las tareas
FROM apache/airflow:2.10.3-python3.11

USER root
ENV ACCEPT_EULA=Y
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl gnupg unixodbc \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft.gpg \
    && echo "deb [arch=amd64,arm64 signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
       > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

USER airflow
# dbt va en la misma imagen por simplicidad. En producción convendría
# aislarlo (KubernetesPodOperator o un venv dedicado) para no mezclar dependencias.
RUN pip install --no-cache-dir \
    "dbt-core~=1.8.0" \
    "dbt-sqlserver~=1.8.0" \
    "pandas>=2.2" "numpy>=1.26" "SQLAlchemy>=2.0.30" \
    "pydantic>=2.7" "pydantic-settings>=2.3" "httpx>=0.27" \
    "pyodbc>=5.1" "psycopg[binary]>=3.1"

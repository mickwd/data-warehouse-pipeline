#!/usr/bin/env bash
# Aplica los scripts DDL de sql/sqlserver en orden, cuando SQL Server ya acepta conexiones.
set -euo pipefail

SQLCMD=/opt/mssql-tools18/bin/sqlcmd

for attempt in $(seq 1 60); do
  if "$SQLCMD" -S sqlserver -U sa -P "$MSSQL_SA_PASSWORD" -C -Q "SELECT 1" > /dev/null 2>&1; then
    break
  fi
  echo "Esperando a SQL Server (intento $attempt)..."
  sleep 2
done

for script in /sql/*.sql; do
  echo "Aplicando $script"
  "$SQLCMD" -S sqlserver -U sa -P "$MSSQL_SA_PASSWORD" -C -b -i "$script"
done

echo "DDL del Data Warehouse aplicado"

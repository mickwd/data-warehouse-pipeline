-- Se ejecuta una sola vez al crear el volumen de Postgres (docker-entrypoint-initdb.d).
-- Una misma instancia aloja tres bases lógicas separadas por usuario:
--   shop    : OLTP de origen para la carga batch
--   metrics : históricos de las ventanas del streaming
--   airflow : metadata de Airflow
CREATE USER shop    WITH PASSWORD 'shop';
CREATE USER metrics WITH PASSWORD 'metrics';
CREATE USER airflow WITH PASSWORD 'airflow';

CREATE DATABASE shop    OWNER shop;
CREATE DATABASE metrics OWNER metrics;
CREATE DATABASE airflow OWNER airflow;

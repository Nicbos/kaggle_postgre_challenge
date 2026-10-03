CREATE ROLE bench WITH LOGIN CREATEDB SUPERUSER PASSWORD 'bench';
CREATE DATABASE postgres_data_manager OWNER bench;
ALTER SYSTEM SET log_min_duration_statement = 200;

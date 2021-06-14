#!/usr/bin/env bash

docker compose up;

docker exec -it 5c7bc6f2c8ce /bin/bash

cd /var/lib/postgresql/external

psql -U airflow

CREATE DATABASE dvdrental;

CREATE ROLE postgres WITH LOGIN PASSWORD 'postgres';

##ALTER ROLE postgres WITH SUPERUSER;

GRANT ALL PRIVILEGES ON DATABASE dvdrental TO postgres;

\q

##psql -U  postgres  -d dvdrental  < restore.sql


pg_restore -U airflow -d dvdrental ./dvdrental


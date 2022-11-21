## Create Star Schema to PostgreSQL with Python-Airflow

In this project, we'll implement the concepts in data modeling with Postgres and build an ETL pipeline using Python on Apache Airflow. 
We will define fact and dimension tables for a star schema for a user behavior, and write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.


## Deployment


1 - Clone this repo

2 - Install Docker and Docker-Compose on local machine

3 - Make sure pip is fully upgraded on local machine

```bash
  pip install --upgrade pip==20.2.4 --user
```

4 - Run pip install apache-airflow

```bash
  pip install apache-airflow
```

5 - Cd to the project root dir (where Dockerfile is located)

6 - Run command: docker-compose up -d (-d runs in detached mode so you don't see the outputs, there will be some errors in outputs, but the webserver still runs ok)

```bash
  docker-compose up -d
```

7 - Run command: docker-compose down to shut down all containers when you want to stop Airflow

```bash
  docker-compose down -v
```

8 - Test that Airflow is running by visiting localhost:8080 on local machine (if it isn't running, make sure docker containers are ok with: docker ps command)

9 - Test that PgAdmin is running by visiting localhost:5050 on local machine


## Used technologies and versions

Python                  3.9.7 \
Apache Airflow          2.3.0 \
Postgres                13.6 \
PgAdmin4 \
Docker Desktop          4.8.0 - version 20.10.14 \
Docker Compose version  v2.5.0

FROM puckel/docker-airflow
LABEL maintainer="Luciana Ferreira da Rocha"

ADD dags/* /usr/local/airflow/dags
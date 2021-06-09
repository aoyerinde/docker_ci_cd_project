ARG PYTHON_BASE_IMAGE="python:3.6-slim-buster"

FROM ${PYTHON_BASE_IMAGE}

RUN apt update && apt upgrade -yq \
    && apt install --no-install-recommends -yq \
    build-essential \
    openssh-client \
    rsync

ENV AIRFLOW_HOME=/root/airflow
COPY ./requirements.txt ./
RUN pip3 install -r requirements.txt
 
RUN airflow initdb \
    && sed -i -e 's/load_examples =.*/load_examples = False/' ${AIRFLOW_HOME}/airflow.cfg \
    && mkdir ${AIRFLOW_HOME}/dags

WORKDIR ${AIRFLOW_HOME}

ENTRYPOINT ["airflow"]
CMD ["-h"]
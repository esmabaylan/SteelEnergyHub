FROM jupyter/all-spark-notebook:x86_64-spark-3.5.0

USER root

RUN wget https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    -P /usr/local/spark/jars/

COPY requirements.txt /tmp/requirements.txt
RUN python -m pip install --no-cache-dir -r /tmp/requirements.txt

USER jovyan
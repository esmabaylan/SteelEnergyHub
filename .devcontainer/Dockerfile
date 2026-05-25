FROM jupyter/all-spark-notebook:x86_64-spark-3.5.0

USER root

RUN wget https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    -P /usr/local/spark/jars/

RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    -P /usr/local/spark/jars/ && \
    wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar \
    -P /usr/local/spark/jars/ && \
    wget https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
    -P /usr/local/spark/jars/ && \
    wget https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    -P /usr/local/spark/jars/

RUN mkdir -p /home/jovyan/work/spark/streaming \
             /home/jovyan/work/spark/jars \
             /home/jovyan/work/src/producer

RUN pip install pyspark==3.5.0

COPY requirements.txt /tmp/requirements.txt
RUN python -m pip install --no-cache-dir -r /tmp/requirements.txt

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH
ENV SPARK_JARS_DIR=/usr/local/spark/jars

USER jovyan
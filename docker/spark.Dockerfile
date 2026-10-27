# Image du traitement : Spark 4.0 Structured Streaming + connecteurs Kafka, Iceberg (S3) et PostgreSQL (ADR 002).
FROM apache/spark:4.0.4-scala2.13-java17-python3-ubuntu

USER root

# Connecteurs Java, versions figées, téléchargés depuis Maven Central à la construction (jamais au démarrage).
ARG MAVEN=https://repo1.maven.org/maven2
RUN cd /opt/spark/jars \
 && for jar in \
      org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.4/spark-sql-kafka-0-10_2.13-4.0.4.jar \
      org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.4/spark-token-provider-kafka-0-10_2.13-4.0.4.jar \
      org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar \
      org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar \
      org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.10.2/iceberg-spark-runtime-4.0_2.13-1.10.2.jar \
      org/apache/iceberg/iceberg-aws-bundle/1.10.2/iceberg-aws-bundle-1.10.2.jar \
      org/postgresql/postgresql/42.7.8/postgresql-42.7.8.jar ; \
    do wget -q "$MAVEN/$jar" || exit 1; done

# Dépendances Python du traitement (pandas et pyarrow pour l'état par borne, psycopg pour la diffusion).
COPY requirements-spark.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements-spark.txt

# pyspark importable sans spark-submit (tests, maintenance) ; src/ et spark/ pour le code du projet.
# Les processus Python « workers » lancés par Spark héritent de cette variable : ils trouvent aussi lumio_stream.
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip:/opt/lumio/src:/opt/lumio/spark \
    TZ=UTC

WORKDIR /opt/lumio
COPY pyproject.toml ./
COPY src ./src
COPY spark ./spark
COPY tests ./tests
RUN mkdir -p /checkpoints && chown -R spark:spark /opt/lumio /checkpoints

USER spark
CMD ["/opt/spark/bin/spark-submit", "--master", "local[4]", "--driver-memory", "1800m", "spark/streaming_job.py"]

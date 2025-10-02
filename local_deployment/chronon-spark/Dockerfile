FROM spark:3.5.3-scala2.12-java11-python3-ubuntu

ENV JAVA_HOME=/opt/java/openjdk \
    SPARK_HOME=/opt/spark \
    SCALA_HOME=/opt/scala \
    SPARK_CONF_DIR=/srv/chronon/conf \
    PATH=/opt/spark/bin:/opt/java/openjdk/bin:$PATH

WORKDIR /srv/chronon

RUN mkdir -p \
    /srv/chronon/conf \
    /srv/chronon/data \
    /srv/chronon/scripts \
    /srv/chronon/metastore \
    /srv/chronon/jars \
    /srv/chronon/app


USER spark

CMD ["bash", "-lc", "sleep infinity"]
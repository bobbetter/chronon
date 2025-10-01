FROM openjdk:11-jre-slim

ARG SPARK_VERSION=3.5.3
ARG HADOOP_VERSION=3
ARG SCALA_VERSION=2.12.18

ENV JAVA_HOME=/usr/local/openjdk-11
ENV SPARK_PACKAGE="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"
ENV SPARK_HOME=/opt/spark
ENV SCALA_HOME=/opt/scala
ENV PATH=${SCALA_HOME}/bin:${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${JAVA_HOME}/bin:${PATH}

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl bash tini procps ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt \
    && curl -fSL --retry 5 --retry-delay 5 --retry-connrefused \
      https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz \
      -o /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && ln -s /opt/${SPARK_PACKAGE} ${SPARK_HOME} \
    && rm /tmp/spark.tgz

RUN curl -fSL --retry 5 --retry-delay 5 --retry-connrefused \
      https://downloads.lightbend.com/scala/${SCALA_VERSION}/scala-${SCALA_VERSION}.tgz \
      -o /tmp/scala.tgz \
    && tar -xzf /tmp/scala.tgz -C /opt \
    && ln -s /opt/scala-${SCALA_VERSION} ${SCALA_HOME} \
    && rm /tmp/scala.tgz

RUN mkdir -p /opt/chronon/jars /opt/chronon/workdir

WORKDIR /opt/chronon/workdir

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["bash"]


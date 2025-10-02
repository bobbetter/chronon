FROM spark:3.5.3-scala2.12-java11-python3-ubuntu

ENV JAVA_HOME=/opt/java/openjdk \
    SPARK_HOME=/opt/spark \
    SCALA_HOME=/opt/scala \
    SPARK_CONF_DIR=/srv/chronon/conf \
    PATH=/home/spark/.local/bin:/opt/spark/bin:/opt/java/openjdk/bin:$PATH

WORKDIR /srv/chronon

ARG ZIPLINE_AI_WHEEL=out/python/wheel.dest/dist/zipline_ai-0.0.32-py3-none-any.whl
COPY ${ZIPLINE_AI_WHEEL} /tmp/zipline_ai-0.0.32-py3-none-any.whl

RUN mkdir -p \
    /srv/chronon/conf \
    /srv/chronon/data \
    /srv/chronon/scripts \
    /srv/chronon/metastore \
    /srv/chronon/jars

USER root

ENV PYTHON_VERSION=3.12.6

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        wget \
        ca-certificates \
        libssl-dev \
        zlib1g-dev \
        libbz2-dev \
        libreadline-dev \
        libsqlite3-dev \
        libffi-dev \
        libgdbm-dev \
        libnss3-dev \
        libncursesw5-dev \
        liblzma-dev \
        uuid-dev \
        tk-dev && \
    rm -rf /var/lib/apt/lists/*

RUN curl -fsSLo /tmp/Python-${PYTHON_VERSION}.tgz https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz && \
    tar -xzf /tmp/Python-${PYTHON_VERSION}.tgz -C /tmp && \
    cd /tmp/Python-${PYTHON_VERSION} && \
    ./configure --enable-optimizations --with-ensurepip=install && \
    make -j"$(nproc)" && \
    make altinstall && \
    ln -sf /usr/local/bin/python3.12 /usr/local/bin/python && \
    ln -sf /usr/local/bin/python3.12 /usr/local/bin/python3 && \
    ln -sf /usr/local/bin/pip3.12 /usr/local/bin/pip && \
    ln -sf /usr/local/bin/pip3.12 /usr/local/bin/pip3 && \
    rm -rf /tmp/Python-${PYTHON_VERSION} /tmp/Python-${PYTHON_VERSION}.tgz


RUN mkdir -p /home/spark && \
    mkdir -p /srv/chronon/app && \
    chown -R spark:spark /srv/chronon /home/spark /srv/chronon/app && \
    chmod -R u+w /srv/chronon /usr/local/lib/python3.12 /usr/local/bin /srv/chronon/app

RUN pip install /tmp/zipline_ai-0.0.32-py3-none-any.whl && \
    rm /tmp/zipline_ai-0.0.32-py3-none-any.whl

USER spark

CMD ["bash", "-lc", "sleep infinity"]
# Airflow + Java + spark-submit, necessarios pro SparkSubmitOperator.

FROM apache/airflow:2.8.4-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.7.1 \
    pyspark==3.5.3 \
    pandas==2.2.1 \
    pyarrow==15.0.2

USER root
RUN mkdir -p /opt/spark-link && chown airflow: /opt/spark-link
USER airflow
RUN ln -s "$(python -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')" /opt/spark-link/pyspark

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark-link/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

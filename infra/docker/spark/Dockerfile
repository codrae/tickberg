FROM bitnamilegacy/spark:3.5.1

USER root

ARG ICEBERG_VERSION=1.5.2
ARG SPARK_MAJOR=3.5
ARG SCALA_MAJOR=2.12
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_BUNDLE_VERSION=1.12.262
ARG KAFKA_CLIENT_VERSION=3.5.1

RUN install_packages curl ca-certificates

WORKDIR /opt/bitnami/spark/jars

# Iceberg runtime + AWS bundle (S3 FileIO + Glue Catalog v2 SDK)
RUN curl -fsSLO "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR}_${SCALA_MAJOR}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR}_${SCALA_MAJOR}-${ICEBERG_VERSION}.jar" \
 && curl -fsSLO "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
 && curl -fsSLO "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
 && curl -fsSLO "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
 && curl -fsSLO "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_MAJOR}/3.5.1/spark-sql-kafka-0-10_${SCALA_MAJOR}-3.5.1.jar" \
 && curl -fsSLO "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_MAJOR}/3.5.1/spark-token-provider-kafka-0-10_${SCALA_MAJOR}-3.5.1.jar" \
 && curl -fsSLO "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENT_VERSION}/kafka-clients-${KAFKA_CLIENT_VERSION}.jar" \
 && curl -fsSLO "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"

COPY spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf
COPY metrics.properties /opt/bitnami/spark/conf/metrics.properties

USER 1001

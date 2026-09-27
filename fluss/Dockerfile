FROM apache/fluss:1.0.0

ARG MAVEN=https://repo1.maven.org/maven2

RUN cd /opt/fluss/plugins/iceberg \
 && wget -q \
      ${MAVEN}/org/apache/iceberg/iceberg-aws/1.10.1/iceberg-aws-1.10.1.jar \
      ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/1.10.1/iceberg-aws-bundle-1.10.1.jar \
      ${MAVEN}/org/apache/iceberg/iceberg-hive-metastore/1.10.1/iceberg-hive-metastore-1.10.1.jar \
      ${MAVEN}/org/apache/hive/hive-exec/2.3.10/hive-exec-2.3.10.jar \
      ${MAVEN}/org/apache/hadoop/hadoop-client-api/3.3.6/hadoop-client-api-3.3.6.jar \
      ${MAVEN}/org/apache/hadoop/hadoop-client-runtime/3.3.6/hadoop-client-runtime-3.3.6.jar \
      ${MAVEN}/commons-logging/commons-logging/1.2/commons-logging-1.2.jar \
      ${MAVEN}/com/fasterxml/jackson/core/jackson-core/2.15.2/jackson-core-2.15.2.jar \
      ${MAVEN}/com/fasterxml/jackson/core/jackson-databind/2.15.2/jackson-databind-2.15.2.jar \
      ${MAVEN}/com/fasterxml/jackson/core/jackson-annotations/2.15.2/jackson-annotations-2.15.2.jar

COPY server.yaml /opt/fluss/conf/server.yaml

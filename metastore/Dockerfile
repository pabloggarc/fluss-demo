# Iceberg 1.10 (used by Fluss 1.0) speaks the HMS 2.x/3.x thrift protocol; 4.x is not compatible.

FROM apache/hive:3.1.3

ENV SERVICE_NAME=metastore \
    HADOOP_CLASSPATH=/opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.1.0.jar:/opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.271.jar

COPY core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
COPY entrypoint.sh /metastore-entrypoint.sh

ENTRYPOINT ["bash", "/metastore-entrypoint.sh"]

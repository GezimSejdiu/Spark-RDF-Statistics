
FROM bde2020/spark-java-template:2.2.0-hadoop2.7

MAINTAINER Gezim Sejdiu <g.sejdiu@gmail.com>

ENV SPARK_APPLICATION_MAIN_CLASS net.sansa_stack.rdf.spark.RDFStats
ENV SPARK_APPLICATION_JAR_NAME Spark-RDF-Statistics-0.0.1-SNAPSHOT-jar-with-dependencies
ENV SPARK_APPLICATION_ARGS "-i hdfs://namenode:8020/user/root/input/rdf.nt -o hdfs://namenode:8020/user/root/output/"
ENV HDFS_URL=hdfs://hdfs:9000


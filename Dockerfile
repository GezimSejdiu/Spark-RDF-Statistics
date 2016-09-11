
FROM bde2020/spark-java-template:1.6.2-hadoop2.6

MAINTAINER Gezim Sejdiu <g.sejdiu@gmail.com>

ENV SPARK_APPLICATION_MAIN_CLASS net.sansa.rdfstatistics.spark.App
ENV SPARK_APPLICATION_JAR_NAME Spark-RDF-Statistics-0.0.1-SNAPSHOT-jar-with-dependencies
ENV SPARK_APPLICATION_ARGS "/data/input /data/output"
COPY src/main/resources/dbpedia_sample.nt /usr/src/app/dbpedia_sample.nt
ENV HDFS_URL=hdfs://hdfs:9000


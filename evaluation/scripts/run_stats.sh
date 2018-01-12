#!/bin/bash

SPARK_PATH=${SPARK_HOME}

INPUT=$1
OUTPUT=$2

# Log Location on Server.
LOG_LOCATION=~/logs
exec > >(tee -i ${INPUT}_Stats.log)
exec 2>&1



# the number of executors requested
NUM_EXECUTORS=35 #11 # 3 executors per node except the master node
# controls the executor heap size
EXECUTOR_MEMORY=19g #50g #255/3=63.75  63.75*0.07=4.46   64.75 - 4.46 ~ 60
# the number of cores
EXECUTOR_CORES=5 #15

HADOOP_MASTER=hdfs://172.18.160.31:8020

$SPARK_PATH/bin/spark-submit \
--class net.sansa_stack.examples.spark.rdf.RDFStats \
--master "spark://qrowd1:7077" \
--num-executors $NUM_EXECUTORS \
--executor-memory $EXECUTOR_MEMORY \
--executor-cores $EXECUTOR_CORES \
--driver-memory 4G \
$HADOOP_MASTER/GezimSejdiu/DistLODStats/RDFStats_2.11-2017-06.1-SNAPSHOT.jar \
-i $HADOOP_MASTER/GezimSejdiu/DistLODStats/${INPUT}.nt \
-o $HADOOP_MASTER/GezimSejdiu/DistLODStats/output/${INPUT}-${OUTPUT}-Stats
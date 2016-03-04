package net.sansa.rdfstatistics.spark.utils

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.{ Map => JMap }

object SparkUtils {

  /** Specify the master for Spark*/
  //final val SPARK_MASTER:String = "spark://spark-master:7077";
  final val SPARK_MASTER: String = "local[2]";

  def createSparkConf(master: String, jobName: String, sparkHome: String, jars: Array[String],
    environment: JMap[String, String]): SparkConf = {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(jobName)
      .setJars(jars)
    conf.set("spark.kryo.registrator", System.getProperty("spark.kryo.registrator", "org.sansa.rdfstatistics.spark.utils.Registrator"))
  }

  def createSparkContext(master: String, jobName: String, sparkHome: String, jars: Array[String],
    environment: JMap[String, String]): SparkContext = {
    val conf = createSparkConf(master, jobName, sparkHome, jars, environment)
    new SparkContext(conf)
  }

}
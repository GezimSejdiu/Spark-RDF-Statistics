package net.sansa.rdfstatistics.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source
import java.io.File
import org.apache.commons.io.FileUtils
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io.PrintWriter
import scala.tools.nsc.io.Jar
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkConf
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION
import net.sansa.rdfstatistics.spark.utils.Logging
import net.sansa.rdfstatistics.spark.io._
import net.sansa.rdfstatistics.spark.rdfstats.RDFStatistics
import org.apache.spark.storage.StorageLevel
import net.sansa.rdfstatistics.spark.utils.SparkUtils

object App extends Logging {

  def main(args: Array[String]): Unit = {

    val sparkMasterUrl = System.getenv("SPARK_MASTER_URL")

    val sparkConf = new SparkConf()
      .setMaster(SparkUtils.getSparkMasterURL())
      .setAppName("Spark-RDF-Statistics")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "512")

    val sparkContext = new SparkContext(sparkConf)
    SparkUtils.setLogLevels(org.apache.log4j.Level.WARN, Seq("org.apache", "spark", "org.eclipse.jetty", "akka", "org"))

    val file = args(0)
    val rdf_stats_file = new File(file).getName
    val outputPath = args(1)

    logger.info("Runing RDF-Statistics....")
    val startTime = System.currentTimeMillis()

    // load triples
    val triples = TripleReader.loadFromFile(file, sparkContext, 2)

    // compute  criterias
    val rdf_statistics = RDFStatistics(triples, sparkContext)

    // write statistics on disk
    TripleWriter.voidify(rdf_statistics,rdf_stats_file , outputPath)
    //TripleWriter.writeToFile(rdf_statistics, outputPath)
    println("finished computing RDF statistics for  " + rdf_stats_file + " in " + (System.currentTimeMillis() - startTime) + "ms.")


    sparkContext.stop()
  }
}


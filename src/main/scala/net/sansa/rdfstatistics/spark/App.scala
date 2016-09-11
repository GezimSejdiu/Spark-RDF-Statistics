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
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.ModelFactory
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION
import org.openjena.riot.RiotReader
import org.openjena.riot.Lang
import net.sansa.rdfstatistics.spark.utils.Logging
import net.sansa.rdfstatistics.spark.io._
import net.sansa.rdfstatistics.spark.rdfstats.RDFStatistics
import org.apache.spark.storage.StorageLevel
import net.sansa.rdfstatistics.spark.rdfstats.RDFStatistics_WithRuntime
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
    sparkContext.setLogLevel("WARN")

    val file = args(1)
    val outputPath = args(1)

    logger.info("Runing RDF-Statistics....")
    val startTime = System.currentTimeMillis()

    // load triples
    val triples = TripleReader.loadFromFile(file, sparkContext, 2)

    // compute  criterias
    val rdf_statistics = new RDFStatistics(triples, sparkContext).apply

    // write triples on disk
    //TripleWriter.writeToFile(rdf_statistics, outputPath)
    println("finished loading " + triples.count() + " triples in " + (System.currentTimeMillis() - startTime) + "ms.")

    sparkContext.stop()
  }
}
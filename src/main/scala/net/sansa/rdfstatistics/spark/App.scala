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

object App extends Logging {

  val SPARK_MASTER = "spark://139.18.2.34:3077" //"spark://139.18.2.34:3077"//"spark://gezim-Latitude-E5550:7077" //

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setMaster("spark://gezim-Latitude-E5550:7077")
      .setAppName("Spark-RDF-Statistics")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "512")
    //.set("spark.executor.memory", "2g")

    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("WARN")
    sparkContext.addJar("/home/gezim/workspace/SparkProjects/Spark-RDF-Statistics/target/Spark-RDF-Statistics-0.0.1-SNAPSHOT-jar-with-dependencies.jar")

    val file = "/opt/spark-1.5.1/nyseSimpl_copy.nt"
    val outputPath = args(1)
    //hdfsfile + "/gsejdiu/DistLODStats/Dbpedia/en/page_links_en.nt.bz2")

    // load triples
    val triples = TripleReader.loadFromFile(file, sparkContext, 2)

    // compute  criterias
    val rdf_statistics = new RDFStatistics(triples,sparkContext).apply

    // write triples on disk
    TripleWriter.writeToFile(rdf_statistics, outputPath)

  }

} 

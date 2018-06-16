package net.sansa_stack.rdf.spark

import org.apache.spark.sql.SparkSession
import java.net.URI
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.riot.Lang
import scala.collection.mutable
import java.io.File
import net.sansa_stack.rdf.spark.stats._

object RDFStats {
  def main(args: Array[String]) {
    parser.parse(args, Config()) match {
      case Some(config) =>
        run(config.in, config.out)
      case None =>
        println(parser.usage)
    }
  }

  def run(input: String, output: String): Unit = {

    val rdf_stats_file = new File(input).getName

    val spark = SparkSession.builder
      .appName(s"RDF Dataset Statistics $rdf_stats_file")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    println("======================================")
    println("|        RDF Statistic example       |")
    println("======================================")

    val lang = Lang.NTRIPLES
    val startTime = System.currentTimeMillis()
    
    val triples = spark.rdf(lang)(input)

    val stats = triples.stats
      .voidify(rdf_stats_file, output)

    println("Finished computing RDF statistics for  " + rdf_stats_file + " in " + (System.currentTimeMillis() - startTime) + "ms.")

    spark.stop()
  }

  // the config object
  case class Config(
    in:  String = "",
    out: String = "")

  // the CLI parser
  val parser = new scopt.OptionParser[Config]("RDF Dataset Statistics Example") {

    head("RDF Dataset Statistics Example")

    opt[String]('i', "input").required().valueName("<path>").
      action((x, c) => c.copy(in = x)).
      text("path to file that contains the data (in N-Triples format)")

    opt[String]('o', "out").required().valueName("<directory>").
      action((x, c) => c.copy(out = x)).
      text("the output directory")

    help("help").text("prints this usage text")
  }
}

  /*def main(args: Array[String]): Unit = {

    val file = args(0)
    val rdf_stats_file = new File(file).getName
    val outputPath =args(1)

    val sparkSession = SparkSession.builder
      .master(SparkUtils.getSparkMasterURL())
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "512")
      .appName("RDF Dataset Statistics (" + rdf_stats_file + ")")
      .getOrCreate()

    SparkUtils.setLogLevels(org.apache.log4j.Level.WARN, Seq("org.apache", "spark", "org.eclipse.jetty", "akka", "org"))

    logger.info("Runing RDF-Statistics....")
    val startTime = System.currentTimeMillis()

    // load triples
    val triples = TripleReader.loadFromFile(file, sparkSession.sparkContext, 2)

    // compute  criteriasx
    val rdf_statistics = RDFStatistics(triples, sparkSession.sparkContext)

    // write statistics on disk  
    TripleWriter.voidify(rdf_statistics, rdf_stats_file, outputPath)
    //TripleWriter.writeToFile(rdf_statistics, outputPath)
    println("finished computing RDF statistics for  " + rdf_stats_file + " in " + (System.currentTimeMillis() - startTime) + "ms.")

    sparkSession.stop()
  }
}

*/
package net.sansa.rdfstatistics.spark.io

/*import org.openjena.riot.RiotReader
import org.openjena.riot.Lang*/
import org.apache.spark.SparkContext
import net.sansa.rdfstatistics.spark.utils.Logging
import java.io.InputStream
import net.sansa.rdfstatistics.spark.model.Triples
import org.apache.spark.rdd.RDD
import org.apache.jena.riot.{Lang, RDFDataMgr}

/**
 * Reads triples.
 *
 * @author Gezim Sejdiu
 *
 */
object TripleReader extends Logging {

  def parseTriples(fn: String) = {
   // val triples = RiotReader.createIteratorTriples(new StringInputStream(fn), Lang.NTRIPLES, "http://example/base").next
      val triples = RDFDataMgr.createIteratorTriples(new StringInputStream(fn), Lang.NTRIPLES, "http://example/base").next()
    Triples(triples.getSubject(), triples.getPredicate(), triples.getObject())
  }

  def loadFromFile(path: String, sc: SparkContext, minPartitions: Int = 2):RDD[Triples] = {

    val triples =
      sc.textFile(path)//, minPartitions) seems to be slower when we cpecify minPartitions
        .filter(line => !line.trim().isEmpty & !line.startsWith("#"))
        .map(parseTriples)

    triples
  }

}

class StringInputStream(s: String) extends InputStream {
  private val bytes = s.getBytes
  private var pos = 0

  override def read(): Int = if (pos >= bytes.length) {
    -1
  } else {
    val r = bytes(pos)
    pos += 1
    r.toInt
  }
}

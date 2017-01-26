package net.sansa.rdfstatistics.spark.io

/*import org.openjena.riot.RiotReader
import org.openjena.riot.Lang*/
import org.apache.spark.SparkContext
import net.sansa.rdfstatistics.spark.utils.Logging
import java.io.InputStream
import net.sansa.rdfstatistics.spark.model.Triples
import org.apache.spark.rdd.RDD
//import org.apache.jena.riot.{ Lang, RDFDataMgr }
//import org.apache.jena.riot.RiotReader
import org.openjena.riot.{ Lang, RiotReader }
import net.sansa.rdfstatistics.spark.model.TriplesRDD

/**
 * Reads triples.
 *
 * @author Gezim Sejdiu
 *
 */
object TripleReader extends Logging {

  def parseTriples(fn: String) = {
    // val triples = RiotReader.createIteratorTriples(new StringInputStream(fn), Lang.NTRIPLES, "http://example/base").next
    /*     val tis = RDFDataMgr.open(fn);
    val lang = RDFDataMgr.determineLang(fn, null, Lang.NTRIPLES);
    val base = tis.getBaseURI();
    val itTriple = RiotReader.createIteratorTriples(tis, lang, base).next*/

    //val triples = RDFDataMgr.createIteratorTriples(new StringInputStream(fn), Lang.NTRIPLES, "http://example/base").next
    val triples = RiotReader.createIteratorTriples(new StringInputStream(fn), Lang.NTRIPLES, "http://example/base").next

    Triples(triples.getSubject(), triples.getPredicate(), triples.getObject())
  }

  def loadFromFile(path: String, sc: SparkContext, minPartitions: Int = 2): RDD[Triples] = {

    val triples =
      sc.textFile(path)
        .filter(line => !line.trim().isEmpty & !line.startsWith("#"))
        .map(parseTriples)
    triples
  }

  def loadFromFile(path: String, sc: SparkContext): TriplesRDD = {
    val triples = sc.textFile(path)
      .map { line =>
        val it = RiotReader.createIteratorTriples(new StringInputStream(path), Lang.NTRIPLES, "http://example/base").next
        Triples(it.getSubject, it.getPredicate, it.getObject)
      }

    TriplesRDD(triples)
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

package net.sansa.rdfstatistics.spark.io

import net.sansa.rdfstatistics.spark.utils.Logging
import org.apache.spark.rdd.RDD
import net.sansa.rdfstatistics.spark.model.Triples
import java.io.PrintWriter
import java.io.File

/**
 * Writes triples to disk.
 *
 * @author Gezim Sejdiu
 *
 */

object TripleWriter extends Logging {

  def writeToFile(triples: RDD[Triples], path: String) = {
    val startTime = System.currentTimeMillis()

    triples
      .map(t => "<" + t.subj + "> <" + t.pred + "> <" + t.obj + "> .")
      .saveAsTextFile(path)
  }

  def writeToFile(triples: RDD[Triples], path: String, partition: Int) = {
    val startTime = System.currentTimeMillis()

    triples
      .map(t => "<" + t.subj + "> <" + t.pred + "> <" + t.obj + "> .")
      .coalesce(partition)
      .saveAsTextFile(path)
  }

  def voidify(stats: RDD[String], source: String, fn: String): Unit = {
    val pw = new PrintWriter(new File(fn))

    val prefix = """@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix void: <http://rdfs.org/ns/void#> .
@prefix void-ext: <http://stats.lod2.eu/rdf/void-ext/> .
@prefix qb: <http://purl.org/linked-data/cube#> .
@prefix dcterms: <http://purl.org/dc/terms/> .
@prefix ls-void: <http://stats.lod2.eu/rdf/void/> .
@prefix ls-qb: <http://stats.lod2.eu/rdf/qb/> .
@prefix ls-cr: <http://stats.lod2.eu/rdf/qb/criteria/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix xstats: <http://example.org/XStats#> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> ."""

    val src = "\n<http://stats.lod2.eu/rdf/void/?source=" + source + ">\n"
    val end = "\na void:Dataset ."

    val voidify = prefix.concat(src).concat(stats.coalesce(1, true).collect().mkString).concat(end)
    pw.println(voidify)
    pw.close
  }

  /*
   *  val vtxt = verts.map(v =>
 +      "{\"val\":\"" + v._2.toString + "\"}").mkString(",\n")
 +
 +    val etxt = edges.map(e =>
 +      "{\"source\":" + nmap(e.srcId).toString +
 +        ",\"target\":" + nmap(e.dstId).toString +
 +        ",\"value\":" + e.attr.toString + "}").mkString(",\n")
 +
 +    "{ \"nodes\":[\n" + vtxt + "\n],\"links\":[" + etxt + "]\n}"
   */

}
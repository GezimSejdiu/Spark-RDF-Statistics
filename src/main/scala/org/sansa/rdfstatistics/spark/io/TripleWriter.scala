package org.sansa.rdfstatistics.spark.io

import org.sansa.rdfstatistics.spark.utils.Logging
import org.apache.spark.rdd.RDD
import org.sansa.rdfstatistics.spark.model.Triples

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
      .map(t => "<" + t.subj.getLiteral() + "> <" + t.pred.getLiteral() + "> <" + t.obj.getLiteral() + "> .")
      .saveAsTextFile(path)
  }

}
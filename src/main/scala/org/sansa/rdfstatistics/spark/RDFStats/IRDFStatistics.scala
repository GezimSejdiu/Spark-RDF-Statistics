package org.sansa.rdfstatistics.spark.RDFStats

import org.apache.spark.rdd.RDD
import org.sansa.rdfstatistics.spark.model.Triples

/**
 * @author Gezim Sejdiu
 */

trait IRDFStatistics {

  def apply(): RDD[Triples]

}
package net.sansa.rdfstatistics.spark.rdfstats

import org.apache.spark.rdd.RDD
import net.sansa.rdfstatistics.spark.model.Triples

/**
 * @author Gezim Sejdiu
 */

trait IRDFStatistics {

  def apply(): RDD[Triples]

}
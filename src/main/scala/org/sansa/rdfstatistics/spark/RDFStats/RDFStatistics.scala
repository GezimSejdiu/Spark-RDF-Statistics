package org.sansa.rdfstatistics.spark.RDFStats

import org.apache.spark.rdd.RDD
import org.sansa.rdfstatistics.spark.utils.Logging
import org.sansa.rdfstatistics.spark.model._

/**
 * A Distributed implementation of RDF Statisctics.
 *
 * @constructor create a new RDFStatistics computation
 *
 * @author Gezim Sejdiu
 */
class RDFStatistics(triples:RDD[Triples]) extends IRDFStatistics with Serializable with Logging {

  def apply(): RDD[Triples] =
    {
      logger.info("Computing RDF Statistics...")
      val startTime = System.currentTimeMillis()

      val triplesRDD = triples.cache

      val usedclasses = UsedClasses(triplesRDD).apply

      val classesdefined = ClassesDefined(triplesRDD).apply

      val classhierarchydepth = ClassHierarchyDepth(triplesRDD).apply.map(f => (f.subj, f.obj))

      //logger.info("...Finished Computing RDF Statistics in " + (System.currentTimeMillis() - startTime) + "ms.")

      usedclasses.union(classesdefined).distinct
    }
}

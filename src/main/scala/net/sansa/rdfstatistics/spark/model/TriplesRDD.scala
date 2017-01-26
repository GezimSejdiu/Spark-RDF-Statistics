package net.sansa.rdfstatistics.spark.model

import org.apache.spark.rdd.RDD

case class TriplesRDD(triples: RDD[Triples]) {

  /**
   * Returns the union of the current RDF graph with the given RDF graph
   *
   * @param other the other RDF graph
   * @return the union of both graphs
   */
  def union(other: TriplesRDD): TriplesRDD = {
    TriplesRDD(triples.union(other.triples))
  }

  /**
   * Returns an RDFGraph with the triples from `this` that are not in `other`.
   *
   * @param other the other RDF graph
   * @return the difference of both graphs
   */
  def subtract(other: TriplesRDD): TriplesRDD = {
    TriplesRDD(triples.subtract(other.triples))
  }

  /**
   * Returns the number of triples.
   *
   * @return the number of triples
   */
  def size() = triples.count()
  
  
  /**
   * Returns a DataSet of triples
   *
   * @return DataSet of triples
   */
  def getTriples = triples

  /**
   * Returns a DataSet of subjects
   *
   * @return DataSet of subjects
   */
  def getSubjects = triples.map(_.getSubject)

  /**
   * Returns a DataSet of predicates
   *
   * @return DataSet of predicates
   */
  def getPredicates = triples.map(_.getPredicate)
  
   /**
   * Returns a DataSet of objects
   *
   * @return DataSet of objects
   */
  def getObjects = triples.map(_.getObject)
  

}
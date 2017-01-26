package net.sansa.rdfstatistics.spark.model
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.rdd.RDD
import com.hp.hpl.jena.graph.Node

case class TriplesDataFrame(triples: DataFrame) {

  /**
   * Returns the union of the current RDF graph with the given RDF graph
   *
   * @param other the other RDF graph
   * @return the union of both graphs
   */
  def union(other: TriplesDataFrame): TriplesDataFrame = {
    TriplesDataFrame(triples.union(other.triples))
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

  def toRDD(): RDD[Triples] = triples.rdd.map(row => Triples(row.get(0).asInstanceOf[Node], row.get(1).asInstanceOf[Node], row.get(2).asInstanceOf[Node]))
  
  def sql(sqlText:String) = triples.sqlContext.sql(sqlText)

}
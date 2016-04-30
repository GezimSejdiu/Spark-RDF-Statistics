package net.sansa.rdfstatistics.spark.rdfstats

import org.apache.spark.rdd.RDD
import com.hp.hpl.jena.graph.Node
import net.sansa.rdfstatistics.spark.utils.Logging
import net.sansa.rdfstatistics.spark.model._
import org.apache.spark.SparkContext

/**
 * A Distributed implementation of RDF Statisctics.
 *
 * @constructor create a new RDFStatistics computation
 *
 * @author Gezim Sejdiu
 */
class RDFStatistics(triples: RDD[Triples], sc: SparkContext) extends IRDFStatistics with Serializable with Logging {

  def apply(): RDD[Triples] =
    {
      logger.info("Computing RDF Statistics...")
      val startTime = System.currentTimeMillis()

      val triplesRDD = triples.cache

      /*
	   * 1.
	   * Used Classes Criterion.
	   */
      var usedclasses = UsedClasses(triplesRDD).apply.map(_.obj)

      /*
	   * 2.
	   * Class usage count
	   * To count the usage of respective classes of a dataset, 
	   * the filter rule that is used to analyze a triple is the same as in the first criterion. 
	   */
      var classusedcount = sc.broadcast(usedclasses
        .map(obj => (obj, 1))
        .reduceByKey(_ + _)
        .take(100) //Postproc
        )

      /*
	   * 3.
	   * To get a set of classes that are defined within a dataset this criterion is being used. 
	   */

      val classesdefined = ClassesDefined(triplesRDD).apply.map(_.subj)

      /*
	   * 4.
	   * Gather hierarchy of c	lasses seen
	   */
      val classhierarchydepth = ClassHierarchyDepth(triplesRDD).apply.map(f => (f.subj, f.obj))

      //classhierarchydepth.treeAggregate(zeroValue)(seqOp, combOp, depth)

      /*
	   * 5.
	   * Property usage. 
	   * This criterion is used to count the usage of properties within triples.
	   */
      val propertyusage = EntityUsage(triplesRDD).apply
        .map(_.pred)
        .map(f => (f, 1))
        .reduceByKey(_ + _)
        .take(100)

      /*
	   * 6.
	   * Property usage distinct per subject.
	   */
      val propertyusagedistinctpersubject = EntityUsage(triplesRDD).apply.distinct
        .groupBy(_.subj)
        .map(f => (f._2.filter(p => p.pred.getLiteral().toString().contains(p)), 1))
        .reduceByKey(_ + _)

      /*
	   * 7.
	   * Property usage distinct per object.
	   * 
	   */
      val propertyusagedistinctperobject = EntityUsage(triplesRDD).apply
        .groupBy(_.obj)
        .map(f => (f._2.filter(p => p.pred.getLiteral().toString().contains(p)), 1))
        .reduceByKey(_ + _)

      /*
	   * 8.
	   *  Properties distinct per subject.
	   * 
	   */
      val propertydistinctpersubject = EntityUsage(triplesRDD).apply
        .groupBy(_.subj)
        .map(f => (f._2.filter(p => p.pred.getLiteral().toString().contains(p)), 1))
        .combineByKey(
          (v) => (v, 1),
          (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
          (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)).map { case (key, value) => (key, value._1 / value._2.toFloat) }

      /*
	   * 9.
	   *  Properties distinct per object.
	   * 
	   */
      val propertydistinctperobejct = EntityUsage(triplesRDD).apply
        .groupBy(_.obj)
        .map(f => (f._2.filter(p => p.pred.getLiteral().toString().contains(p)), 1))
        .combineByKey(
          (v) => (v, 1),
          (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
          (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)).map { case (key, value) => (key, value._1 / value._2.toFloat) }

      /*
	   * 10.
	   * outdegree.
	   * 
	   */
      val outdegree = EntityUsage(triplesRDD).apply
        .map(_.subj)
        .map(f => (f, 1))
        .combineByKey(
          (v) => (v, 1),
          (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
          (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)).map { case (key, value) => (key, value._1 / value._2.toFloat) }

      /*
	   * 11.
	   * indegree.
	   * 
	   */
      val indegree = EntityUsage(triplesRDD).apply
        .map(_.obj)
        .map(f => (f, 1))
        .combineByKey(
          (v) => (v, 1),
          (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
          (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)).map { case (key, value) => (key, value._1 / value._2.toFloat) }

      /*
	   * 12.
	   * Property hierarchy depth.
	   */
      val properthierarchydepth = PropertyHierarchyDepth(triplesRDD).apply.map(f => (f.subj, f.obj))

      /*
	   * 13.
	   * Subclass usage.
	   */
      val subclassusage = SubClassUsage(triplesRDD).apply.count

      /*
	   * 14.
	   * Triples
	   * This criterion is used to measure the amount of triples of a dataset. 
	   */
      val alltriples = triplesRDD.count

      /*
	   * 15.
	   * Entities mentioned
	   * To get a count of entities (resources / IRIs) that are mentioned within a dataset, this criterion is used. 
	   */

      val entitiesmentioned = EntitiesMentioned(triplesRDD).apply.count

      /*
	    16.
	   *  Distinct entities.
	   *  To get a set/list of distinct entities of a dataset all IRIs are extracted from the respective triple and added to the set of entities.
	   */
      val entities = Entities(triplesRDD).apply.distinct

      /*
	   * 17.
	   * Literals
	   * To get the amount of triples that are referencing literals to subjects 
	   * the illustrated filter rule is used to analyze the respective triple. 
	   */
      val literals = Literals(triplesRDD).apply.count

      /*
	   * 18.
	   * Distinct number of entities.
	   * Entity - triple, where ?s is IRI (not blank)
	   */
      val blankassubject = BlanksAsSubject(triplesRDD).apply.count

      /*
	   * 19.
	   * Distinct number of entities.
	   * Entity - triple, where ?s is IRI (not blank)
	   */
      val blankasobject = BlanksAsObject(triplesRDD).apply.count

      /*
	   * 20.
	   * Usually in RDF/S and OWL literals used as objects of triples can be specified narrower. 
	   * Histogram of types used for literals.
	   */
      val datatypes = Datatypes(triplesRDD).apply.count

      /*
	   * 21.
	   * Usually in RDF/S and OWL literals used as objects of triples can be specified narrower. 
	   * Histogram of types used for literals.
	   */
      val languages = Languages(triplesRDD).apply.count

      /*
	   * 22.
	   * Average typed string length.
	   */
      val avg_typed = TypedStringLength(triplesRDD).apply
        .map(f => (f, 1))
        .combineByKey(
          (v) => (v, 1),
          (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
          (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)).map { case (key, value) => (key, value._1 / value._2.toFloat) }

      /*
	   * 23.
	   * Average untyped string length.
	   */
      val avg_untyped = UntypedStringLength(triplesRDD).apply
        .map(f => (f, 1))
        .combineByKey(
          (v) => (v, 1),
          (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
          (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)).map { case (key, value) => (key, value._1 / value._2.toFloat) }

      /*
	   * 24.
	   * Typed subjects
	   */
      val typedsubject = TypedSubject(triplesRDD).apply.count

      /*
	   * 25.
	   * Number of labeled subjects.
	   */
      val labeledsubjects = LabeledSubjects(triplesRDD).apply.count

      /*
	   * 26.
	   * Number of triples with owl#sameAs as predicate
	   */
      val sameas = SameAs(triplesRDD).apply.count

      /*
	   * 27.
	   * Links
	   */
      val links = Links(triplesRDD).apply.map(f => (f.subj.getNameSpace() + f.obj.getNameSpace()))
        .map(f => (f, 1)).reduceByKey(_ + _)

      /*
      * 28.
      * Maximum per property {int,float,time}
      * 
      */

      val max_per_property = Max_Avg_PerProperty(triplesRDD).apply
      /*
        * 29.
        * Average per property {int,float,time}
        
      */
      val M1 = Max_Avg_PerProperty(triplesRDD).apply.map(f => f.obj).count
      val M2 = Max_Avg_PerProperty(triplesRDD).apply.map(f => f.pred).count
      val avg_per_property = M1 / M2;

      /*
      * 30.
      * Subject vocabularies
      * 
      */
      val sub_vocabularies = Vocabularies(triplesRDD).apply
        .map(f => (f.subj.getNameSpace()))
        .map(f => (f, 1)).reduceByKey(_ + _)

      /*
      * 31.
      * Predicate vocabularies
      * 
      */
      val pred_vocabularies = Vocabularies(triplesRDD).apply
        .map(f => (f.pred.getNameSpace()))
        .map(f => (f, 1)).reduceByKey(_ + _)
      /*
      * 32.
      * Object vocabularies
      * 
      */
      val obj_vocabularies = Vocabularies(triplesRDD).apply
        .map(f => (f.obj.getNameSpace()))
        .map(f => (f, 1)).reduceByKey(_ + _)

      //logger.info("...Finished Computing RDF Statistics in " + (System.currentTimeMillis() - startTime) + "ms.")

      //usedclasses.union(classesdefined).distinct
      triplesRDD.distinct
    }
}

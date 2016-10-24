package net.sansa.rdfstatistics.spark.rdfstats

import org.apache.spark.rdd.RDD
import net.sansa.rdfstatistics.spark.utils.Logging
import net.sansa.rdfstatistics.spark.model._
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx._
import net.sansa.rdfstatistics.spark.utils.SparkUtils
import net.sansa.rdfstatistics.spark.utils.Prefixes

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
      /*
      val indexedmap = (classhierarchydepth.map(_._1) union classhierarchydepth.map(_._2)).distinct.zipWithIndex
      val vertices: RDD[(VertexId, String)] = indexedmap.map(x => (x._2, x._1))
      
      val tuples = rs.keyBy(_._1).join(indexedmap).map({
      case (k, ((s, p, o), si)) => (o, (si, p))
    })

    val edges: RDD[Edge[String]] = tuples.join(indexedmap).map({
      case (k, ((si, p), oi)) => Edge(si, oi, p)
    })
    
    val triCounts = graph.triangleCount().vertices
*/
      //classhierarchydepth.treeAggregate(zeroValue)(seqOp, combOp, depth)

      /*
	   * 5.
	   * Property usage. 
//	   * This criterion is used to count the usage of properties within triples.
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
      val entitiess = Entities(triplesRDD).apply.distinct

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
      val avg_per_property = if (M2 != 0) { M1 / M2 } else { 0 };

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

  def run(): RDD[String] = {
    Used_Classes(triples, sc)
      .union(DistinctEntities(triples, sc))
      .union(DistinctSubjects(triples, sc))
      .union(DistinctObjects(triples, sc))
      .union(PropertyUsage(triples, sc))
      .union(SPO_Vocabularies(triples, sc))
  }
}

object RDFStatistics {
  def apply(triples: RDD[Triples], sc: SparkContext) = new RDFStatistics(triples, sc).run()
}

class Used_Classes(triples: RDD[Triples], sc: SparkContext) extends Serializable with Logging {

  //?p=rdf:type && isIRI(?o)
  def Filter() = triples.filter(f =>
    f.pred.toString().equals(Prefixes.RDF_TYPE) && f.obj.isURI())

  //M[?o]++ 
  def Action() = Filter().map(_.obj)
    .map(f => (f, 1))
    .reduceByKey(_ + _)
    .cache()

  //top(M,100)
  def PostProc() = Action().sortBy(_._2, false)
    .take(100)

  def Voidify() = {

    var triplesString = new Array[String](1)
    triplesString(0) = "\nvoid:classPartition "

    val classes = sc.parallelize(PostProc())
    val vc = classes.map(t => "[ \nvoid:class " + "<" + t._1 + ">; \nvoid:triples " + t._2 + ";\n], ")

    var cl_a = new Array[String](1)
    cl_a(0) = "\nvoid:classes " + Action().map(f => f._1).distinct().count
    val c_p = sc.parallelize(triplesString)
    val c = sc.parallelize(cl_a)
    c.union(c_p).union(vc)
  }
}
object Used_Classes {

  def apply(triples: RDD[Triples], sc: SparkContext) = new Used_Classes(triples, sc).Voidify()

}

class Classes_Defined(triples: RDD[Triples], sc: SparkContext) extends Serializable with Logging {

  //?p=rdf:type && isIRI(?s) &&(?o=rdfs:Class||?o=owl:Class)
  def Filter() = triples.filter(f =>
    (f.pred.toString().equals(Prefixes.RDF_TYPE) && f.obj.toString().equals(Prefixes.RDFS_CLASS))
      || (f.pred.toString().equals(Prefixes.RDF_TYPE) && f.obj.toString().equals(Prefixes.OWL_CLASS))
      && !f.subj.isURI())

  //M[?o]++ 
  def Action() = Filter().map(_.subj).distinct()

  def PostProc() = Action().count()

  def Voidify() = {
    var cd = new Array[String](1)
    cd(0) = "\nvoid:classes  " + PostProc() + ";"
    sc.parallelize(cd)
  }
}
object Classes_Defined {

  def apply(triples: RDD[Triples], sc: SparkContext) = new Classes_Defined(triples, sc).Voidify()
}

class PropertiesDefined(triples: RDD[Triples], sc: SparkContext) extends Serializable with Logging {

  def Filter() = triples.filter(f =>
    (f.pred.toString().equals(Prefixes.RDF_TYPE) && f.obj.toString().equals(Prefixes.OWL_OBJECT_PROPERTY))
      || (f.pred.toString().equals(Prefixes.RDF_TYPE) && f.obj.toString().equals(Prefixes.RDF_PROPERTY))
      && !f.subj.isURI())
  def Action() = Filter().map(_.pred).distinct()

  def PostProc() = Action().count()

  def Voidify() = {
    var cd = new Array[String](1)
    cd(0) = "\nvoid:properties  " + PostProc() + ";"
    sc.parallelize(cd)
  }
}
object PropertiesDefined {

  def apply(triples: RDD[Triples], sc: SparkContext) = new PropertiesDefined(triples, sc).Voidify()
}

class PropertyUsage(triples: RDD[Triples], sc: SparkContext) extends Serializable with Logging {

  def Filter() = triples

  //M[?p]++
  def Action() = Filter().map(_.pred)
    .map(f => (f, 1))
    .reduceByKey(_ + _)
    .cache()

  //top(M,100)
  def PostProc() = Action().sortBy(_._2, false)
    .take(100)

  def Voidify() = {

    var triplesString = new Array[String](1)
    triplesString(0) = "\nvoid:propertyPartition "

    val properties = sc.parallelize(PostProc())
    val vp = properties.map(t => "[ \nvoid:property " + "<" + t._1 + ">; \nvoid:triples " + t._2 + ";\n], ")

    var pl_a = new Array[String](1)
    pl_a(0) = "\nvoid:properties " + Action().map(f => f._1).distinct().count
    val c_p = sc.parallelize(triplesString)
    val p = sc.parallelize(pl_a)
    p.union(c_p).union(vp)

    /* var triplesString = new Array[String](1)
    triplesString(0) = "\nvoid:propertyPartition "

    val header = sc.parallelize(triplesString)

    val properties = sc.parallelize(PostProc())
      .map(t => "[ \nvoid:property " + "<" + t._1 + ">; \nvoid:triples " + t._2 + ";\n], ")

    header.union(properties)*/
  }
}
object PropertyUsage {

  def apply(triples: RDD[Triples], sc: SparkContext) = new PropertyUsage(triples, sc).Voidify()

}

class DistinctEntities(triples: RDD[Triples], sc: SparkContext) extends Serializable with Logging {

  def Filter() = triples.filter(f =>
    (f.subj.isURI() && f.pred.isURI() && f.obj.isURI()))

  def Action() = Filter().distinct()

  def PostProc() = Action().count()

  def Voidify() = {
    var ents = new Array[String](1)
    ents(0) = "\nvoid:entities  " + PostProc() + ";"
    sc.parallelize(ents)
  }
}
object DistinctEntities {

  def apply(triples: RDD[Triples], sc: SparkContext) = new DistinctEntities(triples, sc).Voidify()
}

class DistinctSubjects(triples: RDD[Triples], sc: SparkContext) extends Serializable with Logging {

  def Filter() = triples.filter(f => f.subj.isURI())

  def Action() = Filter().distinct()

  def PostProc() = Action().count()

  def Voidify() = {
    var ents = new Array[String](1)
    ents(0) = "\nvoid:distinctSubjects  " + PostProc() + ";"
    sc.parallelize(ents)
  }
}
object DistinctSubjects {

  def apply(triples: RDD[Triples], sc: SparkContext) = new DistinctSubjects(triples, sc).Voidify()
}

class DistinctObjects(triples: RDD[Triples], sc: SparkContext) extends Serializable with Logging {

  def Filter() = triples.filter(f => f.obj.isURI())

  def Action() = Filter().distinct()

  def PostProc() = Action().count()

  def Voidify() = {
    var ents = new Array[String](1)
    ents(0) = "\nvoid:distinctObjects  " + PostProc() + ";"
    sc.parallelize(ents)
  }
}
object DistinctObjects {

  def apply(triples: RDD[Triples], sc: SparkContext) = new DistinctObjects(triples, sc).Voidify()
}

class SPO_Vocabularies(triples: RDD[Triples], sc: SparkContext) extends Serializable with Logging {

  def Filter() = triples

  def Action(node: org.apache.jena.graph.Node) = Filter(). map(f => node.getNameSpace()).cache()

  def SubjectVocabulariesAction() = Filter().filter(f=>f.subj.isURI()).map(f => (f.subj.getNameSpace())).cache
  def SubjectVocabulariesPostProc() = SubjectVocabulariesAction()
    .map(f => (f, 1)).reduceByKey(_ + _)

  def PredicateVocabulariesAction() = Filter().filter(f=>f.pred.isURI()).map(f => (f.pred.getNameSpace())).cache
  def PredicateVocabulariesPostProc() = PredicateVocabulariesAction()
    .map(f => (f, 1)).reduceByKey(_ + _)

  def ObjectVocabulariesAction() = Filter().filter(f=>f.obj.isURI()).map(f => (f.obj.getNameSpace())).cache
  def ObjectVocabulariesPostProc() = ObjectVocabulariesAction()
    .map(f => (f, 1)).reduceByKey(_ + _)

  def PostProc(node: org.apache.jena.graph.Node) = Filter().map(f => node.getNameSpace())
    .map(f => (f, 1)).reduceByKey(_ + _)

  def Voidify() = {
    var ents = new Array[String](1)
    ents(0) = "\nvoid:vocabulary  <" + SubjectVocabulariesAction().union(PredicateVocabulariesAction()).union(ObjectVocabulariesAction()).distinct().take(15).mkString(">, <") + ">;"
    sc.parallelize(ents)
  }
}
object SPO_Vocabularies {

  def apply(triples: RDD[Triples], sc: SparkContext) = new SPO_Vocabularies(triples, sc).Voidify()
}


package net.sansa.rdfstatistics.spark.model

import org.apache.spark.rdd.RDD
import org.apache.ivy.core.module.descriptor.ExtendsDescriptor
import org.apache.spark.SparkContext
import net.sansa.rdfstatistics.spark.utils.Prefixes
import org.apache.spark.{ Partition, TaskContext }
import net.sansa.rdfstatistics.spark.utils.SparkUtils

case class UsedClasses(triples: RDD[Triples]) extends Serializable {

  def apply() = {
    triples.filter(f =>
      f.pred.equals(Prefixes.RDF_TYPE) && f.obj.isURI())
  }
}

case class ClassesDefined(triples: RDD[Triples]) extends Serializable {

  def apply() = {
    triples.filter(f =>
      f.pred.equals(Prefixes.RDF_TYPE) && f.subj.isURI() && (f.obj.equals(Prefixes.RDFS_CLASS) || f.obj.equals(Prefixes.OWL_CLASS)))
  }
}

case class ClassHierarchyDepth(triples: RDD[Triples]) extends Serializable {

  def apply() = {
    triples.filter(f =>
      f.pred.equals(Prefixes.RDFS_SUBCLASS_OF) && f.subj.isURI() && f.obj.isURI())
  }
}

case class EntityUsage(triples: RDD[Triples]) extends Serializable {

  def apply() = {
    triples
  }
}

case class PropertyHierarchyDepth(triples: RDD[Triples]) extends Serializable {

  def apply() = {
    triples.filter(f =>
      f.pred.equals(Prefixes.RDFS_SUBPROPERTY_OF) && f.subj.isURI() && f.obj.isURI())
  }
}

case class SubClassUsage(triples: RDD[Triples]) extends Serializable {

  def apply() = {
    triples.filter(f =>
      f.pred.equals(Prefixes.RDFS_SUBPROPERTY_OF))
  }
}

case class EntitiesMentioned(triples: RDD[Triples]) extends Serializable {

  def apply() = {
    triples.filter(f =>
      (f.subj.isURI() && f.pred.isURI() && f.obj.isURI()))
  }
}

case class Entities(triples: RDD[Triples]) extends Serializable {

  def apply() = {
    triples.filter(f =>
      f.subj.isURI())
      .map(f => f.subj).union(triples.filter(f => f.pred.isURI()).map(f => f.pred).union(triples.filter(f => f.obj.isURI()).map(f => f.obj)))
  }

}

case class Literals(triples: RDD[Triples]) extends Serializable {

  def apply() = {
    triples.filter(f =>
      f.obj.isLiteral())
  }
}

case class BlanksAsSubject(triples: RDD[Triples]) extends Serializable {

  def apply() = {
    triples.filter(f =>
      f.subj.isBlank())
  }
}

case class BlanksAsObject(triples: RDD[Triples]) extends Serializable {

  def apply() = {
    triples.filter(f =>
      f.obj.isBlank())
  }
}

case class Datatypes(triples: RDD[Triples]) extends Serializable {

  def apply() = {
    triples.filter(f =>
      f.obj.isLiteral() && !f.dataType(f.obj.toString()).isEmpty())
      .map(obj => (obj.dataType(obj.obj.toString()), 1))
      .reduceByKey(_ + _)
  }
}

case class Languages(triples: RDD[Triples]) extends Serializable {

  def apply() = {
    triples.filter(f =>
      f.obj.isLiteral() && !f.languageTag(f.obj.toString()).isEmpty())
      .map(obj => (obj.languageTag(obj.obj.toString()), 1))
      .reduceByKey(_ + _)
  }
}
case class TypedStringLength(triples: RDD[Triples]) extends Serializable {

  def apply() = {
    triples.filter(f =>
      f.obj.isLiteral() && f.obj.getLiteralDatatype().toString().equals(Prefixes.XSD_STRING))

  }
}

case class UntypedStringLength(triples: RDD[Triples]) extends Serializable {

  def apply() = {
    triples.filter(f =>
      f.obj.isLiteral() && f.obj.getLiteralDatatype().toString().isEmpty())
  }
}

case class TypedSubject(triples: RDD[Triples]) extends Serializable {

  def apply() = {
    triples.filter(f =>
      f.pred.equals(Prefixes.RDF_TYPE))
  }
}

case class LabeledSubjects(triples: RDD[Triples]) extends Serializable {

  def apply() = {
    triples.filter(f =>
      f.pred.equals(Prefixes.RDFS_LABEL))

  }
}

case class SameAs(triples: RDD[Triples]) extends Serializable {

  def apply() = {
    triples.filter(f =>
      f.pred.equals(Prefixes.OWL_SAME_AS))
  }
}

case class Links(triples: RDD[Triples]) extends Serializable {

  def apply() = {
    triples.filter(f =>
      !f.subj.getNameSpace().equals(f.obj.getNameSpace()))
  }
}

case class Max_Avg_PerProperty(triples: RDD[Triples]) extends Serializable {

  def apply() = {
    triples.filter(f =>
      f.obj.equals(Prefixes.XSD_INT) | f.obj.equals(Prefixes.XSD_float) | f.obj.equals(Prefixes.XSD_datetime))
  }
}

case class Vocabularies(triples: RDD[Triples]) extends Serializable {

  def apply() = {
    triples
  }
}

object Criterias extends Enumeration with Serializable {
  val USEDCLASSES, CLASSUSEDCOUNT = Value
}

class ClassUsageCount(triples: RDD[Triples]) extends RDD[Triples](triples) with Serializable {
  override def compute(split: Partition, context: TaskContext): Iterator[Triples] = {
    firstParent[Triples].iterator(split, context).map(tripleRecord => {
      new Triples(tripleRecord.subj, tripleRecord.pred, tripleRecord.obj)
    })
  }

  override protected def getPartitions: Array[Partition] = firstParent[Triples].partitions

  def filter(triples: RDD[Triples]) {
    triples.filter(f =>
      f.pred.equals(Prefixes.RDF_TYPE) && f.obj.isURI())
  }

  def action(triples: RDD[Triples]) {
    triples.map(_.obj)
      .map(obj => (obj, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
  }

  def hasPostProc() = true

  def postProc(triples: RDD[Triples]) {
    triples.take(100)
    this.write(triples, SparkUtils.HDFSPath)
  }

  def write(triples: RDD[Triples], path: String) {
    triples.map(t => "<" + t.subj.getLiteral() + "> <" + t.pred.getLiteral() + "> <" + t.obj.getLiteral() + "> .")
      .saveAsTextFile(path)
  }
}



















 
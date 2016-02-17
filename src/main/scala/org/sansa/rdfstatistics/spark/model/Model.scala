package org.sansa.rdfstatistics.spark.model

import org.apache.spark.rdd.RDD
import org.apache.ivy.core.module.descriptor.ExtendsDescriptor
import org.apache.spark.SparkContext
import org.sansa.rdfstatistics.spark.utils.Prefixes

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

/*case class PropertyUsage(triples: RDD[Triples]) extends Serializable {

  def apply() {
    triples.collect
  }
}*/

case class PropertyHierarchyDepth(triples: RDD[Triples]) extends Serializable {

  def apply() = {
    triples.filter(f =>
      f.pred.equals(Prefixes.RDFS_SUBPROPERTY_OF) && f.subj.isURI() && f.obj.isURI())
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
      f.obj.isLiteral())
  }
}

case class Languages(triples: RDD[Triples]) extends Serializable {

  def apply() = {
    triples.filter(f =>
      f.obj.isLiteral())
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

object Criterias extends Enumeration with Serializable {
  val USEDCLASSES, CLASSUSEDCOUNT = Value
}


















 
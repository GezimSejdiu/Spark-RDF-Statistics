package net.sansa.rdfstatistics.spark.utils
import java.security.MessageDigest

object Prefixes {
  val RDF_CLASS = "http://www.w3.org/2000/01/rdf-schema#Class"
  val OWL_CLASS = "http://www.w3.org/2002/07/owl#Class"
  val RDFS_CLASS = "http://www.w3.org/2000/01/rdf-schema#Class"
  val RDFS_SUBCLASS_OF = "http://www.w3.org/2000/01/rdf-schema#subClassOf"
  val RDFS_SUBPROPERTY_OF = "http://www.w3.org/2000/01/rdf-schema#subPropertyOf"
  val RDFS_LABEL = "http://www.w3.org/2000/01/rdf-schema#label"
  val RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
  val XSD_STRING = "http://www.w3.org/2001/XMLSchema#string"
  val XSD_INT = "http://www.w3.org/2001/XMLSchema#int"
  val XSD_float = "http://www.w3.org/2001/XMLSchema#float"
  val XSD_datetime = "http://www.w3.org/2001/XMLSchema#datetime"
  val OWL_SAME_AS = "http://www.w3.org/2002/07/owl#sameAs"
  val OWL_OBJECT_PROPERTY= "http://www.w3.org/2002/07/owl#ObjectProperty"
  val RDF_PROPERTY = "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property"

  /*
   *   val RDF_CLASS = getHashCode("http://www.w3.org/2000/01/rdf-schema#Class")
  val OWL_CLASS = getHashCode("http://www.w3.org/2002/07/owl#Class")
  val RDFS_CLASS = getHashCode("http://www.w3.org/2000/01/rdf-schema#Class")
  val RDFS_SUBCLASS_OF = getHashCode("http://www.w3.org/2000/01/rdf-schema#subClassOf")
  val RDFS_SUBPROPERTY_OF = getHashCode("http://www.w3.org/2000/01/rdf-schema#subPropertyOf")
  val RDFS_LABEL = getHashCode("http://www.w3.org/2000/01/rdf-schema#label")
  val RDF_TYPE = getHashCode("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
  val XSD_STRING = getHashCode("http://www.w3.org/2001/XMLSchema#string")
  val XSD_INT = getHashCode("http://www.w3.org/2001/XMLSchema#int")
  val XSD_float = getHashCode("http://www.w3.org/2001/XMLSchema#float")
  val XSD_datetime = getHashCode("http://www.w3.org/2001/XMLSchema#datetime")
  val OWL_SAME_AS = getHashCode("http://www.w3.org/2002/07/owl#sameAs")
   */

  def getHashCode(triple: String) = {
    val mess = MessageDigest.getInstance("MD5")
    mess.reset()
    mess.update(triple.getBytes())
    val result = mess.digest()
    result.slice(result.length - 8, result.length).toSeq
  }
}

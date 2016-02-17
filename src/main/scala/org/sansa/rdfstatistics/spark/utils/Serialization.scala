package org.sansa.rdfstatistics.spark.utils

import org.apache.spark.serializer.{ KryoRegistrator => SparkKryoRegistrator }
import com.esotericsoftware.kryo.Kryo
import org.sansa.rdfstatistics.spark.model.Triples
/*
 * Class for serialization by the Kryo serializer.
 */
class Registrator extends SparkKryoRegistrator {

  override def registerClasses(kryo: Kryo) {
    // model
    kryo.register(classOf[Triples])
    //kryo.register(classOf[TripleRDD])


  }
}
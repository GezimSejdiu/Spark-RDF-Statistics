package org.sansa.rdfstatistics.spark.utils
import org.slf4j.LoggerFactory

trait Logging {
  protected val logger = LoggerFactory.getLogger(getClass.getName)
}
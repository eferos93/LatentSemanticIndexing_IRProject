package org.ir.project
package data_structures.weigher

import scala.math.log

case class TfIdfWeighting() extends WordWeight {
  override def weight(args: Map[String, Long]): Double =
    args("termFrequency") * log(args("numberOfDocuments").toDouble / args("documentFrequency"))
}

package org.ir.project
package data_structures.weigher

case class TermFrequencyWeighting() extends WordWeight {
  override def weight(args: Map[String, Long]): Double =
    args("termFrequency").toDouble
}
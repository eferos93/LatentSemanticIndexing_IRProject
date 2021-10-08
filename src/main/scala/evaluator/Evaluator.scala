package org.ir.project
package evaluator

import data_structures.Document

import org.apache.spark.sql.DataFrame
import sparkSession.implicits._

import scala.collection.mutable
import scala.language.implicitConversions

case class Evaluator[T <: Document](irSystem: IRSystem[T], queryRelevance: DataFrame) {

  private def precision(relevanceSet: mutable.WrappedArray[Int],
                        documentIds: Seq[Long],
                        numberOfDocuments: Int): Double = {
    implicit def bool2int(b: Boolean): Int = if (b) 1 else 0
    documentIds.take(numberOfDocuments).map(relevanceSet.contains(_): Int).sum.toDouble / numberOfDocuments
  }

  def averagePrecision(query: String, relevanceSet: mutable.WrappedArray[Int]): Double = {
    val documentIds = irSystem.answerQuery(query, relevanceSet.length)
      .map(_._1.id + 1) // + 1 cause ids internally starts from 0

    (1 to relevanceSet.length).map { index =>
      precision(relevanceSet, documentIds, index) // precision
    }.sum / relevanceSet.length
  }

  def meanAveragePrecision(): Double = {
    queryRelevance.select("query", "relevanceSet").as[(String, mutable.WrappedArray[Int])].collect
      .map { case (query, relevanceSet) => averagePrecision(query, relevanceSet) }
      .sum / queryRelevance.count
  }


}

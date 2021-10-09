package org.ir.project
package evaluator

import data_structures.Document

import org.apache.spark.mllib.evaluation.RankingMetrics


class Evaluator[T <: Document](rankingMetrics: RankingMetrics[Long]) {
  def meanAveragePrecision(): Double =
    rankingMetrics.meanAveragePrecision

  def normalisedDiscountedCumulativeGainAt(k: Int): Double =
    rankingMetrics.ndcgAt(k)
}

object Evaluator {
  def apply[T <: Document](irSystem: IRSystem[T], queryRelevance: Array[(String, Array[Long])]): Evaluator[T] = {
    val relevantDocuments =
      queryRelevance
        .map {
          case (query, relevanceSet) =>
            (irSystem.answerQuery(query, relevanceSet.length).map(_._1.id).toArray, relevanceSet)
        }.toSeq

    new Evaluator[T](new RankingMetrics(sparkContext.parallelize(relevantDocuments)))
  }
}

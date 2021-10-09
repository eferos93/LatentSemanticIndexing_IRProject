package org.ir.project
package evaluator

import data_structures.Document
import sparkSession.implicits._

import org.apache.spark.mllib.evaluation.RankingMetrics
import org.apache.spark.sql.DataFrame


class Evaluator[T <: Document](rankingMetrics: RankingMetrics[Long]) {
  def meanAveragePrecision(): Double =
    rankingMetrics.meanAveragePrecision

  def normalisedDiscountedCumulativeGain(k: Int): Double =
    rankingMetrics.ndcgAt(k)
}

object Evaluator {
  def apply[T <: Document](irSystem: IRSystem[T], queryRelevance: DataFrame): Evaluator[T] = {
    val relevantDocuments =
      queryRelevance.select("query", "relevanceSet").as[(String, Array[Long])].collect
        .map {
          case (query, relevanceSet) =>
            (irSystem.answerQuery(query, relevanceSet.length).map(_._1.id).toArray, relevanceSet)
        }.toSeq

    val rankingMetrics = new RankingMetrics(sparkContext.parallelize(relevantDocuments))
    new Evaluator[T](rankingMetrics)
  }
}

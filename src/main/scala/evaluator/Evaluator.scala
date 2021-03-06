package org.ir.project
package evaluator

import data_structures.Document

import org.apache.spark.mllib.evaluation.RankingMetrics


class Evaluator(rankingMetrics: RankingMetrics[Long]) {
  def meanAveragePrecision(): Double =
    rankingMetrics.meanAveragePrecision

  def ndcgAt(k: Int): Double =
    rankingMetrics.ndcgAt(k)
}

object Evaluator {
  def apply[T <: Document](irSystem: IRSystem[T], queryRelevance: Array[(String, Array[Long])]): Evaluator = {
    val relevantDocuments =
      queryRelevance
        .map {
          case (query, relevanceSet) =>
//            answer the query and get the set of document ids returned
            (irSystem.answerQuery(query, relevanceSet.length).map(_._1.id).toArray, relevanceSet)
        }.toSeq

    new Evaluator(new RankingMetrics(sparkContext.parallelize(relevantDocuments)))
  }
}

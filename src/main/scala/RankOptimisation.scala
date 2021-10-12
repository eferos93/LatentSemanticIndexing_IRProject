package org.ir.project

import data_structures.CranfieldDocument
import evaluator.Evaluator

import org.apache.spark.sql.Dataset

import java.nio.file.{Files, Paths}

object RankOptimisation extends App {
  val corpus: Dataset[CranfieldDocument] = readCranfieldCorpus()
  val queryAndRelevanceSets: Array[(String, Array[Long])] = readQueryRelevanceCranfield()
  (50 to 500 by 25).map { singularValues =>
    val irSystem = if (Files.exists(Paths.get("/index"))) {
      println("index not found")
      IRSystem(corpus, singularValues, tfidf = false)
    } else {
      println("index found")
      IRSystem(corpus, singularValues, "index", tfidf = false)
    }
    val eval = Evaluator(irSystem, queryAndRelevanceSets)
    (
      s"n° of singular values: $singularValues",
      s"MAP: ${eval.meanAveragePrecision()}",
      s"NDCG at 10: ${eval.normalisedDiscountedCumulativeGainAt(10)}"
    )
  }.foreach(println(_))

}

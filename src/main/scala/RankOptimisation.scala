package org.ir.project

import data_structures.NplDocument
import evaluator.Evaluator

import org.apache.spark.sql.Dataset

import java.nio.file.{Files, Paths}

object RankOptimisation extends App {
  def evaluation(corpus: Dataset[NplDocument],
                 queryAndRelevanceSets: Array[(String, Array[Long])],
                 tfidf: Boolean) = {
    val results =
      (100 to 700 by 100).map { singularValues =>
        println(s"Testing with $singularValues singular values")
        val irSystem = if (Files.exists(Paths.get("index"))) {
          println("index found")
          IRSystem(corpus, singularValues, "index", tfidf = tfidf)
        } else {
          println("index not found")
          IRSystem(corpus, singularValues, tfidf = tfidf)
        }

        val eval = Evaluator(irSystem, queryAndRelevanceSets)
        (
          singularValues,
          eval.meanAveragePrecision(),
          (eval.ndcgAt(5), eval.ndcgAt(10), eval.ndcgAt(20), eval.ndcgAt(40))
        )
      }

    results.map {
      case (singularValues, map, ndcg) =>
        (
          s"n of singular values: $singularValues",
          s"MAP: $map",
          s"ndcg@ 5, 10, 20, 40: ${ndcg.toString}"
        )
    }.foreach(println(_))

    results
  }

  val corpus: Dataset[NplDocument] = readNplCorpus()
  val queryAndRelevanceSets: Array[(String, Array[Long])] = readQueryAndRelevance()

  println("TESTING WITH TF WEIGHT")
  val resTf = evaluation(corpus, queryAndRelevanceSets, tfidf = false)
  println("TESTING WITH TFIDF WEIGHT")
  val resTfIdf = evaluation(corpus, queryAndRelevanceSets, tfidf = true)
}

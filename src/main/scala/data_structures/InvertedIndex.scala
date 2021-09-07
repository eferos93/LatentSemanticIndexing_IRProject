package org.ir.project
package data_structures

import sparkSession.implicits._

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.sum

class InvertedIndex {
  type Term = String
  type DocumentId = Long
  type TermFrequency = Long
  var dictionary: Map[Term, Map[DocumentId, TermFrequency]] = Map.empty
}

object InvertedIndex {
  def apply(corpus: Dataset[Movie]): InvertedIndex = {
    val invertedIndex = new InvertedIndex
    val tokens = removeStopWords(
      corpus.map(movie => clean(movie.plot)).toDF("tokens")
    )
    invertedIndex.dictionary =
      tokens.as[Seq[String]].rdd
        .zipWithIndex // zip with document Id
        .flatMap {
          case (tokens, documentId) => tokens.map(term => (term, documentId, 1))
        }
        .toDF("term", "documentId", "count")
        .groupBy("term", "documentId")
        .agg(sum("count").as("termFrequency"))
        .as[(String, Long, Long)] // convert DataFrame to Dataset[(String, Long, Long)]
        .collect() // collect the dataset and send it to the memory of the driver application
        .groupBy(_._1).mapValues {
          _.map { case (_, docId, termFrequency) => (docId, termFrequency) }.toMap
        }
    invertedIndex
  }

}

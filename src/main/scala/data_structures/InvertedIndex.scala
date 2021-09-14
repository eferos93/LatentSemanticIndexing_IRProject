package org.ir.project
package data_structures

import sparkSession.implicits._

import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{DataFrame, Dataset}

class InvertedIndex(val dictionary: DataFrame, val numberOfDocuments: Long)

object InvertedIndex {
  def apply(corpus: Dataset[Movie]): InvertedIndex = {
    val dictionary: DataFrame =
      removeStopWords(
        corpus
          .map(movie => (movie.id, clean(movie.plot)))
          .toDF("documentId", "tokens")
      ).as[(Int, Seq[String])].rdd //convert to Dataset[(Int, Seq[String])] then to RDD[(Int, Seq[String])]
        .flatMap {
          case (documentId, tokens) => tokens.map(term => (term, documentId, 1))
        }
        .toDF("term", "documentId", "count")
        .groupBy("term", "documentId") // groupBy together with agg, is a relational style aggregation
        .agg(sum("count").as("termFrequency"))
    dictionary.repartition(1).write.option("delimiter", "\t").option("header", "true").csv("dictionary/")
    new InvertedIndex(dictionary, corpus.count)
  }

  def apply(pathToDictionary: String): InvertedIndex = {
    val dictionary = readData(pathToDictionary)
    new InvertedIndex(dictionary, dictionary.select("documentId").distinct.count)
  }
}

package org.ir.project
package data_structures

import sparkSession.implicits._

import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{DataFrame, Dataset}

class InvertedIndex(val dictionary: DataFrame) {
  val numberOfDocuments: Long = dictionary.select("documentId").distinct().count()
}

object InvertedIndex {
  def apply(corpus: Dataset[Movie]): InvertedIndex = {
    val dictionary: DataFrame =
      removeStopWords(
        corpus.map(movie => clean(movie.plot)).toDF("tokens")
      ).as[Seq[String]].rdd //convert to Dataset[Seq[String]] then to RDD[Seq[String]]
        .zipWithIndex // zip with documentId
        .flatMap {
          case (tokens, documentId) => tokens.map(term => (term, documentId, 1))
        }
        .toDF("term", "documentId", "count")
        .groupBy("term", "documentId") // groupBy together with agg, is a relational style aggregation
        .agg(sum("count").as("termFrequency"))
    new InvertedIndex(dictionary)
  }

}

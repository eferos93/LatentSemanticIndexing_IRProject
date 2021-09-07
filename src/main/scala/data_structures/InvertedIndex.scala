package org.ir.project
package data_structures

import sparkSession.implicits._

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.sum

class InvertedIndex(val dictionary: Map[String, Map[Long, Long]]) {
  def keys(): Seq[String] = dictionary.keys.toSeq.sorted
}

object InvertedIndex {
  def apply(corpus: Dataset[Movie]): InvertedIndex = {
    val dictionary: Map[String, Map[Long, Long]] =
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
        .as[(String, Long, Long)] // convert DataFrame to Dataset[(String, Long, Long)]
        .collect() // collect the dataset as an Array[(String, Long, Long)] and send it to the memory of the driver application
        .groupBy(_._1).mapValues { // convert the array to a Map of Maps to a HashMap
          _.map { case (_, docId, termFrequency) => (docId, termFrequency) }.toMap
        }
    new InvertedIndex(dictionary)
  }

}

package org.ir.project
package data_structures

import sparkSession.implicits._

import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import org.apache.spark.storage.StorageLevel

class InvertedIndex(val dictionary: DataFrame, val numberOfDocuments: Long)

object InvertedIndex {
  def apply[T <: Document](corpus: Dataset[T]): InvertedIndex = {
    val index: DataFrame =
      removeStopWords(
        corpus
          .map(document => (document.id, clean(document.description)))
          .toDF("documentId", "tokens")
      ).as[(Long, Seq[String])]
        .flatMap { case (documentId, tokens) => tokens.map(term => (term, documentId, 1)) }
        .toDF("term", "documentId", "count")
        .groupBy("term", "documentId") // groupBy together with agg, is a relational style aggregation
        .agg(sum("count").as("termFrequency"))

    index.repartition(1)
      .write.mode(SaveMode.Ignore)
      .option("delimiter", ",").option("header", "true")
      .csv("index/")
    new InvertedIndex(index.persist(StorageLevel.MEMORY_ONLY_SER), corpus.count)
  }

  def apply(pathToDictionary: String): InvertedIndex = {
    val dictionary = readData(pathToDictionary, delimiter = ",", isHeader = true)
    new InvertedIndex(dictionary, dictionary.select("documentId").distinct.count)
  }
}

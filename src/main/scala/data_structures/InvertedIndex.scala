package org.ir.project
package data_structures

import sparkSession.implicits._

import org.apache.spark.sql.functions.{explode, sum}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import org.apache.spark.storage.StorageLevel

class InvertedIndex(val dictionary: DataFrame, val numberOfDocuments: Long)

object InvertedIndex {
  def apply[T <: Document](corpus: Dataset[T]): InvertedIndex = {
    val index: DataFrame =
      pipelineClean(corpus)
        .select($"id" as "documentId", explode($"tokens") as "term") // explode creates a new row for each element in the given array column
        .groupBy("term", "documentId").count //group by and then count number of rows per group, returning a df with groupings and the counting
        .withColumnRenamed("count", "termFrequency")

    index.repartition(1)
      .write.mode(SaveMode.Ignore)
      .option("delimiter", ",").option("header", "true")
      .csv("index/")
    new InvertedIndex(index.persist(StorageLevel.MEMORY_ONLY_SER), corpus.count)
  }

  def apply(pathToIndex: String): InvertedIndex = {
    val index = readData(pathToIndex, delimiter = ",", isHeader = true)
    new InvertedIndex(index, index.select("documentId").distinct.count)
  }
}

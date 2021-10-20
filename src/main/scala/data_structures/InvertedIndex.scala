package org.ir.project
package data_structures

import sparkSession.implicits._

import org.apache.spark.sql.functions.{collect_list, explode, struct}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import org.apache.spark.storage.StorageLevel

class InvertedIndex(val dictionary: DataFrame, val numberOfDocuments: Long)


//factory methods here (apply)
object InvertedIndex {
  def apply[T <: Document](corpus: Dataset[T]): InvertedIndex = {
    val index: DataFrame =
      pipelineClean(corpus)
        .select($"id" as "documentId", explode($"tokens") as "term") // explode creates a new row for each element in the given array column
        .groupBy("term", "documentId").count //group by and then count number of rows per group, returning a df with groupings and the counting
        .where($"term" =!= "") // seems like there are some tokens that are empty, even though Tokenizer should remove them
        .withColumn("posting", struct($"documentId", $"count")) // merge columns as a single {docId, termFreq}
        .select("term", "posting")
        .groupBy("term").agg(collect_list($"posting") as "postingList") // we do another grouping in order to collect the postings into a list
        .orderBy("term")
        .persist(StorageLevel.MEMORY_ONLY_SER)

    index.write.mode(SaveMode.Ignore) //ignore if already present
      .option("header", "true")
      .parquet("index/")
    new InvertedIndex(index, corpus.count)
  }

  def apply(pathToIndex: String): InvertedIndex = {
    val index = sparkSession.read.option("header", "true").parquet(pathToIndex).persist(StorageLevel.MEMORY_ONLY_SER)
    new InvertedIndex(index, index.select("documentId").distinct.count)
  }
}

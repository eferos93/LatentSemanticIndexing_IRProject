package org.ir.project
package data_structures

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{sum, length}
import sparkSession.implicits._

class InvertedIndex {
  val schema: StructType = StructType(
    Array(
      StructField("term", StringType, nullable = false),
      StructField("documentId", LongType, nullable = false),
      StructField("termFrequency", IntegerType, nullable = false)
    )
  )

  var dictionary: DataFrame = sparkSession.createDataFrame(sparkContext.emptyRDD[Row], schema)
}

object InvertedIndex {
  def apply(tokenizedCorpus: DataFrame): InvertedIndex = {
    val invertedIndex = new InvertedIndex
    invertedIndex.dictionary = makeIndex(tokenizedCorpus).cache()
    invertedIndex
  }

  def apply(pathToTokenizedCorpus: String = "data/cleaned/cleaned.json"): InvertedIndex = {
    val invertedIndex = new InvertedIndex
    invertedIndex.dictionary = makeIndex(readData(pathToTokenizedCorpus).select("title", "tokens", "documentId")).cache()
    invertedIndex
  }

  def makeIndex(tokenizedCorpus: DataFrame): DataFrame = {
      tokenizedCorpus.as[(String, Seq[String], Long)]
        .flatMap {
          case (_, tokens, documentId) => tokens.map(term => (term, documentId, 1))
        }
        .toDF("term", "documentId", "count")
        .groupBy("term", "documentId")
        .agg(sum("count"))
        .orderBy($"sum(count)".desc)
        .withColumnRenamed("sum(count)", "termFrequency")
  }
}

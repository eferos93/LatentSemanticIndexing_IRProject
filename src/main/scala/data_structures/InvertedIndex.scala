package org.ir.project
package data_structures

import sparkSession.implicits._

import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

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
  def apply(corpus: Dataset[Movie]): InvertedIndex = {
    val invertedIndex = new InvertedIndex
    val tokens = removeStopWords(
      corpus.map(movie => clean(movie.plot)).toDF("tokens")
    )

    invertedIndex.dictionary =
      tokens.as[Seq[String]].rdd
        .zipWithIndex
        .flatMap {
          case (tokens, documentId) => tokens.map(term => (term, documentId, 1))
        }
        .toDF("term", "documentId", "count")
        .groupBy("term", "documentId")
        .agg(sum("count").as("termFrequency"))
        .cache()

    invertedIndex
  }

}

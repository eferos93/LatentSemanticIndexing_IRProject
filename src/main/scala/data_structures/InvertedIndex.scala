package org.ir.project
package data_structures

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.sum
import sparkSession.implicits._

class InvertedIndex {
  val schema: StructType = StructType(
    Array(
      StructField("term", StringType, nullable = false),
      StructField("documentId", StringType, nullable = false),
      StructField("count", IntegerType, nullable = false)
    )
  )

  var dictionary: DataFrame = sparkSession.createDataFrame(sparkContext.emptyRDD[Row], schema)
}

object InvertedIndex {
  def apply(corpus: DataFrame) = {
    import sparkSession.implicits._
    val invertedIndex = new InvertedIndex
    val iIndex =
      corpus.as[(String, Seq[String])]
        .flatMap {
          case (docId, tokens) => tokens.map(term => (term, docId, 1))
        }
        .toDF("term", "docId", "count")
        .groupBy("term", "docId")
        .agg(sum("count"))
    println(iIndex)
    iIndex
  }
}

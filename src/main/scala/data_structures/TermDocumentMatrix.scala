package org.ir.project
package data_structures

import org.apache.spark.sql.Dataset
import org.apache.spark.mllib.linalg.SparseMatrix

import scala.math.log
import sparkSession.implicits._

import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
class TermDocumentMatrix(val invertedIndex: InvertedIndex, val matrix: CoordinateMatrix)

object TermDocumentMatrix {
  def apply(corpus: Dataset[Movie]): TermDocumentMatrix = {
    val invertedIndex = InvertedIndex(corpus)
    val numberOfDocuments = invertedIndex.dictionary.select("documentId").distinct().count()
    val matrixEntries =
      invertedIndex.dictionary.as[(String, Long, Long)].rdd
        .groupBy(_._1)//.mapValues(_.map { case (_, documentId, termFrequency) => (documentId, termFrequency) }) //leave out term in value array
        .sortByKey() // sort by term
        .zipWithIndex // add term index
        .flatMap {
          case ((_, docIdsAndFrequencies), termIndex) =>
            val length = docIdsAndFrequencies.toSeq.length
            docIdsAndFrequencies.map {
              case (_, documentId, termFrequency) =>
                MatrixEntry(termIndex, documentId, termFrequency * log(numberOfDocuments/length))
            }
        }
    new TermDocumentMatrix(invertedIndex, new CoordinateMatrix(matrixEntries))
  }
}

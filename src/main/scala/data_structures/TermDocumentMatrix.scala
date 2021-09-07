package org.ir.project
package data_structures

import sparkSession.implicits._

import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry, RowMatrix}
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition}
import org.apache.spark.sql.Dataset

import scala.math.log
class TermDocumentMatrix(val invertedIndex: InvertedIndex, val matrix: RowMatrix) {
  def computeSVD(numberOfSingularValues: Int): SingularValueDecomposition[RowMatrix, Matrix] =
    matrix.computeSVD(numberOfSingularValues, computeU = true)
}

object TermDocumentMatrix {
  def apply(corpus: Dataset[Movie]): TermDocumentMatrix = {
    val invertedIndex = InvertedIndex(corpus)
    val numberOfDocuments = invertedIndex.dictionary.select("documentId").distinct().count()
    val matrixEntries =
      invertedIndex.dictionary.as[(String, Long, Long)].rdd
        .groupBy(_._1) //group by term
        .sortByKey() // sort by term, as ordering might be lost with grouping
        .zipWithIndex // add term index
        .flatMap {
          case ((_, docIdsAndFrequencies), termIndex) =>
            val length = docIdsAndFrequencies.toSeq.length
            docIdsAndFrequencies.map {
              case (_, documentId, termFrequency) =>
                MatrixEntry(termIndex, documentId, termFrequency * log(numberOfDocuments/length))
            }
        }
    new TermDocumentMatrix(invertedIndex, new CoordinateMatrix(matrixEntries).toRowMatrix)
  }
}

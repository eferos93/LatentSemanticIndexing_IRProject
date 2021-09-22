package org.ir.project
package data_structures

import sparkSession.implicits._

import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix

// for the SVD I have to use the old mllib API
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition}
import org.apache.spark.sql.Dataset

import scala.math.log

class TermDocumentMatrix(val invertedIndex: InvertedIndex, val matrix: IndexedRowMatrix) {
  def computeSVD(numberOfSingularValues: Int): SingularValueDecomposition[IndexedRowMatrix, Matrix] =
    matrix.computeSVD(numberOfSingularValues, computeU = true)

  def getVocabulary: Dataset[String] = invertedIndex.dictionary.select("term").distinct.as[String]
}

object TermDocumentMatrix {
  def apply[T <: Document](corpus: Dataset[T]): TermDocumentMatrix =
    computeTermDocumentMatrix(InvertedIndex(corpus))

  def apply(pathToIndex: String): TermDocumentMatrix =
    computeTermDocumentMatrix(InvertedIndex(pathToIndex))

  private def computeTermDocumentMatrix(invertedIndex: InvertedIndex): TermDocumentMatrix = {
    val numberOfDocuments = invertedIndex.numberOfDocuments
    val matrixEntries = invertedIndex.dictionary
      .as[(String, Long, Long)].rdd // (term, docId, termFrequency)
      .groupBy(_._1) //group by term
      .sortByKey() // sort by term, as ordering might be lost with grouping
      .zipWithIndex // add term index
      .flatMap {
        case ((_, docIdsAndFrequencies), termIndex) =>
          val documentFrequency = docIdsAndFrequencies.toSeq.length
          docIdsAndFrequencies.map {
            case (_, documentId, termFrequency) =>
              MatrixEntry(
                termIndex,
                documentId,
                termFrequency * log(numberOfDocuments.toDouble / documentFrequency.toDouble)
              )
          }
      }
    new TermDocumentMatrix(invertedIndex, new CoordinateMatrix(matrixEntries).toIndexedRowMatrix)
  }
}

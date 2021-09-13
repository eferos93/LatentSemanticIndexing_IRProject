package org.ir.project
package data_structures

import sparkSession.implicits._

// for the SVD I have to use the old mllib API
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry, RowMatrix}
import org.apache.spark.mllib.linalg.{SingularValueDecomposition, Matrix}
import org.apache.spark.sql.Dataset

import scala.math.log

class TermDocumentMatrix(val invertedIndex: InvertedIndex, val matrix: RowMatrix) {
  def computeSVD(numberOfSingularValues: Int): SingularValueDecomposition[RowMatrix, Matrix] =
    matrix.computeSVD(numberOfSingularValues, computeU = true)

  def getVocabulary: Dataset[String] = invertedIndex.dictionary.select("term").as[String]
}

object TermDocumentMatrix {
  def apply(corpus: Dataset[Movie]): TermDocumentMatrix =
    computeTermDocumentMatrix(InvertedIndex(corpus))

  def apply(pathToIndex: String): TermDocumentMatrix =
    computeTermDocumentMatrix(InvertedIndex(pathToIndex))

  def computeTermDocumentMatrix(invertedIndex: InvertedIndex): TermDocumentMatrix = {
    val numberOfDocuments = invertedIndex.numberOfDocuments
    val matrixEntries = invertedIndex.dictionary.as[(String, Long, Long)].rdd
      .groupBy(_._1) //group by term
      .sortByKey() // sort by term, as ordering might be lost with grouping
      .zipWithIndex // add term index
      .flatMap {
        case ((_, docIdsAndFrequencies), termIndex) =>
          val length = docIdsAndFrequencies.toSeq.length
          docIdsAndFrequencies.map {
            case (_, documentId, termFrequency) =>
              MatrixEntry(termIndex, documentId, termFrequency * log(numberOfDocuments / length))
          }
      }
    new TermDocumentMatrix(invertedIndex, new CoordinateMatrix(matrixEntries).toRowMatrix())
  }
}

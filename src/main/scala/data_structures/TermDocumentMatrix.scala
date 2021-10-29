package org.ir.project
package data_structures

import data_structures.weigher.{TermFrequencyWeighting, TfIdfWeighting, WordWeight}
import sparkSession.implicits._

import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix

// for the SVD I have to use the old mllib API
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition}
import org.apache.spark.sql.Dataset



class TermDocumentMatrix(val invertedIndex: InvertedIndex, val matrix: IndexedRowMatrix) {
  def computeSVD(numberOfSingularValues: Int): SingularValueDecomposition[IndexedRowMatrix, Matrix] =
    matrix.computeSVD(numberOfSingularValues, computeU = true)

  def getVocabulary: Dataset[String] = invertedIndex.dictionary.select("term").as[String]
}

//here we define factory methods (apply)
//it is possible to invoke them implicitly, e.g. TermDocumentMatrix(corpus, tfidf = true) will invoke the first apply below
object TermDocumentMatrix {
  def apply[T <: Document](corpus: Dataset[T], tfidf: Boolean): TermDocumentMatrix = 
    computeTermDocumentMatrix(InvertedIndex(corpus), getWeigher(tfidf))

  def apply(pathToIndex: String, tfidf: Boolean): TermDocumentMatrix =
    computeTermDocumentMatrix(InvertedIndex(pathToIndex), getWeigher(tfidf))

  def getWeigher(tfidf: Boolean): WordWeight =
    if (tfidf) TfIdfWeighting() else TermFrequencyWeighting()

  private def computeTermDocumentMatrix[T <: WordWeight](invertedIndex: InvertedIndex,
                                                         wordWeight: T): TermDocumentMatrix = {
    val numberOfDocuments = invertedIndex.numberOfDocuments
    val matrixEntries = invertedIndex.dictionary
      .as[(String, Array[(Long, Long)])].rdd // RDD[(term, Array[(docId, termFreq)])]
      .zipWithIndex // add term index: RDD[((term, Array[(docId, termFreq)]), termIndex)]
      .flatMap {
        case ((_, docIdsAndFrequencies: Array[(Long, Long)]), termIndex: Long) =>
          val documentFrequency = docIdsAndFrequencies.toSeq.length
          docIdsAndFrequencies.map {
            case (documentId: Long, termFrequency: Long) =>
              val args = Map[String, Long](
                "termFrequency" -> termFrequency,
                "numberOfDocuments" -> numberOfDocuments,
                "documentFrequency" -> documentFrequency
              )
              MatrixEntry(
                termIndex,
                documentId,
                wordWeight.weight(args)
              )
          }
      } // here we end up with a RDD[MatrixEntry]
    new TermDocumentMatrix(invertedIndex, new CoordinateMatrix(matrixEntries).toIndexedRowMatrix)
  }
}

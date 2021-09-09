package org.ir.project

import data_structures.{Movie, TermDocumentMatrix}

import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition}
import org.apache.spark.sql.Dataset

class IRSystem(corpus: Dataset[Movie],
               vocabulary: Dataset[String],
               singularValueDecomposition: SingularValueDecomposition[RowMatrix, Matrix])

object IRSystem {
  def apply(corpus: Dataset[Movie], k: Int): IRSystem = {
    val termDocumentMatrix = TermDocumentMatrix(corpus)
    initializeIRSystem(corpus, termDocumentMatrix, k)
  }

  def apply(corpus: Dataset[Movie], pathToDictionary: String, k: Int): IRSystem = {
    val termDocumentMatrix = TermDocumentMatrix(pathToDictionary)
    initializeIRSystem(corpus, termDocumentMatrix, k)
  }

  def initializeIRSystem(corpus: Dataset[Movie], termDocumentMatrix: TermDocumentMatrix, k: Int): IRSystem = {
    val vocabulary = termDocumentMatrix.getVocabulary
    val singularValueDecomposition = termDocumentMatrix.computeSVD(k)
    new IRSystem(corpus, vocabulary, singularValueDecomposition)
  }
}

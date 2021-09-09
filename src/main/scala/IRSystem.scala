package org.ir.project

import data_structures.{Movie, TermDocumentMatrix}

import breeze.linalg.Transpose
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{DenseVector, Matrices, Matrix, SingularValueDecomposition}
import org.apache.spark.sql.Dataset

class IRSystem(corpus: Dataset[Movie],
               vocabulary: Dataset[String],
               singularValueDecomposition: SingularValueDecomposition[RowMatrix, Matrix]) {
  def mapQueryVector(queryVector: DenseVector): DenseVector = {
    val inverseDiagonalSigma = Matrices.diag(new DenseVector(singularValueDecomposition.s.toArray.map(math.pow(_, -1))))
    inverseDiagonalSigma.multiply()
    Matrices.
  }
}

object IRSystem {
  def apply(corpus: Dataset[Movie], k: Int): IRSystem = {
    val termDocumentMatrix = TermDocumentMatrix(corpus)
    initializeIRSystem(corpus, termDocumentMatrix, k)
  }

  def apply(corpus: Dataset[Movie], pathToIndex: String, k: Int): IRSystem = {
    val termDocumentMatrix = TermDocumentMatrix(pathToIndex)
    initializeIRSystem(corpus, termDocumentMatrix, k)
  }

  def initializeIRSystem(corpus: Dataset[Movie], termDocumentMatrix: TermDocumentMatrix, k: Int): IRSystem = {
    val vocabulary = termDocumentMatrix.getVocabulary
    val singularValueDecomposition = termDocumentMatrix.computeSVD(k)
    new IRSystem(corpus, vocabulary, singularValueDecomposition)
  }
}

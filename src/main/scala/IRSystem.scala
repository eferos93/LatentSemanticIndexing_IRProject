package org.ir.project

import data_structures.{Movie, TermDocumentMatrix}

import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{DenseVector, Matrices, Matrix, Vector}
import org.apache.spark.sql.Dataset

class IRSystem(corpus: Dataset[Movie],
               vocabulary: Dataset[String],
               U: RowMatrix, sigma: Vector, V: Matrix) {
  def mapQueryVector(queryVector: DenseVector): DenseVector = {
    val inverseDiagonalSigma = Matrices.diag(new DenseVector(sigma.toArray.map(math.pow(_, -1))))
    inverseDiagonalSigma.multiply(transposeRowMatrix(U)).multiply(queryVector)
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
    val U = singularValueDecomposition.U
    val sigma = singularValueDecomposition.s
    val V = singularValueDecomposition.V
    new IRSystem(corpus, vocabulary, U, sigma, V)
  }
}

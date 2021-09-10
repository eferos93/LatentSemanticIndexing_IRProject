package org.ir.project

import data_structures.{Movie, TermDocumentMatrix}

import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{DenseVector, Matrices, Matrix, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.sql.Dataset
import sparkSession.implicits._

class IRSystem(corpus: Dataset[Movie],
               vocabulary: Dataset[String],
               U: RowMatrix, sigma: Vector, V: RowMatrix) {
  def mapQueryVector(queryVector: Vector): DenseVector = {
    val inverseDiagonalSigma = Matrices.diag(new DenseVector(sigma.toArray.map(math.pow(_, -1))))
    inverseDiagonalSigma.multiply(transposeRowMatrix(U)).multiply(queryVector)
  }

  def buildQueryVector(textQuery: String): Vector = {
    val tokens = removeStopWords(List(clean(textQuery)).toDF("tokens")).first().getAs[Seq[String]](0)
    val asRDD = vocabulary.rdd.zipWithIndex.map { case (word, index) => (index.toInt, tokens.count(_ == word).toDouble) }
    val queryVector = Vectors.sparse(vocabulary.count().toInt, asRDD.collect())
    mapQueryVector(queryVector)
  }

  def answerQuery(textQuery: String, top: Int = 5) = {
    val queryVector = buildQueryVector(textQuery)
    val scores = V.rows.map(queryVector.dot).zipWithIndex().sortByKey(ascending = false).take(top)

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
    val V = matrixToRowMatrix(singularValueDecomposition.V)
    new IRSystem(corpus, vocabulary, U, sigma, V)
  }
}

package org.ir.project

import data_structures.{Movie, TermDocumentMatrix}
import sparkSession.implicits._

import org.apache.spark.ml.linalg.{DenseMatrix, DenseVector, Matrices, Vector, Vectors}
import org.apache.spark.sql.Dataset

class IRSystem(corpus: Dataset[Movie],
               vocabulary: Dataset[String],
               U: DenseMatrix, sigma: DenseVector, V: DenseMatrix) {

  def mapQueryVector(queryVector: Vector): DenseVector = {
    val inverseDiagonalSigma = Matrices.diag(new DenseVector(sigma.toArray.map(math.pow(_, -1))))
    inverseDiagonalSigma.multiply(U.transpose).multiply(queryVector)
  }

  def buildQueryVector(textQuery: String): Vector = {
    val tokens = removeStopWords(List(clean(textQuery)).toDF("tokens")).first().getAs[Seq[String]](0)
    val asRDD = vocabulary.rdd.zipWithIndex.map { case (word, index) => (index.toInt, tokens.count(_ == word).toDouble) }
    val queryVector = Vectors.sparse(vocabulary.count.toInt, asRDD.collect)
    mapQueryVector(queryVector)
  }

  def answerQuery(textQuery: String, top: Int = 5): Seq[(Movie, Double)] = {
    val queryVector = buildQueryVector(textQuery)
    V.rowIter.toStream
      .map(queryVector.dot).zipWithIndex.sortBy(_._1)
      .map { case (score, documentId) => (corpus.where($"id" === documentId + 1).first, score) }
      .take(top)
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
    val singularValueDecomposition = termDocumentMatrix.computeSVD(k)
    val U = singularValueDecomposition.U
    val UasDense = new DenseMatrix(U.numRows.toInt, U.numCols.toInt, U.rows.flatMap(_.toArray).collect, isTransposed = false)
    val sigma = singularValueDecomposition.s.asML.toDense
    val V = normaliseMatrix(singularValueDecomposition.V.asML.toDense)
    new IRSystem(corpus, termDocumentMatrix.getVocabulary, UasDense, sigma, V)
  }
}

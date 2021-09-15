package org.ir.project

import data_structures.{Movie, TermDocumentMatrix}
import sparkSession.implicits._

import org.apache.spark.ml.linalg.{DenseMatrix, DenseVector, Matrices, Vector, Vectors}
import org.apache.spark.sql.Dataset

class IRSystem(val corpus: Dataset[Movie],
               val vocabulary: Dataset[String],
               val U: DenseMatrix, val sigma: DenseVector, val V: DenseMatrix) {

  private def mapQueryVector(queryVector: DenseVector): DenseVector = {
    val inverseDiagonalSigma = Matrices.diag(new DenseVector(sigma.toArray.map(math.pow(_, -1))))
    val partial = inverseDiagonalSigma.multiply(U.transpose)
    partial.multiply(queryVector)
  }

  private def buildQueryVector(textQuery: String): DenseVector = {
    val tokens = removeStopWords(
      List(clean(textQuery)).toDF("tokens"), extraColumns = Seq.empty
    ).first().getAs[Seq[String]](0)
//    val asRDD = vocabulary.rdd.zipWithIndex.map { case (word, index) => (index.toInt, tokens.count(_ == word).toDouble) }
    val queryVector = Vectors.dense(vocabulary.map(word => tokens.count(_ == word).toDouble).collect)
    mapQueryVector(queryVector.toDense)
  }

  private def answerQuery(textQuery: String, top: Int): Seq[(Movie, Double)] = {
    val queryVector = buildQueryVector(textQuery)
    V.rowIter.toStream
      .map(queryVector.dot).zipWithIndex.sortBy(_._1)
      .map { case (score, documentId) => (corpus.where($"id" === documentId + 1).first, score) }
      .take(top)
  }

  def query(query: String, top: Int = 5): Unit =
    println(answerQuery(query, top).mkString("\n"))

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

  private def initializeIRSystem(corpus: Dataset[Movie], termDocumentMatrix: TermDocumentMatrix, k: Int): IRSystem = {
    val singularValueDecomposition = termDocumentMatrix.computeSVD(k)
    val U = singularValueDecomposition.U
    val UasDense =
      new DenseMatrix(U.numRows.toInt, U.numCols.toInt, U.rows.flatMap(_.vector.toArray).collect, isTransposed = false)
    val sigma = singularValueDecomposition.s.asML.toDense
//  normalising is just needed to have scores between 0 and 1, but it won't change the rank
//    it is kinda expensive as the matrix is big, thus this step is skipped
//    val V = normaliseMatrix(singularValueDecomposition.V.asML.toDense)
    new IRSystem(corpus, termDocumentMatrix.getVocabulary, UasDense, sigma, singularValueDecomposition.V.asML.toDense)
  }
}

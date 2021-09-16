package org.ir.project

import data_structures.{Movie, TermDocumentMatrix}
import sparkSession.implicits._

import org.apache.spark.ml.linalg.{DenseMatrix, DenseVector, Matrices, Vector, Vectors}
import org.apache.spark.sql.Dataset
import org.apache.spark.storage.StorageLevel

class IRSystem(val corpus: Dataset[Movie],
               val vocabulary: Dataset[String],
               val U: DenseMatrix, val sigma: DenseVector, val V: DenseMatrix) extends Serializable {

  private def mapQueryVector(queryVector: Vector): DenseVector = {
    val inverseDiagonalSigma = Matrices.diag(new DenseVector(sigma.toArray.map(math.pow(_, -1))))
    inverseDiagonalSigma.multiply(U.transpose).multiply(queryVector)
  }

  private def buildQueryVector(textQuery: String): Vector = {
    val tokens = removeStopWords(
      List(clean(textQuery)).toDF("tokens"), extraColumns = Seq.empty
    ).first().getAs[Seq[String]](0)
    val queryVector = Vectors.dense(vocabulary.map(word => tokens.count(_ == word).toDouble).collect)
    mapQueryVector(queryVector)
  }

  private def answerQuery(textQuery: String, top: Int): Seq[(Movie, Double)] = {
    val queryVector = buildQueryVector(textQuery)
    sparkContext.parallelize(V.rowIter.toSeq).zipWithIndex
      .map { case (vector, documentId) => (documentId, -queryVector.dot(vector))}
      .sortBy(_._2, ascending = false) // descending sorting
      .take(top)
      .map { case (documentId, score) => (corpus.where($"id" === documentId).first, score)}
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
    new IRSystem(corpus.persist(StorageLevel.MEMORY_ONLY_SER),
      termDocumentMatrix.getVocabulary.persist(StorageLevel.MEMORY_ONLY_SER),
      UasDense, sigma, singularValueDecomposition.V.asML.toDense)
  }
}

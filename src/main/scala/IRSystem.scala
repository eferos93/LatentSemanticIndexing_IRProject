package org.ir.project

import data_structures.{Document, TermDocumentMatrix}
import sparkSession.implicits._

import org.apache.spark.ml.linalg.{DenseMatrix, DenseVector, Matrices, Matrix, Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.storage.StorageLevel

class IRSystem[T <: Document](val corpus: Dataset[T],
               val vocabulary: Dataset[String],
               val U: DenseMatrix, val sigma: Matrix, val V: RDD[(Vector, Long)]) extends Serializable {

  def mapQueryVector(queryVector: Vector): DenseVector =
    sigma.multiply(U.transpose).multiply(queryVector)

  def buildQueryVector(textQuery: String): Vector = {
    val tokens = removeStopWords(
      List(clean(textQuery)).toDF("tokens"), extraColumns = Seq.empty
    ).first().getAs[Seq[String]](0)
    val queryVector = Vectors.dense(vocabulary.map(word => tokens.count(_.equals(word)).toDouble).collect)
    mapQueryVector(queryVector)
  }

  def computeCosineSimilarity(firstVector: Vector, secondVector: Vector): Double =
    firstVector.dot(secondVector) / (Vectors.norm(firstVector, 2.0) * Vectors.norm(secondVector, 2.0))

  def answerQuery(textQuery: String, top: Int): Seq[(T, Double)] = {
    val queryVector = buildQueryVector(textQuery)
    V.map { case (documentVector, documentId) => (documentId, computeCosineSimilarity(queryVector, documentVector)) }
      .sortBy(_._2, ascending = false)
      .take(top)
      .map { case (documentId, score) => (corpus.where($"id" === documentId).first, score) }
  }

  def query(query: String, top: Int = 5): Unit =
    println(answerQuery(query, top).mkString("\n"))

  def saveIrSystem(): Unit = {
    sparkContext.parallelize(U.rowIter.toSeq, numSlices = 1).saveAsTextFile("matrices/U")
    V.map(_._1).repartition(1).saveAsTextFile("matrices/V")
    sparkContext.parallelize(Seq(sigma), numSlices = 1).saveAsTextFile("matrices/s")
  }


}

object IRSystem {
  def apply[T <: Document](corpus: Dataset[T], k: Int): IRSystem[T] = {
    val termDocumentMatrix = TermDocumentMatrix(corpus)
    initializeIRSystem(corpus, termDocumentMatrix, k)
  }

  def apply[T <: Document](corpus: Dataset[T], pathToIndex: String, k: Int): IRSystem[T] = {
    val termDocumentMatrix = TermDocumentMatrix(pathToIndex)
    initializeIRSystem(corpus, termDocumentMatrix, k)
  }

  def apply(corpus: Dataset[Document], pathToIndex: String, pathToMatrices: String, k: Int) = {
    val U = readMatrix(s"$pathToMatrices/U/part-00000")
    val V = readMatrix(s"$pathToMatrices/V/part-00000")

  }

  def readMatrix(pathToMatrix: String): Matrix = {
    val matrixAsRDD = sparkContext.textFile(pathToMatrix).zipWithIndex
      .map { case (line, index) => IndexedRow(index, OldVectors.parse(line)) }
//    println(matrixAsRDD.first())
//    println(asRowMatrix.rows.first().equals(matrixAsRDD.first()))
//    Matrices.dense(
//      asRowMatrix.numRows.toInt,
//      asRowMatrix.numCols.toInt,
//      asRowMatrix.rows.flatMap(_.toArray).collect
//    ).transpse //transposing because Matrices.dense creates a column major matrix
//    val block = new IndexedRowMatrix(matrixAsRDD, matrixAsRDD.count, matrixAsRDD.first().vector.size)
//      .toBlockMatrix()
//    OldMatrices.dense()
    new IndexedRowMatrix(matrixAsRDD, matrixAsRDD.count, matrixAsRDD.first().vector.size)
      .toBlockMatrix().toLocalMatrix().asML.toDense
  }

  def initializeIRSystem[T <: Document](corpus: Dataset[T],
                                 termDocumentMatrix: TermDocumentMatrix, k: Int): IRSystem[T] = {
    val singularValueDecomposition = termDocumentMatrix.computeSVD(k)
    val UasDense = singularValueDecomposition.U.toBlockMatrix().toLocalMatrix().asML.toDense
    val V = singularValueDecomposition.V
//    val UasDense =
//      Matrices.dense(U.numRows.toInt, U.numCols.toInt, U.rows.flatMap(_.vector.toArray).collect)
//        .toDense.transpose

//    val VAsDense =
//      Matrices.dense(V.numRows, V.numCols, V.rowIter.flatMap(_.toArray).toArray)
//        .toDense.transpose
//    val VAsDense = V.asML.toDenseRowMajor
    val VAsDense = sparkContext.parallelize(V.asML.rowIter.toSeq).zipWithIndex.persist(StorageLevel.MEMORY_ONLY_SER)
    val sigma = Matrices.diag(new DenseVector(singularValueDecomposition.s.toArray.map(1/_)))
//  normalising is just needed to have scores between 0 and 1, but it won't change the rank
//    it is kinda expensive as the matrix is big, thus this step is skipped
//    val V = normaliseMatrix(singularValueDecomposition.V.asML.toDense)
    new IRSystem(corpus.persist(StorageLevel.MEMORY_ONLY_SER),
      termDocumentMatrix.getVocabulary.persist(StorageLevel.MEMORY_ONLY_SER),
      UasDense, sigma, VAsDense)
  }
}

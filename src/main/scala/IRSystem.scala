package org.ir.project

import data_structures.{Document, TermDocumentMatrix}
import sparkSession.implicits._

import org.apache.spark.ml.linalg.{DenseMatrix, DenseVector, Matrices, Matrix, Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.storage.StorageLevel

class IRSystem[T <: Document](val corpus: Dataset[T],
                              val vocabulary: Dataset[String],
                              val U: DenseMatrix, val inverseSigma: Matrix, val V: RDD[(Vector, Long)]) extends Serializable {

  def mapQueryVector(queryVector: Vector): DenseVector =
    inverseSigma.multiply(U.transpose).multiply(queryVector)

  def buildQueryVector(textQuery: String): Vector = {
    val tokens = pipelineClean(
      List(textQuery).toDF("description"), extraColumns = Seq.empty
    ).first.getAs[Seq[String]](0) // after cleaning, we have a df with only one row: we get the first element which will be as Row type and get its content as Seq[String]
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
    implicit val encoder: Encoder[Vector] = ExpressionEncoder(): Encoder[Vector]
    sparkSession.createDataset[Vector](sparkContext.parallelize(U.rowIter.toSeq))//(ExpressionEncoder(): Encoder[Vector])
      .write.parquet("matrices/U")
    sparkSession.createDataset(V.map(_._1))//(ExpressionEncoder(): Encoder[Vector])
      .write.parquet("matrices/V")//.saveAsTextFile("matrices/V")
    sparkSession.createDataset(sparkContext.parallelize(inverseSigma.rowIter.toSeq))//(ExpressionEncoder(): Encoder[Vector])
      .write.parquet("matrices/s")//.saveAsTextFile("matrices/s")
  }


}

object IRSystem {
  def apply[T <: Document](corpus: Dataset[T], numberOfSingularValues: Int, tfidf: Boolean): IRSystem[T] = {
    val termDocumentMatrix = TermDocumentMatrix(corpus, tfidf)
    initializeIRSystem(corpus, termDocumentMatrix, numberOfSingularValues)
  }

  def apply[T <: Document](corpus: Dataset[T],
                           pathToIndex: String,
                           numberOfSingularValues: Int,
                           tfidf: Boolean): IRSystem[T] = {
    val termDocumentMatrix = TermDocumentMatrix(pathToIndex, tfidf)
    initializeIRSystem(corpus, termDocumentMatrix, numberOfSingularValues)
  }

  def apply(corpus: Dataset[Document], pathToIndex: String, pathToMatrices: String): IRSystem[Document] = {
    val U = readMatrix(s"$pathToMatrices/U/").toDense
    val V = readV(s"$pathToMatrices/V/")
    val inverseSigma = readMatrix(s"$pathToMatrices/s/")
    val index = sparkSession.read.option("header", "true").parquet(pathToIndex)
    new IRSystem(corpus,
      index.select("term").distinct.as[String].persist(StorageLevel.MEMORY_ONLY_SER),
      U, inverseSigma, V
    )
  }

  def readV(pathToV: String): RDD[(Vector, Long)] =
    sparkContext.textFile(pathToV).zipWithIndex
      .map { case (line, index) => (OldVectors.parse(line).asML, index) }

  def readMatrix(pathToMatrix: String): Matrix = {
    val matrixAsRDD = sparkContext.textFile(pathToMatrix).zipWithIndex
      .map { case (line, index) => IndexedRow(index, OldVectors.parse(line)) }
    new IndexedRowMatrix(matrixAsRDD, matrixAsRDD.count, matrixAsRDD.first.vector.size)
      .toBlockMatrix.toLocalMatrix.asML.toDense
  }

  def initializeIRSystem[T <: Document](corpus: Dataset[T],
                                        termDocumentMatrix: TermDocumentMatrix,
                                        numberOfSingularValues: Int): IRSystem[T] = {
    val singularValueDecomposition = termDocumentMatrix.computeSVD(numberOfSingularValues)
    val UasDense = singularValueDecomposition.U.toBlockMatrix.toLocalMatrix.asML.toDense
    val VAsDense = sparkContext.parallelize(singularValueDecomposition.V.asML.rowIter.toSeq)
      .zipWithIndex.persist(StorageLevel.MEMORY_ONLY_SER)
    val inverseSigma = Matrices.diag(new DenseVector(singularValueDecomposition.s.toArray.map(1/_)))
    new IRSystem(corpus,
      termDocumentMatrix.getVocabulary.persist(StorageLevel.MEMORY_ONLY_SER),
      UasDense, inverseSigma, VAsDense)
  }
}

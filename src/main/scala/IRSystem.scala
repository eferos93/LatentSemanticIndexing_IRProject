package org.ir.project

import data_structures.{Document, TermDocumentMatrix}
import sparkSession.implicits._

import org.apache.spark.ml.linalg.{DenseMatrix, DenseVector, Matrices, Matrix, Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.sql.{Dataset, SaveMode}
import org.apache.spark.storage.StorageLevel

class IRSystem[T <: Document](val corpus: Dataset[T],
                              val vocabulary: Dataset[String],
                              val U: DenseMatrix, val inverseSigma: Matrix, val V: Dataset[(Vector, Int)]) extends Serializable {

  /**
   * Map a queryVector to the lower dimensional space
   * @param queryVector vector representation of the query
   * @return vector representing the queryVector in a Lower dimensional space
   */
  def mapQueryVector(queryVector: Vector): DenseVector =
    inverseSigma.multiply(U.transpose).multiply(queryVector)


  /**
   * Given a text query, it cleans it and convert it to vector representation
   * @param textQuery text query
   * @return vector representation of the query
   */
  def buildQueryVector(textQuery: String): Vector = {
    val tokens = pipelineClean(
      List(textQuery).toDF("description"), extraColumns = Seq.empty
    ).first.getAs[Seq[String]](0) // after cleaning, we have a df with only one row: we get the first element which will be as Row type and get its content as Seq[String]
    val queryVector = Vectors.dense(vocabulary.map(word => tokens.count(_.equals(word)).toDouble).collect)
    mapQueryVector(queryVector)
  }

  def computeCosineSimilarity(firstVector: Vector, secondVector: Vector): Double =
    firstVector.dot(secondVector) / (Vectors.norm(firstVector, 2.0) * Vectors.norm(secondVector, 2.0))

  /**
   * Given a text query, gets the query vector
   * @param textQuery plain text query
   * @param top how many documents with the highest score we want to keep
   * @return Sequence of (document, score)
   */
  def answerQuery(textQuery: String, top: Int): Seq[(T, Double)] = {
    val queryVector = buildQueryVector(textQuery)
    V.map { case (documentVector, documentId) => (documentId, computeCosineSimilarity(queryVector, documentVector)) }
      .orderBy($"_2".desc) //order by the second column (cosine sim. score) descending
      .take(top)
      .map { case (documentId, score) => (corpus.where($"id" === documentId).first, score) }
  }

  def query(query: String, top: Int = 10): Unit =
    println(answerQuery(query, top).mkString("\n"))

  /**
   * save the matrices U, V, sigma
   */
  def saveIrSystem(): Unit = {
    sparkSession.createDataset(U.rowIter.toSeq.zipWithIndex)
      .write.mode(SaveMode.Overwrite).parquet("matrices/U")
    V.write.parquet("matrices/V")
    sparkSession.createDataset(inverseSigma.rowIter.toSeq.zipWithIndex)
      .write.mode(SaveMode.Overwrite).parquet("matrices/s")
  }
}

object IRSystem {
  def apply[T <: Document](corpus: Dataset[T], numberOfSingularValues: Int, tfidf: Boolean): IRSystem[T] = {
    val termDocumentMatrix = TermDocumentMatrix(corpus, tfidf)
    initializeIRSystem(corpus, termDocumentMatrix, numberOfSingularValues)
  }

  def apply[T <: Document](corpus: Dataset[T],
                           numberOfSingularValues: Int,
                           pathToIndex: String,
                           tfidf: Boolean): IRSystem[T] = {
    val termDocumentMatrix = TermDocumentMatrix(pathToIndex, tfidf)
    initializeIRSystem(corpus, termDocumentMatrix, numberOfSingularValues)
  }

  def apply[T <: Document](corpus: Dataset[T], pathToIndex: String, pathToMatrices: String): IRSystem[T] = {
    val U = readMatrix(s"$pathToMatrices/U/").toDense
    val V = readV(s"$pathToMatrices/V/")
    val inverseSigma = readMatrix(s"$pathToMatrices/s/")
    val index = sparkSession.read.option("header", "true").parquet(pathToIndex)
    new IRSystem(corpus,
      index.select("term").distinct.as[String].persist(StorageLevel.MEMORY_ONLY_SER),
      U, inverseSigma, V
    )
  }

  def readV(pathToV: String): Dataset[(Vector, Int)] =
    sparkSession.read.parquet(pathToV).orderBy($"_2").as[(Vector, Int)]

  def readMatrix(pathToMatrix: String): Matrix = {
    val matrixDF = sparkSession.read.parquet(pathToMatrix).orderBy($"_2").as[(Vector, Int)]
      .map { case (vector, index) => IndexedRow(index, OldVectors.parse(vector.toString)) }
    new IndexedRowMatrix(matrixDF.rdd, matrixDF.count, matrixDF.first.vector.size)
      .toBlockMatrix.toLocalMatrix.asML.toDense
  }

  def initializeIRSystem[T <: Document](corpus: Dataset[T],
                                        termDocumentMatrix: TermDocumentMatrix,
                                        numberOfSingularValues: Int): IRSystem[T] = {
    val singularValueDecomposition = termDocumentMatrix.computeSVD(numberOfSingularValues)
    val UasDense = singularValueDecomposition.U.toBlockMatrix.toLocalMatrix.asML.toDense
    val V = singularValueDecomposition.V.asML.rowIter.toSeq
      .zipWithIndex //zip with the documentIDs
      .toDS.persist(StorageLevel.MEMORY_ONLY_SER)
    val inverseSigma = Matrices.diag(new DenseVector(singularValueDecomposition.s.toArray.map(1/_)))
    new IRSystem(corpus,
      termDocumentMatrix.getVocabulary.persist(StorageLevel.MEMORY_ONLY_SER),
      UasDense, inverseSigma, V)
  }
}

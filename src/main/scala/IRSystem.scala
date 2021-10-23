package org.ir.project

import data_structures.{Document, TermDocumentMatrix}
import sparkSession.implicits._

import org.apache.spark.ml.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.{BlockMatrix, IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.linalg.{Matrices => OldMatrices, Vectors => OldVectors}
import org.apache.spark.sql.{Dataset, SaveMode}
import org.apache.spark.storage.StorageLevel

class IRSystem[T <: Document](val corpus: Dataset[T],
                              val vocabulary: Dataset[String],
                              val U: BlockMatrix, val inverseSigma: BlockMatrix, val V: Dataset[(Vector, Int)]) extends Serializable {

  /**
   * Map a queryVector to the lower dimensional space
   * @param queryVector vector representation of the query
   * @return vector representing the queryVector in a Lower dimensional space
   */
  def mapQueryVector(queryVector: BlockMatrix): DenseVector =
    inverseSigma.multiply(U.transpose).multiply(queryVector) // here we end up with a column vector represented as a BlockMatrix
      .toLocalMatrix.asML.colIter.toSeq.head.toDense // convert the result BlockMatrix to a local Vector


  /**
   * Given a text query, it cleans it and convert it to vector representation
   * @param textQuery text query
   * @return vector representation of the query
   */
  def buildQueryVector(textQuery: String): Vector = {
    val tokens = pipelineClean(
      List(textQuery).toDF("description"), extraColumns = Seq.empty
    ).first.getAs[Seq[String]](0) // after cleaning, we have a df with only one row: we get the first element which will be as Row type and get its content as Seq[String]
    val queryVector = vocabulary.rdd.zipWithIndex.map {
      case (word, index) =>
        IndexedRow(
          index,
          OldVectors.dense(tokens.count(_.equals(word)).toDouble)
        )
    }
    val columnVector = new IndexedRowMatrix(queryVector.persist(StorageLevel.MEMORY_ONLY_SER), vocabulary.count, 1)
    mapQueryVector(columnVector.toBlockMatrix)
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

  def query(query: String, top: Int = 5): Unit =
    println(answerQuery(query, top).mkString("\n"))

  /**
   * save the matrices U, V, sigma
   */
  def saveIrSystem(): Unit = {
    sparkSession.createDataset(U.toLocalMatrix.asML.rowIter.toSeq.zipWithIndex)
      .write.mode(SaveMode.Overwrite).parquet("matrices/U")
    V.write.parquet("matrices/V")
    sparkSession.createDataset(inverseSigma.toLocalMatrix.asML.rowIter.toSeq.zipWithIndex)
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
    val U = readMatrix(s"$pathToMatrices/U/")
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

  def readMatrix(pathToMatrix: String): BlockMatrix = {
    val matrixDF = sparkSession.read.parquet(pathToMatrix).as[(Vector, Int)]
      .map { case (vector, index) => IndexedRow(index, OldVectors.parse(vector.toString)) }
    new IndexedRowMatrix(matrixDF.rdd, matrixDF.count, matrixDF.first.vector.size)
      .toBlockMatrix
  }

  def initializeIRSystem[T <: Document](corpus: Dataset[T],
                                        termDocumentMatrix: TermDocumentMatrix,
                                        numberOfSingularValues: Int): IRSystem[T] = {
    val singularValueDecomposition = termDocumentMatrix.computeSVD(numberOfSingularValues)
    val UasBlock = singularValueDecomposition.U.toBlockMatrix
    val V = singularValueDecomposition.V.asML.rowIter.toSeq
      .zipWithIndex //zip with the documentIDs
      .toDS.persist(StorageLevel.MEMORY_ONLY_SER) // convert to DataSet[(Vector, Int)]
    val inverseSigmaAsLocal = OldMatrices.diag(OldVectors.dense(singularValueDecomposition.s.toArray.map(1/_)))
    val rddMatrix = sparkContext.parallelize(inverseSigmaAsLocal.rowIter.zipWithIndex.toSeq).map {
      case (row, index) => IndexedRow(index, row)
    }
    val inverseSigmaAsBlock = new IndexedRowMatrix(rddMatrix, numberOfSingularValues, numberOfSingularValues).toBlockMatrix
    new IRSystem(corpus,
      termDocumentMatrix.getVocabulary.persist(StorageLevel.MEMORY_ONLY_SER),
      UasBlock, inverseSigmaAsBlock, V)
  }
}

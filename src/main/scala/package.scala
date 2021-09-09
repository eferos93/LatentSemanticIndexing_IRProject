package org.ir


import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.ir.project.data_structures.Movie


package object project {

  lazy val sparkConfiguration: SparkConf = new SparkConf()
    .setAppName("Latent Semantic Indexing")
    .setMaster("local[*]")
    .set("spark.cores.max", Runtime.getRuntime.availableProcessors.toString)

  lazy val sparkSession: SparkSession = SparkSession.builder.config(sparkConfiguration).getOrCreate()
  lazy val sparkContext: SparkContext = sparkSession.sparkContext

  import sparkSession.implicits._

  //  utility function
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0).toDouble / 1000000000 + "seconds")
    result
  }

  val normaliseText: String => String = text =>
    //    pattern is:
    //    - ^\\w : not a word
    //    - ^\\s : not a  space
    //    - ^- : not a -
    text.replaceAll("[^\\w^\\s^-]", "").toLowerCase()

  val removeStopWords: DataFrame => DataFrame = dataFrame =>
    new StopWordsRemover()
      .setInputCol("tokens")
      .setOutputCol("tokensCleaned")
      .transform(dataFrame)
      .select( "tokensCleaned")
      .withColumnRenamed("tokensCleaned", "tokens")

  val tokenize: String => Seq[String] = _.split(" ").filterNot(_.isEmpty).toSeq

  val clean: String => Seq[String] = (normaliseText andThen tokenize)(_)

  /**
   * data downloadable from https://github.com/davidsbatista/text-classification/blob/master/movies_genres.csv.bz2
   * function used also to read back the index
   */
  val readData: String => DataFrame =
    sparkSession.read.option("delimiter", "\t").option("header", "true").csv(_)

  /**
   * Read the Corpus data
   * @param filepath path to the corpus
   * @return The corpus represented as a Dataset[Movie]
   */
  def readCorpus(filepath: String = "data/movies_genres.csv"): Dataset[Movie] =
    readData(filepath).select("title", "plot").orderBy($"title".asc).as[Movie]

  def rowToTransposedTriplet(row: Vector, rowIndex: Long): Array[(Long, (Long, Double))] = {
    val indexedRow = row.toArray.zipWithIndex
    indexedRow.map{ case (value, colIndex) => (colIndex.toLong, (rowIndex, value)) }
  }

  def buildRow(rowWithIndexes: Iterable[(Long, Double)]): Vector = {
    val resArr = new Array[Double](rowWithIndexes.size)
    rowWithIndexes.foreach{ case (index, value) =>
      resArr(index.toInt) = value
    }
    new DenseVector(resArr)
  }

  def transposeRowMatrix(m: RowMatrix): RowMatrix = {
    val transposedRowsRDD = m.rows.zipWithIndex.map{case (row, rowIndex) => rowToTransposedTriplet(row, rowIndex)}
      .flatMap(x => x) // now we have triplets (newRowIndex, (newColIndex, value))
      .groupByKey
      .sortByKey().map(_._2) // sort rows and remove row indexes
      .map(buildRow) // restore order of elements in each row and remove column indexes
    new RowMatrix(transposedRowsRDD)
  }
}

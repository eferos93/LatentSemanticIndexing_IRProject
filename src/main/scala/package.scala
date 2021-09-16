package org.ir


import org.apache.spark.ml.feature.{Normalizer, StopWordsRemover}
import org.apache.spark.ml.linalg.{DenseMatrix, Matrices}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.apache.spark.{SparkConf, SparkContext}
import org.ir.project.data_structures.Movie


package object project {

  lazy val sparkConfiguration: SparkConf = new SparkConf()
    .setAppName("Latent Semantic Indexing")
    .setMaster("local[*]")
    .set("spark.cores.max", Runtime.getRuntime.availableProcessors.toString)

  lazy val sparkSession: SparkSession = SparkSession.builder.config(sparkConfiguration).getOrCreate()
  lazy val sparkContext: SparkContext = sparkSession.sparkContext
  sparkContext.setLogLevel("WARN")

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

  def removeStopWords(dataFrame: DataFrame, extraColumns: Seq[ColumnName] = Seq($"documentId")): DataFrame =
    new StopWordsRemover()
      .setInputCol("tokens")
      .setOutputCol("tokensCleaned")
      .transform(dataFrame)
      .select(extraColumns :+ $"tokensCleaned":_*) // :+ ::= append element to Seq; :_* ::= convert Seq[ColumnName] to ColumnName*
      .withColumnRenamed("tokensCleaned", "tokens")

  val tokenize: String => Seq[String] = _.split(" ").filterNot(_.isEmpty).toSeq

  val clean: String => Seq[String] = (normaliseText andThen tokenize)(_)

  /**
   * data downloadable from https://github.com/davidsbatista/text-classification/blob/master/movies_genres.csv.bz2
   * function used also to read back the index
   */
  def readData(filepath: String, delimiter: String = "\t"): DataFrame =
    sparkSession.read
      .option("delimiter", delimiter)
      .option("inferSchema", value = true)
      .option("header", "true").csv(filepath)

  /**
   * Read the Corpus data
   * @param filepath path to the corpus
   * @return The corpus represented as a Dataset[Movie]
   */
  def readCorpus(filepath: String = "data/movies_genres.csv"): Dataset[Movie] =
    readData(filepath)
      .select("title", "plot")
      .withColumn("id", row_number.over(Window.orderBy($"title".asc))) // Window functions https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html
      .select("id", "title", "plot")
      .as[Movie]

  def normaliseMatrix(matrix: DenseMatrix): DenseMatrix = {
    val matrixAsDataFrame = matrix.rowIter.toSeq.map(_.toArray).toDF("unnormalised")
    val normalisedMatrix = new Normalizer()
      .setInputCol("unnormalised")
      .setOutputCol("normalised")
      .transform(matrixAsDataFrame)
      .select("normalised")
    Matrices.dense(matrix.numRows, matrix.numCols, normalisedMatrix.as[Array[Double]].collect.flatten).toDense
  }
}

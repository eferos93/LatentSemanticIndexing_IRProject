package org.ir


import org.apache.spark.ml.feature.{Normalizer, StopWordsRemover}
import org.apache.spark.ml.linalg.{DenseMatrix, Matrices}
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.ir.project.data_structures.{Book, Document}


package object project {

  lazy val sparkConfiguration: SparkConf = new SparkConf()
    .setAppName("Latent Semantic Indexing")
    .setMaster("local[*]")
    .set("spark.cores.max", Runtime.getRuntime.availableProcessors.toString)
//    .set("spark.driver.memory", "3g")

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
      .select(extraColumns :+ $"tokensCleaned": _*) // :+ ::= append element to Seq; :_* ::= convert Seq[ColumnName] to ColumnName*
      .withColumnRenamed("tokensCleaned", "tokens")

  val tokenize: String => Seq[String] = _.split(" ").filterNot(_.isEmpty).toSeq

  val clean: String => Seq[String] = (normaliseText andThen tokenize) (_)

  /**
   * data downloadable from https://github.com/davidsbatista/text-classification/blob/master/movies_genres.csv.bz2
   * function used also to read back the index
   */
  def readData(filepath: String,
               delimiter: String = "\t",
               columnsToSelect: Option[Seq[ColumnName]] = None,
               isHeader: Boolean = false): DataFrame = {
    val data =
      sparkSession.read
        .option("delimiter", delimiter)
        .option("inferSchema", value = true)
        .option("header", isHeader).csv(filepath)

    columnsToSelect match {
      case Some(columns) => data.select(columns:_*)
      case None => data
    }
  }

//  /**
//   * Read the Corpus data
//   *
//   * @param filepathTitles path to the corpus
//   * @return The corpus represented as a Dataset[Movie]
//   */
//  def readCorpus(filepathTitles: String = "data/movie.metadata.tsv",
//                 filepathDescriptions: String = "data/plot_summaries.txt"): Dataset[Book] = {
//    val titles =
//      readData(filepathTitles, columnsToSelect = Option(Seq($"_c0", $"_c2")))
//      .toDF("internalId", "title")
//    val descriptions = readData(filepathDescriptions).toDF("internalId", "description")
//
//    titles
//      .join(descriptions, titles("internalId") === descriptions("internalId"))
//      .select("title", "description")
//      .orderBy("title").rdd
//      .zipWithIndex.map { case (row, documentId) => (row.getString(0), row.getString(1), documentId) }
//      .toDF("title", "description", "id")
////      .withColumn("id", row_number.over(Window.orderBy($"title".asc))) // Window functions https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html
//      .select("id", "title", "description")
//      .as[Book]
//  }

  def readBooksCorpus(path: String = "data/booksummaries.txt"): Dataset[Book] = {
    readData(path, columnsToSelect = Option(Seq($"_c2", $"_c3", $"_c6")))
      .toDF("title", "author", "description")
      .orderBy($"title")
      .rdd.zipWithIndex
      .map { case (row, documentId) => (documentId, row.getString(0), row.getString(1), row.getString(2)) }
      .toDF("id", "title", "author", "description")
      .as[Book]
  }

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

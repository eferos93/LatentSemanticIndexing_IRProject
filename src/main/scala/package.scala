package org.ir


import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.ir.project.data_structures.ArxivArticle


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
    //    filter(_ >= " ") selects every that is not a control character
    text.filter(_ >= ' ').replaceAll("[^\\w^\\s-]", "").toLowerCase()

  val removeStopWords: DataFrame => DataFrame = dataFrame =>
    new StopWordsRemover()
      .setInputCol("tokens")
      .setOutputCol("tokensCleaned")
      .transform(dataFrame)
      .select("title", "tokensCleaned")
      .withColumnRenamed("tokensCleaned", "tokens")

  val tokenize: String => Seq[String] = _.split(" ").toSeq

  val clean: String => Seq[String] = (normaliseText andThen tokenize)(_)

  /**
   * read the raw data, downloadable from https://www.kaggle.com/Cornell-University/arxiv
   */
  val readData: String => DataFrame = sparkSession.read.json(_)

  val saveCorpus: DataFrame => Unit =
    _.withColumnRenamed("abstract", "articleAbstract")
      .select("title", "articleAbstract")
      .as[ArxivArticle].orderBy('title.asc)
      .repartition(1)
      .write.mode(SaveMode.Overwrite).json("data/corpus")

  /**
   * Read raw data downloaded from https://www.kaggle.com/Cornell-University/arxiv
   * turn it into a corpus and save it
   * @param filepath Path to the raw data
   */
  def readDataAndSaveAsCorpus(filepath: String = "data/arxiv-metadata-oai-snapshot.json"): Unit =
    (readData andThen saveCorpus)(filepath)

  /**
   * Read the Corpus data
   * @param filepath path to the corpus
   * @return The corpus represented as a Dataset[ArxivArticle]
   */
  def readCorpus(filepath: String = "data/corpus/corpus.json"): Dataset[ArxivArticle] =
    readData(filepath).as[ArxivArticle]

  def cleanAndSaveData(corpus: Dataset[ArxivArticle]): DataFrame = {
    lazy val partiallyCleanedDataFrame: DataFrame =
      corpus
        .map(article => (article.title.filter(_ >= ' '), clean(article.articleAbstract).filterNot(_.isEmpty)))
        .toDF("title", "tokens")
    lazy val tokenizedCorpus: DataFrame = removeStopWords(partiallyCleanedDataFrame)
    tokenizedCorpus
      .repartition(1) // to write it in a single json file
      .write.mode(SaveMode.Overwrite).json("data/cleaned")
    tokenizedCorpus
  }
}

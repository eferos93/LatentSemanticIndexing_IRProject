package org.ir


import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.ir.project.model.ArxivArticle

package object project {

  lazy val sparkConfiguration: SparkConf = new SparkConf()
    .setAppName("Latent Semantic Indexing")
    .setMaster("local[*]")
    .set("spark.cores.max", Runtime.getRuntime.availableProcessors().toString)

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
    text.replaceAll("[^\\w^\\s^-]", "").toLowerCase().filter(_ >= ' ')

  val stopWordsRemover: DataFrame => DataFrame = abstracts => {
    new StopWordsRemover()
      .setInputCol("abstract")
      .setOutputCol("abstractCleaned")
      .transform(abstracts)
      .select("id", "title", "abstractCleaned")
      .withColumnRenamed("abstractCleaned", "abstract")
  }

  val tokenize: String => Array[String] = _.split(" ").filterNot(_.isEmpty)

  def clean(text: String): Array[String] =
    (normaliseText andThen tokenize)(text)

  def readCorpus(filepath: String = "data/arxiv-metadata-oai-snapshot.json"): DataFrame =
//    download and extract data from https://www.kaggle.com/Cornell-University/arxiv
    sparkSession.read.json(filepath)

  def cleanAndSaveData(originalCorpus: DataFrame): Unit = {
    lazy val partiallyCleanedDataFrame: DataFrame =
      originalCorpus
      .select("id", "title", "abstract")
      .sort($"id".asc)
      .map(row => (row.getString(0), row.getString(1).filter(_ >= ' '), clean(row.getString(2))))
      .toDF("id", "title", "abstract")
    stopWordsRemover(partiallyCleanedDataFrame)
      .repartition(1) // to write it in a single json file
      .write.json("data/cleaned")
  }

  def asArray(dataFrame: DataFrame): Vector[Row] = {
    dataFrame.collect.toVector
  }

  def readCleanedDataAndConvertToRDD(filepath: String = "data/cleaned/cleaned.json"): Vector[Row] =
    (readCorpus _ andThen asArray)(filepath)
}

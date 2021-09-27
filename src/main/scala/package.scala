package org.ir


import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.ir.project.data_structures.{Book, Document, Movie}
import com.johnsnowlabs.nlp.annotator.{Stemmer, Tokenizer}
import com.johnsnowlabs.nlp.annotators.StopWordsCleaner
import com.johnsnowlabs.nlp.base.DocumentAssembler
import org.apache.spark.sql.functions.udf



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
    text.replaceAll("[^\\w^\\s^-]", "").toLowerCase


  def removeStopWords(dataFrame: DataFrame, extraColumns: Seq[ColumnName] = Seq($"documentId")): DataFrame =
    new StopWordsRemover()
      .setInputCol("tokens")
      .setOutputCol("tokensCleaned")
      .transform(dataFrame)
      .select(extraColumns :+ $"tokensCleaned": _*) // :+ -> append element to Seq; :_* -> convert Seq[ColumnName] to ColumnName*
      .withColumnRenamed("tokensCleaned", "tokens")

  val tokenize: String => Seq[String] = _.split(" ").filterNot(_.isEmpty).toSeq

  val clean: String => Seq[String] = (normaliseText andThen tokenize) (_)

  def pipelineClean(corpus: Dataset[_], extraColumns: Seq[String] = Seq("id")): DataFrame = {
//    user defined column function
    val udfNormaliseText = udf(normaliseText)
    val filteredCorpus = corpus.withColumn("normalisedDescription", udfNormaliseText($"description"))

    val documentAssembler = new DocumentAssembler()
      .setInputCol("normalisedDescription")
      .setOutputCol("text")
    val tokenizer = new Tokenizer()
      .setInputCols("text")
      .setOutputCol("tokens")
    val stemmer = new Stemmer()
      .setInputCols("tokens")
      .setOutputCol("stem")
    val stopWordsCleaner = StopWordsCleaner.pretrained // english stop words
      .setInputCols("stem")
      .setOutputCol("stemNoStopWords")

    val pipeline = new Pipeline().setStages(Array(
      documentAssembler,
      tokenizer,
      stemmer,
      stopWordsCleaner
    ))

    pipeline.fit(filteredCorpus).transform(filteredCorpus)
      .selectExpr(extraColumns :+ "stemNoStopWords.result":_*)
      .withColumnRenamed("result", "tokens")
//      .select(extraColumns :+ $"tokens":_*)
  }

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

  /**
   * Read the Corpus data
   *
   * @param filepathTitles path to the corpus
   * @return The corpus represented as a Dataset[Movie]
   */
  def readMovieCorpus(filepathTitles: String = "data/movie.metadata.tsv",
                      filepathDescriptions: String = "data/plot_summaries.txt"): Dataset[Movie] = {
    val titles =
      readData(filepathTitles, columnsToSelect = Option(Seq($"_c0", $"_c2")))
      .toDF("internalId", "title")
    val descriptions = readData(filepathDescriptions).toDF("internalId", "description")

    titles
      .join(descriptions, titles("internalId") === descriptions("internalId"))
      .select("title", "description")
      .orderBy("title").rdd
      .zipWithIndex.map { case (row, documentId) => (documentId, row.getString(0), row.getString(1)) }
      .toDF("id", "title", "description")
      .as[Movie].persist(StorageLevel.MEMORY_ONLY_SER)
  }

  def readBooksCorpus(path: String = "data/booksummaries.txt"): Dataset[Book] = {
    readData(path, columnsToSelect = Option(Seq($"_c2", $"_c3", $"_c6")))
      .toDF("title", "author", "description")
      .orderBy($"title")
      .rdd.zipWithIndex
      .map { case (row, documentId) => (documentId, row.getString(0), row.getString(1), row.getString(2)) }
      .toDF("id", "title", "author", "description")
      .as[Book].persist(StorageLevel.MEMORY_ONLY_SER)
  }
}

package org.ir


import org.apache.spark.ml.Pipeline
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.ir.project.data_structures.{CranfieldDocument, Movie, NplDocument}
import com.johnsnowlabs.nlp.annotator.{Stemmer, Tokenizer}
import com.johnsnowlabs.nlp.annotators.StopWordsCleaner
import com.johnsnowlabs.nlp.base.DocumentAssembler
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{dayofweek, split, udf}
import org.apache.spark.sql.types.IntegerType

import scala.util.matching.Regex
import scala.io.Source



package object project {

  lazy val sparkConfiguration: SparkConf = new SparkConf()
    .setAppName("Latent Semantic Indexing")
    .setMaster("local[*]")
    .set("spark.cores.max", Runtime.getRuntime.availableProcessors.toString)

  lazy val sparkSession: SparkSession = SparkSession.builder.config(sparkConfiguration).getOrCreate()
  lazy val sparkContext: SparkContext = sparkSession.sparkContext
  sparkContext.setLogLevel("OFF")

  import sparkSession.implicits._

  //  utility function
  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0).toDouble / 1000000000 + "seconds")
    result
  }

 // function adapted to a column function: see its application at line 64
  val normaliseText: UserDefinedFunction = udf { text: String =>
    //    pattern is:
    //    - ^\\w : not a word
    //    - ^\\s : not a  space
    //    - ^- : not a -
    text.replaceAll("[^\\w^\\s^-]", "").toLowerCase
  }

  def pipelineClean(dataSet: Dataset[_],
                    columnToClean: ColumnName = $"description",
                    extraColumns: Seq[String] = Seq("id")
                    ): DataFrame = {
    val filteredCorpus = dataSet.withColumn("normalisedDescription", normaliseText(columnToClean))

    val documentAssembler = new DocumentAssembler()
      .setInputCol("normalisedDescription")
      .setOutputCol("text")
    val tokenizer = new Tokenizer()
      .setInputCols("text")
      .setOutputCol("tokens")
      .setMinLength(1)
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
  }

  /**
   * data downloadable from https://github.com/davidsbatista/text-classification/blob/master/movies_genres.csv.bz2
   * function used also to read back the index
   */
  def readData(filepath: String,
               delimiter: String = "\t",
               columnsToSelect: Option[Seq[ColumnName]] = None,
               headerPresent: Boolean = false): DataFrame = {
    val data =
      sparkSession.read
        .option("delimiter", delimiter)
        .option("inferSchema", value = true)
        .option("header", headerPresent).csv(filepath)

    columnsToSelect match {
      case Some(columns) => data.select(columns:_*)
      case None => data
    }
  }

  /**
   * Read the Corpus data.
   * Download it from http://www.cs.cmu.edu/~ark/personas/ extract the data inside the data/ directory
   *
   * @param filepathTitles path to the corpus
   * @return The corpus represented as a Dataset[Movie]
   */
  def readMovieCorpus(filepathTitles: String = "data/MovieSummaries/movie.metadata.tsv",
                      filepathDescriptions: String = "data/MovieSummaries/plot_summaries.txt"): Dataset[Movie] = {
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

  /**
   * Data can be downloaded with the link below, but use the data in this repo, as there were some problems
   *  with the file rlv-ass http://ir.dcs.gla.ac.uk/resources/test_collections/npl/
   */
  def readNplCorpus(path: String = "data/npl/doc-text"): Dataset[NplDocument] = {
    val df: DataFrame = sparkSession.read.option("lineSep", "   /\n").text(path)
    val columnsSplit = split(df("value"), "\n")
    df.withColumn("id", columnsSplit.getItem(0).cast(IntegerType) - 1) //I want indexes to start from 0 (for matrix)
      .withColumn("description", columnsSplit.getItem(1))
      .select("id", "description")
      .as[NplDocument].persist(StorageLevel.MEMORY_ONLY_SER)
  }

  def readQueryAndRelevance(pathToQueries: String = "data/npl/query-text",
                            pathToRelevance: String = "data/npl/rlv-ass"): Array[(String, Array[Long])] = {
    var queryDf = sparkSession.read.option("lineSep", "/\n").text(pathToQueries)
    var relevanceDf = sparkSession.read.option("lineSep", "/\n").text(pathToRelevance)
    val queryColumnsSplit = split(queryDf("value"), "\n", 2)
    val relevanceColumnSplit = split(relevanceDf("value"), "\n", 2)
    queryDf =
      queryDf
        .withColumn("id", queryColumnsSplit.getItem(0).cast(IntegerType))
        .withColumn("query", queryColumnsSplit.getItem(1))
        .select("id", "query")
    relevanceDf =
      relevanceDf
        .withColumn("queryId", relevanceColumnSplit.getItem(0).cast(IntegerType))
        .withColumn("relevanceSet", relevanceColumnSplit.getItem(1))
        .select("queryId", "relevanceSet")

    val stringToList: UserDefinedFunction = udf { relevanceSet: String =>
      // withFilter is a MonadicFilter, thus it doesn't return a new collection like filter(),
      // but it restricts the domain of the subsequent transformations. In simple words, it is lazy
      relevanceSet.split("[\n|\\s+]").withFilter(_.trim.nonEmpty).map(_.toInt)
    }

    queryDf
      .join(relevanceDf, queryDf("id") === relevanceDf("queryId"))
      .withColumn("relevanceList", stringToList($"relevanceSet"))
      .selectExpr("query", "relevanceList AS relevanceSet")
      .as[(String, Array[Long])]
      .collect
  }

  def readCranfield(path: String = "data/cranfield/cran.all.1400"): Seq[CranfieldDocument] = {
    var corpus: Seq[CranfieldDocument] = Seq.empty
    var isTitle = false
    var isText = false
    var text = ""
    var title = ""
    var id: Long = 0
    val source = Source.fromFile(path)
    source
      .getLines()
      .foreach { line =>
        if (line.startsWith(".A"))
          isTitle = false
        else if (line.startsWith(".I")) {
          isText = false
          if (text.nonEmpty) {
            corpus = new CranfieldDocument(
              id,
              title.replace("\n", " "),
              text.replace("\n", " ")
            ) +: corpus
            title = ""
            text = ""
          }
          id = "[0-9]+".r.findFirstIn(line).get.toLong
        }
        if (isTitle)
          title += line
        if (isText)
          text += line
        else if (line.startsWith(".W"))
          isText = true
      }
    source.close()
    corpus
  }
}

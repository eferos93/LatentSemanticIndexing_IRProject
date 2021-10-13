package org.ir


import com.johnsnowlabs.nlp.annotator.{Stemmer, Tokenizer}
import com.johnsnowlabs.nlp.annotators.StopWordsCleaner
import com.johnsnowlabs.nlp.base.DocumentAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{collect_set, udf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.ir.project.data_structures.{CranfieldDocument, Movie}

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

 // function adapted to a column function: see its application at line 5c4
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
      .toDF("id", "title")
    val descriptions = readData(filepathDescriptions).toDF("movieId", "description")

    titles
      .join(descriptions, titles("id") === descriptions("movieId"))
      .select("id", "title", "description")
      .orderBy("id")
      .as[Movie].persist(StorageLevel.MEMORY_ONLY_SER)
  }

  /**
   * data can be found here http://ir.dcs.gla.ac.uk/resources/test_collections/cran/
   * @param path to the file containing the corpus
   * @return dataset representation of the corpus
   */

  def readCranfieldCorpus(path: String = "data/cranfield/cran.all.1400"): Dataset[CranfieldDocument] = {
    var corpus: Seq[CranfieldDocument] = Seq.empty
    var isTitle = false
    var isText = false
    var text = ""
    var title = ""
    var id: Long = 0
    val source = Source.fromFile(path)
    source.getLines().foreach { line =>
      if (line.startsWith(".A"))
        isTitle = false
      else if (line.startsWith(".I")) {
        isText = false
        if (text.nonEmpty) {
          corpus = new CranfieldDocument(
            id,
            title.replaceAll("\n", " "),
            text.replaceAll("\n", " ")
          ) +: corpus //prepend to corpus
          title = ""
          text = ""
        }
        id = "[0-9]+".r.findFirstIn(line).get.toLong
      }
      if (isTitle)
        title += line
      if (isText)
        text += line
      if (line.startsWith(".T"))
        isTitle = true
      else if (line.startsWith(".W"))
        isText = true
    }
    source.close()
    corpus.toDS.orderBy($"id").persist(StorageLevel.MEMORY_ONLY_SER)
  }

  def readQueryRelevanceCranfield(pathToRelevance: String = "data/cranfield/cranqrel",
                                  pathToQueries: String = "data/cranfield/cran.qry"): Array[(String, Array[Long])] = {
    val queryRelevance = readData(pathToRelevance, delimiter = " ", columnsToSelect = Option(Seq($"_c0", $"_c1")))
      .withColumnRenamed("_c0", "queryId")
      .withColumnRenamed("_c1", "relevantDocument")
      .groupBy("queryId").agg(collect_set("relevantDocument").as("relevantDocuments"))

    var queriesAndRelevanceSets: Array[(String, Array[Long])] = Array.empty
    var isText = false
    var text = ""
    var queryId = 1
    val source = Source.fromFile(pathToQueries)
    source.getLines().foreach { line =>
       if (line.startsWith(".I")) {
         isText = false
         if (text.nonEmpty) {
           queriesAndRelevanceSets =
             (
               text.replaceAll("\n", " "),
               //get the relevance set associated to the query
               queryRelevance.where($"queryId" === queryId).as[(String, Array[Long])].first._2
             ) +: queriesAndRelevanceSets //prepend to queriesAndRelevanceSets
           queryId += 1
           text = ""
         }
       }
      if (isText)
        text += line
      else if (line.startsWith(".W"))
        isText = true
    }
    queriesAndRelevanceSets
  }
}

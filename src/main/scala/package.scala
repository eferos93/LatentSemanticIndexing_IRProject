package org.ir


import com.johnsnowlabs.nlp.annotator.{Stemmer, Tokenizer}
import com.johnsnowlabs.nlp.annotators.StopWordsCleaner
import com.johnsnowlabs.nlp.base.DocumentAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{split, udf}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.ir.project.data_structures.{Movie, NplDocument}


package object project {

  lazy val sparkConfiguration: SparkConf = new SparkConf()
    .setAppName("Latent Semantic Indexing")
    .setMaster("local[*]")
    .set("spark.cores.max", Runtime.getRuntime.availableProcessors.toString)
    .set("spark.executor.memory", "10g")
    .set("spark.driver.memory", "10g")

  lazy val sparkSession: SparkSession = SparkSession.builder.config(sparkConfiguration).getOrCreate()
  lazy val sparkContext: SparkContext = sparkSession.sparkContext
  sparkContext.setLogLevel("OFF")

  import sparkSession.implicits._


  def pipelineClean(dataSet: Dataset[_],
                    columnToClean: ColumnName = $"description",
                    extraColumns: Seq[String] = Seq("id")
                   ): DataFrame = {
    // function adapted to a column function
    val normaliseText: UserDefinedFunction = udf { text: String =>
      //    pattern is:
      //    - ^\\w : not a word
      //    - ^\\s : not a  space
      //    - ^- : not a -
      text.replaceAll("[^\\w^\\s^-]", "").toLowerCase
    }

    val filteredCorpus = dataSet.withColumn("normalisedDescription", normaliseText(columnToClean))

    // we define some transformers
    // as the tokenizer, stmmer and stop words remover are from the spark.nlp library
    // we need to convert the document with the below DocumentAssembler
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

    //define the pipeline of execution
    val pipeline = new Pipeline().setStages(Array(
      documentAssembler,
      tokenizer,
      stemmer,
      stopWordsCleaner
    ))

    //transform the data
    pipeline.fit(filteredCorpus).transform(filteredCorpus)
//      :+ appends the element to the Seq, :_* will convert the Seq[String] to String*
      .selectExpr(extraColumns :+ "stemNoStopWords.result": _*)
      .withColumnRenamed("result", "tokens")
  }

  
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
      case Some(columns) => data.select(columns: _*)
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
        .toDF("internalId", "title") // to Dataframe
    val descriptions = readData(filepathDescriptions).toDF("movieId", "description")

    titles
      .join(descriptions, titles("internalId") === descriptions("movieId"))
      .select("title", "description")
      .orderBy("title").rdd // covert it to RDD[Row]
      .zipWithIndex.map { case (Row(title: String, description: String), id) => (id, title, description) }
      .toDF("id", "title", "description") // back to Dataframe
      .as[Movie].persist(StorageLevel.MEMORY_ONLY_SER) //convert it to Dataset[Movie]
  }

  def readNplCorpus(path: String = "data/npl/doc-text"): Dataset[NplDocument] = {
    val df: DataFrame = sparkSession.read.option("lineSep", "   /\n").text(path) // Dataframe with one single column "value"
    val columnsSplit = split(df("value"), "\n") //split the column
    df.withColumn("id", columnsSplit.getItem(0).cast(IntegerType) - 1) //I want indexes to start from 0 (for matrix)
      .withColumn("description", columnsSplit.getItem(1))
      .select("id", "description")
      .as[NplDocument].persist(StorageLevel.MEMORY_ONLY_SER) // covert it to Dataset[NplDocument]
  }

  def readQueryAndRelevance(pathToQueries: String = "data/npl/query-text",
                            pathToRelevance: String = "data/npl/rlv-ass"): Array[(String, Array[Long])] = {
    //both Dataframes have one single column
    var queryDf = sparkSession.read.option("lineSep", "/\n").text(pathToQueries)
    var relevanceDf = sparkSession.read.option("lineSep", "/\n").text(pathToRelevance)
    // split their columns with pattern limited 2 times
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

    // column function to convert the relevance set from String to Array[Int]
    val stringToList: UserDefinedFunction = udf { relevanceSet: String =>
      // withFilter is a MonadicFilter, thus it doesn't return a new collection like filter(),
      // but it restricts the domain of the subsequent transformations. In simple words, it is lazy
      relevanceSet.split("[\n|\\s+]").withFilter(_.nonEmpty).map(_.toInt)
    }

    // join the two dataframes and return a Array of tuples (query, relevanceSet)
    queryDf
      .join(relevanceDf, queryDf("id") === relevanceDf("queryId"))
      .withColumn("relevanceArray", stringToList($"relevanceSet"))
      .select("query", "relevanceArray")
      .as[(String, Array[Long])].collect
  }
}

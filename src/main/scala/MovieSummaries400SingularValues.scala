package org.ir.project

import data_structures.Movie

import org.apache.spark.sql.Dataset

import java.nio.file.{Files, Paths}

object MovieSummaries400SingularValues extends App {
  def buildIrSystem(numberOfSingularValues: Int, corpus: Dataset[Movie]): IRSystem[Movie] = {
    if (Files.exists(Paths.get("index"))) {
      println("index found")
      IRSystem(corpus, numberOfSingularValues, "index", tfidf = true)
    } else {
      println("index not found")
      IRSystem(corpus, numberOfSingularValues, tfidf = true)
    }
  }

  def queryIrSystem(irSystem: IRSystem[Movie], numberOfSingularValues: Int): Unit = {
    println(s"QUERYING IR SYSTEM WITH $numberOfSingularValues SINGULAR VALUES")
    println("Query: \"murder detective action\"")
    irSystem.query("murder detective action")
    println("\nQuery: \"detective kill police\"")
    irSystem.query("detective kill police")
    println("\nQuery: \"party student university school\"")
    irSystem.query("party student university school")
    println("\nQuery: \"ancient rome roman legions\"")
    irSystem.query("ancient rome roman legions")
  }

  val corpus: Dataset[Movie] = readMovieCorpus()
  println("Spark Web UI:")
  sparkContext.uiWebUrl.foreach(println(_))
  queryIrSystem(buildIrSystem(100, corpus), 100)
  queryIrSystem(buildIrSystem(400, corpus), 400)
}

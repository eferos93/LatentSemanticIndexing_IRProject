package org.ir.project

import data_structures.Movie

import org.apache.spark.sql.Dataset

import java.nio.file.{Files, Paths}

object MovieSummaries400SingularValues extends App {
  val corpus: Dataset[Movie] = readMovieCorpus()
  println(sparkContext.uiWebUrl)
  val irSystem: IRSystem[Movie] =
    if (Files.exists(Paths.get("index"))) {
      println("index found")
      IRSystem(corpus, 400, "index", tfidf = true)
    } else {
      println("index not found")
      IRSystem(corpus, 400, tfidf = true)
    }

  irSystem.query("murder detective action")
  irSystem.query("detective kill police")
  irSystem.query("party student university school")
  irSystem.query("ancient rome roman legions")

}

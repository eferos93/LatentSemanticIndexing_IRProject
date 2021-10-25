package org.ir.project

import data_structures.Movie

import org.apache.spark.sql.Dataset

import java.nio.file.{Files, Paths}

object MovieSummaries400SingularValues extends App {
  val corpus: Dataset[Movie] = readMovieCorpus()
  println("Spark Web UI:")
  println(sparkContext.uiWebUrl)
  val irSystem: IRSystem[Movie] =
    if (Files.exists(Paths.get("index"))) {
      println("index found")

      if (Files.exists(Paths.get("matrices"))) {
        println("matrices found")
        IRSystem(corpus, "index", "matrices")
      } else {
        IRSystem(corpus, 400, "index", tfidf = true)
      }
    } else {
      println("index not found")
      IRSystem(corpus, 400, tfidf = true)
    }

  if (Files.notExists(Paths.get("matrices"))) {
    irSystem.saveIrSystem()
  }

  println("Query: \"murder detective action\"")
  irSystem.query("murder detective action")
  println("\nQuery: \"detective kill police\"")
  irSystem.query("detective kill police")
  println("\nQuery: \"party student university school\"")
  irSystem.query("party student university school")
  println("\nQuery: \"ancient rome roman legions\"")
  irSystem.query("ancient rome roman legions")

}

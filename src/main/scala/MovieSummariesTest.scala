package org.ir.project

object MovieSummariesTest extends App {
  val corpus = readMovieCorpus()
  val irSystem = IRSystem(corpus, 500, tfidf = true)

}

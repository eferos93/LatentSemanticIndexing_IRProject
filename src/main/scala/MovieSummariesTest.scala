package org.ir.project

object MovieSummariesTest extends App {
  val corpus = readMovieCorpus()
  var irSystem = IRSystem(corpus, 100, tfidf = true)
  irSystem.query("murder detective action")
  irSystem.query("detective kill police")
  irSystem.query("party student university school")
  irSystem.query("ancient rome roman legions")

  irSystem = IRSystem(corpus, 200, "index", tfidf = true)
  irSystem.query("murder detective action")
  irSystem.query("detective kill police")
  irSystem.query("party student university school")
  irSystem.query("ancient rome roman legions")
}

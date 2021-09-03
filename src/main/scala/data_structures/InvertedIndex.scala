package org.ir.project
package data_structures

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class InvertedIndex {
  type Term = String
  type DocumentId = String
  var dictionary: Map[Term, Map[DocumentId, Int]] = Map.empty
}

object InvertedIndex {
  def apply(corpus: DataFrame): InvertedIndex = {
//    corpus.rdd.map
  }
}

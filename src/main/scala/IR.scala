package org.ir.project

import data_structures.InvertedIndex

import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, SingularValueDecomposition}
import org.apache.spark.sql.{DataFrame, Dataset}

class IR(corpus: DataFrame,
         vocabulary: Dataset[String],
         singularValueDecomposition: SingularValueDecomposition[RowMatrix, Matrix])

object IR {
  def apply(corpus: DataFrame, k: Int) = {

  }
}

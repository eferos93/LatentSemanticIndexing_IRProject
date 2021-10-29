package org.ir.project
package data_structures.weigher

// trait is like interface in java
trait WordWeight {
  def weight(args: Map[String, Long]): Double
}

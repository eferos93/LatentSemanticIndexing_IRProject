package org.ir.project
package data_structures

case class Movie(id: Int, title: String, plot: String) {
  override def toString: String = title
}

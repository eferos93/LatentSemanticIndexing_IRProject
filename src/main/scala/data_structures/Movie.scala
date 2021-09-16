package org.ir.project
package data_structures

case class Movie(id: Long, title: String, plot: String) {
  override def toString: String = title
}

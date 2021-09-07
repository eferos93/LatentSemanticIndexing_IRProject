package org.ir.project
package data_structures

case class Movie(title: String, plot: String) {
  override def toString: String = plot
}

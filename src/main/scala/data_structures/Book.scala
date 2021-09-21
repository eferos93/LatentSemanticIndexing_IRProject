package org.ir.project
package data_structures

class Book(id: Long, title: String, description: String) extends Document(id, title, description) {
  override def toString: String = super.toString
}

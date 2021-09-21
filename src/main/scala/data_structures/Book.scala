package org.ir.project
package data_structures

class Book(id: Long, title: String, author: String, description: String) extends Document(id, title, description) {
  override def toString: String = s"${super.toString}, $author"
}

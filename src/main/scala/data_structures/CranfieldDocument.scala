package org.ir.project
package data_structures

class CranfieldDocument(id: Long, title: String, description: String) extends Document(id, description) {
  override def toString: String = title
}

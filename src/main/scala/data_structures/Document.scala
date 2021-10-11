package org.ir.project
package data_structures

abstract case class Document(id: Long, description: String) {
  override def toString: String = id.toString
}

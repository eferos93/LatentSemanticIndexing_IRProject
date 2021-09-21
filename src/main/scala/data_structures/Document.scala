package org.ir.project
package data_structures

abstract case class Document(id: Long, title: String, description: String) {
  override def toString: String = title
}

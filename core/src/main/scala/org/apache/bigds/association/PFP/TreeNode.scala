package org.apache.bigds.association.PFP

import scala.collection.mutable.ArrayBuffer

/**
 * TreeNode.scala
 * Description: This is the definition of TreeNode of FP-Tree
 * Author: Lin, Chen
 * E-mail: chlin.ecnu@gmail.com
 * Version: 1.0
 */

class TreeNode (val name: String = null, var count: Long = 0, var parent: TreeNode = null, val children: ArrayBuffer[TreeNode] = new ArrayBuffer[TreeNode](),  var nextHomonym: TreeNode = null){
  def findChild(name: String): TreeNode = {
    children.find(_.name == name) match {
      case Some(node) => node
      case None => null
    }
  }
}

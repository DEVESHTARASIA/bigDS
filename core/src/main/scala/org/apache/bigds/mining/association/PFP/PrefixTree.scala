package org.apache.bigds.mining.association.PFP

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}

class ItemNode (
       var item : Int = -1,
       var parent: ItemNode = null,
       var children: HashMap[Int, ItemNode] = null) {
}

class IndexNode(
       var item : Int = -1,
       var count : Int = 0,
       var child: Int = -1,
       var sibling: Int = -1) extends Serializable {

  def printInfo() : String = {
    "[" + item + ", " + count + ", " + child + ", " + sibling + "]"
  }
}

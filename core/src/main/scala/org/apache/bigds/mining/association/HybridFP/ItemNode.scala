package org.apache.bigds.mining.association.HybridFP

import scala.collection.mutable.{ArrayBuffer, HashMap}

class ItemNode (
       var item : Int = -1,
       var parent: ItemNode = null,
       var children: HashMap[Int, ItemNode] = null) {
}

class PrefixTree (
       var root : ItemNode = null,
       var hashCount : HashMap[ItemNode, Int] = null
                   ) {

}
package org.apache.bigds.mining.association.HybridFP

import scala.collection.mutable.{ArrayBuffer, HashMap}
/*
class POCNode (
       var item : Int = -1,
       var parent: POCNode = null,
       var children : HashMap[Int, POCNode] = null) extends Serializable {
}
*/

class POCNode (
       var item : Int = -1,
       var count : Int = 0,
       var parent: POCNode = null,
       var children : HashMap[Int, POCNode] = null) extends Serializable {
}

/*
class POCNode (
       var item : Int = -1,
       var count : Int = 0,
       var child : POCNode = null,
       var sibling : POCNode = null) extends Serializable {
}
*/

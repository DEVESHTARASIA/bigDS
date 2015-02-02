package org.apache.bigds.mining.association.SyncFPGrowth

class POCNode (
       var item : Int = -1,
       var parent: POCNode = null) extends Serializable {
}

/*
class POCNode (
       var item : Int = -1,
       var count : Int = 0,
       var child : POCNode = null,
       var sibling : POCNode = null) extends Serializable {
}
*/

package org.apache.bigds.nbodyframework.model

import org.apache.spark.Logging


/**
 * Node in a distributed tree
 */

abstract class NBodyTreeNode (
                      val children: Array[NBodyTreeNode],
                      var level: Int,
                      var isLeafNode: Boolean,
                      val id: Int,
                      val NodeInfo: Body) extends DistributedTreeNode with Serializable with Logging {

  def memoryAllocated {

  }


}
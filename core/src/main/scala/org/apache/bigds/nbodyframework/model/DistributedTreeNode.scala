package org.apache.bigds.nbodyframework.model

/**
 * Created by qhuang on 1/11/15.
 */
trait DistributedTreeNode {
  /**
   * build the sub-nodes if not leaf
   */
  def build(nodes : Array[Unit]) : Unit

  /**
   * Recursive print function.
   */
  def subtreeToString()

  /**
   * Get the number of nodes in tree below this node, including leaf nodes.
   * E.g., if this is a leaf, returns 0.  If both children are leaves, returns the number of children.
   */
  def numDescendants: Int

  /**
   * Get depth of tree from this node.
   * E.g.: Depth 0 means this is a leaf node.
   */
  def subtreeDepth: Int

}

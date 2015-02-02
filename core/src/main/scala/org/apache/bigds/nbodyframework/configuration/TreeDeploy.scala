package org.apache.bigds.nbodyframework.configuration

import org.apache.bigds.nbodyframework.configuration._
import org.apache.spark.mllib.tree.impurity.{Entropy, Gini}

/**
 * :: Experimental ::
 * Stores all the configuration options for tree construction
 * @param treeType  Type of tree.  Supported:
 *              [[org.apache.bigds.nbodyframework.configuration.TreeType.KDTree]],
 *              [[org.apache.bigds.nbodyframework.configuration.TreeType.BallTree]],
 *              [[org.apache.bigds.nbodyframework.configuration.TreeType.CoverTree]]
 *                 Supported for Regression: [[org.apache.spark.mllib.tree.impurity.Variance]].
 * @param maxDepthBC Maximum depth of the tree to broadcast.
 *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
 * @param maxDepth Maximum depth of the tree in each node.
 *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
 * @param maxMemoryInMB Maximum memory in MB allocated to . Default value is
 *                      1024 MB.
 */


class TreeDeploy (
    val treeType: TreeType,
    val maxDepthBC: Int,
    val maxDepth: Int,
    val maxMemoryInMB: Int = 1024) extends Serializable {

  /**
   * Check validity of parameters.
   * Throws exception if invalid.
   */
  private[nbodyframework] def assertValid(): Unit = {
  }

}

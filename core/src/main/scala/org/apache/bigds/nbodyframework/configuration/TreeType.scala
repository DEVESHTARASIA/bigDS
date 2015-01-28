package org.apache.bigds.nbodyframework.configuration

/**
 * Enum to select the space-partitioning method for the tree
 */
class TreeType extends Enumeration{
  type TreeType = Value
  val KDTree, BallTree, CoverTree = Value

  private[nbodyframework] def fromString(name: String) : TreeType = name match {
    case "KDTree" => KDTree
    case "BallTree" => BallTree
    case "CoverTree" => CoverTree
    case _ => throw new IllegalArgumentException(s"Did not recognize WhichTree name: $name")
  }

}
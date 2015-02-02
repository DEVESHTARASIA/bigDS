package org.apache.bigds.nbodyframework.application

import org.apache.bigds.nbodyframework.model.NBodyTreeNode
import org.apache.bigds.nbodyframework.spacepartitioning.KDTree

/**
 * Created by qhuang on 1/28/15.
 */
class KNNModel(val topNode: KDTree) extends  Serializable {


  def predict() : Unit = {

  }
}

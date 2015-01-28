package org.apache.bigds.nbodyframework.application


import org.apache.bigds.nbodyframework.model.Body
import org.apache.spark.Logging
import org.apache.bigds.nbodyframework.configuration.TreeDeploy
import org.apache.spark.rdd.RDD

/**
 * Created by qhuang on 1/28/15.
 */
class KNN(private val treeDeploy: TreeDeploy) extends Serializable with Logging {

  treeDeploy.assertValid()

  def train(input: RDD[Body]): KNNModel = {

  }

}


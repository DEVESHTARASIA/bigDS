package org.apache.spark.mllib.bigds.sensitivity

import org.apache.spark.mllib.bigds.ann.MLP

/**
 * Created by clin3 on 2015/3/27.
 */
object SensitivityAnalyser {
  def createAnalyser(model: Any): Any = {
    model match {
      case mlp: MLP =>
        new MLPAnalyser(mlp)
      case _ =>
        throw new IllegalArgumentException("Unsupported model: "
          + model.getClass)
    }
  }
}

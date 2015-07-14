package org.apache.spark.mllib.bigds.pmml

import org.apache.spark.mllib.bigds.ann.MLP

/**
 * Created by clin3 on 2015/3/18.
 */
object ModelExporterFactory {

  /**
   * Factory object to help creating the necessary ModelExporter implementation
   * taking as input the machine learning model (for example MLP).
   */
  def createModelExporter(model: Any): ModelExporter = {
    return model match{
      case mlp: MLP =>
        new MLPExporter(mlp)
      case _ =>
        throw new IllegalArgumentException("Model Exporter not supported for model: "
          + model.getClass)
    }
  }
}

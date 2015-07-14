package org.apache.spark.mllib.bigds.pmml

import java.io.{File, FileInputStream}

import org.apache.spark.mllib.bigds.ann.{Layer, MLP}
import org.dmg.pmml.FieldName
import org.jpmml.evaluator.{Evaluator, ModelEvaluatorFactory}
import org.jpmml.manager.PMMLManager
import org.jpmml.model.{JAXBUtil, ImportFilter}
import org.xml.sax.InputSource
import org.jpmml.evaluator.FieldValue;

import java.util.HashMap

/**
 * Created by clin3 on 2015/3/19.
 */
object MLPImporter {
  def importMLP(filePath: String, values: Array[Double]): Unit = {
    val is = new FileInputStream(new File(filePath))
    val source = ImportFilter.apply((new InputSource(is)))
    val pmml = JAXBUtil.unmarshalPMML(source);
    val pmmlManager = new PMMLManager(pmml)
    val evaluator = pmmlManager.getModelManager("MLP", ModelEvaluatorFactory.getInstance()).asInstanceOf[Evaluator]
    val activeFields = evaluator.getActiveFields
    val arguments = new HashMap[FieldName, FieldValue]()
    var i = 0
    println("activeField.size = " + activeFields.size())
    for(j <- 0 until activeFields.size()) {
      val activeField = activeFields.get(j)
      println("activeField = " + activeField)
      val rawValue = values(i)
      val activeValue: FieldValue = evaluator.prepare(activeField, rawValue)
      arguments.put(activeField, activeValue)
      i = i + 1
    }
    println("arguments.size = " + arguments.size())
    val results = evaluator.evaluate(arguments)
    val targetNames = evaluator.getTargetFields
    for(k <- 0 until targetNames.size()) {
      val targetName = targetNames.get(k)
      println("target = " + results.get(targetName))
    }
  }
}

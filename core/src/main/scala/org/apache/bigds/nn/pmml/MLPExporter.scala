package org.apache.spark.mllib.bigds.pmml

import org.apache.spark.mllib.bigds.ann.MLP
import org.dmg.pmml._
import org.xml.sax.Locator

import scala.Array

/**
 * Created by clin3 on 2015/3/15.
 */
class MLPExporter(mlp: MLP) extends ModelExporter {
  populateMLP()

  def populateMLP(): Unit = {
    pmml.getHeader.setDescription("MLP")
    val fields = new Array[FieldName](mlp.numInput)
    val dataDictionary = new DataDictionary()
    val miningSchema = new MiningSchema()
    val neuralInputs = new NeuralInputs()
    val neuralOutputs = new NeuralOutputs()
    neuralInputs.setNumberOfInputs(mlp.numInput)
    neuralOutputs.setNumberOfOutputs(mlp.numOut)
    val nn = new NeuralNetwork(miningSchema, neuralInputs, MiningFunctionType.CLASSIFICATION, ActivationFunctionType.LOGISTIC)
    nn.setModelName("MLP")
    nn.withNeuralOutputs(neuralOutputs)
    nn.setAlgorithmName("BProp")
    //nn.setNormalizationMethod(NnNormalizationMethodType.SOFTMAX)
    nn.setWidth(0D)
    nn.setNumberOfLayers(mlp.numLayer)

    for(i <- 0 until mlp.numInput) {
      fields(i) = FieldName.create("field_" + i)
      dataDictionary.withDataFields(new DataField(fields(i), OpType.CONTINUOUS, DataType.DOUBLE))
      miningSchema.withMiningFields(new MiningField(fields(i)).withUsageType(FieldUsageType.ACTIVE))
      val derivedField = new DerivedField(OpType.CONTINUOUS, DataType.DOUBLE)
      derivedField.setExpression(new FieldRef(fields(i)))
      neuralInputs.withNeuralInputs(new NeuralInput(derivedField, "0, " + i))
    }

    for(i <- 0 until mlp.numLayer) {
      val neuralLayer = new NeuralLayer()
      for(j <- 0 until mlp.topology(i + 1)) {
        val neuron = new Neuron((i + 1) + ", " + j)
        neuron.setBias(mlp.innerLayers(i).bias(j))

        for(k <- 0 until mlp.topology(i)) {
          val con = new Connection()
          con.setFrom(i + ", " + k)
          con.setWeight(mlp.innerLayers(i).weight(j, k))
          neuron.withConnections(con)
        }
        neuralLayer.withNeurons(neuron)
      }
      nn.withNeuralLayers(neuralLayer)
    }

    val target =  FieldName.create("number")
    val targetField = new DataField(target, OpType.CONTINUOUS, DataType.DOUBLE)
    for(i <- 0 until mlp.numOut) {
      targetField.withValues(new Value(i.toString))
      val derivedField = new DerivedField(OpType.CONTINUOUS, DataType.DOUBLE)
      derivedField.setExpression(new NormDiscrete(target, i.toString))
      neuralOutputs.withNeuralOutputs(new NeuralOutput(derivedField, mlp.numLayer + ", " + i))
    }
    dataDictionary.withDataFields(targetField)
    miningSchema.withMiningFields(new MiningField(target).withUsageType(FieldUsageType.TARGET))

    dataDictionary.withNumberOfFields((dataDictionary).getDataFields.size())
    pmml.setDataDictionary(dataDictionary)
    pmml.withModels(nn)
  }
}

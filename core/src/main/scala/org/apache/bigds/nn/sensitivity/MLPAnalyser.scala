package org.apache.spark.mllib.bigds.sensitivity

import org.apache.spark.mllib.bigds.ann.{SigmoidLayer, MLP}

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV, DenseMatrix => BDM, Matrix => BM,
axpy => brzAxpy, argmax => brzArgMax, max => brzMax, sum => brzSum, norm => brzNorm}

import org.apache.spark.mllib.linalg.{DenseMatrix => SDM, SparseMatrix => SSM, Matrix => SM,
SparseVector => SSV, DenseVector => SDV, Vector => SV, Vectors, Matrices, BLAS}
import org.apache.spark.rdd.RDD

/**
 * Created by clin3 on 2015/3/27.
 */
class MLPAnalyser(mlp: MLP) {
  def analyse(rdd: RDD[(SV, SV)]): BDM[Double] = {
    val numIn = mlp.numInput
    val numLabel = mlp.numOut
    val innerLayers = mlp.innerLayers
    val numLayer = mlp.numLayer
    val sensitivity = rdd.mapPartitions(t => Iterator(t)).treeAggregate(BDM.zeros[Double](numLabel, numIn))(
      seqOp = (c, v) => {
        val numCol = v.size
        val inputMatrix = BDM.zeros[Double](numIn, numCol)
        val labelMatrix = BDM.zeros[Double](numLabel, numCol)
        v.zipWithIndex.foreach { case ((data, label), index) =>
          val brzData = data.toBreeze
          val brzLabel = label.toBreeze
          inputMatrix(::, index) := brzData
          labelMatrix(::, index) := brzLabel
        }

        val in = new Array[SM](numLayer)
        val out = new Array[SM](numLayer)
        val der = new Array[SM](numLayer)
        for (i <- 0 until numLayer) {
          val input = if(i == 0) {
            Matrices.fromBreeze(inputMatrix)
          } else {
            out(i - 1)
          }
          val layer = innerLayers(i)
          in(i) = input
          val output = layer.preActivation(input)
          der(i) = innerLayers(i).computeNeuronPrimitive(output)
          layer.computeNeuron(output)
          out(i) = output
        }


        val weight = innerLayers.head.weight
        der.map(_.toBreeze)
        for(i <- 0 until numCol) {
          val sensitivityMatrix = new BDM[Double](weight.numRows, weight.numCols, weight.toArray)
          for(j <- 0 until der(0).numRows) {
            sensitivityMatrix(j, ::) := sensitivityMatrix(j, ::) :* der(0)(j, i)
          }
          val ret = SDM.zeros(innerLayers(1).numOut, innerLayers(0).numIn)
          BLAS.gemm(1.0, innerLayers(1).weight, new SDM(sensitivityMatrix.rows, sensitivityMatrix.cols, sensitivityMatrix.toArray), 1.0, ret)
          val temp = new BDM(ret.numRows, ret.numCols, ret.toArray)
          for(j <- 0 until der(1).numRows) {
            temp(j, ::) := temp(j, ::) :* der(1)(j, i)
          }
          c :+= temp
        }
        c
      },
      combOp = (c1, c2) => {
        c2 :+= c1
      }
    )
    sensitivity
  }
}

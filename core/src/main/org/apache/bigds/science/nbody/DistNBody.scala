/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.intel.nbody

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector
import scala.math._


/**
 *  Distributed N-body simulation with support for multiple parallel runs.
 *
 *  It only includes Direct N-body method at present.
 *  
 */

trait DistNBody extends Serializable with Logging{

  /** Representing the time steps for an n-body simulation*/
  var cycles: Int = 1

  /** Representing the checkpoint() threshold to cut the lineage to prevent from StackOverFlowError*/
  var checkPointThreshold: Int = 150

  /** Set the number of cycles */
  def setCycles(cycles: Int): this.type = {
    if(cycles < 1) {
      throw new IllegalArgumentException("Number of cycles must be larger than 0")
    }
    this.cycles = cycles
    this
  }

  /** Set the threshold of checkpoint() */
  def setCheckPointThreshold(checkPointThreshold: Int): this.type = {
    if(checkPointThreshold < 1) {
      throw new IllegalArgumentException("Number of CheckPoint Threshold must be larger than 0")
    }
    this.checkPointThreshold = checkPointThreshold
    this
  }


  /**
   * Implementation of N-body simulations.
   * @param parInfoI Input data formed as RDD[Vector]
   *                 A vector represent
   */
  def simulation(parInfoI:RDD[Vector]): RDD[Vector] = {

    if(checkRDD(parInfoI)) {
      throw new Error("The format of input data is error!")
    }

    var parInfo = parInfoI
    val sc = parInfo.sparkContext
    var bcParInfo = sc.broadcast(parInfo.collect())
    val pathRDD = "nParticles_location.RDD.txt"

    checkWrite(pathRDD, bcParInfo.value)

    for (k <- 0 until cycles) {

      parInfo = parInfo.map(p => update(p, bcParInfo.value)).cache()

      if (k % checkPointThreshold == (checkPointThreshold - 1) || k == cycles - 1) {
        // Cut the lineage ! To prevent from StackOverFlowError
        parInfo.checkpoint()
        parInfo.count()

//        parInfo.saveAsTextFile(pathRDD)
        if(checkWrite(pathRDD, parInfo.collect())){
          throw new InterruptedException("This N-body simulation is interrupted at iteration " + k + " !")
        }
      }
      bcParInfo = sc.broadcast(parInfo.collect())
    }

    logInfo("iteration number: " + cycles)
    logInfo("This N-body simulation is completed!")
    logInfo("*****************************************")

    parInfo

  }

  /**
   * Update one time step of N-body simulations.
   * @param aParI the particle to be computed
   * @param allPar all particles including aParI collected after the last update
   */
  private [nbody] def update(aParI: Vector, allPar: Array[Vector]) : Vector = {
    newPosition(
      nBodyInteraction(aParI,  allPar))
  }


  /**
   * @param aParI the particle to be computed
   * @param allPar all particles including aParI collected after the last update
   */
  private [nbody] def nBodyInteraction(aParI: Vector, allPar: Array[Vector]): Vector ={

    var aPar = aParI
    val size = allPar.size
    for (i <- 0 until size){
      // eliminating interacting by itself
      // (0) records the No. of particles
      if(abs(aPar(0) - allPar(i)(0)) > 1e-6){
        aPar = bodyBodyInteraction(aPar, allPar(i))
      }
    }
    aPar
  }

  /**
   * This method computes forces (acceleration) between aPar and allPar
   * @param aParI the particle to be computed
   * @param bParI the other particle interacting the first one
   */
  private [nbody] def bodyBodyInteraction(aParI: Vector, bParI: Vector): Vector

  /**
   * This method computes position and velocity of a particle
   * @param aParI the particle to be computed
   */
  private [nbody] def newPosition(aParI: Vector): Vector

  /**
   * TODO
   */
  def checkRDD(parInfoI:RDD[Vector]): Boolean = {false}

  /**
   * TODO
   */
  private [nbody] def checkWrite(pathRDD: String, vectors: Array[Vector]): Boolean = { false }

}

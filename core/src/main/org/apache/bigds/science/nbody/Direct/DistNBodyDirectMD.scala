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
package com.intel.nbody.Direct

import com.intel.nbody.DistNBody
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD



/**
 * This is an application for Van der Waals interactions,
 * a Non-bonded interaction in  Molecular Dynamics.
 */

class DistNBodyDirectMD (
//  L: linear size of cubical volume
//  dt: one time step
//  Verlet cutoff on the potential and neighbor list approximation
//  rCutoff = Cutoff * Cutoff
    private var L: Double,
    private var dt: Double,
    private var rCutoff: Double) extends DistNBody {


  def this() = this(0, 0.01, 2.5*2.5)

  def setL(L: Double): this.type = {
    if(L <= 0) {
      throw new IllegalArgumentException("Linear size of cubical volume must be positive")
    }
    this.L = L
    this
  }

  def setDt(dt: Double): this.type = {
    if(dt <= 0) {
      throw new IllegalArgumentException("one time step must be positive")
    }
    this.dt = dt
    this
  }

  def setRCutoff(rCutoff: Double): this.type = {
    if(rCutoff <= 0) {
      throw new IllegalArgumentException("rCutoff must be positive")
    }
    this.rCutoff = rCutoff * rCutoff
    this
  }

  override private [nbody] def update(aParI: Vector, allPar: Array[Vector]) = {
    // Velocity Verlet Integration Algorithm is used to solve second order ordinary differential equations
    newPosition2(
      nBodyInteraction(
        newPosition(
          nBodyInteraction(aParI,  allPar)), allPar))
  }



  private [nbody] def bodyBodyInteraction(aParI: Vector, bParI: Vector): Vector ={
  // Computing weak van der Waals forces, Cutoff = 2.5

    val a = aParI.toArray
    val b = bParI.toArray

    var rx = a(1) - b(1)
    // using periodic boundary conditions
    if(rx > 0.5 * L) {rx = rx - L}
    if(rx < -0.5 * L) {rx = rx + L}

    var ry = a(2) - b(2)
    if(ry > 0.5 * L) {ry = ry - L}
    if(ry < -0.5 * L) {ry = ry + L}

    var rz = a(3) - b(3)
    if(rz > 0.5 * L) {rz = rz - L}
    if(rz < -0.5 * L) {rz = rz + L}

    val r2 = rx*rx + ry*ry + rz*rz

    if( r2 < rCutoff){
      /*
       * Lennard-Jones function
       * f = 24 * ( 2 * math.pow(r2, -7) - math.pow(r2,-4))
       */
      val r2inv = 1/r2
      val r6inv = r2inv * r2inv * r2inv
      val f = 24 * r2inv * r6inv * ( 2 * r6inv - 1 )

      a(4) += rx * f
      a(5) += ry * f
      a(6) += rz * f
    }

    Vectors.dense(a)

  }


  private [nbody] def newPosition2(aParI: Vector): Vector ={
    // Verlet Integration Algorithm

    val a = aParI.toArray
    for( i <- 1 to 3){
      // update positions
      a(i) += a(i+6) * dt + 0.5 * a(i+3) * dt * dt
      // using periodic boundary conditions
      if(a(i) < 0) {a(i) += L;}
      if(a(i) > L) {a(i) -= L;}
      // update velocities
      a(i+6) += a(i+3) * dt * 0.5
      // set all accelerations to zero
      a(i+3) = 0.0
    }
    Vectors.dense(a)
  }

  override private [nbody] def newPosition(aParI: Vector): Vector ={
  // Velocity Verlet Integration Algorithm

    val a = aParI.toArray
    a(7) += a(4) * dt * 0.5
    a(8) += a(5) * dt * 0.5
    a(9) += a(6) * dt * 0.5

    // set all accelerations to zero
    a(4) = 0.0
    a(5) = 0.0
    a(6) = 0.0

    Vectors.dense(a)
  }


}




object DistNBodyDirectMD {

  def run(
           g: RDD[Vector],
           cycles: Int,
           L: Double,
           checkPointThreshold: Int,
           dt: Double,
           rCutoff: Double) : RDD[Vector] = {
    new DistNBodyDirectMD()
      .setCycles(cycles)
      .setL(L)
      .setDt(dt)
      .setRCutoff(rCutoff)
      .setCheckPointThreshold(checkPointThreshold)
      .simulation(g)
  }

  def run(
           g: RDD[Vector],
           cycles: Int,
           L: Double) : RDD[Vector] = {
    new DistNBodyDirectMD()
      .setCycles(cycles)
      .setL(L)
      .simulation(g)


  }

}


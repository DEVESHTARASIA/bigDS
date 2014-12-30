package com.intel.nbody

import org.apache.spark.SparkContext

import scala.collection.mutable
import scala.math._
import scala.util.Random

/**
 *
 *  Created by qhuang on 11/13/14.
 */
/**
 * @param dirPath The directory path of an input file recording crystal coordination system
 */
class GenLattice(sc: SparkContext, nParticles:Int, slices:Int, dirPath: String)
  extends GenLatticeExample(sc, nParticles, slices){

}

/**
 * Generate a simple cubic lattice lattice taking an example of solid argon
 * @param sc  SparkContext
 * @param nParticles number of particles in a molecular or crystal
 * @param slices  number of slices for spark running
 */
class GenLatticeExample(sc: SparkContext, nParticles:Int, slices:Int){
  val rand = new Random(42)
  def generateData = {
    var x: mutable.Set[Array[Array[Double]]] = mutable.Set.empty
    var iii = 0
    val nn = ceil(pow(nParticles, 1.0/3)).toInt
    val L = pow(nParticles/0.8, 1.0/3)  // linear size of cubical volume
    val pa = L / nn     // lattice spacing
    val vScale = 0.1    // maximum initial velocity component

    /** initialize positions
     * (0): x direction, (1): y direction, (2): z direction
     * */

    val lattice = Array.ofDim[Double](nParticles, 3)
    for(k <- 0 until nn){
      for(i <- 0 until nn){
        for(j <- 0 until nn){
          lattice(iii)(0) = (k + 0.5) * pa
          lattice(iii)(1) = (i + 0.5) * pa
          lattice(iii)(2) = (j + 0.5) * pa
          iii += 1
        }
      }
    }
    iii = 0
    /**
     *  initialize particle data structure
     *  y()(0): No.
     *  y()(1-3): Positions
     *  y()(4-6): Acceleration
     *  y()(7-9): Velocity
     * */

     while (x.size < slices) {
      val y = Array.ofDim[Double](nParticles/slices, 10)
      for (i <- 0 until nParticles/slices){
        y(i)(0)= iii
        y(i)(1)= lattice(iii)(0)
        y(i)(2)= lattice(iii)(1)
        y(i)(3)= lattice(iii)(2)
        y(i)(4)= 0.0
        y(i)(5)= 0.0
        y(i)(6)= 0.0

        y(i)(7)= (rand.nextDouble() * 2 - 1) * vScale
        y(i)(8)= (rand.nextDouble() * 2 - 1) * vScale
        y(i)(9)= (rand.nextDouble() * 2 - 1) * vScale

        iii += 1

      }
      x.+=(y)
    }
    x.toSeq.toArray

  }
}

class GenLatticeExampleVector(sc: SparkContext, nParticles:Int, slices:Int){
  val rand = new Random(42)
  def generateData = {
    var x: mutable.Set[Array[Array[Double]]] = mutable.Set.empty
    var iii = 0
    val nn = ceil(pow(nParticles, 1.0/3)).toInt
    val L = pow(nParticles/0.8, 1.0/3)  // linear size of cubical volume
    val pa = L / nn     // lattice spacing
    val vScale = 0.1    // maximum initial velocity component

    /** initialize positions
      * (0): x direction, (1): y direction, (2): z direction
      * */

    val lattice = Array.ofDim[Double](nParticles, 3)
    for(k <- 0 until nn){
      for(i <- 0 until nn){
        for(j <- 0 until nn){
          lattice(iii)(0) = (k + 0.5) * pa
          lattice(iii)(1) = (i + 0.5) * pa
          lattice(iii)(2) = (j + 0.5) * pa
          iii += 1
        }
      }
    }
    iii = 0
    /**
     *  initialize particle data structure
     *  y()(0): No.
     *  y()(1-3): Positions
     *  y()(4-6): Acceleration
     *  y()(7-9): Velocity
     * */


      val y = Array.ofDim[Double](nParticles, 10)
      for (i <- 0 until nParticles){
        y(i)(0)= iii
        y(i)(1)= lattice(iii)(0)
        y(i)(2)= lattice(iii)(1)
        y(i)(3)= lattice(iii)(2)
        y(i)(4)= 0.0
        y(i)(5)= 0.0
        y(i)(6)= 0.0

        y(i)(7)= (rand.nextDouble() * 2 - 1) * vScale
        y(i)(8)= (rand.nextDouble() * 2 - 1) * vScale
        y(i)(9)= (rand.nextDouble() * 2 - 1) * vScale

        iii += 1

      }

    y

  }
}
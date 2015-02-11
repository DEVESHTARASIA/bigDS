package org.apache.bigds.science.nbody

import com.esotericsoftware.kryo.Kryo
import org.apache.bigds.science.nbody.Direct.DistNBodyDirectMD
import org.apache.spark._
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.serializer.KryoRegistrator
import scopt.OptionParser
import scala.compat.Platform._

import scala.math._

/**
 * Created by qhuang on 12/25/14.
 */

object DistNBodyDirectMDTest {

  case class Params(
      master: String = "spark://sr471:7177",
      pathToDir: String = null,
      numParticle: Int = 1,
      timeSteps: Int = 1,
      slices: Int = 1) extends AbstractParams[Params]

  /* dt: one time step
     Verlet cutoff on the potential and neighbor list approximation
     rCutoff = Cutoff * Cutoff
   */

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("NbodyBF") {
      opt[String]("master")
        .text(s"master, default: ${defaultParams.master}}")
        .action((x, c) => c.copy(master = x))
      opt[String]("pathToDir")
        .text(s"pathToDir, default: ${defaultParams.pathToDir}}")
        .action((x, c) => c.copy(pathToDir = x))
      opt[Int]("numParticle")
        .text(s"numParticle, default: ${defaultParams.numParticle}}")
        .action((x, c) => c.copy(numParticle = x))
      opt[Int]("timeSteps")
        .text(s"timeSteps, default: ${defaultParams.timeSteps}}")
        .action((x, c) => c.copy(timeSteps = x))
      opt[Int]("slices")
        .text(s"slices, default: ${defaultParams.slices}}")
        .action((x, c) => c.copy(slices = x))
    }


    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      System.exit(1)
    }
  }


  def run(params: Params) {

    println("*****************NbodyBF*******************")

    val start = System.currentTimeMillis / 1000
//
//    if (args.length < 5) {
//      System.err.println("Usage: NbodyBF <master> <path to directory of generated data> <numParticle(x direction)> <time_steps> <slices>")
//      System.exit(1)
//    }

    val conf = new SparkConf()
      .setAppName("SparkNbodyBF")
      .setMaster(params.master)
      .set("spark.executor.memory", "160g")
      .set("spark.cores.max", "256")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.apache.bigds.science.nbody.NbodyRegistrator")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir(params.pathToDir)

    val nParticles = params.numParticle * params.numParticle * params.numParticle
    val cycles = params.timeSteps
    val slices = params.slices
    //
    if (nParticles % slices != 0 || nParticles / slices == 0) {
//      System.err.println("number of particles % number of threads != 0")
//      System.exit(1)
    }
    val L = pow(nParticles/0.8, 1.0/3)  // linear size of cubical volume

    val g = new GenLatticeExampleVector(sc, nParticles, slices)

    val gg = sc.parallelize(g.generateData, slices).map(p => Vectors.dense(p)).cache()


    val mid = System.currentTimeMillis / 1000

    DistNBodyDirectMD.run(gg, cycles, L)

    val end = System.currentTimeMillis / 1000

    println("*********************************************************************************")
    println("*********************************************************************************")
    println((mid - start) + ", " + (end - mid))
    println("*********************************************************************************")
    println("*********************************************************************************")

  }
}


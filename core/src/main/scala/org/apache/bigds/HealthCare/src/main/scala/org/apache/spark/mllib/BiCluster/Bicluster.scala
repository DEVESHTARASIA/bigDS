package com.intel.genbase.trumpet


import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast

import org.apache.spark.mllib.linalg.distributed.{PatchedRowMatrix, RowMatrix}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg._
import scala.collection.mutable.HashMap
import com.github.fommil.netlib.BLAS.{getInstance => blas}
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
/*

/**
 * Created by lionel.yi on 14-6-25.
 */
object BiCluster extends Serializable {

  def main(args: Array[String]) {
    println("BiCluster")
    if (args.length!=9){
      System.err.println("ERROR:BiCluster <master> <path to directory of generated data> <nparts> <age> <gender>(<gene table name> <geo table name> <go table name> <patient table name>)")
      System.exit(1)
    }
    println("====================="+args.mkString(",") + "=======================")
    val conf = new SparkConf()
      .setMaster(args(0))
      .setAppName("BiCluster")
      //                 .setSparkHome(System.getenv("SPARK_HOME"))
      //                .setJars(SparkContext.jarOfClass(this.getClass).toSeq)
      //                 .set("spark.executor.memory", "120g")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "com.intel.genbase.trumpet.BiclustKryoRegistrator")


    @transient val sc = new SparkContext(conf)

    val gene_name = args(5)
    val geo_name = args(6)
    val go_name = args(7)
    val patient_name = args(8)
    val g = new GenAllTables(sc, args(1), gene_name, geo_name, go_name, patient_name)
    val Gene_ref = g.Gene_ref
    val Go_ref = g.Go_ref
    val Patient_ref = g.Patient_ref
    val Geo_ref = g.Geo_ref

    //---------------------------------------------------------------------------------------
    // set number of partitions
    val nparts = args(2).toInt
    val gender = args(4).toInt
    val age    = args(3).toInt

    val start = System.currentTimeMillis / 1000

    val patientIdHash = new HashMap[Int, Int]

    // select target patients with gender and age filter
    g.Patient.table.filter{ p => p(Patient_ref("gender")).toInt == gender && p(Patient_ref("age")).toInt <= age }
      .map{ p => p(Patient_ref("id")).toInt}
      .collect
      .map{ pid => patientIdHash(pid) = 0 }


    if(patientIdHash.keys.isEmpty){
      System.err.println("No patient satisfy the age constraint. Please increase 'age' 'gender' parameter")
      System.exit(1)
    }
    val bcPatientIdHash = sc.broadcast(patientIdHash)

    // build experiment data in format of RDD[(patient_id , iterator_of_expr_values)]
    val arr_data =g.Geo.table.filter{ geo => bcPatientIdHash.value.contains(geo(Geo_ref("patientid")).toInt) }
      .map{ geo => (geo(Geo_ref("patientid")).toInt, (geo(Geo_ref("geneid")).toInt, geo(Geo_ref("expr_value")).toDouble)) }
      .groupByKey(nparts)
      .map{ case(pid, ges) =>
      var arr = new Array[Double](ges.size)
      ges.map{ case(gid, expv) => arr(gid) = expv }
      arr
    }

    val matrix = new PatchedRowMatrix(sc, nparts, arr_data.map{ e => Vectors.dense(e) })

    val mid = System.currentTimeMillis / 1000

    //use spark bicluster
    //calcBiclust return: Array[(Array[Int], Array[Int], Int)]
    val result = new SparkBicluster(matrix, nparts).calcBiclust(1)(0)

    val end = System.currentTimeMillis / 1000

    println("*********************************************************************************")
    println("*********************************************************************************")
    println((mid - start) + ", " + (end - mid))
    println("*********************************************************************************")
    println("*********************************************************************************")

    println("Master: " + args(0) + "; directory: " + args(1) + "; nparts: " + nparts.toString + "; age: " + age.toString + "; gender: " + gender.toString )

    println("Biclust result: ")
    print("u: [")
    result._1.map{ e => print(e + ", ") }
    println("]")

    print("v: [")
    result._2.map{ e => print(e + ", ") }
    println("]")

    println("Number: " + result._3)

    sc.stop()
  }

}*/

class BiclustKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[HashMap[Int, Int]])
    kryo.register(classOf[Array[Double]])
    kryo.register(classOf[(Int, Int, Int, Array[Double])])
  }
}

class SparkBicluster(X: PatchedRowMatrix, nparts: Int) extends Serializable {

  @transient val sc = X.rows.context

  // bool to double, corresponding to (TRUE=1, FALSE=0) in R in calculating
  private def b2d(b: Boolean): Double = {
    b match {
      case true => 1.0
      case false => 0.0
    }
  }

  // implementation of thresh in R s4vd lib
  private def thresh(zv: Double, func_type: Int = 1, delta: Double, a: Double = 3.7): Double = {

    func_type match {
      case 1 => zv.signum * b2d(zv.abs >= delta) * (zv.abs - delta)
      case 2 => zv * b2d(zv.abs > delta)
      case 3 => zv.signum * b2d(zv.abs >= delta) * (zv.abs - delta) * b2d(zv.abs <= 2 * delta) +
        ((a - 1) * zv - zv.signum * a * delta) / (a - 2) * (2 * b2d(delta < zv.abs)) * b2d(zv.abs <= a * delta) + zv * b2d(zv.abs > a * delta)
    }
  }

  // row multiply -- matrix x V
  private def Matrix_x_V(X: RowMatrix, V: Array[Double]): Array[Double] = {

    val ncols = X.numCols().toInt
    require(ncols == V.length)
    val br_v = X.rows.context.broadcast(V)
    X.rows.mapPartitions{ iter => {
      val V_value = br_v.value
      iter.map(rv =>
        blas.ddot(ncols, rv.toArray, 1, V_value, 1)
      )
      /*
      val row = rv.toArray
      var sum = 0.0
      var i = 0
      while(i < ncols) {
        sum += row(i) * V(i)
        i += 1
      }
      sum
       */
    }
    }.collect
  }

  private def printMatrix(X: RDD[Array[Double]]): Unit = {
    X.collect.map { row =>
      row.map { e => print(e + ", ")}
      println
    }
  }

  private def printTMatrix_x_UInput(X: RowMatrix, U: Array[Double]): Unit = {

    println("X: ")
    printMatrix(X.rows.map { e => e.toArray})
    println

    printNamedArray("U", U)
  }

  // col multiply -- t_matrix x U
  private def old_tMatrix_x_U(X: RowMatrix, U: Array[Double]): Array[Double] = {

    // printTMatrix_x_UInput(X, U)

    val nrows = X.numRows().toInt
    val ncols = X.numCols().toInt

    val sum = new Array[Double](ncols)
    val U_br = X.rows.context.broadcast(U)

    val segs = X.rows
      .zipWithIndex()
      .mapPartitions { iter =>
      val u = new Array[Double](ncols)
      val U_value = U_br.value
      iter.map {
        case (rv, row_id) =>
          val Ur = U_value(row_id.toInt)
          val row = rv.toArray
          var i = 0
          while (i < ncols) {
            u(i) = u(i) + (row(i) * Ur)
            i += 1
          }

      }

      Iterator(u)
    }.collect

    segs.map { segsum =>
      var i = 0
      while (i < ncols) {
        sum(i) += segsum(i)
        i += 1
      }
    }

    // printNamedArray("tMatrix_x_U output", sum)

    sum
  }

  // col multiply -- t_matrix x U
  private def tMatrix_x_U(X: RowMatrix, U: Array[Double]): Array[Double] = {

    // printTMatrix_x_UInput(X, U)

    val result = X.rows
      .zipWithIndex()
      .map {
      case (rv, row_id) =>
        rv.toArray.map { e => e * U(row_id.toInt)}
    }.reduce((r1, r2) => r1.zip(r2).map { case (e1, e2) => e1 + e2})

    // printNamedArray("tMatrix_x_U output", result)

    result
  }

  private def printPickUVInput(X: RDD[(Double, Double, Array[Double])], y: Array[Double], SST: Double, ssv: Double, sigsq: Double): Unit = {

    println("X:")
    X.collect.map {
      case (ssx, ilogx, x) =>
        print("(" + ssx + ")(" + ilogx + ")")
        x.map { e => print(e + ", ")}
        println
    }
    println

    printNamedArray("y", y)

    print("SST: " + SST + ", ssv: " + ssv + ", sigsq: " + sigsq)
    println
  }

  private def printNamedArray(name: String, arr: Array[Double]): Unit = {
    println(name + ":")
    arr.map { e => print(e + ", ")}
    println
  }

  // pick out the vector v from array of vectors X, where v get
  // max value for equation bv = (SST + ssx*ssv - 2* v dot y) / sigsq + ilogx
  // if v1 and v2 have the same bv value, then choose the one with smaller
  // ilogx (i.e., the smaller sortid * lognd)
  private def old_pickUV(X: RDD[(Double, Double, Array[Double])], y: Array[Double], SST: Double, ssv: Double, sigsq: Double): Array[Double] = {

    printPickUVInput(X, y, SST, ssv, sigsq)

    var minBv = Double.MaxValue
    var mini = Double.MaxValue

    val output = X.map {
      case (ssx, ilogx, x) =>
        var i = 0
        var dot = 0.0
        while (i < y.length) {
          dot += x(i) * y(i)
          i += 1
        }

        val bv = (SST + ssx * ssv - 2 * dot) / sigsq + ilogx
        if ((bv < minBv) || ((bv == minBv) && (ilogx < mini))) {
          minBv = bv
          mini = ilogx
        }

        (bv, ilogx, x)
    }.filter { case (bv, ilogx, rowx) => (bv == minBv) && (ilogx == mini)}.first._3

    printNamedArray("pickUVOutput", output)
    output
  }

  private def pickUV(X: RDD[(Double, Double, Array[Double])], y: Array[Double], SST: Double, ssv: Double, sigsq: Double): Array[Double] = {

    // printPickUVInput(X, y, SST, ssv, sigsq)

    val output = X.map {
      case (ssx, ilogx, x) =>
        var i = 0
        var dot = 0.0
        while (i < y.length) {
          dot += x(i) * y(i)
          i += 1
        }

        val bv = (SST + ssx * ssv - 2 * dot) / sigsq + ilogx
        (bv, ilogx, x)
    }
      .reduce((vc1, vc2) =>
      if ((vc1._1 < vc2._1) || ((vc1._1 == vc2._1) && (vc1._2 < vc2._2))) vc1
      else vc2)
      ._3

    // printNamedArray("pickUVOutput", output)
    output
  }

  // bicluster by spark_ssvd
  private def spark_ssvd(X: PatchedRowMatrix,
                         U0: Broadcast[Array[Double]], V0: Broadcast[Array[Double]],
                         threu: Int = 1, threv: Int = 1,
                         gamu: Int = 0, gamv: Int = 0,
                         merr: Double = 0.0001,
                         niter: Int = 100): (Array[Double], Array[Double], Int, Boolean) = {

    val n = X.numRows().toInt
    val d = X.numCols().toInt

    val dataX = X.rows.map { rv => rv.toArray}

    var ud = 1.0
    var vd = 1.0

    var iter = 0
    var SST = dataX.map { row =>
      row.map { e => math.pow(e, 2)}.sum
    }.sum()

    var stop = false
    var u0 = U0.value
    var v0 = V0.value
    var u1 = u0
    var v1 = v0

    val lognd = math.log(n * d)
    val ndd = n * d - d
    val ndn = n * d - n

    var ssz = 0.0
    var ssu = 0.0
    var ssv = 0.0
    var sssu = 0.0 // sqrt of ssu
    var sssv = 0.0 // sqrt of ssv

    var sigsq = 0.0

    while (!stop && (ud > merr || vd > merr)) {
      iter += 1

      println("iter: " + iter + "......")
      println("v: " + v0.filter(_ != 0).size)
      println("u: " + u0.filter(_ != 0).size)

      // Updating v
      ssz = 0.0
      // var zz = tMatrix_x_U(X, u0)
      var zz = X.multiplyTBlockBy(u0)

      var z = zz
        .map { zvalue =>
        val absz = zvalue.abs
        ssz += math.pow(absz, 2)
        val winvz = math.pow(absz, gamv)
        val tvz = absz * winvz
        (zvalue, winvz, tvz)
      }
      //(ssx, ilogx, x)
      /*
      print("u0: [")
      u0.map{ e => print(e + ", ") }
      println

      print("zz: [")
      z.map{ e => print(e + ", ") }
      println
       */

      sigsq = (SST - ssz).abs / ndd

      var bc_z = sc.broadcast(z.map { case (zvalue, winvz, _) => (zvalue, winvz)})

      var sort_z = sc.parallelize(
        z.filter { case (zvalue, _, _) => zvalue != 0}
          .map { case (_, _, tvz) => tvz}
          .sortWith(_ > _)
          .zipWithIndex,
        nparts)
        .map {
        case (tvz, sortid) =>
          ssv = 0.0
          val vc = bc_z.value.map {
            case (zvalue, winvz) =>
              if (winvz != 0) {
                val threshv = thresh(zvalue, threv, tvz / winvz)
                if (threshv != 0) ssv += math.pow(threshv, 2)
                threshv
              } else 0
          }
          (ssv, (sortid + 1) * lognd, vc)
      }

      ssu = 0.0
      u0.map { e => ssu += math.pow(e, 2)}

      v1 = pickUV(sort_z, z.map { case (zvalue, winvz, tvz) => zvalue}, SST, ssu, sigsq)

      ssv = 0.0
      v1.map { e => if (e != 0) ssv += math.pow(e, 2)}

      sssv = math.sqrt(ssv)
      v1 = v1.map { e => e / sssv} // v_new

      println("v1 " + v1.size + ",  nonzero(v1): " + v1.filter(_ != 0).size)

      // Updating u
      ssz = 0.0
      // z = Matrix_x_V(X, v1).map{ zvalue =>
      z = X.multiplyBlockBy(v1).map { zvalue =>
        val absz = zvalue.abs
        ssz += math.pow(absz, 2)
        val winuz = math.pow(absz, gamu)
        val tuz = absz * winuz
        (zvalue, winuz, tuz)
      }

      sigsq = (SST - ssz).abs / ndn

      bc_z = sc.broadcast(z.map { case (zvalue, winuz, _) => (zvalue, winuz)})

      sort_z = sc.parallelize(
        z.filter { case (zvalue, _, _) => zvalue != 0}
          .map { case (_, _, tuz) => tuz}
          .sortWith(_ > _)
          .zipWithIndex,
        nparts)
        .map {
        case (tuz, sortid) =>
          ssv = 0.0
          val uc = bc_z.value.map {
            case (zvalue, winuz) =>
              if (winuz != 0) {
                val threshu = thresh(zvalue, threu, tuz / winuz)
                if (threshu != 0) ssu += math.pow(threshu, 2)
                threshu
              } else 0
          }
          (ssu, (sortid + 1) * lognd, uc)
      }
      /*
      sort_z = sc.parallelize(z, nparts)
                 .filter{ case(zvalue, _, _) => zvalue != 0 }
                 .map{ case(zvalue, winuz, tuz) => (tuz, tuz) }
                 .sortByKey(false)
                 .values
                 .zipWithIndex
                 .map{ case(tuz, sortid) =>
                   ssu = 0.0
                   val uc = bc_z.value.map{ case(zvalue, winuz) =>
                     if (winuz != 0) {
                       val threshu = thresh(zvalue, threu, tuz / winuz)
                       if (threshu != 0) ssu += math.pow(threshu, 2)
                       threshu
                     } else 0
                   }
                   (ssu, (sortid+1)*lognd, uc)
                 }
       */

      ssv = 0.0
      v1.map { e => ssv += math.pow(e, 2)}

      u1 = pickUV(sort_z, z.map { case (zvalue, winuz, tuz) => zvalue}, SST, ssv, sigsq)

      ssu = 0.0
      u1.map { e => if (e != 0) ssu += math.pow(e, 2)}

      var sssu = math.sqrt(ssu)
      u1 = u1.map { e => e / sssu} //u_new

      println("u1  " + u1.size + ",  nonzero(u1): " + u1.filter(_ != 0).size)

      ud = 0.0
      vd = 0.0
      u1.zip(u0).map { case (u1e, u0e) => ud += math.pow((u1e - u0e), 2)}
      v1.zip(v0).map { case (v1e, v0e) => vd += math.pow((v1e - v0e), 2)}

      if (iter > niter) {
        println("Fail to converge! Increase the niter!")
        stop = true
      }

      println("ud: " + ud + ", vd: " + vd)
      println

      u0 = u1
      v0 = v1
    }
    (u1, v1, iter, stop)
  }

  def calcBiclust(
                   K: Int = 10,
                   threu: Int = 1, threv: Int = 1,
                   gamu: Int = 0, gamv: Int = 0,
                   merr: Double = 0.0001,
                   niter: Int = 100): Array[(Array[Int], Array[Int], Int)] = {

    var varX = X
    var i = 0
    var stop = false
    val ncols = varX.numCols.toInt

    Array[Int](K).zipWithIndex.map {
      case (zero_v, k) =>

        val svd = varX.computeSVD(1, true)

        // get the 1st U and V vectors correspondingly
        val U0 = sc.broadcast(svd.U.rows.map { row => row.toArray(0)}.collect)
        val V0 = sc.broadcast(svd.V.toArray.slice(0, ncols))

        // println("U0 values: ")
        // var newline = 0
        // U0.value.map{ e =>

        //   print(e + ", ")

        //   if (newline == 4) {
        //     newline = 0
        //     println
        //   }
        //   else { newline += 1 }
        // }
        // println
        // println

        // println("V0 values: ")
        // newline = 0
        // V0.value.map{ e =>

        //   print(e + ", ")

        //   if (newline == 4) {
        //     newline = 0
        //     println
        //   }
        //   else { newline += 1 }
        // }
        // println
        // println

        val res = spark_ssvd(varX, U0, V0, threu, threv, gamu, gamv, merr, niter)

        val RowxNumber = res._1.map { e => if (e != 0) 1 else 0}
        val NumberxCol = res._2.map { e => if (e != 0) 1 else 0}

        var d = 0.0
        // Matrix_x_V(varX, NumberxCol.map{ e => e.toDouble }).zip(RowxNumber)
        varX.multiplyBlockBy(NumberxCol.map { e => e.toDouble}).zip(RowxNumber)
          .map { case (e1, e2) => d += e1 * e2}

        val U = sc.broadcast(RowxNumber)
        val V = sc.broadcast(NumberxCol)

        val dataX = varX.rows.map { e => e.toArray}.zipWithIndex.map { case (row, i) => row.zipWithIndex.map { case (e, j) => e - d * U.value(i.toInt) * V.value(j.toInt)}}
        varX = new PatchedRowMatrix(sc, nparts, dataX.map { e => Vectors.dense(e)})

        (RowxNumber, NumberxCol, k)
    }
  }
}
/*
  def calcFirstLayer(
                      dataX: Array[Array[Double]],
                      U0: Array[Double], V0: Array[Double],
                      threu: Int = 1, threv: Int = 1,
                      gamu: Int = 0, gamv: Int = 0,
                      merr: Double = 0.0001,
                      niter: Int = 100): (Array[Int], Array[Int], Int) = {

    val matrix = new PatchedRowMatrix(sc, nparts, sc.parallelize(dataX).map { row => Vectors.dense(row) })

    val res = spark_ssvd(matrix, sc.broadcast(U0), sc.broadcast(V0), threu, threv, gamu, gamv, merr, niter)

    val RowxNumber = res._1.map { e => if (e != 0) 1 else 0 }
    val NumberxCol = res._2.map { e => if (e != 0) 1 else 0 }

    // var d = 0.0
    // Matrix_x_V(matrix, NumberxCol.map{ e => e.toDouble })
    //                              .zip(RowxNumber)
    //                              .map{ case(e1, e2) => d += e1 * e2 }

    (RowxNumber, NumberxCol, 1)
  }

  // calculate ssvd of X
  // spark_ssvd(X, U0, V0)
  // val result = calcBiclust(1)

}

class RBiCluster(X: RowMatrix, nparts: Int) extends Serializable {

  @transient val sc = X.rows.context

  // bool to double, corresponding to (TRUE=1, FALSE=0) in R in calculating
  private def b2d(b: Boolean): Double = {
    b match {
      case true => 1.0
      case false => 0.0
    }
  }

  // implementation of thresh in R s4vd lib
  private def thresh(zv: Double, func_type: Int = 1, delta: Double, a: Double = 3.7): Double = {

    func_type match {
      case 1 => zv.signum * b2d(zv.abs >= delta) * (zv.abs - delta)
      case 2 => zv * b2d(zv.abs > delta)
      case 3 => zv.signum * b2d(zv.abs >= delta) * (zv.abs - delta) * b2d(zv.abs <= 2 * delta) +
        ((a - 1) * zv - zv.signum * a * delta) / (a - 2) * (2 * b2d(delta < zv.abs)) * b2d(zv.abs <= a * delta) + zv * b2d(zv.abs > a * delta)
    }
  }

  private def calcB(dataX: RDD[Vector], U: Array[Double], V: Array[Double], sigsq: Double, ilognd: Double): Double = {

    var ssxuv = sc.accumulator(0.0)
    val bcU = sc.broadcast(U)
    val bcV = sc.broadcast(V)
    val ncols = V.size

    dataX.zipWithIndex.map {
      case (vec, i) =>
        var arrVec = vec.toArray
        var sumrow = 0.0
        for (j <- 0 until ncols) {
          sumrow += math.pow((arrVec(j) - bcU.value(i.toInt) * bcV.value(j.toInt)), 2)
        }
        ssxuv += sumrow
    }

    ssxuv.value / sigsq + ilognd
  }

  // bicluster by ssvd, exact implementation of the ssvd in R s4vd lib
  private def ssvd(X: RowMatrix,
                   t_X: RowMatrix,
                   U0: RDD[Double], V0: RDD[Double],
                   threu: Int = 1, threv: Int = 1,
                   gamu: Int = 0, gamv: Int = 0,
                   merr: Double = 0.0001,
                   niter: Int = 100): (RDD[Double], RDD[Double], Int, Boolean) = {

    val n = X.numRows().toInt
    val d = X.numCols().toInt

    val dataX = X.rows

    var ud = 1.0
    var vd = 1.0

    var iter = 0
    var SST = sc.accumulator(0.0)

    dataX.map { row =>
      var rSST = 0.0
      row.toArray.map { e => rSST += math.pow(e, 2) }
      SST += rSST
    }

    var stop = false
    var u0 = U0
    var v0 = V0
    var u1 = U0
    var v1 = V0
    while (!stop && (ud > merr || vd > merr)) {
      iter += 1

      println("iter: " + iter + "\n")
      println("v: " + v0.filter(_ != 0).toArray.size + "\n")
      println("u: " + u0.filter(_ != 0).toArray.size + "\n")

      // Updating v
      var z = t_X.multiply(Matrices.dense(n, 1, u0.toArray))

      var ssz = sc.accumulator(0.0)

      val v = z.rows.map {
        r =>
        {
          val r0 = r.toArray(0)
          val absz = r0.abs
          ssz += math.pow(absz, 2)
          val winvz = math.pow(absz, gamv)
          val tvz = absz * winvz
          (r0, winvz, tvz)
        }
      }

      var sigsq = (SST.value - ssz.value).abs / (n * d - d)
      val lognd = math.log(n * d)

      var Bv = Double.MaxValue
      v.map { case (zvalue, winvz, tvz) => (tvz, tvz) }
        .sortByKey(false)
        .values
        .toArray
        .zipWithIndex
        .map {
        case (tvz, sortid) => {
          if (tvz > 0) {
            val vc = v.map { case (zvalue, winvz, _) => if (winvz != 0) thresh(zvalue, threv, tvz / winvz) else 0 }
            val newBv = calcB(dataX, u0.toArray, vc.toArray, sigsq, (sortid + 1) * lognd)
            if (newBv < Bv) {
              v1 = vc
              Bv = newBv
            }
          }
        }
      }

      var ssv = sc.accumulator(0.0)
      v1.map { d => if (d != 0) ssv += math.pow(d, 2) }

      var ssvvalue = math.sqrt(ssv.value)
      v1 = v1.map { d => d / ssvvalue } // v_new

      println("v1" + v1.filter(_ != 0).toArray.size + "\n")

      // Updating u
      z = X.multiply(Matrices.dense(d, 1, v1.toArray))
      ssz = sc.accumulator(0.0)
      val u = z.rows.map {
        r =>
        {
          val r0 = r.toArray(0)
          val absz = r0.abs
          ssz += math.pow(absz, 2)
          val winuz = math.pow(absz, gamu)
          val tuz = absz * winuz
          (r0, winuz, tuz)
        }
      }

      sigsq = (SST.value - ssz.value).abs / (n * d - n)

      var Bu = Double.MaxValue
      u.map { case (zvalue, winuz, tuz) => (tuz, tuz) }
        .sortByKey(false)
        .values
        .toArray
        .zipWithIndex
        .map {
        case (tuz, sortid) =>
        {
          if (tuz > 0) {
            val uc = u.map { case (zvalue, winuz, _) => if (winuz != 0) thresh(zvalue, threu, tuz / winuz) else 0 }
            val newBu = calcB(dataX, uc.toArray, v1.toArray, sigsq, (sortid + 1) * lognd)
            if (newBu < Bu) {
              u1 = uc
              Bu = newBu
            }
          }
        }
      }

      var ssu = sc.accumulator(0.0)
      u1.map { d => if (d != 0) ssu += math.pow(d, 2) }

      val ssuvalue = ssu.value
      u1 = u1.map { d => d / math.sqrt(ssuvalue) } //u_new

      println("u1" + u1.filter(_ > 0).toArray.size + "\n")

      ud = 0.0
      vd = 0.0
      u1.zip(u0).map { case (u1v, u0v) => ud += math.pow((u1v - u0v), 2) }
      v1.zip(v0).map { case (v1v, v0v) => vd += math.pow((v1v - v0v), 2) }

      if (iter > niter) {
        println("Fail to converge! Increase the niter!")
        stop = true
      }

      u0 = u1
      v0 = v1
    }
    (u1, v1, iter, stop)
  }

  // transpose matrix X, in the transposed matrix, each
  // partition contains lines of rows (except the last one)
  private def transpose(X: RDD[(Array[Double], Long)], nrows: Int, ncols: Int, nparts: Int): RDD[Array[Double]] = {

    // assume ncols = nparts * q + r (0 <= r < nparts)
    val q = ncols / nparts
    val r = ncols % nparts

    // for transposed matrix, calculate num of cols in each partitions
    val (lines, last_lines) = r match {
      case 0 => (q, q)
      case _ => (q + 1, ncols % (q + 1))
    }

    // transpose and slice each partition
    val t_block_X = X.mapPartitions { iter =>
    {
      var start_index = -1
      iter.map {
        case (arr, row_index) => {
          if (start_index < 0) start_index = row_index.toInt
          arr
        }
      }.toArray.transpose.sliding(lines, lines).zipWithIndex.map { case (block_matrix, i) => (i, (start_index, block_matrix)) }
    }
    }.groupByKey().sortByKey(true, nparts)

    t_block_X.flatMap {
      case (row_block_index, iter_blocks) =>

        var arr =
          if (row_block_index == (nparts - 1)) Array.ofDim[Double](last_lines, nrows)
          else Array.ofDim[Double](lines, nrows)

        iter_blocks.map {
          case (start_index, block_matrix) =>
            block_matrix.zipWithIndex.map { case (e, i) => e.copyToArray(arr(i), start_index) }
        }

        arr
    }
  }

  var data = X.rows.map { e => e.toArray }

  val nrows = X.numRows.toInt
  val ncols = X.numCols.toInt

  // transpose experiment data
  val t_data = transpose(data.zipWithIndex, nrows, ncols, nparts)

  // build transpose matrix t_X
  val tX = new PatchedRowMatrix(sc, nparts, t_data.map { e => Vectors.dense(e) })

  // compute ssvd biclustering
  val svd = X.computeSVD(ncols, true)
  val U0 = svd.U.rows.map { row => row.toArray(0) }
  val V0 = sc.parallelize(svd.V.toArray.slice(0, ncols))

  val result = ssvd(X, tX, U0, V0)

}
*/
package sjtu.spark.example

import org.apache.spark.{Logging}

/**
 * Created by Ou Huisi on 15/1/16.
 */

/**
 * Abstract class of tensor
 */
trait Ratings extends Serializable with Logging {
  val tshape: Array[Int]
  val ndim : Int
  val v: Double
}

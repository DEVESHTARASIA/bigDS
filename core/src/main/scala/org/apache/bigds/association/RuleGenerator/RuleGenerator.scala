import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

/**
 * Created by clin3 on 2015/2/2.
 */
object RuleGenerator {
  def run(
      data: Array[(String, Long)],
      minConfidence: Double,
      minLift: Double,
      lengthOfDB: Long,
      delimiter: String): ArrayBuffer[RuleModel] = {
    val patterns = new ArrayBuffer[RuleModel]()
    for(i <- 0 until(data.length - 1)) {
      for(j <- (i + 1) until(data.length)) {
        var first = data(i)
        var second = data(j)
        if(first._1.length < second._1.length) {
          val temp = first
          first = second
          second = temp
        }
        val confidence = first._2.toDouble / second._2
        if(confidence >= minConfidence) {
          genRule(first._1, second._1, delimiter) match {
            case Some(rule) => {
              rule.support = first._2
              rule.confidence = confidence
              computeLift(rule, data, lengthOfDB) match {
                case Some(rule) => {
                  if(rule.lift >= minLift)
                    patterns += rule
                }
                case None =>
              }
            }
            case None =>
          }
        }
      }
    }
    patterns
  }

  def genRule(first: String, second: String, delimiter: String): Option[RuleModel] = {
    val left = new ArrayBuffer[String]()
    val right = new ArrayBuffer[String]()
    var x = first.split(delimiter).toBuffer
    val y = second.split(delimiter)
    var isBreak = false
    val loop = new Breaks()
    loop.breakable(
      for(i <- y) {
        if(x.contains(i)) {
          left += i
          x -= i
        } else {
          isBreak = true
          loop.break()
        }
      }
    )
    if(isBreak == false) {
      for(i <- x) {
        right += i
      }
      val rule = new RuleModel(left.mkString(delimiter), right.sortWith(_ < _).mkString(delimiter))
      Some(rule)
    } else {
      None
    }
  }

  def computeLift(
      rule: RuleModel,
      data: Array[(String, Long)],
      lengthOfDB: Long ): Option[RuleModel] = {
    val map = data.toMap
    var lift = Double.MinValue
    map.get(rule.rhs) match {
      case Some(support) => {
        lift = rule.confidence / (support.toDouble / lengthOfDB)
        rule.lift = lift
        Some(rule)
      }
      case None => {
        None
      }
    }
  }
}

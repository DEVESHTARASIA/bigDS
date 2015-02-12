package org.apache.bigds.association.PFP

import org.apache.bigds.association.RuleGenerator.RuleGenerator
import org.apache.spark.{SparkContext, SparkConf}
import scopt.OptionParser


/**
 * Created by clin3 on 2014/12/24.
 */
object DistFPGrowthTest {
  case class Params(
      fileName: String = null,
      supportThreshold: Double = 0,
      numGroup: Int = 1,
      minConfidence: Double = 0,
      minLift: Double = 0,
      delimiter: String = " ") extends AbstractParams[Params]

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("PFP") {
      opt[String]("fileName")
        .text(s"fileName, default: ${defaultParams.fileName}}")
        .action((x, c) => c.copy(fileName = x))
      opt[Int]("numGroup")
        .text(s"numGroup, default: ${defaultParams.numGroup}}")
        .action((x, c) => c.copy(numGroup = x))
      opt[Double]("minConfidence")
        .text(s"minConfidence, default: ${defaultParams.minConfidence}}")
        .action((x, c) => c.copy(minConfidence = x))
      opt[Double]("minLift")
        .text(s"minLift, default: ${defaultParams.minLift}}")
        .action((x, c) => c.copy(minLift = x))
      opt[String]("delimiter")
        .text(s"delimiter, default: ${defaultParams.delimiter}}")
        .action((x, c) => c.copy(delimiter = x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    } getOrElse {
      System.exit(1)
    }
  }

  def run(params: Params) {

    //Initialize SparkConf.
    val conf = new SparkConf()
    conf.setMaster("spark://sr471:7177")
      .setAppName("FPGrowth")
      .set("spark.cores.max", "256")
      .set("spark.executor.memory", "160G")

    //Initialize SparkContext.
    val sc = new SparkContext(conf)

    //Create distributed datasets from hdfs.
    val input = sc.textFile("hdfs://sr471:54311/user/clin/fpgrowth/input/" + params.fileName, 256)

    val rdd = DistFPGrowth.run(input, params.supportThreshold, params.delimiter, params.numGroup)

    val patterns = RuleGenerator.run(rdd.collect(), params.minConfidence, params.minLift, input.count(), params.delimiter)

    for(i <- patterns) {
      println(i.lhs + " => " + i.rhs + " support = " + i.support + " confidence = " + i.confidence + " lift = " + i.lift)
    }

    //Stop SparkContext.
    sc.stop()
  }
}

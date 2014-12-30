package org.apache.bigds.repl

import scala.Predef._
import org.apache.spark.repl._

class BigdsCommandLine extends SparkCommandLine {

    override def this(args: List[String]) {
        this(args, str => Console.println("ERROR:: " + str))
    }

}

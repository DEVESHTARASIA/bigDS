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
package org.apache.bigds.mining.association

import org.apache.bigds.mining.association.PFP.FPTree
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}

trait FrequentItemsetMining extends Serializable with Logging {

  /** Set the support threshold. Support threshold must be defined on interval [0, 1]. Default: 0. */
  private [association] def setSupportThreshold(supportThreshold: Double): this.type

  /** Set the splitter pattern within which we can split transactions into items. */
  private [association] def setSplitterPattern(splitterPattern: String): this.type

  /** Implementation of DistFPGrowth. */
  private [association] def run(data: RDD[String]): RDD[(String, Int)]
}


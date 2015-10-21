package org.myorg.quickstart

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.scala._

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over some sample data
 *
 * This example shows how to:
 *
 *   - write a simple Flink program.
 *   - use Tuple data types.
 *   - write and use user-defined functions.
 */
object WordCount {
  def main(args: Array[String]) {

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment


    /*
    // get input data
    val text = env.fromElements("2 2 2 2 2")

    // SELECT SUM(pages) / Count(*) FROM TABLE
    val counts = text.flatMap {_.toLowerCase.split("\\W+")}
      .map(pages => (pages.toDouble, 1.0))
      .reduce((t1,t2) => Tuple2((t1._1 +t2._1),(t1._2 +t2._2)))
      .map(items => (items._1/items._2))
    */

    val itemList: DataSet[List[Int]] = env.fromElements(List(1,2,3),List(4,5,6),List(7,8))

    val result = itemList.flatMap(items => for (a <- items; b <- items; if a < b) yield Seq(a, b))

    // execute and print result
    result.print()

  }
}

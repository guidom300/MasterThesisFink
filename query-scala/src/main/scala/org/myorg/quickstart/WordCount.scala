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

import scala.collection.mutable.ArrayBuffer

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



    // get input data
    //val text = env.fromElements("To|be, or not to be,--that is the question:--",
    //"Whether 'tis nobler in the mind to suffer", "The slings and arrows of outrageous fortune",
    //"Or to take arms against a sea of troubles,")
    val text = env.fromElements("33742|3128||524|29|98657")

    // SELECT SUM(pages) / Count(*) FROM TABLE
    //val outputList: ArrayBuffer[String] = null
    val result = text.flatMap {item =>  item.toLowerCase.split("\\|")}


    /*

    val itemList: DataSet[List[Int]] = env.fromElements(List(1,2,3),List(4,5,6),List(7,8))

    val result = itemList.flatMap(items => for (a <- items; b <- items; if a < b) yield Seq(a, b))

    // execute and print result*/
    result.print()

  }
}

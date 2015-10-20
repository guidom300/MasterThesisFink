package de.tub.cs.testDB

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.api.table.expressions.Literal

/*
 Reference Example of Table API: https://github.com/apache/flink/blob/master/flink-staging/flink-table/src/main/scala/org/apache/flink/examples/scala/TPCHQuery3Table.scala
*/

case class tableWordCount(word: String, count: Int)

object tableDB{

  val outputPath = "/home/jjoon/flinkTest/test001/"

  def main(args: Array[String]): Unit = {

    if(parseParameters(args)){
      return
    }

    // set up the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    val input = env.fromElements(tableWordCount("Hello1",1),tableWordCount("Hello2",2),tableWordCount("Hello",3),tableWordCount("Hello",3))
    val exp = input.toTable
    val result = exp.groupBy("word").select("word, count.sum as count").toDataSet[tableWordCount]

    //result.collect()
    result.writeAsCsv(outputPath, "\n", "|", WriteMode.OVERWRITE)

    env.execute("Test Table API")

  }

  // *************************************************************************
  //     UTIL METHODS
  // *************************************************************************

  /*
  private var lineitemPath: String = null
  private var customerPath: String = null
  private var ordersPath: String = null
  private var outputPath: String = null

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 4) {
      lineitemPath = args(0)
      customerPath = args(1)
      ordersPath = args(2)
      outputPath = args(3)
      true
    } else {
      System.err.println("Usage: <outputPath>");
      false
    }
  }
  */
}

class tableDB {

}

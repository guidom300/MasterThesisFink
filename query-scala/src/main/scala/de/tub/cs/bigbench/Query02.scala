package de.tub.cs.bigbench

package de.tub.cs.bigbench

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector

/*
 * Edit Conf: /home/jjoon/bigBench/data-generator/output/web_clickstreams.dat  /home/jjoon/bigBench/results
*/

object Query02{

  // arg_configuration
  val LIMIT = 30
  val ITEM = 10001

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    val env = ExecutionEnvironment.getExecutionEnvironment

    val clickAndWebPageType = getWebClickDataSet(env)
      .filter(items => (!items._item_sk.equals(null) && !items._user_sk.equals(null)))
      .map(items => new ClickWebPageType(items._user_sk,items._item_sk,items._click_date * 24 * 60 * 60 + items._click_time))
      .groupBy(0)
      .sortGroup(0,Order.ASCENDING)
      .sortGroup(2, Order.ASCENDING)

    val tmpSession = clickAndWebPageType
      .reduceGroup((in, out : Collector[(Long, String)]) => reducePython(in, out))
      .sortPartition(1,Order.DESCENDING)

    val pairs = tmpSession
      .groupBy(1)
      .reduceGroup(in => in.map(v => v._1).toSet.toArray.sorted)                          // collect_set(wcs_item_sk) as itemArray
      .filter(items => items.contains(ITEM))
      .flatMap(items => for (a <- items; b <- items; if a < b) yield Seq(a, b))           // makePairs(sort_array(itemArray), false) as item_1, item_2

    val realQuery = pairs
      .filter(items => items.contains(ITEM))
      .map{items => (items(0),items(1),1)}
      .groupBy(0,1)
      .sum(2)
      .sortPartition(2,Order.DESCENDING).setParallelism(1)                                // ORDER BY cnt DESC, item_1, item_2
      //.sortPartition(1,Order.ASCENDING).setParallelism(1)                               // FLINK RC 0.10 will fix this issue
      //.sortPartition(0,Order.ASCENDING).setParallelism(1)
      .first(LIMIT)

    //realQuery.print()
    realQuery.writeAsCsv(outputPath + "/result-02.dat","\n", "|",WriteMode.OVERWRITE)

    env.execute("Big Bench Query2 Test")
  }

  def reducePython(in: Iterator[ClickWebPageType], out : Collector[(Long, String)]) = {
    var userId: Long = 0
    var userItem: Long = 0
    var last_click_time: Long = 0
    var tmp_time: Long = 0

    var perUser_counter = 1
    var output_sessionId: String = null

    in.foreach{ userInfo =>
      userId = userInfo._user_sk
      userItem = userInfo._item_sk
      tmp_time = userInfo._sum_date

      if (tmp_time - last_click_time > 3600){
        perUser_counter += 1
      }

      last_click_time = tmp_time
      output_sessionId = userId.toString + "_" + perUser_counter.toString
      out.collect(userItem, output_sessionId)
    }
  }


  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class WebClick(_click_date: Long, _click_time: Long, _item_sk: Long, _user_sk: Long)
  case class ClickWebPageType(_user_sk: Long, _item_sk: Long, _sum_date: Long)

  // *************************************************************************
  //     UTIL METHODS
  // *************************************************************************

  private var webClickPath: String = null
  private var outputPath: String = null

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 2) {
      webClickPath = args(0)
      outputPath = args(1)
      true
    } else {
      System.err.println("Usage: Big Bench <web_clickstream-csv path> <ITEM>  <result path>")
      false
    }
  }

  private def getWebClickDataSet(env: ExecutionEnvironment): DataSet[WebClick] = {
    env.readCsvFile[WebClick](
      webClickPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 1, 3, 5),
      lenient = true)
  }
}

class Query02 {

}

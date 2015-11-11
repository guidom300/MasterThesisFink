package de.tub.cs.bigbench

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.util.Collector

/*
Developed By Philip Lee

Configuration
/home/jjoon/bigBench/data-generator/output/web_clickstreams.dat  /home/jjoon/bigBench/results
*/

object Query02_TableAPI{

  // arg_configuration
  val LIMIT = 30
  val ITEM = 10001

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    // set up execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    val clickAndWebPageType = getWebClickDataSet(env).as('wcs_click_date_sk, 'wcs_click_time_sk, 'wcs_item_sk, 'wcs_user_sk)
      .where('wcs_item_sk.isNotNull && 'wcs_user_sk.isNotNull)
      .select('wcs_user_sk as 'user, 'wcs_item_sk as 'item, ('wcs_click_date_sk * 24 * 60 * 60 + 'wcs_click_time_sk) as 'sum_date)
      .toDataSet[ClickWebPageType]
      .groupBy(0)                                                        // DISTRIBUTE BY wcs_user_sk SORT BY wcs_user_sk, tstamp_inSec     // .groupBy('wcs_user_sk)
      .sortGroup(0,Order.ASCENDING)
      .sortGroup(2, Order.ASCENDING)

    val tmpSession = clickAndWebPageType
      .reduceGroup((in, out : Collector[(Int, String)]) => reducePython(in, out))       // reduce three columns; wcs_user_sk, tstamp_inSec, wcs_item_sk using Python Code //.as('wcs_item_sk, 'sessionId)
      .partitionByHash(1).sortPartition(1,Order.DESCENDING)                             // CLUSTER BY sessionId

    val pairs = tmpSession
      .groupBy(1)                                                                       // GROUP BY sessionId
      .reduceGroup(in => in.map(v => v._1).toSet.toArray.sorted)                          // collect_set(wcs_item_sk) as itemArray    //.reduceGroup(new MyCollectSetReducer).as(`itemArray)
      .filter(items => items.contains(ITEM))                                      // HAVING array_contains(itemArray, cast(q02 AS BIGINT))
      .flatMap(items => for (a <- items; b <- items; if a < b) yield Seq(a, b))         // makePairs(sort_array(itemArray), false) as item_1, item_2

    val realQuery = pairs
      .filter(items => items.contains(ITEM))
      .map{items => (items(0),items(1),1)}
      .groupBy(0,1)
      .sum(2)
      .sortPartition(2,Order.DESCENDING).setParallelism(1)                                // ORDER BY cnt DESC, item_1, item_2
      //.sortPartition(1,Order.ASCENDING).setParallelism(1)
      //.sortPartition(0,Order.ASCENDING).setParallelism(1)

      .first(LIMIT).print()

    //env.execute("Big Bench Query2 Test")
  }

  // Python Code as Scala Version; Return item, sessionId
  def reducePython(in: Iterator[ClickWebPageType], out : Collector[(Int, String)]) = {
    var userId: Int = 0
    var userItem: Int = 0
    var last_click_time: Long = 0
    var tmp_time: Long = 0

    var perUser_counter = 1
    var output_sessionId: String = null

    in.foreach{ userInfo =>
      userId = userInfo.user
      userItem = userInfo.item
      tmp_time = userInfo.sum_date

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

  case class WebClick(_click_date: Long, _click_time: Long, _item_sk: Int, _user_sk: Int)
  case class ClickWebPageType(user: Int, item: Int, sum_date: Long)
  case class TmpSession(item: Int, sessionId: String)
  case class CollectedList(itemArray: Set[Int])

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
class Query02_TableAPI {

}

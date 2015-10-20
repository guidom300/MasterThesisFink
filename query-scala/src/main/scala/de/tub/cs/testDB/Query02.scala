package de.tub.cs.testDB

import org.apache.flink.api.common.functions.RichGroupReduceFunction.Combinable
import org.apache.flink.api.common.functions.{GroupReduceFunction}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.util.Collector

object Query02{

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    // set up execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // Sessionize by streaming
    val clickAndWebPageType = getWebClickDataSet(env).as('wcs_click_date_sk, 'wcs_click_time_sk, 'wcs_item_sk, 'wcs_user_sk)
      .where('wcs_item_sk.isNotNull && 'wcs_user_sk.isNotNull)
      .select('wcs_user_sk, 'wcs_item_sk, ('wcs_click_date_sk * 24 * 60 * 60 + 'wcs_click_sk) as 'tstamp_inSec)
      .groupBy('wcs_user_sk).toDataSet[ClickWebPageType]               // DiSTRIBUTE BY wcs_user_sk SORT BY wcs_user_sk, tstamp_inSec
      .sortPartition("user",Order.ASCENDING)
      .sortPartition("sum_date", Order.ASCENDING)



    val tmpSessionize = clickAndWebPageType
      .reduceGroup(new MyGroupReducer)
      .groupBy(1)                           // CLUSTER BY sessionId && GROUP BY sessionId
      .filter()                             // HAVING array_contains(itemArray, cast(q02_item_sk) as BIGINT))
      .distinct(0)                          // collect_set(wcs_item_sk)

    val realQuery = tmpSessionize.



    env.execute("Big Bench Query2 Test")

  }

  // q02-sessionize.py code
  @Combinable
  class MyGroupReducer extends GroupReduceFunction[ClickWebPageType, TmpSession] {
    override def reduce(in: java.lang.Iterable[ClickWebPageType], out: Collector[TmpSession]): Unit = {

      val itr = in.iterator()

      var current_userId: Int = 0
      var userId: Int = 0
      var userItem: Int = 0
      var last_click_time: Int = 0
      var tmp_time: Int = 0

      var perUser_sessionID_counter = 1

      while(itr.hasNext){

        val userInfo = itr.next()

        userId = userInfo.user
        userItem = userInfo.item
        tmp_time = userInfo.sum_date

        if(current_userId != userId){
            current_userId = userId
            perUser_sessionID_counter = 1
            last_click_time = tmp_time
        }

        if(tmp_time - last_click_time > 3600){
          perUser_sessionID_counter += 1
        }

        last_click_time = tmp_time


        out.collect(userItem,userId.toString + "_" + perUser_sessionID_counter.toString)
      }
    }
  }



  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class WebClick(user: Int, item: Int, click_date: Int, click_time: Int)
  case class ClickWebPageType(user: Int, item: Int, sum_date: Int)
  case class TmpSession(item: String, sessionId: String)
  case class CollectedList()

  // *************************************************************************
  //     UTIL METHODS
  // *************************************************************************

  private var webClickPath: String = null
  private var searchItem: String = null
  private var outputPath: String = null


  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 3) {
      webClickPath = args(0)
      searchItem = args(1)
      outputPath = args(2)
      true
    } else {
        System.err.println("Usage: Big Bench <web_clickstream-csv path> <search_item>  <result path>")
      false
    }
  }

  private def getWebClickDataSet(env: ExecutionEnvironment): DataSet[WebClick] = {
    env.readCsvFile[WebClick](
      webClickPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 1, 3, 5) )
  }

}
class Query02 {

}

package de.tub.cs.bigbench


import org.apache.flink.api.common.functions.{FlatMapFunction, RichGroupReduceFunction}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.util.Collector



import scala.sys

/*
 * Query 04 without Table API
 * Edit Conf: /home/jjoon/bigBench/data-generator/output/web_clickstreams.dat  /home/jjoon/bigBench/data-generator/output/web_page.dat home/jjoon/bigBench/results
 * Questions;
 * apply function vs map function
 * wcs_sales_sk == NULL
 *
 * TODO:
 * .map(pageCount => Tuple2(pageCount.toDouble,1.0))
 * .sortBy
 * Check reduceShopCartPython
 */
/*

 val filteredWebClickStream =  webClickStream
    .filter(click => click._page != null)
    .filter(click => click._user != null)
    .filter(click => click._sales == null)
    .filter(click =>click._user.isNotNull).filter(click =>click._sales.isNull)
 */

object Query04{

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    // set up execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment


    // e.g  _date|_click|_sales|_item|_web_page|_user
    //val webClickStream = getWebClickDataSet(env)
    //val webPage = getWebPageDataSet(env).as('wp_web_page_sk, 'wp_type)

    val clickAndWebPageType = getWebClickDataSet(env)

    clickAndWebPageType.first(1).print()


  /*
    val clickAndWebPageType = getWebClickDataSet(env).join(webPage).where(4).equalTo(0)   // 'wcs_web_page_sk === 'wp_web_page_sk &&
      .as('wcs_click_date_sk, 'wcs_click_time_sk, 'wcs_sales_time_sk, 'wcs_item_sk, 'wcs_web_page_sk,'wcs_user_sk)
      .where( 'wcs_web_page_sk.isNotNull && 'wcs_user_sk.isNotNull && 'wcs_sales_time_sk.isNull)
      .select('wcs_user_sk as '_user, 'wp_type as '_type, ('wcs_click_date_sk * 24 * 60 * 60 + 'wcs_click_time_sk) as '_sum_date)
      .toDataSet[ClickWebPageType]
      .partitionByHash("_user")                  // DISTRIBUTE BY wcs_user_sk SORT BY wcs_user_sk, tstamp_inSec
      .sortPartition("_user",Order.ASCENDING)
      // groupBy() for ordering of sorting
      .sortPartition("_sum_date", Order.ASCENDING)
      .reduceGroup((in, out : Collector[(String, Long, String)]) => reduceSessionPython(in, out))   // return _type, tst_amp, _sessionId

    val realQuery = clickAndWebPageType
      .partitionByHash("_sessionId")                        // DISTRIBUTE BY sessionId SORT BY sessionId, tst_amp, wp_type
      .groupBy(1)
      .sortGroup(1,Order.ASCENDING)
      //.sortPartition(1,Order.ASCENDING)
      //.sortPartition(0,Order.ASCENDING)
      .reduceGroup((in, out : Collector[(Int)]) => reduceShopCartPython(in, out))   // return _session_row_counterer

  */
      /*
      .map(pageCount => Tuple2(pageCount.toDouble,1.0))				// SELECT SUM(Pages) / Coun(*)
      .reduce((t1,t2) => (t1._1 + t2._1), (t1._2,t2._2))
      .map(item => (item._1 / item._2))
*/
/*
    realQuery.print()*/

    //env.execute("Big Bench Query2 Test")
  }



  def reduceSessionPython(in: Iterator[ClickWebPageType], out : Collector[(String, Long, String)]) = {
    var userId: Int = 0
    var userType: String = ""
    var last_click_time: Long = 0
    var tmp_time: Long = 0

    var perUser_counter = 1
    var output_sessionId: String = null

    in.foreach{ userInfo =>
      userId = userInfo._user
      userType = userInfo._type
      tmp_time = userInfo._sum_date

      if (tmp_time - last_click_time > 3600){
        perUser_counter += 1
      }

      last_click_time = tmp_time
      output_sessionId = userId.toString + "_" + perUser_counter.toString
      out.collect(userType, tmp_time, output_sessionId)
    }
  }

  // Iterator( _type, tst_amp, _sessionId)
  def reduceShopCartPython(in: Iterator[(String, Long, String)], out : Collector[(Int)]) = {

    var userType: String = null
    var sessionId: String = null

    var session_row_counter = 0
    var current_key: String = null
    var last_order_row = -1
    var last_dynamic_row = -1


    in.foreach { userInfo =>
      userType = userInfo._1
      sessionId = userInfo._3

      if (last_dynamic_row > last_order_row)
        out.collect(session_row_counter)

      session_row_counter = 1
      current_key = sessionId
      last_order_row = -1
      last_dynamic_row = -1

      if (userType.equals("order"))
        last_order_row = session_row_counter

      if (userType.equals("dynamic"))
        last_dynamic_row = session_row_counter

    }
    if (last_dynamic_row > last_order_row)
      out.collect(session_row_counter)
  }



  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class WebClick(_click_date: Long, _click_time: Long, _sales: Int, _item: Int,_page: Int, _user: Int)
  case class WebPage(_page: Int, _type: String)
  case class ClickWebPageType(_user: Int, _type: String, _sum_date: Long)
  case class TmpSession(_type: String, _tst_amp: Long, _sessionId: String)


  // *************************************************************************
  //     UTIL METHODS
  // *************************************************************************

  private var webClickPath: String = null
  private var webPagePath: String = null
  private var outputPath: String = null

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 3) {
      webClickPath = args(0)
      webPagePath = args(1)
      outputPath = args(2)
      true
    } else {
      System.err.println("Usage: Big Bench <web_clickstream-csv path> <web_page-csv path>  <result path>")
      false
    }
  }

  // e.g. 36890|26789|0|3725|20|85457
  // e.g  _date|_click|_sales|_item|_web_page|_user
  private def getWebClickDataSet(env: ExecutionEnvironment)= {

    //env.readTextFile(webClickPath).flatMap{ _.split("\\|")}//.flatMap(new TokenizeInput)





    env.readCsvFile[WebClick](


      webClickPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 1, 2, 3, 4, 5)
      //lenient = true
    )
  }

  class TokenizeInput extends FlatMapFunction[String, List[String]] {
      override def flatMap(value: String, out: Collector[List[String]]): Unit = { // (in: Iterable[TmpSession], out: Collector[CollectedList]) = {

      val tokens = value.split("|")
      var token: String = ""

      // emit the pairs
      //for (token <- tokens) {
        //if (token.length() > 0) {

          out.collect(tokens.toList)

        //}
      //}
    }
  }

  // e.g. 33|AAAAAAAAAAAAAABH|2000-11-01||1930|31567|0|534|http://www.UZKjf3qcGotmY.com|general|2404|10|5|0
  // e.g  web_page_sk|_page_id|_start_date|_end_date|_creation_date|_access_date|_flag|_customer|_url|_type|_char_count|_link_count|_img_count|_ad_count
  private def getWebPageDataSet(env: ExecutionEnvironment): DataSet[WebPage] = {
    env.readCsvFile[WebPage](
      webPagePath,
      fieldDelimiter = "|",
      includedFields = Array(0, 9),   // _page | _type |
      lenient = true
    )
  }
}

class Query04 {

}

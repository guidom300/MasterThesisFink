package de.tub.cs.bigbench

import org.apache.flink.api.common.functions.{FlatMapFunction, RichGroupReduceFunction}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector


import scala.collection.mutable.ArrayBuffer
import scala.sys

/*
Developed By Philip Lee

Configuration
/home/jjoon/bigBench/data-generator/output/web_clickstreams.dat  /home/jjoon/bigBench/data-generator/output/web_page.dat /home/jjoon/bigBench/results
 */

object Query04{

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    val env = ExecutionEnvironment.getExecutionEnvironment

    val webClickStream = getWebClickDataSetNull(env).flatMap(new NullTokenizer)
      .map(items => new WebClick(items(0).toLong,items(1).toLong,items(2),items(3),items(4),items(5)))          // parse elements on matching fiter fucntion
      .filter(items => !items._page_sk.equals("") && !items._user_sk.equals("") && items._sales_sk.equals(""))

    val webPage = getWebPageDataSet(env)
      .filter(items => items._type.equals("order") || items._type.equals("dynamic"))

    val clickAndWebPageType = webClickStream.join(webPage).where(_._page_sk.toLong).equalTo(_._page_sk)
      .apply((wc,wp) => new ClickWebPageType(wc._user_sk.toLong,wp._type,(wc._click_date * 24 * 60 * 60 + wc._click_time)))
      .groupBy(0)
      .sortGroup(2, Order.ASCENDING)
      .reduceGroup((in, out : Collector[(String, Long, String)]) => reduceSessionPython(in, out))

    val realQuery = clickAndWebPageType
      .groupBy(2)
      .sortGroup(1,Order.ASCENDING)             // SORT BY sessionId, tst_amp, wp_type; groupBy also sort the selected column
      .sortGroup(0,Order.ASCENDING)
      .reduceGroup((in, out : Collector[(Int)]) => reduceShopCartPython(in, out))
      .reduceGroup((in, out : Collector[(Double)]) => reduceAvg(in, out))

    //realQuery.print()
    realQuery.writeAsText(outputPath + "/result-04.dat",WriteMode.OVERWRITE)

    env.execute("Big Bench Query4 Test")
  }

  def reduceSessionPython(in: Iterator[ClickWebPageType], out : Collector[(String, Long, String)]) = {
    var userId: Long = 0
    var userType: String = ""
    var last_click_time: Long = 0
    var tmp_time: Long = 0

    var perUser_counter = 1
    var output_sessionId: String = null

    in.foreach{ userInfo =>
      userId = userInfo._user_sk
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

  def reduceShopCartPython(in: Iterator[(String, Long, String)], out : Collector[(Int)]) = {

    var userType: String = null
    var session_row_counter = 1
    var last_order_row = -1
    var last_dynamic_row = -1

    // count dynamic and order value in the same session
    in.foreach { userInfo =>
      userType = userInfo._1

      session_row_counter += 1

      if (userType.equals("order"))
        last_order_row = session_row_counter

      if (userType.equals("dynamic"))
        last_dynamic_row = session_row_counter
    }

    if (last_dynamic_row > last_order_row)
      out.collect(session_row_counter)
  }

  def reduceAvg(in: Iterator[Int], out : Collector[Double]) = {
    var cnt = 0
    var sum: Double = 0

    in.foreach { userInfo =>
      sum += userInfo
      cnt += 1
    }

    out.collect(((sum/cnt)*10).round/10.toDouble)
  }

  class NullTokenizer extends FlatMapFunction[String, ArrayBuffer[String]] {
    override def flatMap(in: String, out: Collector[ArrayBuffer[String]]) {

      val tuple = ArrayBuffer[String]()
      var cnt: Int = 0

      var tokens = in.split("|")
      for(token <- tokens)
        if(token.equals("|"))
          cnt += 1

      tokens = in.split("\\|")
      for(token <- tokens)
        tuple += token

      if(cnt.equals(tuple.length))
        tuple += ""

      out.collect(tuple)
    }
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************
  // BIGINT: e.g  _date|_click|_sales|_item|_web_page|_user
  //case class WebClick(_click_date: Long, _click_time: Long, _sales_sk: Long, _item_sk: Long,_page_sk: Long, _user_sk: Long)
  case class WebClick(_click_date: Long, _click_time: Long, _sales_sk: String, _item_sk: String, _page_sk: String, _user_sk: String)
  case class WebPage(_page_sk: Long, _type: String)
  // UDC
  case class ClickWebPageType(_user_sk: Long, _type: String, _sum_date: Long)

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
      System.err.println("Usage: Big Bench <web_clickstream-csv path> <web_page-csv path> <result path>")
      false
    }
  }

  // e.g. 36890|26789|0|3725|20|85457
  // e.g  _date|_click|_sales|_item|_web_page|_user
//  private def getWebClickDataSet(env: ExecutionEnvironment): DataSet[WebClick]= {
//    env.readCsvFile[WebClick](
//      webClickPath,
//      includedFields = Array(0, 1, 2, 3, 4, 5),
//      fieldDelimiter = "|",
//      lenient = true
//    )
//  }

  private def getWebClickDataSetNull(env: ExecutionEnvironment): DataSet[String]= {
    env.readTextFile(
      webClickPath
    )
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

/*
 Table API
 .where( 'wcs_web_page_sk.isNotNull && 'wcs_user_sk.isNotNull && 'wcs_sales_time_sk.isNull)
 .select('wcs_user_sk as '_user, 'wp_type as '_type, ('wcs_click_date_sk * 24 * 60 * 60 + 'wcs_click_time_sk) as '_sum_date)

 val filteredWebClickStream =  webClickStream
    .filter(click => click._page != null)
    .filter(click => click._user != null)
    .filter(click => click._sales == null)
    .filter(click =>click._user.isNotNull).filter(click =>click._sales.isNull)
 */
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
 * wcs_sales_sk == NULL
 *
 * TODO:
 * .map(pageCount => Tuple2(pageCount.toDouble,1.0))
 * .sortBy
 * Check reduceShopCartPython
 */

object Query04{

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    // set up execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    val webClickStream = getWebClickDataSet(env)
      .filter(items => items._page_sk != null && items._user_sk != null && items._sales_sk == null)

    val webPage = getWebPageDataSet(env)          // required by abandonment session
      .filter(items => items._type.equals("order") || items._type.equals("dynamic"))

    val clickAndWebPageType = getWebClickDataSet(env).join(webPage).where(_._page_sk).equalTo(_._page_sk)
      .apply((wc,wp) => new ClickWebPageType(wc._user_sk,wp._type,(wc._click_date * 24 * 60 * 60 + wc._click_time)))
      .sortPartition(0,Order.ASCENDING)
      .sortPartition(2, Order.ASCENDING)
      .reduceGroup((in, out : Collector[(String, Long, String)]) => reduceSessionPython(in, out))   // return _type, tst_amp, _sessionId

    val realQuery = clickAndWebPageType
      .groupBy(2)                               // required by reduceShopCartPython and groupBy sorts the column
      //.sortGroup(2,Order.ASCENDING)           // SORT BY sessionId, tst_amp, wp_type
      .sortGroup(1,Order.ASCENDING)
      .sortGroup(0,Order.ASCENDING)
      .reduceGroup((in, out : Collector[(Int)]) => reduceShopCartPython(in, out))
        // return _session_row_counterer
      //.first(10).print()



      //.map(pageCount => (pageCount,1))				                // SELECT SUM(Pages) / Count(*)
      //.reduce((t1,t2) => (t1._ + t2._1), (t1._2 + t2._2))
      //.map(item => (item._1 / item._2))

    //realQuery.print()

    //env.execute("Big Bench Query2 Test")
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

  // Iterator( _type, tst_amp, _sessionId)
  def reduceShopCartPython(in: Iterator[(String, Long, String)], out : Collector[(Int)]) = {

    var userType: String = null
    //var sessionId: String = null
    //var broadcastSet: Traversable[Int] = getRuntimeContext().getBroadcastVariable[Int]("row_count").asScala

    var session_row_counter = 0
    var current_key: String = null
    var last_order_row = -1
    var last_dynamic_row = -1

    // count dynamic and order value in the same session
    in.foreach { userInfo =>
      userType = userInfo._1
      //sessionId = userInfo._3

      session_row_counter += 1

      if (userType.equals("order"))
        last_order_row = session_row_counter

      if (userType.equals("dynamic"))
        last_dynamic_row = session_row_counter
    }


    if (last_dynamic_row > last_order_row)
      out.collect(session_row_counter)
    // new Tuple2
  }



  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************
  // BIGINT: e.g  _date|_click|_sales|_item|_web_page|_user
  case class WebClick(_click_date: Long, _click_time: Long, _sales_sk: Long, _item_sk: Long,_page_sk: Long, _user_sk: Long)
  case class WebPage(_page_sk: Long, _type: String)
  // UDC
  case class ClickWebPageType(_user_sk: Long, _type: String, _sum_date: Long)
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
      System.err.println("Usage: Big Bench <web_clickstream-csv path> <web_page-csv path> <result path>")
      false
    }
  }

  // e.g. 36890|26789|0|3725|20|85457
  // e.g  _date|_click|_sales|_item|_web_page|_user
  private def getWebClickDataSet(env: ExecutionEnvironment): DataSet[WebClick]= {
    env.readCsvFile[WebClick](
      webClickPath,
      includedFields = Array(0, 1, 2, 3, 4, 5),
      fieldDelimiter = "|",
      lenient = true
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
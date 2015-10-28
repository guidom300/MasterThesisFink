package de.tub.cs.bigbench

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.table.expressions.Avg

/*
* web_sales, household_demographics, time_dim, web_page
*
set q14_dependents=5;
set q14_morning_startHour=7;
set q14_morning_endHour=8;

set q14_evening_startHour=19;
set q14_evening_endHour=20;

set q14_content_len_min=5000;
set q14_content_len_max=6000;
*
 */

object Query14{

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }
    // TEST 001
    // set up execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    /*
    *SELECT COUNT(*) amc
    FROM web_sales ws
    JOIN household_demographics hd ON hd.hd_demo_sk = ws.ws_ship_hdemo_sk
    1)
    JOIN time_dim td ON td.t_time_sk = ws.ws_sold_time_sk
    2)
    JOIN web_page wp ON wp.wp_web_page_sk = ws.ws_web_page_sk
    3)
     */

    val webSales = getWebSalesDataSet()
    val houseHold = getHouseHoldDataSet()
    val timeDim = getTimeDimDataSet()
    val webPage = getWebPageDataSet()

    /* 3)
    AND wp.wp_char_count >= ${hiveconf:q14_content_len_min}
    AND wp.wp_char_count <= ${hiveconf:q14_content_len_max}
     */
    val contentsWebPage = webPage.fitler()
    /* 2)
    AND td.t_hour >= ${hiveconf:q14_morning_startHour}
    AND td.t_hour <= ${hiveconf:q14_morning_endHour}
    */
    val morningTimeDim = timeDim.filter()
    val eveningTimeDim = timeDim.filter()
    /* 1)
    AND hd.hd_dep_count = ${hiveconf:q14_dependents}
     */
    val dependtsHouseHold  =houseHold.filter()



    val webSalesJoin1 =

    val webSalesJoin2 =




  }






  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class WebSales()
  case class HouseHold()
  case class TimeDim()
  case class WebPage()


  // *************************************************************************
  //     UTIL METHODS
  // *************************************************************************

  private var webSalePath: String = null
  private var houseHoldPath: String = null
  private var timeDimPath: String = null
  private var webPagePath: String = null
  private var outputPath: String = null

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 5) {
      webSalePath = args(0)
      houseHoldPath = args(1)
      timeDimPath = args(2)
      webPagePath = args(3)
      outputPath = args(4)
      true
    } else {
      System.err.println("Usage: Big Bench 6 Arguements")
      false
    }
  }

  // TABLE:  web_sales, household_demographics, time_dim, web_page

  private def getWebSalesDataSet(env: ExecutionEnvironment): DataSet[WebSales] = {
    env.readCsvFile[WebSales](
      webSalePath,
      fieldDelimiter = "|",
      includedFields = Array(0, 5, 12),
      lenient = true
    )
  }


  private def getHouseHoldDataSet(env: ExecutionEnvironment): DataSet[HouseHold] = {
    env.readCsvFile[HouseHold](
      houseHoldPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 1, 3, 5),
      lenient = true)
  }

  private def getTimeDimDataSet(env: ExecutionEnvironment): DataSet[TimeDim] = {
    env.readCsvFile[TimeDim](
      timeDimPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 6, 8),
      lenient = true
    )
  }


  private def getWebPageDataSet(env: ExecutionEnvironment): DataSet[WebPage] = {
    env.readCsvFile[WebPage](
      webPagePath,
      fieldDelimiter = "|",
      includedFields = Array(0, 2, 3),
      lenient = true
    )
  }


}
class Query14 {

}

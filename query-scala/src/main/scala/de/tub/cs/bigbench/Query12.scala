package de.tub.cs.bigbench

import java.text.SimpleDateFormat

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.table.expressions.Avg

/*
 * TODO
 * Analyze each required column based on 4 tables
 * set q12_startDate=2001-09-02;
set q12_endDate1=2001-10-02;
set q12_endDate2=2001-12-02;
set q12_i_category_IN='Books', 'Electronics';
 *
 */

object Query12{

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    // set filter date
    val dateFormat = new SimpleDateFormat("yyy-MM-dd")
    val startDate = dateFormat.parse("2001-09-02")
    val endDate1 = dateFormat.parse("2001-10-02")
    val endDate2 = dateFormat.parse("2001-12-02")

    /*
    USAGE
    .filter( l => dateFormat.prase(l.shipDate).after(startDate)
    .filter( l => dateFormat.prase(l.shipDate).before(startDate)

     */


    // set up execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    val filteredItemTable = getItemDataSet(env)
    /*
    AND i_category IN (${hiveconf:q12_i_category_IN}) -- filter given category
     */

    val filteredwebClickTable = getWebClickDataSet(env)
    /*
    WHERE wcs_click_date_sk BETWEEN 37134 AND (37134 + 30) -- in a given month and year

    AND wcs_user_sk IS NOT NULL
    AND wcs_sales_sk IS NULL --only views, not purchases
     */
    val storeTable = getStoreSalesDataSet(env)
    /*
    WHERE ss_sold_date_sk BETWEEN 37134 AND (37134 + 90) -- in the three consecutive months.

    AND ss_customer_sk IS NOT NULL
    */

    val webInRange =
    /*
    **AND wcs_item_sk = i_item_sk
    Join webClick and Item where item == item
     */

    val storeInRange =
    /*
    **AND ss_item_sk = i_item_sk
    Join Sales and Item where item == item
     */

    val realQuery =
    /*
    Join webInRange and storeInRange where user == customer && click_date < ss_sold_date
    .sortPartition("user").setParallism(1)
    .flatMap(_.distinct)

     */

  }






  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  // _item_sk(0), _current_price(5), _category(12)
  case class Item(_item_sk: Long, _current_price: Double, _category: String)
  case class User()
  case class WebClick(click_date: Long, click_time: Long, user: Int, item: Int)


  //_customer_sk(3), _item_sk(2),_sold_date_sk(0)
  case class Store_sales(_sold_date_sk: Long, _item_sk: Long, _customer_sk: Long)


  // *************************************************************************
  //     UTIL METHODS
  // *************************************************************************

  private var itemPath: String = null
  private var webClickPath: String = null

  private var storeSalesPath: String = null
  private var outputPath: String = null

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 4) {
      itemPath = args(0)
      webClickPath = args(1)
      storeSalesPath = args(2)
      outputPath = args(3)
      true
    } else {
      System.err.println("Usage: Big Bench 6 Arguements")
      false
    }
  }

  private def getItemDataSet(env: ExecutionEnvironment): DataSet[Item] = {
    env.readCsvFile[Item](
      itemPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 5, 12),
      lenient = true
    )
  }


  private def getWebClickDataSet(env: ExecutionEnvironment): DataSet[WebClick] = {
    env.readCsvFile[WebClick](
      webClickPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 1, 3, 5),
      lenient = true)
  }


  private def getStoreSalesDataSet(env: ExecutionEnvironment): DataSet[Store_sales] = {
    env.readCsvFile[Store_sales](
      storeSalesPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 2, 3),
      lenient = true
    )
  }


}
class Query12 {

}

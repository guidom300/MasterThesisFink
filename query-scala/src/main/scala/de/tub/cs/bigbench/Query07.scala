package de.tub.cs.bigbench

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.util.Collector

/**
 * Created by jjoon on 10/25/15.
 */


object Query07{

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    // set up execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment


    // e.g  _date|_click|_sales|_item|_web_page|_user
    //val webClickStream = getWebClickDataSet(env)
    val webPage = getWebPageDataSet(env).as('wp_web_page_sk, 'wp_type)



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


        realQuery.print()*/

    //env.execute("Big Bench Query2 Test")
  }






  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  // _item_sk(0), _current_price(5), _category(12)
  case class Item(_item_sk: Long, _current_price: Double, _category: String)
  // _date_sk(0), _year(6), _moy(8)
  case class Date_dim(_date_sk: Long, _year: Int, _moy: Int)
  // _address_sk(0),_state(8)
  case class Customer_address(_address_sk: Long, _state: String)
  // _customer_sk(0),_current_addr_sk(4),
  case class Customer(_customer_sk: Long, _current_addr_sk: Long)
  //_customer_sk(3), _item_sk(2),_sold_date_sk(0)
  case class Store_sales(_sold_date_sk: Long, _item_sk: Long, _customer_sk: Long)


  // *************************************************************************
  //     UTIL METHODS
  // *************************************************************************

  private var itemPath: String = null
  private var dateDimPath: String = null
  private var customerAddrPath: String = null
  private var customerPath: String = null
  private var storeSalesPath: String = null
  private var outputPath: String = null

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 6) {
      itemPath = args(0)
      dateDimPath = args(1)
      customerAddrPath = args(2)
      customerPath = args(3)
      storeSalesPath = args(4)
      outputPath = args(5)
      true
    } else {
      System.err.println("Usage: Big Bench 6 Arguements")
      false
    }
  }

  private def getDateDimDataSet(env: ExecutionEnvironment): DataSet[Item] = {
    env.readCsvFile[Item](
      itemPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 5, 12),
      lenient = true
    )
  }

  private def getDateDimDataSet(env: ExecutionEnvironment): DataSet[Date_dim] = {
    env.readCsvFile[Date_dim](
      dateDimPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 6, 8),
      lenient = true
    )
  }

  private def getCustomerAddressDataSet(env: ExecutionEnvironment): DataSet[Customer_address] = {
    env.readCsvFile[Customer_address](
      customerAddrPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 8),
      lenient = true
    )
  }

  private def getCustomerDataSet(env: ExecutionEnvironment): DataSet[Customer] = {
    env.readCsvFile[Customer](
      customerPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 4),
      lenient = true
    )
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
class Query07 {

}

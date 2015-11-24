package de.tub.cs.bigbench

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.common.operators.Order
import org.apache.flink.core.fs.FileSystem.WriteMode

/*
Developed By Philip Lee

Configuration
/home/jjoon/bigBench/data-generator/output/date_dim.dat /home/jjoon/bigBench/data-generator/output/store_sales.dat /home/jjoon/bigBench/data-generator/output/web_sales.dat /home/jjoon/bigBench/data-generator/output/store.dat /home/jjoon/bigBench/data-generator/output/store_returns.dat /home/jjoon/bigBench/data-generator/output/item.dat /home/jjoon/bigBench/results
*/

object Query21 {

  // arg_configuration
  val YEAR = 2003
  val MONTH = 1
  val LIMIT = 100

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    val env = ExecutionEnvironment.getExecutionEnvironment

    val item = getItemDataSet(env)
    val store = getStoreDataSet(env)
    val dateDim =getDateDimDataSet(env)

    val dateDim1 = dateDim.filter(items => items._year.equals(YEAR) && items._moy.equals(MONTH))
    val dateDim2 = dateDim.filter(items => items._year.equals(YEAR) && items._moy >= MONTH && items._moy <= (MONTH + 6))
    val dateDim3 = dateDim.filter(items => items._year >= YEAR && items._year <= (YEAR + 2))

//  3)
    val storeSales = getStoreSalesDataSet(env).join(dateDim1).where(_._sold_date_sk).equalTo(_._date_sk).apply((ss,dd)=> ss)
      .join(item).where(_._item_sk).equalTo(_._item_sk)
      .join(store).where(_._1._store_sk).equalTo(_._store_sk)
      .apply((ss,s) => (ss._1._item_sk,ss._1._customer_sk,ss._1._ticket_number,ss._1._quantity,ss._2._item_id,ss._2._item_desc,s._store_id,s._store_name))

//  1)
    val storeReturn = getStoreReturnDataSet(env).join(dateDim2).where(_._returned_date_sk).equalTo(_._date_sk).apply((sr,dd)=>sr)
    val webSales = getWebSalesDataSet(env).join(dateDim3).where(_._sold_date_sk).equalTo(_._date_sk).apply((ws,dd)=>ws)
    val srJoinWs = storeReturn.join(webSales).where(1,2).equalTo(1,2)
      .apply((sr,ws) => (sr._item_sk,sr._customer_sk,sr._ticket_number,sr._return_quantity,ws._quantity))

//  2)
    val ssJoinSr = storeSales.join(srJoinWs).where(0,1,2).equalTo(0,1,2).apply((ss,sr) => (ss._5,ss._6,ss._7,ss._8,ss._4,sr._4,sr._5))
      .groupBy(0,1,2,3)
      .reduce((t1,t2) => (t1._1,t1._2,t1._3,t1._4,(t1._5+t2._5),(t1._5+t2._6),(t1._7+t2._7)))       //  i_item_id, i_item_desc, s_store_id, s_store_name, SUM(ss_quantity) AS store_sales_quantity, SUM(sr_return_quantity) AS store_returns_quantity, SUM(ws_quantity) AS web_sales_quantity
      .sortPartition(0,Order.ASCENDING).setParallelism(1)
//      .sortPartition(1,Order.ASCENDING)           // flink 0.10 fix this bug
//      .sortPartition(2,Order.ASCENDING)
//      .sortPartition(3,Order.ASCENDING)

//    ssJoinSr.print()
    ssJoinSr.writeAsCsv(outputPath,"\n", ",",WriteMode.OVERWRITE)

    env.execute("Big Bench Query21 Test")
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  // _date_sk(0), _year(6), _moy(8)
  case class DateDim(_date_sk: Long, _year: Int, _moy: Int)
  //Long(0) Long(2) Long (3) Long (7) Long(9) Int(10)
  case class StoreSales(_sold_date_sk: Long, _item_sk: Long, _customer_sk: Long, _store_sk: Long, _ticket_number: Long, _quantity: Int)
  // Long(0)  Long(3) Long(4) Long (18)
  case class WebSales(_sold_date_sk: Long, _item_sk: Long, _bill_customer_sk: Long, _quantity: Long)
  // Long(0) / String(1) / String (5)
  case class Store(_store_sk: Long, _store_id: String, _store_name: String)
  // Long(0) / Long (2) / Long(3) / Long(9) / Int 10
  case class StoreReturn(_returned_date_sk: Long, _item_sk: Long, _customer_sk: Long, _ticket_number: Long, _return_quantity: Int)
  // Long (0) String (1) / String (4)
  case class Item(_item_sk: Long, _item_id: String, _item_desc: String)

  // *************************************************************************
  //     UTIL METHODS
  // *************************************************************************

  private var dateDimPath: String = null
  private var storeSalesPath: String = null
  private var webSalesPath: String = null
  private var storePath: String = null
  private var storeReturnPath: String = null
  private var itemPath: String = null
  private var outputPath: String = null

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 7) {
      dateDimPath = args(0)
      storeSalesPath = args(1)
      webSalesPath = args(2)
      storePath = args(3)
      storeReturnPath = args(4)
      itemPath = args(5)
      outputPath = args(6)
      true
    } else {
      System.err.println("Usage: Big Bench 7 Arguements")
      false
    }
  }

  private def getDateDimDataSet(env: ExecutionEnvironment): DataSet[DateDim] = {
    env.readCsvFile[DateDim](
      dateDimPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 6, 8),
      lenient = true
    )
  }
  private def getStoreSalesDataSet(env: ExecutionEnvironment): DataSet[StoreSales] = {
    env.readCsvFile[StoreSales](
      storeSalesPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 2, 3, 7, 9, 10),
      lenient = true
    )
  }
  // TABLE:  web_sales, household_demographics, time_dim, web_page
  private def getWebSalesDataSet(env: ExecutionEnvironment): DataSet[WebSales] = {
    env.readCsvFile[WebSales](
      webSalesPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 3, 4, 18),
      lenient = true
    )
  }
  private def getStoreDataSet(env: ExecutionEnvironment): DataSet[Store] = {
    env.readCsvFile[Store](
      storePath,
      fieldDelimiter = "|",
      includedFields = Array(0, 1, 5),
      lenient = true
    )
  }
  private def getStoreReturnDataSet(env: ExecutionEnvironment): DataSet[StoreReturn] = {
    env.readCsvFile[StoreReturn](
      storeReturnPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 2, 3, 9, 10),
      lenient = true
    )
  }
  private def getItemDataSet(env: ExecutionEnvironment): DataSet[Item] = {
    env.readCsvFile[Item](
      itemPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 1, 4),
      lenient = true
    )
  }
}
class Query21 {

}

//1)
//    AND ws_sold_date_sk     = d3.d_date_sk
//    AND sr_returned_date_sk = d2.d_date_sk
//    AND sr_customer_sk      = ws_bill_customer_sk
//    AND sr_item_sk          = ws_item_sk
//2)
//    AND ss_item_sk          = sr_item_sk
//    AND ss_customer_sk      = sr_customer_sk
//    AND ss_ticket_number    = sr_ticket_number
//3)
//    AND i_item_sk           = ss_item_sk
//    AND s_store_sk          = ss_store_sk

//      .reduceGroup(new GroupReduceFunction[(String, String, String, String, Int, Int, Long),(String, String, String, String, Int, Int, Long)] {
//        override def reduce(values: java.lang.Iterable[(String, String, String, String, Int, Int, Long)], out: Collector[(String, String, String, String, Int, Int, Long)]): Unit = {
//          var item_id: String = ""
//          var item_desc: String = ""
//          var store_id: String = ""
//          var store_name: String = ""
//          var sr_sum_quantity: Int = 0
//          var sr_sum_return_quantity: Int = 0
//          var ws_quantity: Long = 0
//
//          values.asScala.foreach{ items =>
//            item_id = items._1
//            item_desc = items._2
//            store_id = items._3
//            store_name = items._4
//            sr_sum_quantity += items._5
//            sr_sum_return_quantity += items._6
//            ws_quantity += items._7
//          }
//          out.collect((item_id,item_desc,store_id,store_name,sr_sum_quantity,sr_sum_return_quantity,ws_quantity))
//        }
//      })

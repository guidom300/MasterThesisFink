package de.tub.cs.bigbench

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

/*
Developed By Philip Lee

Configuration
"/home/jjoon/bigBench/data-generator/output/item.dat" "/home/jjoon/bigBench/data-generator/output/date_dim.dat" "/home/jjoon/bigBench/data-generator/output/customer_address.dat" "/home/jjoon/bigBench/data-generator/output/customer.dat" "/home/jjoon/bigBench/data-generator/output/store_sales.dat" "/home/jjoon/bigBench/"
 */

object Query07{

  // arg_configuration
  val YEAR = 2004
  val MONTH = 7
  val HIGHER_PRICE_RATIO=1.2;
  val HAVING_COUNT_GE=10;
  val LIMIT=10;


  def main(args: Array[String]) {

    if (!parseParameters(args)) {
      return
    }

    val env = ExecutionEnvironment.getExecutionEnvironment

    val avgCategoryPrice = getItemJDataSet(env)
      .groupBy(_._category)
      .reduceGroup((in, out : Collector[(String,Double)]) => reduceAvg(in, out))

    val highPriceItems = getItemKDataSet(env).join(avgCategoryPrice).where(_._category).equalTo(_._1)
      .apply((k,j) =>(k._item_sk,k._current_price,j._2 * HIGHER_PRICE_RATIO))
      .filter(items => items._2 >= items._3)

    val filterDateDim = getDateDimDataSet(env)
      .filter(items => items._year.equals(YEAR) && items._moy.equals(MONTH))
      .map(items => items._date_sk).collect()

    val filterCustomerAddress = getCustomerAddressDataSet(env)
      .filter(items => !items._state.isEmpty)

    val joinedCustomer = getCustomerDataSet(env).join(filterCustomerAddress).where(_._current_addr_sk).equalTo(_._address_sk)
      .apply((ct,ca) => (ct._customer_sk,ca._state))

    val filterStoreSales = getStoreSalesDataSet(env)
      .filter(items => filterDateDim.contains(items._sold_date_sk))
      .join(highPriceItems).where(_._item_sk).equalTo(_._1).apply((ss,hp)=> (ss._customer_sk,ss._sold_date_sk))
      .join(joinedCustomer).where(_._1).equalTo(_._1).apply((ss,ct) => ct._2)

    val realQuery = filterStoreSales
        .map(items => (items,1))
        .groupBy(0)
        .sum(1)
        .filter(items => items._2 >= HAVING_COUNT_GE)
        .sortPartition(1,Order.DESCENDING).setParallelism(1)
        .first(LIMIT)

    //realQuery.print()
    realQuery.writeAsCsv(outputPath,"\n", ",",WriteMode.OVERWRITE)

    env.execute("Big Bench Query7 Test")
  }

  def reduceAvg(in: Iterator[ItemJ], out : Collector[(String,Double)]) = {
    var cnt = 0
    var sum: Double = 0
    var category: String = null

    in.foreach { userInfo =>
      category = userInfo._category
      sum += userInfo._current_price
      cnt += 1
    }
    out.collect(category,(sum/cnt))
  }


  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************
  // Double: Decimal(7,2), Long: BIGINT, Int: int
  // _item_sk(0), _current_price(5), _category(12)
  case class ItemK(_item_sk: Long, _current_price: Double, _category: String)
  case class ItemJ(_current_price: Double, _category: String)
  // _date_sk(0), _year(6), _moy(8)
  case class DateDim(_date_sk: Long, _year: Int, _moy: Int)
  // _address_sk(0),_state(8)
  case class CustomerAddress(_address_sk: Long, _state: String)
  // _customer_sk(0),_current_addr_sk(4),
  case class Customer(_customer_sk: Long, _current_addr_sk: Long)
  //_customer_sk(3), _item_sk(2),_sold_date_sk(0)
  case class StoreSales(_sold_date_sk: Long, _item_sk: Long, _customer_sk: Long)


  // *************************************************************************
  //     UTIL METHODS
  // *************************************************************************
  private var itemPath: String  = null
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

  private def getItemKDataSet(env: ExecutionEnvironment): DataSet[ItemK] = {
    env.readCsvFile[ItemK](
      itemPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 5, 12),
      lenient = true
    )
  }

  private def getItemJDataSet(env: ExecutionEnvironment): DataSet[ItemJ] = {
    env.readCsvFile[ItemJ](
      itemPath,
      fieldDelimiter = "|",
      includedFields = Array(5, 12),
      lenient = true
    )
  }

  private def getDateDimDataSet(env: ExecutionEnvironment): DataSet[DateDim] = {
    env.readCsvFile[DateDim](
      dateDimPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 6, 8),
      lenient = true
    )
  }

  private def getCustomerAddressDataSet(env: ExecutionEnvironment): DataSet[CustomerAddress] = {
    env.readCsvFile[CustomerAddress](
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

  private def getStoreSalesDataSet(env: ExecutionEnvironment): DataSet[StoreSales] = {
    env.readCsvFile[StoreSales](
      storeSalesPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 2, 3),
      lenient = true
    )
  }


}
class Query07 {

}

/* .
Conditions
WHERE a.ca_address_sk = c.c_current_addr_sk 1)
AND c.c_customer_sk = s.ss_customer_sk      2)
AND ca_state IS NOT NULL
AND ss_item_sk = highPriceItems.i_item_sk   3)
AND s.ss_sold_date_sk IN                    4)
 */

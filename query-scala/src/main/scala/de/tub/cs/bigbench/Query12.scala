package de.tub.cs.bigbench

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._


/*
 * Edit Configuration
 * "/home/jjoon/bigBench/data-generator/output/item.dat" "/home/jjoon/bigBench/data-generator/output/web_clickstreams.dat" "/home/jjoon/bigBench/data-generator/output/store_sales.dat" "/home/jjoon/bigBench/"
 * TODO
 * Analyze each required column based on 4 tables
 * Handling null-value with the column of web_sales
 */

object Query12{

  // arg_configuration
  val startDate = "2001-09-02"
  val endData1 = "2001-10-02"
  val endData2 = "2001-12-02"
  val i_category_IN1 = "Books"
  val i_category_IN2 = "Electronics"

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    val env = ExecutionEnvironment.getExecutionEnvironment

    val filteredItemTable = getItemDataSet(env).filter(items => items._category.equals(i_category_IN1) || items._category.equals(i_category_IN2))
    val filteredWebClickTable = getWebClickDataSet(env).filter(items => items._click_date >= 37134 && items._click_date <= 37164)// && items._user_sk != null && items._sales_sk == null)
    val storeTable = getStoreSalesDataSet(env).filter(items => items._sold_date_sk >= 37134 && items._sold_date_sk <= 37224 && items._customer_sk != null)

    val webInRange = filteredWebClickTable.join(filteredItemTable).where(_._item_sk).equalTo(_._item_sk).apply((wc,i) => (wc))
    val storeInRange = storeTable.join(filteredItemTable).where(_._item_sk).equalTo(_._item_sk).apply((st,i) => (st))

    val realQuery = webInRange.join(storeInRange).where(_._user_sk).equalTo(_._customer_sk).apply((wc,st) => (wc._user_sk,wc._click_date,st._sold_date_sk))
      .filter(items => items._2 < items._3)
      .map(items => Tuple1(items._1))
      .sortPartition(0,Order.ASCENDING)         // OREDER BY _user_sk
      .setParallelism(1)
      .distinct(0)                              // SELECT DISTINCT _user_sk

    .count()            // must be 403
    println(realQuery)
    //realQuery.print()

    //env.execute("Scala Query 14 Example")
  }






  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  // _item_sk(0), _current_price(5), _category(12)
  case class Item(_item_sk: Long, _current_price: Double, _category: String)

  // _click_date_sk(0), _sales_sk(2), _item_sk(3), _user_sk(5),
  case class WebClick(_click_date: Long, _sales_sk:Long, _item_sk: Long, _user_sk: Long)

  //_customer_sk(3), _item_sk(2),_sold_date_sk(0)
  case class StoreSales(_sold_date_sk: Long, _item_sk: Long, _customer_sk: Long)


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

  // null value on _sales_sk
  private def getWebClickDataSet(env: ExecutionEnvironment): DataSet[WebClick] = {
    env.readCsvFile[WebClick](
      webClickPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 2, 3, 5),
      lenient = true)
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
class Query12 {

}

/*
1)
AND i_category IN (${hiveconf:q12_i_category_IN}) -- filter given category
2)
WHERE wcs_click_date_sk BETWEEN 37134 AND (37134 + 30) -- in a given month and year
AND wcs_user_sk IS NOT NULL
3)
AND wcs_sales_sk IS NULL --only views, not purchases
WHERE ss_sold_date_sk BETWEEN 37134 AND (37134 + 90) -- in the three consecutive months.
AND ss_customer_sk IS NOT NULL

set q12_startDate=2001-09-02;
set q12_endDate1=2001-10-02;
set q12_endDate2=2001-12-02;
set q12_i_category_IN='Books', 'Electronics';

val dateFormat = new SimpleDateFormat("yyy-MM-dd")
val startDate = dateFormat.parse(startDate)
val endDate1 = dateFormat.parse(endData1)
val endDate2 = dateFormat.parse(endData2 )

USAGE
.filter( l => dateFormat.prase(l.shipDate).after(startDate)
.filter( l => dateFormat.prase(l.shipDate).before(startDate)

*/
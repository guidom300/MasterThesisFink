package de.tub.cs.bigbench

import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.api.scala._
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._

/*
Developed By Philip Lee

Configuration
"/home/jjoon/bigBench/data-generator/output/item.dat" "/home/jjoon/bigBench/data-generator/output/store_sales.dat" "/home/jjoon/bigBench/results/"
*/

object Query22{

  // arg_configuration
  val DATE= "2001-05-08"
  val price_min1=0.98
  val price_max1=1.5

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    val env = ExecutionEnvironment.getExecutionEnvironment

    //val filterItem = getItemDataSet(env)
    //val filterDate = getDateDimDataSet(env).filter( d_date >= -30 && <= 30)
    //val filterInventory = getInventoryDataSet(env).filter(min < current_price < max)
    //  .join(filterDate).where(date).equals(date).apply()

    // val inventoryJoinWithOthers = filterInventory.join(item).where(item).equals(item).apply()
    //  .join(warehouse).where(_warehouse).equals(_warehouse).apply( warehouse_name, item_id, date,date)
    //  .groupBy(warehouse_name, item_id)
    //  .sum(3,4)
    //  .filter( _3 <= 0 && _4 >= 0)
    //  .filter( inv_before > 0)
    //  .filter( inv_after/inv_before >=  && inv_after/ inv_before <= 3.0 / 2.0)
    //  .sortPartition(warehouse.Order.ASECNDING).setParallism(10)
    //  .first(100)
    //  .print()


    env.execute("Big Bench Query14 Test")
  }


  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************
  // Long(2) Long (3)
  case class StoreSales(_item_sk: Long, _customer_sk: Long)
  // Long (0) Int (9) / String (12)
  case class Item(_item_sk: Long, _class_id: Int, _category: String )

  // *************************************************************************
  //     UTIL METHODS
  // *************************************************************************

  private var storeSalesPath: String = null
  private var itemPath: String = null
  private var outputPath: String = null

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 3) {
      itemPath = args(0)
      storeSalesPath = args(1)
      outputPath = args(2)
      true
    } else {
      System.err.println("Usage: Big Bench 3 Arguements")
      false
    }
  }

  private def getStoreSalesDataSet(env: ExecutionEnvironment): DataSet[StoreSales] = {
    env.readCsvFile[StoreSales](
      storeSalesPath,
      fieldDelimiter = "|",
      includedFields = Array(2, 3),
      lenient = true
    )
  }

  private def getItemDataSet(env: ExecutionEnvironment): DataSet[Item] = {
    env.readCsvFile[Item](
      itemPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 9, 12),
      lenient = true
    )
  }
}

class Query22 {

}

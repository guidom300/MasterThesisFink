package de.tub.cs.bigbench

package de.tub.cs.bigbench

import org.apache.flink.api.common.functions.{GroupReduceFunction, RichReduceFunction}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._

/*
Developed By Philip Lee

Configuration
set q26_i_category_IN='Books';
set q26_count_ss_item_sk=5;
*/

object Query26{

  // arg_configuration
  val i_category_IN = "Books"
  val count_ss_item_sk = 5


  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    val env = ExecutionEnvironment.getExecutionEnvironment

    val item = getItemDataSet(env).filter(items => items._category.equals(i_category_IN))

    val storeSales = getStoreSalesDataSet(env).join(item).where(_._item_sk).equalTo(_._item_sk).apply((ss,i)=>(ss._customer_sk,i._class_id))
      .groupBy(0)       // groupBy ss._customer_sk
//    HAVING count(ss.ss_item_sk) > ${hiveconf:q26_count_ss_item_sk}

//    webSalesEvening.writeAsText(outputPath + "/result-14.dat",WriteMode.OVERWRITE)

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
      storeSalesPath = args(0)
      itemPath = args(1)
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
class Query26 {

}

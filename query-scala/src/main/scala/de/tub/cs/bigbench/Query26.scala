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
"/home/jjoon/bigBench/data-generator/output/item.dat" "/home/jjoon/bigBench/data-generator/output/store_sales.dat" "/home/jjoon/bigBench/results/"

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

    val storeSales = getStoreSalesDataSet(env).join(item).where(_._item_sk).equalTo(_._item_sk).apply((ss,i)=>(ss._customer_sk,i._class_id,ss._item_sk))
      .groupBy(0)       // groupBy ss._customer_sk
      .reduceGroup(new ParserItems)
      .filter(items => items._3 > count_ss_item_sk)
      .map(items => (items._1,items._2))
      .sortPartition(0,Order.ASCENDING)
      .setParallelism(1)

//    storeSales.print()
    storeSales.writeAsCsv(outputPath + "/result-26.dat","\n", ",",WriteMode.OVERWRITE)
    // run K-means algorithms on Mahout for clustering

    env.execute("Big Bench Query14 Test")
  }

  //(ss._customer_sk,i._class_id,ss._item_sk)
  class ParserItems extends GroupReduceFunction[(Long,Int,Long),(Long,String,Int)]{
    override def reduce(in: java.lang.Iterable[(Long,Int,Long)], out: Collector[(Long,String,Int)]) = {

      val index = new Array[Double](15)
      var cnt_item: Int = 0
      var customer: Long = 0

      in.asScala.foreach{ items =>

        customer = items._1
        items._2 match {
// Master Version
//          case 1 => index(0) += 1
//          case 3 => index(1) += 1
//          case 5 => index(2) += 1
//          case 7 => index(3) += 1
//          case 9 => index(4) += 1
//          case 11 =>  index(5) += 1
//          case 13 =>  index(6) += 1
//          case 15 =>  index(7) += 1
//          case 2 => index(8) += 1
//          case 4 => index(9) += 1
//          case 6 => index(10) += 1
//          case 8 => index(11) += 1
//          case 10 =>  index(12) += 1
//          case 14 =>  index(13)+= 1
//          case 16 =>  index(14) += 1

// Next-version
            case 1 => index(0) += 1
            case 2 => index(1) += 1
            case 3 => index(2) += 1
            case 4 => index(3) += 1
            case 5 => index(4) += 1
            case 6 =>  index(5) += 1
            case 7 =>  index(6) += 1
            case 8 =>  index(7) += 1
            case 9 => index(8) += 1
            case 10 => index(9) += 1
            case 11 => index(10) += 1
            case 12 => index(11) += 1
            case 13 =>  index(12) += 1
            case 14 =>  index(13)+= 1
            case 15 =>  index(14) += 1

        }
        cnt_item += 1
      }
      out.collect((customer,index.mkString(","),cnt_item))
    }
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
class Query26 {

}

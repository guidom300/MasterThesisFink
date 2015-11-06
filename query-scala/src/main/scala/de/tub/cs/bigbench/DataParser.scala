package de.tub.cs.bigbench

import org.apache.flink.api.common.functions.{RichFlatMapFunction, FlatMapFunction}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import scala.collection.mutable.ArrayBuffer

object DataParser{

  def main(args: Array[String]) {

    // set up execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    val webClickStreamsNull = getWebClickDataSetNull(env)
    webClickStreamsNull.flatMap(new NullTokenizer)
      .first(10).print()
  }

  /*
   * Name: Philip
   * Explain: Handle null columns to the number of tuple
   *
   * Usage:
   * val webClick = env.readTextFile(webClickPath)
   * webClick.flatMap(new NullTokenizer).print()
   */
  class NullTokenizer extends FlatMapFunction[String, WebClickNull] {
    override def flatMap(in: String, out: Collector[WebClickNull]) {

      val tuple = ArrayBuffer[String]()
      var cnt: Int = 0

      var tokens = in.split("|")
      for(token <- tokens)
        if(token.equals("|"))
          cnt += 1

      tokens = in.split("\\|")
      for(token <- tokens)
        tuple += token

      if(cnt.equals(tuple.length))
        tuple += " "

      out.collect(new WebClickNull(tuple(0),tuple(1),tuple(2),tuple(3),tuple(4),tuple(5)))
    }
  }



//  case class WebClick(_click_date: Long, _click_time: Long, _sales_sk: Long, _item_sk: Long,_page_sk: Long, _user_sk: Long)
  case class WebClickNull(_click_date: String, _click_time: String, _sales_sk: String, _item_sk: String, _page_sk: String, _user_sk: String)

  private val webClickPath: String = "/home/jjoon/bigBench/data-generator/output/web_clickstreams.dat"

  private def getWebClickDataSetNull(env: ExecutionEnvironment): DataSet[String] = {
    env.readTextFile(webClickPath)
    // use NullTokenizer function to make tuple
  }

//  private def getWebClickDataSet(env: ExecutionEnvironment): DataSet[WebClick] = {
//    env.readCsvFile[WebClick](
//      webClickPath,
//      fieldDelimiter = "|",
//      includedFields = Array(0, 1, 2, 3, 4, 5),
//      lenient = true)
//  }

}
class DataParser {

}

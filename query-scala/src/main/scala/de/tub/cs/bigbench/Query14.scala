package de.tub.cs.bigbench

import org.apache.flink.api.common.functions.{GroupReduceFunction, RichReduceFunction}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.util.Collector


/*
Developed By Philip Lee

Configuration
"/home/jjoon/bigBench/data-generator/output/web_sales.dat" "/home/jjoon/bigBench/data-generator/output/household_demographics.dat" "/home/jjoon/bigBench/data-generator/output/time_dim.dat" "/home/jjoon/bigBench/data-generator/output/web_page.dat" "/home/jjoon/bigBench/"
* TODO
* how to pass argument in groupReduce
*/

object Query14{

  // arg_configuration
  val dependents = 5
  val morning_startHour = 7
  val morning_endHour = 8
  val evening_startHour = 19
  val evening_endHour = 20
  val content_len_min = 5000
  val content_len_max = 6000

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    val env = ExecutionEnvironment.getExecutionEnvironment

    val webSales = getWebSalesDataSet(env)

    val contentsWebPage = getWebPageDataSet(env).filter(items => (items._char_count >= content_len_min && items._char_count <= content_len_max))
    val morningTimeDim = getTimeDimDataSet(env).filter(items => (items._t_hour >= morning_startHour && items._t_hour <= morning_endHour))
    val eveningTimeDim = getTimeDimDataSet(env).filter(items => (items._t_hour >= evening_startHour && items._t_hour <= evening_endHour))
    val dependentsHouseHold  =getHouseHoldDataSet(env).filter(items => items._dep_count == dependents)

    val webSalesMorning = webSales.join(dependentsHouseHold).where(_._ship_hdemo_sk).equalTo(_._demo_sk).apply((ws,hh) => ws)
      .join(morningTimeDim).where(_._sold_time_sk).equalTo(_._time_sk).apply((ws,mt) => ws)
      .join(contentsWebPage).where(_._web_page_sk).equalTo(_._web_page_sk).apply((ws,wp) => ws)
      .count().toDouble

    val webSalesEvening = webSales.join(dependentsHouseHold).where(_._ship_hdemo_sk).equalTo(_._demo_sk).apply((ws,hh) => ws)
      .join(eveningTimeDim).where(_._sold_time_sk).equalTo(_._time_sk).apply((ws,mt) => ws)
      .join(contentsWebPage).where(_._web_page_sk).equalTo(_._web_page_sk).apply((ws,wp) => ws)
      .reduceGroup(new GroupReduceFunction[WebSales, Double] {
        override def reduce(values: _root_.java.lang.Iterable[WebSales], out: Collector[Double]): Unit = {
          var cnt: Double = 0
          val itr = values.iterator()
          while(itr.hasNext()){
            itr.next()
            cnt += 1
          }
          out.collect(((webSalesMorning/cnt)*10000).round/10000.toDouble)
        }
      })

    webSalesEvening.writeAsText(outputPath + "/result-14.dat",WriteMode.OVERWRITE)

    env.execute("Big Bench Query14 Test")
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  //BIGINT: _ship_hdemo_sk(10), _web_page_sk(12), _sold_time_sk(1)
  case class WebSales(_sold_time_sk: Long, _ship_hdemo_sk: Long,_web_page_sk: Long)
  //_demo_sk(0): BIGINT, _dep_count(3): int
  case class HouseHold(_demo_sk: Long, _dep_count: Int)
  //_time_sk(0): BIGINT, _t_hour(3): int
  case class TimeDim(_time_sk: Long, _t_hour: Int)
  //_web_page_sk(0): BIGINT, _char_count(10): int
  case class WebPage(_web_page_sk: Long, _char_count: Int)


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
      includedFields = Array(1, 10, 12),
      lenient = true
    )
  }
  private def getHouseHoldDataSet(env: ExecutionEnvironment): DataSet[HouseHold] = {
    env.readCsvFile[HouseHold](
      houseHoldPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 3),
      lenient = true)
  }

  private def getTimeDimDataSet(env: ExecutionEnvironment): DataSet[TimeDim] = {
    env.readCsvFile[TimeDim](
      timeDimPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 3),
      lenient = true
    )
  }

  private def getWebPageDataSet(env: ExecutionEnvironment): DataSet[WebPage] = {
    env.readCsvFile[WebPage](
      webPagePath,
      fieldDelimiter = "|",
      includedFields = Array(0, 10),
      lenient = true
    )
  }


}
class Query14 {

}

// how to pass argument in groupReduce
//     .reduceGroup(new reduceAvg(webSalesMorning))
//  @Combinable
//  class reduceAvg extends GroupReduceFunction[WebSales,Double] {
//    override def reduce(in: java.lang.Iterable[WebSales], out: Collector[Double]) = {
//      var cnt = 0
//      val itr = in.iterator()
//      while(itr.hasNext){
//        itr.next()
//        cnt += 1
//      }
//      // scala
////      val itr = in.iterator
////      itr.foreach{ items =>
////      cnt += 1
////      }
//      out.collect(value / cnt)
//    }
//  }
/* Table API
val webSales = getWebSalesDataSet(env).as('_sold_time_sk, '_ship_hdemo_sk, '_web_page_sk).toDataSet[WebSales]
val houseHold = getHouseHoldDataSet(env).as('_demo_sk, '_dep_count)
val timeDim = getTimeDimDataSet(env).as('_time_sk, '_t_hour)
val webPage = getWebPageDataSet(env).as('_web_page_sk, '_char_count)

val morningTimeDim = timeDim.where('_t_hour >= morning_startHour && '_t_housr <= morning_endHour).toDataSet[TimeDim]
val eveningTimeDim = timeDim.where('_t_hour >= evening_startHour && '_t_housr <= evening_endHour).toDataSet[TimeDim]
val dependtsHouseHold  =houseHold.where('_dep_count === dependents).toDataSet[HouseHold]
*/

/*
Query 14
AND wp.wp_char_count >= ${hiveconf:q14_content_len_min}
AND wp.wp_char_count <= ${hiveconf:q14_content_len_max}
AND td.t_hour >= ${hiveconf:q14_morning_startHour}
AND td.t_hour <= ${hiveconf:q14_morning_endHour}
AND hd.hd_dep_count = ${hiveconf:q14_dependents}

SELECT CAST(amc as double) / CAST(pmc as double) am_pm_ratio
FROM ( webSalesMorning ) at
Join ( webSalesEvening ) pt

SELECT COUNT(*) amc
FROM web_sales ws
JOIN household_demographics hd ON hd.hd_demo_sk = ws.ws_ship_hdemo_sk
JOIN time_dim td ON td.t_time_sk = ws.ws_sold_time_sk
JOIN web_page wp ON wp.wp_web_page_sk = ws.ws_web_page_sk
*/
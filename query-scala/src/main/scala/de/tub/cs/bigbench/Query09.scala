package de.tub.cs.bigbench

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem.WriteMode

/*
Developed By Philip Lee

Configuration
"/home/jjoon/bigBench/data-generator/output/store_sales.dat" "/home/jjoon/bigBench/data-generator/output/date_dim.dat" "/home/jjoon/bigBench/data-generator/output/customer_address.dat" "/home/jjoon/bigBench/data-generator/output/store.dat" "/home/jjoon/bigBench/data-generator/output/customer_demographics.dat" "/home/jjoon/bigBench/results"
*/

object Query09{

  // arg_configuration
  val YEAR=2001

  val ca_country1= "United States"
  val ca_state_IN1= Array("KY", "GA", "NM")
  val net_profit_min1=0
  val net_profit_max1=2000
  val education_status1="4 yr Degree"
  val marital_status1="M"
  val sales_price_min1=100
  val sales_price_max1=150

  val ca_country2="United States"
  val ca_state_IN2= Array("MT", "OR", "IN")
  val net_profit_min2=150
  val net_profit_max2=3000
  val education_status2="4 yr Degree"
  val marital_status2="M"
  val sales_price_min2=50
  val sales_price_max2=200

  val ca_country3="United States"
  val ca_state_IN3= Array("WI", "MO", "WV")
  val net_profit_min3=50
  val net_profit_max3=25000
  val education_status3="4 yr Degree"
  val marital_status3="M"
  val sales_price_min3=150
  val sales_price_max3=200


  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    val env = ExecutionEnvironment.getExecutionEnvironment

    val store = getStoreDataSet(env)
    val customerDemo = getCustomerDemoDataSet(env)
    val customerAddr = getCustomerAddrDataSet(env)
    val yearDateDim = getDateDimDataSet(env).filter(items => items._year.equals(YEAR))

    val filterStoreSalesWithOthers = getStoreSalesDataSet(env)
      .join(yearDateDim).where(_._sold_date_sk).equalTo(_._date_sk).apply((ss,dd) => ss)
      .join(store).where(_._store_sk).equalTo(_._store_sk).apply((ss,s) => ss)
      .join(customerAddr).where(_._addr_sk).equalTo(_._address_sk)
      .join(customerDemo).where(_._1._cdemo_sk).equalTo(_._demo_sk).apply((ssCa,cd)=> (ssCa._1,ssCa._2,cd))

    val realQuery = filterStoreSalesWithOthers
      .filter(items =>  ((items._3._material_status.equals(marital_status1) && items._3._education_status.equals(education_status1) && items._1._sales_price >= sales_price_min1 && items._1._sales_price <= sales_price_max1)
        ||  (items._3._material_status.equals(marital_status2) && items._3._education_status.equals(education_status2) && items._1._sales_price >= sales_price_min2 && items._1._sales_price <= sales_price_max2)
        ||  (items._3._material_status.equals(marital_status3) && items._3._education_status.equals(education_status3) && items._1._sales_price >= sales_price_min3 && items._1._sales_price <= sales_price_max3))
        &&  ((items._2._country.equals(ca_country1) && ca_state_IN1.contains(items._2._state) && items._1._net_profit >= net_profit_min1 && items._1._net_profit <= net_profit_max1)
        ||  (items._2._country.equals(ca_country2) && ca_state_IN2.contains(items._2._state) && items._1._net_profit >= net_profit_min2 && items._1._net_profit <= net_profit_max2)
        ||  (items._2._country.equals(ca_country3) && ca_state_IN3.contains(items._2._state) && items._1._net_profit >= net_profit_min3 && items._1._net_profit <= net_profit_max3)))
      .map(items => items._1._quantity)
      .reduce((t1,t2) => t1+t2)

//    realQuery.print()
    realQuery.writeAsText(outputPath, WriteMode.OVERWRITE)

    env.execute("Big Bench Query09 Test")
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************
  //(0,4,6,7,10,13,22)
  case class StoreSales(_sold_date_sk: Long, _cdemo_sk: Long, _addr_sk: Long, _store_sk: Long, _quantity: Int, _sales_price: Double, _net_profit: Double)
  case class DateDim(_date_sk: Long, _year: Int)
  case class Store(_store_sk: Long)
  case class CustomerAddress(_address_sk: Long, _state: String, _country: String)
  case class CustomerDemographics(_demo_sk: Long, _material_status: String, _education_status: String)

  // *************************************************************************
  //     UTIL METHODS
  // *************************************************************************

  private var storeSalesPath: String = null
  private var dateDimPath: String = null  
  private var customerAddrPath: String = null
  private var storePath: String = null
  private var customerDemoPath: String = null
  private var outputPath: String = null

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 6) {
      storeSalesPath = args(0)
      dateDimPath = args(1)
      customerAddrPath = args(2)
      storePath = args(3)
      customerDemoPath = args(4)
      outputPath = args(5)
      true
    } else {
      System.err.println("Usage: Big Bench 6 Arguements")
      false
    }
  }


  private def getStoreSalesDataSet(env: ExecutionEnvironment): DataSet[StoreSales] = {
    env.readCsvFile[StoreSales](
      storeSalesPath,
      fieldDelimiter = "|",
      includedFields = Array(0,4,6,7,10,13,22),
      lenient = true
    )
  }
  private def getDateDimDataSet(env: ExecutionEnvironment): DataSet[DateDim] = {
    env.readCsvFile[DateDim](
      dateDimPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 6),
      lenient = true)
  }

  private def getCustomerAddrDataSet(env: ExecutionEnvironment): DataSet[CustomerAddress] = {
    env.readCsvFile[CustomerAddress](
      customerAddrPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 8, 10),
      lenient = true
    )
  }

  private def getStoreDataSet(env: ExecutionEnvironment): DataSet[Store] = {
    env.readCsvFile[Store](
      storePath,
      fieldDelimiter = "|",
      includedFields = Array(0),
      lenient = true
    )
  }

  private def getCustomerDemoDataSet(env: ExecutionEnvironment): DataSet[CustomerDemographics] = {
    env.readCsvFile[CustomerDemographics](
      customerDemoPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 2, 3),
      lenient = true
    )
  }


}


class Query09 {

}

//ss
//_sales_price(13): Double, _net_profit(22):Double, _addr_sk(6), _store_sk(7),_cdemo_sk(4),_sold_date_sk(0)
//dd
//_year(6): Int, _date_sk(0): Long
//ca
//_state(8): String, _country(10): String, _address_sk(0): Long
//s
//_store_sk: Long(0)
//cd
//_demo_sk:Long(0), _material:String(2), _education:String(3)

//  WHERE ss1.ss_sold_date_sk = dd.d_date_sk
//  AND dd.d_year=${hiveconf:q09_year}
//  AND ss1.ss_addr_sk = ca1.ca_address_sk (*
//  AND s.s_store_sk = ss1.ss_store_sk
//  AND cd.cd_demo_sk = ss1.ss_cdemo_sk (*
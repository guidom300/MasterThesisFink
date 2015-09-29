package de.tub.cs.testDB

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.table.expressions.Literal
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
/**
 * This program implements a modified version of the TPC-H query 3. The
 * example demonstrates how to assign names to fields by extending the Tuple class.
 * The original query can be found at
 * Reference: http://www.tpc.org/tpc_documents_current_versions/pdf/tpch2.17.1.pdf (29p)
 *
 * This program implements the following SQL equivalent:
 *
 * {{{
 * SELECT
 *      l_orderkey,
 *      SUM(l_extendedprice*(1-l_discount)) AS revenue,
 *      o_orderdate,
 *      o_shippriority
 * FROM customer,
 *      orders,
 *      lineitem
 * WHERE
 *      c_mktsegment = '[SEGMENT]'
 *      AND c_custkey = o_custkey
 *      AND l_orderkey = o_orderkey
 *      AND o_orderdate < date '[DATE]'
 *      AND l_shipdate > date '[DATE]'
 * GROUP BY
 *      l_orderkey,
 *      o_orderdate,
 *      o_shippriority;
 * }}}
 *
 * Compared to the original TPC-H query this version does not sort the result by revenue
 * and orderdate.
 *
 * Input files are plain text CSV files using the pipe character ('|') as field separator
 * as generated by the TPC-H data generator which is available at
 * [http://www.tpc.org/tpch/](a href="http://www.tpc.org/tpch/).
 *
 * Usage:
 * {{{
 * TPCHQuery3Expression <lineitem-csv path> <customer-csv path> <orders-csv path> <result path>
 * }}}
 *
 * This example shows how to use:
 *  - Table API expressions
 *
 */


 */
object TPCHQuery3Table {

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    // set filter date
    val dateFormat = new _root_.java.text.SimpleDateFormat("yyyy-MM-dd")
    val date = dateFormat.parse("1995-03-12")

    // get execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    val lineitems = getLineitemDataSet(env)
      .filter( l => dateFormat.parse(l.shipDate).after(date) )
      .as('id, 'extdPrice, 'discount, 'shipDate)

    val customers = getCustomerDataSet(env)
      .as('id, 'mktSegment)
      .filter( 'mktSegment === "AUTOMOBILE" )

    val orders = getOrdersDataSet(env)
      .filter( o => dateFormat.parse(o.orderDate).before(date) )
      .as('orderId, 'custId, 'orderDate, 'shipPrio)

    val items =
      orders.join(customers)
        .where('custId === 'id)
        .select('orderId, 'orderDate, 'shipPrio)
        .join(lineitems)
        .where('orderId === 'id)
        .select(
          'orderId,
          'extdPrice * (Literal(1.0f) - 'discount) as 'revenue,
          'orderDate,
          'shipPrio)

    val result = items
      .groupBy('orderId, 'orderDate, 'shipPrio)
      .select('orderId, 'revenue.sum, 'orderDate, 'shipPrio)

    // emit result
    result.writeAsCsv(outputPath, "\n", "|")

    // execute program
    env.execute("Scala TPCH Query 3 (Expression) Example")
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class Lineitem(id: Long, extdPrice: Double, discount: Double, shipDate: String)
  case class Customer(id: Long, mktSegment: String)
  case class Order(orderId: Long, custId: Long, orderDate: String, shipPrio: Long)

  // *************************************************************************
  //     UTIL METHODS
  // *************************************************************************

  private var lineitemPath: String = null
  private var customerPath: String = null
  private var ordersPath: String = null
  private var outputPath: String = null

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 4) {
      lineitemPath = args(0)
      customerPath = args(1)
      ordersPath = args(2)
      outputPath = args(3)
      true
    } else {
      System.err.println("This program expects data from the TPC-H benchmark as input data.\n" +
        " Due to legal restrictions, we can not ship generated data.\n" +
        " You can find the TPC-H data generator at http://www.tpc.org/tpch/.\n" +
        " Usage: TPCHQuery3 <lineitem-csv path> <customer-csv path>" +
        "<orders-csv path> <result path>");
      false
    }
  }

  private def getLineitemDataSet(env: ExecutionEnvironment): DataSet[Lineitem] = {
    env.readCsvFile[Lineitem](
      lineitemPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 5, 6, 10) )
  }

  private def getCustomerDataSet(env: ExecutionEnvironment): DataSet[Customer] = {
    env.readCsvFile[Customer](
      customerPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 6) )
  }

  private def getOrdersDataSet(env: ExecutionEnvironment): DataSet[Order] = {
    env.readCsvFile[Order](
      ordersPath,
      fieldDelimiter = "|",
      includedFields = Array(0, 1, 4, 7) )
  }

}
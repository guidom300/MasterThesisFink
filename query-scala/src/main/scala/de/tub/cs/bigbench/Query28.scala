package de.tub.cs.bigbench

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem.WriteMode

/*
Developed By Philip Lee

Configuration
"/home/jjoon/bigBench/data-generator/output/product_reviews.dat" "/home/jjoon/bigBench/results/"
*/

object Query28{

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    val env = ExecutionEnvironment.getExecutionEnvironment

    val realQuery1 = getProductReviewsDataSet(env).filter(items => Array(1,2,3).contains(items._review_sk % 5))
      .map { items => (
        items._review_rating,
        items._review_rating match {
          case 1 => "NEG"
          case 2 => "NEG"
          case 3 => "NEU"
          case 4 => "POS"
          case 5 => "POS"
        },
        items._review_content
        )
      }

    val realQuery2 = getProductReviewsDataSet(env).filter(items => Array(0,4).contains(items._review_sk % 5))
      .map { items => (
        items._review_rating,
        items._review_rating match {
          case 1 => "NEG"
          case 2 => "NEG"
          case 3 => "NEU"
          case 4 => "POS"
          case 5 => "POS"
        },
        items._review_content
        )
      }

//    println(realQuery1.count())
//    println(realQuery2.count())

    realQuery1.writeAsCsv(outputPath + "/result28-temp1.dat","\n",",",WriteMode.OVERWRITE)//.setParallelism(1)
    realQuery2.writeAsCsv(outputPath + "/result28-temp2.dat","\n",",",WriteMode.OVERWRITE)//.setParallelism(1)

    env.execute("Big Bench Query28 Test")
  }


  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************
  // Long(0) Int (3) String(7)
  case class ProductReview(_review_sk: Long, _review_rating: Int, _review_content: String)


  // *************************************************************************
  //     UTIL METHODS
  // *************************************************************************

  private var productReview: String = null
  private var outputPath: String = null

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length == 2) {
      productReview = args(0)
      outputPath = args(1)
      true
    } else {
      System.err.println("Usage: Big Bench 2 Arguements")
      false
    }
  }

  private def getProductReviewsDataSet(env: ExecutionEnvironment): DataSet[ProductReview] = {
    env.readCsvFile[ProductReview](
      productReview,
      fieldDelimiter = "|",
      includedFields = Array(0, 3, 7),
      lenient = true
    )
  }

}

class Query28 {

}

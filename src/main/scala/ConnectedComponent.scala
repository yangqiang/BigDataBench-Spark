/*
 * Connected Component workload for BigDataBench
 */
package cn.ac.ict.bigdatabench

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.graphx._
import org.apache.spark.SparkConf

//import DepthClone
// treat edge as CC contains 2 nodes
object ConnectedComponent {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: ConnectedComponent <master> <data_file>" +
        " [<slices>]")
      System.exit(1)
    }

    val host = args(0)
    var splits = 0
    val conf = new SparkConf().setAppName("BigDataBench ConnectedComponent")
    val spark = new SparkContext(conf)
    val filename = args(1)

    if (args.length > 2) splits = args(2).toInt
    val graph = GraphLoader.edgeListFile(spark, filename, false, splits)
    val cc = graph.connectedComponents()

    System.exit(0)
  }

}

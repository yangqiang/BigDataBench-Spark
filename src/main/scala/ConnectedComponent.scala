/*
* Connected Component workload for BigDataBench
*/
package cn.ac.ict.bigdatabench

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable.ListBuffer

import BigDataBenchConf._

//import DepthClone
// treat edge as CC contains 2 nodes
object ConnectedComponent {

  type CC = ListBuffer[String]
  type CCS = ListBuffer[CC]
  // treat Graph as a special CCS of which each CC contains two nodes
  type Graph = ListBuffer[CC]

  def deep_clone_CCS(src: CCS): CCS = {
    val dst = new CCS
    src.foreach { cc =>
      val newcc = new CC
      newcc ++= cc
      dst += newcc
    } // end foreach
    dst
  } // end deep clone

  def deep_clone_graph(src: Array[CC]): Array[CC] = {
    val dst = new CCS
    src.foreach { cc =>
      val newcc = new CC
      newcc ++= cc
      dst += newcc
    } // end foreach
    dst.toArray[CC]
  } // end deep clone

  def find_cc_general(graph: Graph): CCS = {
    val ccs = new CCS()
    val g = graph

    val nodes = ListBuffer[String]()
    var old_nodes_size = 0
    while (!g.isEmpty) {
      nodes.clear()
      nodes ++= g(0)
      do {
        old_nodes_size = nodes.size
        for (edge <- g) {
          // find the connected edges
          if (cc_connects(nodes, edge)) {
            // collected connected nodes
            edge.foreach { node =>
              if (!nodes.contains(node)) nodes += node
            } // end foreach
            // delete the connected edges from original graph
            g -= edge
          } // end if
        } // end for
      } while (nodes.size > old_nodes_size)
      ccs += nodes.clone()
    } // end while
    ccs
  } // end find_cc_general

  def cc_connects(first: CC, second: CC): Boolean = {
    for (e_sec <- second) {
      if (first.contains(e_sec)) {
        //println("connect at: " + e_sec)
        return true
      } // end if
    } // end for
    false
  } // end cc_connects

  def print_ccs(ccs: CCS): Unit = {
    for (cc <- ccs) {
      for (node <- cc) {
        print(node.toString + " ")
      } // end for
      println()
    } // end for
  } // end print_css

  def print_ccs(ccs: Array[CC]): Unit = {
    for (cc <- ccs) {
      for (node <- cc) {
        print(node.toString + " ")
      } // end for
      println()
    } // end for
  } // end print_css

  // function to split graph into given slices
  def split_graph(g: Array[CC], slices: Int): ListBuffer[Graph] = {
    val graphs = new ListBuffer[Graph]
    var start = 0
    var sub_g_size = g.size / slices

    // if not perfectly divided, use a larger partition size
    if (sub_g_size * slices < g.size) sub_g_size += 1

    for (i <- 0 until slices) {
      val new_g = new Graph()
      val start = 0 + i * sub_g_size
      var end = sub_g_size - 1 + i * sub_g_size

      if (end > g.size - 1) end = g.size - 1
      var j = start
      while (j <= end) {
        new_g += g(j)
        j += 1
      } // end while
      graphs += new_g
    } // end for
    graphs
  } // end split_graph

  def main(args: Array[String]) {
    type CC = ListBuffer[String]
    type CCS = ListBuffer[CC]
    // treat Graph as a special CCS of which each CC contains two nodes
    type Graph = ListBuffer[CC]

    if (args.length < 2) {
      System.err.println("Usage: ConnectedComponent <master> <data_file> [<slices>]")
      System.exit(1)
    } // end if

    val host = args(0)
    val spark = new SparkContext(host, "ConnectedComponent",
      SPARK_HOME, List(TARGET_JAR_BIGDATABENCH), null, null)
    val filename = args(1)
    val slices = if (args.length > 2) args(2).toInt else 3
    val lines = spark.textFile(filename, slices)
    println("load data")
    var g = lines.map { line =>
      val nodes = line.split("\\s+")
      ListBuffer(nodes(0), nodes(1))
    }.collect()

    // loop until no more cc generated
    var cc_num = -1
    var i = 1
    do {
      println("size of current graph #" + i.toString + ": " + g.size.toString)
      i += 1
      //print_ccs(g)
      // split to smaill graphs
      val graphs = split_graph(g, slices)

      cc_num = g.size
      val graphs_rdd = spark.parallelize(graphs, slices)
      // new graph
      g = graphs_rdd.flatMap { graph => find_cc_general(graph) }.collect()
    } while (g.size < cc_num)

    // merge the final graph
    // this may lead to a single node[driver] to compute if the data is strange enough
    val g_css = new CCS
    g_css ++= g
    val final_graph = deep_clone_CCS(g_css)

    println("Final graph size: " + final_graph.size.toString)
    //print_ccs(final_graph)
    var ccs = find_cc_general(final_graph)

    println("How many connected components: " + ccs.size.toString)
    //print_ccs(ccs)

    System.exit(0)
  }

}
/**
 * This is the Bayes Classification workload in BigDataBench
 * @author YangQiang yangqiang@ict.ac.cn
 * @date 2014/4/8
 */
package cn.ac.ict.bigdatabench

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import collection.mutable.HashMap
import org.apache.spark.SparkConf


object NaiveBayesClassifier {
  // name of class, {word1: p1, word2: p2}
  type MODEL = Pair[String, HashMap[String, Double]]

  def classify(line: String, model: Array[MODEL]): String = {
    var result = "default"
    var max_p = -1.0
    var missing_p = 0.0001 // if a word is not in class, set this value
    val words = line.split(" ").toList.distinct
    var cur_p = 1.0

    for (m <- model) {
      cur_p = 1.0
      for (w <- words) {
        cur_p *= m._2.getOrElse(w, missing_p)
      }
      if (cur_p > max_p) {
        max_p = cur_p
        result = m._1
      }
    }

    result
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: BayesClassifier <master> <data_file> " +
        "<model_file> <result_file> [<slices>]")
      System.exit(1)
    }

    val host = args(0)
    val conf = new SparkConf().setAppName("BigDataBench NaiveBayesClassifier")
    val spark = new SparkContext(conf)
    val data_fn = args(1)
    val model_fn = args(2)
    val save_path = args(3)
    val slices = if (args.length > 4) args(3).toInt else 1

    println("Loading data, please wait...")
    val data = spark.textFile(data_fn, slices)
    println("Loading model, please wait...")
    val model: RDD[MODEL] = spark.objectFile(model_fn, slices)
    val model_list: Array[MODEL] = model.collect

    println("Doing Bayes classification...")
    val result = data.map(
      line => {
        classify(line, model_list) + " " + line
      })

    println("Saving result...")
    result.saveAsTextFile(save_path)
  }

}
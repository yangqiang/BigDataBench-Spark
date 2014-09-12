/*
 * Naive Bayes workload for BigDataBench
 */

package cn.ac.ict.bigdatabench

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable.ListBuffer
import collection.mutable.HashMap
import org.apache.spark.SparkConf

object NaiveBayesTrainer {

  type WCOUNT = HashMap[String, Int]
  /*
   * String: name of class
   * WCOUNT: count for each word
   * Int: number of class lines
   */
  type MODEL = Pair[String, Pair[WCOUNT, Int]]

  def train(line: String): MODEL = {
    val _wcount = new WCOUNT() // [word, count]

    val tokenizer = line.split(" ")
    val category = tokenizer(0)

    // count word with 1 if it appears in the line
    for (i <- 1 until tokenizer.length) {
      val word = tokenizer(i)
      _wcount.put(word, 1)
    }

    (category, (_wcount, 1))
  }

  def main(args: Array[String]) {

    if (args.length < 3) {
      System.err.println("Usage: NaiveBayes <master> <data_file> " +
        "<save_file> [<slices>]")
      System.exit(1)
    }

    val host = args(0)
    val conf = new SparkConf().setAppName("BigDataBench NaiveBayesTrainer")
    val spark = new SparkContext(conf)
    val filename = args(1)
    val save_path = args(2)
    val slices = if (args.length > 3) args(3).toInt else 1
    println("Loading data, please wait...")
    val lines = spark.textFile(filename, slices)

    // train every line and return a RDD of classifier
    val models_rdd = lines.map { line => train(line) }

    // merge all classifier
    println("Reducing...")

    val models_rk = models_rdd.reduceByKey(
      (m1, m2) => {
        val wcount = m1._1
        var ccount = m1._2
        for (word <- m2._1.keySet) {
          wcount.put(word, 1 + wcount.get(word).getOrElse(0))
        }
        ccount += m2._2
        (wcount, ccount)
      })

    // calculate the possibility
    val models_p = models_rk.map(model => {
      val _pos = new HashMap[String, Double]

      for (word <- model._2._1.keySet) {
        _pos.put(word, (model._2._1.get(word).get + 0.0) / model._2._2)
      }

      (model._1, _pos)
    })

    //models_p.saveAsTextFile(save_path + "_readable")
    models_p.saveAsObjectFile(save_path)
    println("Model has been saved to: " + save_path)

    System.exit(0)
  }
}

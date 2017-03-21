package com.ywsuen

import org.apache.spark.{SparkConf, SparkContext}

object SparkCore {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparkTest").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val sum = sc.parallelize(1 to 10000).reduce(_+_)
    println(s"1+2+3+...+10000 = ${sum}")

    sc.stop()
  }
}

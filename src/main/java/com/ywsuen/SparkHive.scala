package com.ywsuen

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkHive {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:\\hadoop")
   // System.setProperty("hive.metastore.uris","thrift://127.0.0.1:9083")

    val conf = new SparkConf().setAppName("sparkTest").setMaster("local[*]").set("spark.sql.hive.thriftServer.singleSession", "true")
    val sc = new SparkContext(conf)//.getConf.getAll.foreach(println)
    val hiveContext = new HiveContext(sc)//.getAllConfs.foreach(println)

    hiveContext.getAllConfs.foreach(s => println("---"+s))
    hiveContext.sql("show tables").collect().foreach(println)
    sc.stop()
  }
}

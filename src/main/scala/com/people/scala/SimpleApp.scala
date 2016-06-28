package com.people.scala

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhuxt on 2016/6/28.
  */
object SimpleApp {

  def main(args: Array[String]) {
    if (args.length < 1){
      System.err.println("Usage: <file>")
      System.exit(1)
    }
    val logFile = args(0)
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile)
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))

  }

}

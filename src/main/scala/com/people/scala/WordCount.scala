package com.people.scala

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by zhuxt on 2016/6/28.
  */
object WordCount {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: <file> <hdfsPath>")
      System.exit(1)
    }


    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args(0))

    val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    counts.saveAsTextFile(args(1))

  }

}

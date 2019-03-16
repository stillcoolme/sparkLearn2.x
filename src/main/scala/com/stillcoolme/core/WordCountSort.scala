package com.stillcoolme.core

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * wordCount并排序
  * @author stillcoolme
  * @date 2019/2/14 22:57
  */
object WordCountSort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SortWordCount").setMaster("local")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc = sparkSession.sparkContext

    sc.textFile("src/main/resources/bs.log", 1)
      .flatMap(_.split(","))
      .map((_, 1))
      .reduceByKey(_ + _, 1)
      .sortBy(_._2, false)
      .foreach(println(_))

  }

}

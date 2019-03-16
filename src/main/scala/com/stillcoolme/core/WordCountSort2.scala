package com.stillcoolme.core

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * wordCount并排序
  * @author stillcoolme
  * @date 2019/2/17 10:24
  */
object WordCountSort2 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SortWordCount").setMaster("local")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc = sparkSession.sparkContext

    sc.textFile("src/main/resources/bs.log", 1)
      .flatMap(line => line.split(","))
      .map(word => (word, 1))
      .reduceByKey(_ + _, 1)
      //没有用 sortBy 就要弄两次map
      .map(wordCount => (wordCount._2, wordCount._1))
      .sortByKey(false)
      .map(sortedWord => (sortedWord._2, sortedWord._1))
      .foreach(println(_))


  }

}

package com.stillcoolme.ml

import org.apache.spark.sql.SparkSession

class BaseSpark {

  val spark = SparkSession
    .builder
    .master("local[4]")
    .appName("TfIdfExample")
    .getOrCreate()


  def close() = {
    spark.stop()
  }

}

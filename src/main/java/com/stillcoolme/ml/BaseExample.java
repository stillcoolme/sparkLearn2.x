package com.stillcoolme.ml;

import org.apache.spark.sql.SparkSession;

/**
 * @author: stillcoolme
 * @date: 2020/1/9 9:06
 */
public class BaseExample {

    public static SparkSession spark = SparkSession
            .builder()
            .master("local[4]")
            .appName("mlTest")
            .getOrCreate();

}

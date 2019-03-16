package com.stillcoolme.sql.userlocation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Created by zhangjianhua on 2018/9/17.
 */
public class UserLocation{

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("UserLocation")
                .config("spark.master", "local")
                .getOrCreate();

        //bs_log  (手机,时间,基站ID,进入或离开 1,0）
        Dataset<String> dataset = sparkSession.read().textFile("src/main/resources/bs_info.txt");

        dataset.show();

    }



}

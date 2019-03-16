package com.stillcoolme.sql.ipanalysis

import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
  * Created by zhangjianhua on 2018/9/21.
  */
object IPAnalysisScala {

  def main(args: Array[String]): Unit = {
    //创建sparkSession
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("spark session example")
      .getOrCreate()

    val df = sparkSession.read.csv("src/main/resources/cdn.csv")

    //将加载的数据临时命名为log
    df.createOrReplaceTempView("log")

    def  getHour(time:String)={
      val date=new Date(Integer.valueOf(time)*1000);
      val sf=new SimpleDateFormat("HH");
      sf.format(date)
    }

    //查询每个小时视频流量
    val hourCdnSQL="select _c4,_c8 from log "
    //取出时间和大小将格式化时间，RDD中格式为 (小时,大小)
    val dataRdd= sparkSession.sql(hourCdnSQL)
      .rdd
      .map(row=>Row(getHour(row.getString(0)),java.lang.Long.parseLong(row.get(1).toString)))

    val schema=StructType(
      Seq(
        StructField("hour",StringType,true),
        StructField("size",LongType,true)
      )
    )

    //将dataRdd转成DataFrame
    val peopleDataFrame = sparkSession.createDataFrame(dataRdd,schema)
    peopleDataFrame.createOrReplaceTempView("cdn")
    //按小时分组统计
    val results = sparkSession.sql("SELECT hour , sum(size) as size  FROM cdn group by hour  order by hour ")
    results.foreach(row=>println(row.get(0)+"时 流量:"+row.getLong(1)/(1024*1024*1024)+"G"))
  }


}

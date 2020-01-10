package com.stillcoolme.ml

import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object ColleagueAnalyse extends BaseSpark {

  def main(args: Array[String]): Unit = {

    val car = "粤1234"
    val startTime = "2016-11-17 00:00:00"
    val endTime = "2016-11-18 00:00:00"
    val bayonet_ids = Seq(200, 201)

    val csvData = spark.read
      .format("csv")
      .option("sep", ",")
      .option("header", "true")
      .csv(this.getClass.getResource("/") + "mllib/car.csv")

    import spark.implicits._

    case class CarRecord(bayonet: String, time: String)

    val car_data = csvData
      .toDF("carId", "bayonet_id", "passing_time")
      .filter("passing_time >= '" + startTime + "' and passing_time <= '" + endTime + "'")
        .map(row => {
          (row.getString(0),
          row.getString(1) + row.getString(2))
        })
    car_data.show(false)


    val df = spark.read.format("json").json("file:///e:\\a.json")
    def encoder(columns: Seq[String]): Encoder[Row] = RowEncoder(StructType(columns.map(StructField(_, StringType, nullable = false))))
    val outputCols = Seq("ckey", "sum")

    val result = df.groupByKey(_.getString(0))(Encoders.kryo[String])
      .flatMapGroups((key, rowsForEach) => {
        val list1 = scala.collection.mutable.ListBuffer[Row]()
        var sum = 0L
        var count = 0
        for (elem <- rowsForEach) {
          sum += elem.getLong(1)
          count = count + 1
        }
        val avg = sum / count
        list1.append(Row(key, avg.toString))
        list1
      })(encoder(outputCols)).toDF
    result.show(10)
  }

}

package com.stillcoolme.ml

import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object ColleagueAnalyse extends BaseSpark {

  case class CarRecord(carId: String, bayonetId: String, passingTime: String)
  case class CarRecordWithoutTime(carId: String, bayonetId: String)

  def main(args: Array[String]): Unit = {

    val carId = "京D690CJ"
    val startTime = "2016-11-17 00:00:00"
    val endTime = "2016-11-17 00:05:00"

    // 导入spark的隐式转换
    import spark.implicits._

    val csvData = spark.read
      .format("csv")
      .option("sep", ",")
      .option("header", "true")
      .csv(this.getClass.getResource("/") + "mllib/car.csv")

    // 获取源数据的抓拍记录
    val querySourceCar = "passingTime >= '%s' AND passingTime <= '%s' AND carId == '%s'"
    val csvDataSet = csvData
      .map(row =>
        (
          row.getAs[String](0),
          row.getString(1),
          row.getString(2)
        )
      )
      .toDF("carId", "bayonetId", "passingTime")
      .as[CarRecord]

    val source_car_data = csvDataSet
      .filter(querySourceCar.format(startTime, endTime, carId))
    source_car_data.show(false)

    // 获取在时间范围内的 同一卡点的 其他车辆
    val queryOtherCar = "passingTime >= '%s' AND passingTime <= '%s' AND carId != '%s'"

    // 空列表
    // val empty: List[Row] = List()
    val rowArray = source_car_data.collect()
    var ds = spark.emptyDataset[CarRecord]

    // 要通过数组来迭代
    for ( i <- 0 to (rowArray.length - 1)) {
      val sourceTime = rowArray(i).passingTime
//      println("目标车辆过车时间: " + sourceTime)
      val otherStartTime = sourceTime
      val otherEndTime = endTime
      val filterSet = csvDataSet
          .filter(queryOtherCar.format(otherStartTime, otherEndTime, carId))
      ds = ds.union(filterSet)
    }
    ds = ds.distinct()

 /*   val schema = StructType(Array(
      StructField("carId", StringType, false),
      StructField("bayonetId", StringType, false),
      StructField("passingTime", StringType, false)))
    val encoder = RowEncoder(schema)*/
    def encoder(columns: Seq[String]): Encoder[Row] =
      RowEncoder(
        StructType(
          columns.map(
            StructField(_, StringType, false)
          )
        )
      )
    val outputCols1 = Seq("carId", "bayonetId")
    val outputCols2 = Seq("carId", "bayonetId", "passingTime")

    /*
      def map[U : Encoder](func: T => U): Dataset[U] = withTypedPlan {
        MapElements[T, U](func, logicalPlan)
      }
     */
    // 获得 CarRecordWithoutTime
    ds.map((datasetElem) => {
      CarRecordWithoutTime(datasetElem.carId, datasetElem.bayonetId)
    })(Encoders.product[CarRecordWithoutTime])


    /**
     * flatMapGroups[U : Encoder](f: (K, Iterator[V]) => TraversableOnce[U]): Dataset[U] = {
     */
    val ss = ds.groupByKey(_.carId)
      .flatMapGroups((key, rowsForEach) => {
        val list = scala.collection.mutable.ListBuffer[Row]()
        for (elem <- rowsForEach) {
          list.append(Row(elem.carId, elem.bayonetId, elem.passingTime))
        }
        list
      })(encoder(outputCols2)).toDF()

    ss.show(false)

    // 居然操作 DataFrame DataSet之前要转成rdd。不然就需要Encoder
/*
    val ss = source_car_data.rdd.map(row => {
          // 目标车辆经过该卡口的时间
          val sourceTime = row.passingTime
          print(sourceTime)
          val otherStartTime = sourceTime
          val otherEndTime = endTime
          if(csvData != null) {
            csvData
              .filter(queryOtherCar.format(otherStartTime, otherEndTime, carId))
          } else {
            print("haha")
          }
    })
    ss.collect()
*/

//    empty.foreach(print)

  }

}

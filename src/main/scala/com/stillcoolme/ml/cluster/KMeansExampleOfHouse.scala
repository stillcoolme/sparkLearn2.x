package com.stillcoolme.ml.cluster

import com.stillcoolme.ml.BaseSpark
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler

import scala.util.Random

object KMeansExampleOfHouse extends BaseSpark{
  def main(args: Array[String]): Unit = {

    val file = spark.read.format("csv")
      .option("sep", ";")
      .option("header", "true")
      .load(this.getClass.getResource("/") + "mllib/house.csv")

    import spark.implicits._
    val random = new Random()
    val data = file.map(row => {
      (
        row.getString(0).toDouble,
        row.getString(1).toDouble,
        row.getString(2).toDouble,
        random.nextDouble())
    }).toDF("position", "square", "label", "rand").sort("rand")

    // 特征包装，位置和面积 作为特征
    val ass = new VectorAssembler()
      .setInputCols(Array("position", "square")).setOutputCol("features")
    val dataset = ass.transform(data)

    val Array(train, test) = dataset.randomSplit(Array(0.8, 0.2))
    train.show(false)

    // 用 特征 训练一个 kmeans 模型
    val kmeans = new KMeans()
      .setFeaturesCol("features")
      .setK(3)
      .setMaxIter(20)
    val model = kmeans.fit(train)

    model.transform(test).show()

    spark.stop()
  }

}

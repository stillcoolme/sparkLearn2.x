package com.stillcoolme.ml.cluster

import com.stillcoolme.ml.BaseSpark
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler

import scala.util.Random

/**
 * 对 iris 的种类进行预测
 */
object KMeansExample2 extends BaseSpark{
  def main(args: Array[String]): Unit = {

    val file = spark.read.format("csv")
      .load(this.getClass.getResource("/") + "mllib/iris.data")

    import spark.implicits._
    val random = new Random()
    val data = file.map(row => {
      val label = row.getString(4) match {
        case "Iris-setosa" => 0
        case "Iris-versicolor" => 1
        case "Iris-virginica" => 2
      }

      (row.getString(0).toDouble,
        row.getString(1).toDouble,
        row.getString(2).toDouble,
        row.getString(3).toDouble,
        label,
        random.nextDouble())
    }).toDF("_c0", "_c1", "_c2", "_c3", "label", "rand").sort("rand")

    val assembler = new VectorAssembler()
      .setInputCols(Array("_c0", "_c1", "_c2", "_c3"))
      .setOutputCol("features")
    val dataset = assembler.transform(data)
    val Array(train, test) = dataset.randomSplit(Array(0.8, 0.2))
    train.show(false)

    // 用 特征 训练一个 kmeans 模型
    val kmeans = new KMeans().setFeaturesCol("features").setK(3).setMaxIter(20)
    val model = kmeans.fit(train)

    val result = model.transform(test).show(false)


    spark.stop()
  }
}

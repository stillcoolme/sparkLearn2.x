package com.stillcoolme.ml.linear

import com.stillcoolme.ml.BaseSpark
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler

import scala.util.Random

/**
 * 根据面积预测房价
 */
object LogisticRegressionExample extends BaseSpark{

  def main(args: Array[String]): Unit = {
    // 加载文件
    val file = spark.read.format("csv")
      .option("sep", ";")
      .option("header", "true")
      .load(this.getClass.getResource("/") + "mllib/house.csv")
    import spark.implicits._
    // 开始shuffle
    // 打乱顺序
    val rand = new Random()
    val data = file.select("square", "price").map(
      row => (row.getAs[String](0).toDouble, row.getString(1).toDouble, rand.nextDouble()))
      .toDF("square", "price", "rand").sort("rand") //强制类型转换过程

    // Dataset(Double, Double)
    // Dataframe = Dataset(Row)

    val ass = new VectorAssembler().setInputCols(Array("square")).setOutputCol("features")
    val dataset = ass.transform(data) //特征包装

    // 训练集， 测试集
    val Array(train, test) = dataset.randomSplit(Array(0.8, 0.2)) // 拆分成训练数据集和测试数据集

    val lr = new LogisticRegression()
      .setLabelCol("price")
      .setFeaturesCol("features")
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setMaxIter(10)
    val model = lr.fit(train)

    model.transform(test).show()
    val s = model.summary.totalIterations
    println(s"iter: ${s}")
  }

}

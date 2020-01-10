package com.stillcoolme.ml

import org.apache.spark.ml.fpm.FPGrowth

object FPGrowthExample extends BaseSpark {

  def main(args: Array[String]): Unit = {

    import spark.implicits._
    val dataset = spark.createDataset(Seq(
      "1 2 5",
      "1 2 3 5",
      "1 2")
    ).map(t => t.split(" ")).toDF("items")
    dataset.show(20)

    // 设置 支持度，置信度
    val fpgrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.01).setMinConfidence(0.06)
    val model = fpgrowth.fit(dataset)

    // Display frequent itemsets. 查看频繁项集
    model.freqItemsets.show()

    // Display generated association rules.
    model.associationRules.show()

    // transform examines the input items against all the association rules and summarize the
    // consequents as prediction
    model.transform(dataset).show()

    spark.stop()
  }

}

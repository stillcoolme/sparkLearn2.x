package com.stillcoolme.ml.tfidf

import com.stillcoolme.ml.BaseSpark
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

object TfIdfExample extends BaseSpark {

  def main(args: Array[String]): Unit = {
    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)

    val hasfingTf = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
    val featurizedData = hasfingTf.transform(wordsData)
    featurizedData.show(false)

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("label", "features").show(false)
    /**
     * [0,5,9,17] 各个向量位置，对应的向量的特征值[0.6931471805599453,0.6931471805599453,0.28768207245178085,1.3862943611198906]
     * +-----+----------------------------------------------------------------------------------------------------------------------+
     * |label|features                                                                                                              |
     * +-----+----------------------------------------------------------------------------------------------------------------------+
     * |0.0  |(20,[0,5,9,17],[0.6931471805599453,0.6931471805599453,0.28768207245178085,1.3862943611198906])                        |
     * |0.0  |(20,[2,7,9,13,15],[0.6931471805599453,0.6931471805599453,0.8630462173553426,0.28768207245178085,0.28768207245178085]) |
     * |1.0  |(20,[4,6,13,15,18],[0.6931471805599453,0.6931471805599453,0.28768207245178085,0.28768207245178085,0.6931471805599453])|
     * +-----+----------------------------------------------------------------------------------------------------------------------+
     */
    close()
  }
}

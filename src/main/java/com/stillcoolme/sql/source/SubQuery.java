package com.stillcoolme.sql.source;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// 查看执行计划  https://www.cnblogs.com/lyr999736/p/10204619.html
// storage模块  http://jerryshao.me/2013/10/08/spark-storage-module-analysis/
class  SubQuery{

    public static void main(String[] args) throws AnalysisException {
        // $example on:init_session$
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .getOrCreate();
        // $example off:init_session$
        runBasicDataFrameExample(spark);
        spark.stop();
    }

    private static void runBasicDataFrameExample(SparkSession spark) throws AnalysisException {
        // $example on:create_df$
        Dataset<Row> df1 = spark.read().json("src/main/resources/t1.json");
        Dataset<Row> df2 = spark.read().json("src/main/resources/t2.json");
        df1.registerTempTable("t1");
        df2.registerTempTable("t2");
        spark.sql("SELECT * FROM t1 WHERE EXISTS " +
                "(SELECT * FROM t2 WHERE t2b = t1b)").explain(true);
        Dataset dataset = spark.sql("SELECT * FROM t1 WHERE EXISTS " +
                "(SELECT * FROM t2 WHERE t2b = t1b)");
        dataset.show();

    }
}
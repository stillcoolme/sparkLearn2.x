package com.stillcoolme.core;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * 用sparkcore rdd来实现 topN ， 用的都是技战法里的方法流程。
 * Created by zhangjianhua on 2018/9/21.
 */
public class StockTopNRDD {

    /**
     * 其实goinsight的技战法对rdd的操作过程就是挺典型的topn吧
     */
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("StockTopN")
                .config("spark.master", "local")
                .getOrCreate();
        SparkContext sparkContext = sparkSession.sparkContext();
        JavaSparkContext jsc = new JavaSparkContext(sparkContext);

        JavaRDD rdd1 = jsc.parallelize(Arrays.asList("class1", "class2"));
        JavaRDD rdd2 = jsc.parallelize(Arrays.asList(90, 87, 79, 99));
        JavaPairRDD<String, Integer> pairs = rdd1.cartesian(rdd2);
        System.out.println(pairs.collect());


//        JavaPairRDD<String, Integer> pairs = rdd.mapToPair(new PairFunction<String, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(String line) throws Exception {
//                String[] lineSplited = line.split(" ");
//                return new Tuple2<String, Integer>(lineSplited[0], Integer.valueOf(lineSplited[1]));
//            }
//        });
        //得到pairs是一个二元组的RDD，直接调用groupByKey()函数，就可以按照key来进行分组了
        JavaPairRDD<String, Iterable<Integer>> grouped = pairs.groupByKey();

        //分组后每个key对应的这一个value的集合，这里，需要对每个key对应的value集合首先进行排序，然后取其前N个元素即可
        JavaPairRDD<String, Iterable> groupedTopN = grouped.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable>() {
            @Override
            public Tuple2<String, Iterable> call(Tuple2<String, Iterable<Integer>> values) throws Exception {
                Iterator<Integer> iter = values._2.iterator();
                List<Integer> list = new ArrayList<Integer>();
                while (iter.hasNext()) {
                    list.add(iter.next());
                }
                // 将list中的元素排序
                list.sort(new Comparator<Integer>() {
                    @Override
                    public int compare(Integer t1, Integer t2) {
                        int i1 = t1;
                        int i2 = t2;
                        return -(i1 - i2);// 逆序排列
                    }
                });
                List<Integer> top3 = list.subList(0, 3);//直接去前3个元素
                return new Tuple2<String, Iterable>(values._1, top3);
            }
        });

        groupedTopN.foreach(new VoidFunction<Tuple2<String, Iterable>>() {

            @Override
            public void call(Tuple2<String, Iterable> t) throws Exception {
                System.out.println(t._1);
                Iterator iterator = t._2.iterator();
                while (iterator.hasNext()) {
                    System.out.println(iterator.next());
                }
                System.out.println("====华丽的分割线=======");
            }

        });

    }


}

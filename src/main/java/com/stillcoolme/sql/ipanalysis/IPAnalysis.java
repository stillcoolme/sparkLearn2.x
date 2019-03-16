package com.stillcoolme.sql.ipanalysis;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 日志格式
 * 视频ID  IP 命中率 响应时间 请求时间 请求方法 请求URL    请求协议 状态吗 响应大小 referer 用户代理
   ClientIP Hit/Miss ResponseTime [Time Zone] Method URL Protocol StatusCode TrafficSize Referer UserAgent
 *
 * Created by zhangjianhua on 2018/9/20.
 */
public class IPAnalysis {

    public static class Flow{
        public  String hour;
        public long fowCount;

        public String getHour() {
            return hour;
        }

        public void setHour(String hour) {
            this.hour = hour;
        }

        public long getFowCount() {
            return fowCount;
        }

        public void setFowCount(long fowCount) {
            this.fowCount = fowCount;
        }
    }

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("IPAnalysis")
                .config("spark.master", "local")
                .getOrCreate();

        String path = IPAnalysis.class.getClassLoader().getResource("cdn.csv").getPath();
        Dataset<Row> dataset = sparkSession.read().csv(path);
        dataset.createOrReplaceTempView("cdn_log");

        dataset.show();

        /**
         * 计算独立IP总数和每个IP访问数
         */
        String allIpCountSQL = "select count(DISTINCT _c1) from cdn_log";
        String ipCountSQL="select _c1 as IP,count(_c1) as ipCount from cdn_log group by _c1 order by ipCOunt desc limit 10";
        //查询独立IP总数
        sparkSession.sql(allIpCountSQL)
                .foreach((ForeachFunction<Row>) row -> System.out.println("独立IP总数:"+ row.get(0)));
        //查看IP数前10
        sparkSession.sql(ipCountSQL)
                .foreach((ForeachFunction<Row>) row -> System.out.println("IP:"+ row.get(0) +" 次数:"+ row.get(1)));


        /**
         * 计算每个视频独立IP总数
         */
        String videoIpSQL = "select _c0, count(DISTINCT _c1) ipcount FROM cdn_log GROUP BY _c0 ORDER BY ipcount desc limit 5";
        sparkSession.sql(videoIpSQL)
                .foreach((ForeachFunction<Row>) row -> System.out.println("视频ID:"+ row.get(0) +" IP数:"+ row.get(1)));


        /**
         * 计算每个小时CDN流量, 结果： 时间，流量 （1点，2G；2点，2.2G......)
         *
         * 这里面主要有一个时间段的问题，不然和上面的都是一样 groupby 一下再 sum 一下就OK了，
         * 日志中记录的是Unix时间戳，只能按秒去分组统计每秒的流量，我们要按每小时分组去统计，所以核心就是将时间戳转化成小时，总体过程如下
         1. 通过SQL查出时间和大小
         2. 将结果中的时间转成小时
         3. 将时间格式化好后的RDD转成DataFrame，用于SQL查询
         4. 通过SQL按小时分组查出结果
         */
        String getTimeSql = "SELECT _c4, _c8 FROM cdn_log";

        /**
         * 要先转成RDD来map一下啊。
         * 1. 这写法就是一直在goinsight里面的写法: 通过sparksql取到dataset，然后传成rdd来做算子玩。居然不会。。。
         * 2. java里面没有map，就用mapToPair
         * 3. map需要Function， 是通过 new Function，传入和返回的参数写在< > 里面啊，然后实现其方法
         * 4. Tuple2就是里面有两个参数，Tuple3就是里面三个参数，汗。。。
         */


//        JavaPairRDD<String, Long> pairRdd = sparkSession.sql(getTimeSql)
//                .toJavaRDD()
//                .mapToPair(new PairFunction<Row, String, Long>(){
//                    @Override
//                    public Tuple2<String, Long> call(Row row) throws Exception {
//                        return new Tuple2<String, Long>(getHour((String) row.get(0)), Long.parseLong((String) row.get(1)));
//                    }
//                });

        //对RDD的结构了解一点了
        JavaRDD<Row> javaRDD = sparkSession.sql(getTimeSql)
                .javaRDD().map( new Function<Row, Row>() {
                    @Override
                    public Row call(Row row) throws Exception {
                        //这里我老是想这 new JavaRDD();..... RDD相当于一个大集合来的，你这里只是弄其中一行数据而已。
                        //RowFactory.create()找了好久。。。。
                        return RowFactory.create(getHour((String) row.get(0)), Long.parseLong((String) row.get(1)));
                    }
                });

        List<StructField> fieldList = new ArrayList();

        StructField  hourField = DataTypes.createStructField("hour", DataTypes.StringType, true);
        StructField  countField = DataTypes.createStructField("flowCount", DataTypes.LongType, true);
        fieldList.add(hourField);
        fieldList.add(countField);
        StructType schema = DataTypes.createStructType(fieldList);

        Dataset<Row> df = sparkSession.createDataFrame(javaRDD, schema);
        df.createOrReplaceTempView("flow");  //字段为 hour,flowCount的表

        Dataset<Row> flowResult = sparkSession.sql("select hour,sum(flowCount) as flowCount from flow group by hour order by hour");

        flowResult.foreach(new ForeachFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                System.out.println(row.get(0)+"时 流量:"+row.getLong(1)/(1024*1024*1024)+"G");
            }
        });

    }

    private static String getHour(String time) {
        int millisecond = Integer.valueOf(time);
        Date date = new Date(millisecond * 1000);
        SimpleDateFormat sdf = new SimpleDateFormat("HH");
        return sdf.format(date);
    }
}

/**
 RDD方法思路

 将日志中的访问时间及请求大小两个数据提取出来形成 RDD (访问时间,访问大小)，这里要去除404之类的非法请求
 按访问时间分组形成 RDD （访问时间，[大小1，大小2，….]）
 将访问时间对应的大小相加形成 (访问时间，总大小)
 计算代码

 //[15/Feb/2017:11:17:13 +0800]  匹配 2017:11 按每小时播放量统计
 val  timePattern=".*(2017):([0-9]{2}):[0-9]{2}:[0-9]{2}.*".r
 //匹配 http 响应码和请求数据大小
 val httpSizePattern=".*\\s(200|206|304)\\s([0-9]+)\\s.*".r

 def  isMatch(pattern:Regex,str:String)={
 str match {
 case pattern(_*) => true
 case _ => false
 }
 }

 //3.统计一天中每个小时间的流量
 input.filter(x=>isMatch(httpSizePattern,x)).filter(x=>isMatch(timePattern,x)).map(x=>getTimeAndSize(x)).groupByKey()
 .map(x=>(x._1,x._2.sum)).sortByKey().foreach(x=>println(x._1+"时 CDN流量="+x._2/(1024*1024*1024)+"G"))

 计算过程

 filter(x=>isMatch(httpSizePattern,x)).filter(x=>isMatch(timePattern,x)) 过滤非法请求
 map(x=>getTimeAndSize(x)) 将日志格式化成 RDD(请求小时,请求大小)
 groupByKey() 按请求时间分组形成 RDD(请求小时，[大小1，大小2，….])
 map(x=>(x._1,x._2.sum)) 将每小时的请求大小相加，形成 RDD(请求小时,总大小)

 **/
## 项目介绍
练手的spark项目，基于spark2.4

# java/sql
## orgexample包：官方示例
代码都是spark getstart页面一路下来讲的代码，首先是
JavaSparkSQLExample：
 1. 创建DataSet，直接做sql查询
 2. 使用DataSet，注册临时表，做sql查询
 3. RDD转成DataSets的两种方法。
 
## ipanalysis包：分析ip
## topn： 货物订单topn
## 


## 注意

1. DataFrame也可以叫Dataset[Row],每一行的类型是Row，不解析，每一行究竟有哪些字段和类型都不知道。Dataset和DataFrame拥有完全相同的成员函数，区别只是每一行的数据类型不同。

## 遇到的问题

* 运行scala的WordCount报错：tried to access method com.google.common.base.Stopwatch.()V from class org.apache.hadoop.mapre
添加以下依赖解决：
```
<dependency>
  <groupId>org.apache.hadoop</groupId>
  <artifactId>hadoop-common</artifactId>
  <version>2.7.3</version>
</dependency>
<dependency>
  <groupId>org.apache.hadoop</groupId>
  <artifactId>hadoop-mapreduce-client-core</artifactId>
  <version>2.7.3</version>
</dependency>
```
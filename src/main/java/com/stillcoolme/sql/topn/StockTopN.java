package com.stillcoolme.sql.topn;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * 现在的需求是， 计算所有订单月销售额前十名
 * 那么最后的结果是： 年月份，订单号，销售额
 * 而这三个字段分别位于三个表中，那我们可以先创建一个临时表，连接三个表，把上述三个字段的数据查出来
 *
 * 使用窗口函数
 *
 * Created by zhangjianhua on 2018/9/18.
 */
public class StockTopN {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("StockTopN")
                .config("spark.master", "local")
                .getOrCreate();
        SQLContext sqlContext = sparkSession.sqlContext();

        JavaRDD<TbDate> tbDateJavaRDD = sparkSession.read().textFile("src/main/resources/topn/tbDate.txt")
                .javaRDD()
                .map(line -> {
                    String[] cloumn = line.split(",");
                    TbDate tbDate = new TbDate(cloumn[0], cloumn[1], cloumn[2], cloumn[3], cloumn[4], cloumn[5], cloumn[6], cloumn[7], cloumn[8], cloumn[9]);
                    return tbDate;
                });
        JavaRDD<TbStock> tbStockJavaRDD = sparkSession.read().textFile("src/main/resources/topn/tbStock.txt")
                .javaRDD()
                .map(line -> {
                    String[] cloumn = line.split(",");
                    TbStock tbStock = new TbStock(cloumn[0], cloumn[1], cloumn[2]);
                    return tbStock;
                });
        JavaRDD<TbStockDetail> tbStockDetailJavaRDD = sparkSession.read().textFile("src/main/resources/topn/tbStockDetail.txt")
                .javaRDD()
                .map(line -> {
                    String[] cloumn = line.split(",");
                    TbStockDetail tbStockDetail = new TbStockDetail(cloumn[0], Integer.valueOf(cloumn[1]), cloumn[2], Integer.valueOf(cloumn[3]), Double.valueOf(cloumn[4]), Double.valueOf(cloumn[5]));
                    return tbStockDetail;
                });

        Dataset<Row> tbDateDF = sparkSession.createDataFrame(tbDateJavaRDD, TbDate.class);
        Dataset<Row> tbStockDF = sparkSession.createDataFrame(tbStockJavaRDD, TbStock.class);
        Dataset<Row> tbStockDetailDF = sparkSession.createDataFrame(tbStockDetailJavaRDD, TbStockDetail.class);
        tbDateDF.createOrReplaceTempView("tbDate");
        tbStockDF.createOrReplaceTempView("tbStock");
        tbStockDetailDF.createOrReplaceTempView("tbStockDetail");

        //查看注册的表
//        Dataset<String> dataset = sparkSession.sql("show tables")
//                .map((MapFunction<Row, String>) row -> row.toString(), Encoders.STRING());
//        dataset.show();

        //创建一个临时表 tempyearmonthorder，将三表数据关联。
        sparkSession.sql("select a.theyearmonth,b.ordernumber,c.amount from tbDate as a join tbStock as b join tbStockDetail as c on a.dateID = b.dateID and b.ordernumber = c.ordernumber").createOrReplaceTempView("tempyearmonthorder");
        sparkSession.sql("SELECT * FROM tempyearmonthorder").show();

        //sparkSession.sql("select theyearmonth, ordernumber, count(1) from tempyearmonthorder group by ordernumber, theyearmonth");

        //最后执行查询,为了避免内存溢出，可以把结果先存到一个临时表.
        sparkSession.sqlContext().cacheTable("tempyearmonthorder");

        sqlContext.sql("select * from (select theyearmonth, ordernumber, amount, Row_Number() OVER (partition by theyearmonth order by amount desc) as rank from tempyearmonthorder) as temp where temp.rank <= 10")
                .drop("rank").createOrReplaceTempView("tempyearmonthtopten");

        sqlContext.sql("select * from tempyearmonthtopten").show();

//        tbDateDF.show();
//        tbStockDF.show();
//        tbStockDetailDF.show();
    }


    public static class TbDate implements Serializable{
        private String dateID;
        private String theyearmonth;
        private String theyear;
        private String themonth;
        private String thedate;
        private String theweek;
        private String theweeks;
        private String thequot;
        private String thetenday;
        private String thehalfmonth;

        public String getDateID() {
            return dateID;
        }

        public void setDateID(String dateID) {
            this.dateID = dateID;
        }

        public String getTheyearmonth() {
            return theyearmonth;
        }

        public void setTheyearmonth(String theyearmonth) {
            this.theyearmonth = theyearmonth;
        }

        public String getTheyear() {
            return theyear;
        }

        public void setTheyear(String theyear) {
            this.theyear = theyear;
        }

        public String getThemonth() {
            return themonth;
        }

        public void setThemonth(String themonth) {
            this.themonth = themonth;
        }

        public String getThedate() {
            return thedate;
        }

        public void setThedate(String thedate) {
            this.thedate = thedate;
        }

        public String getTheweek() {
            return theweek;
        }

        public void setTheweek(String theweek) {
            this.theweek = theweek;
        }

        public String getTheweeks() {
            return theweeks;
        }

        public void setTheweeks(String theweeks) {
            this.theweeks = theweeks;
        }

        public String getThequot() {
            return thequot;
        }

        public void setThequot(String thequot) {
            this.thequot = thequot;
        }

        public String getThetenday() {
            return thetenday;
        }

        public void setThetenday(String thetenday) {
            this.thetenday = thetenday;
        }

        public String getThehalfmonth() {
            return thehalfmonth;
        }

        public void setThehalfmonth(String thehalfmonth) {
            this.thehalfmonth = thehalfmonth;
        }

        public TbDate(String dateID, String theyearmonth, String theyear, String themonth, String thedate, String theweek, String theweeks, String thequot, String thetenday, String thehalfmonth) {
            this.dateID = dateID;
            this.theyearmonth = theyearmonth;
            this.theyear = theyear;
            this.themonth = themonth;
            this.thedate = thedate;
            this.theweek = theweek;
            this.theweeks = theweeks;
            this.thequot = thequot;
            this.thetenday = thetenday;
            this.thehalfmonth = thehalfmonth;
        }
    }

    public static class TbStock implements Serializable{
        private String ordernumber;
        private String locationid;
        private String dateID;

        public String getOrdernumber() {
            return ordernumber;
        }

        public void setOrdernumber(String ordernumber) {
            this.ordernumber = ordernumber;
        }

        public String getLocationid() {
            return locationid;
        }

        public void setLocationid(String locationid) {
            this.locationid = locationid;
        }

        public String getDateID() {
            return dateID;
        }

        public void setDateID(String dateID) {
            this.dateID = dateID;
        }

        public TbStock(String ordernumber, String locationid, String dateID) {
            this.ordernumber = ordernumber;
            this.locationid = locationid;
            this.dateID = dateID;
        }
    }

    public static class TbStockDetail implements Serializable{
        private String ordernumber;
        private int rownum;
        private String itemid;
        private int qty;
        private double price;
        private double amount;

        public String getOrdernumber() {
            return ordernumber;
        }

        public void setOrdernumber(String ordernumber) {
            this.ordernumber = ordernumber;
        }

        public int getRownum() {
            return rownum;
        }

        public void setRownum(int rownum) {
            this.rownum = rownum;
        }

        public String getItemid() {
            return itemid;
        }

        public void setItemid(String itemid) {
            this.itemid = itemid;
        }

        public int getQty() {
            return qty;
        }

        public void setQty(int qty) {
            this.qty = qty;
        }

        public double getPrice() {
            return price;
        }

        public void setPrice(double price) {
            this.price = price;
        }

        public double getAmount() {
            return amount;
        }

        public void setAmount(double amount) {
            this.amount = amount;
        }

        public TbStockDetail(String ordernumber, int rownum, String itemid, int qty, double price, double amount) {
            this.ordernumber = ordernumber;
            this.rownum = rownum;
            this.itemid = itemid;
            this.qty = qty;
            this.price = price;
            this.amount = amount;
        }
    }
}

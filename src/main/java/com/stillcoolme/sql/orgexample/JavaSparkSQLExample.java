/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stillcoolme.sql.orgexample;

// $example on:programmatic_schema$
import java.util.ArrayList;
import java.util.List;
// $example off:programmatic_schema$
// $example on:create_ds$
import java.util.Arrays;
import java.util.Collections;
import java.io.Serializable;
// $example off:create_ds$

// $example on:schema_inferring$
// $example on:programmatic_schema$
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
// $example off:programmatic_schema$
// $example on:create_ds$
import org.apache.spark.api.java.function.MapFunction;
// $example on:create_df$
// $example on:run_sql$
// $example on:programmatic_schema$
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
// $example off:programmatic_schema$
// $example off:create_df$
// $example off:run_sql$
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
// $example off:create_ds$
// $example off:schema_inferring$
import org.apache.spark.sql.RowFactory;
// $example on:init_session$
import org.apache.spark.sql.SparkSession;
// $example off:init_session$
// $example on:programmatic_schema$
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
// $example off:programmatic_schema$
import org.apache.spark.sql.AnalysisException;

// $example on:untyped_ops$
// col("...") is preferable to df.col("...")
import static org.apache.spark.sql.functions.col;
// $example off:untyped_ops$

public class JavaSparkSQLExample {
    // $example on:create_ds$
    public static class Person implements Serializable {
        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }
    }
    // $example off:create_ds$

    public static void main(String[] args) throws AnalysisException {
        // $example on:init_session$
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .getOrCreate();
        // $example off:init_session$

//    runBasicDataFrameExample(spark);
//    runDatasetCreationExample(spark);
        runInferSchemaExample(spark);
//    runProgrammaticSchemaExample(spark);

        spark.stop();
    }

    private static void runBasicDataFrameExample(SparkSession spark) throws AnalysisException {
        // $example on:create_df$
        Dataset<Row> df = spark.read().json("src/main/resources/people.json");

        // Displays the content of the DataFrame to stdout
        df.show();
        // +----+-------+
        // | age|   name|
        // +----+-------+
        // |null|Michael|
        // |  30|   Andy|
        // |  19| Justin|
        // +----+-------+
        // $example off:create_df$

        // $example on:untyped_ops$
        // Print the schema in a tree format
        df.printSchema();
        // root
        // |-- age: long (nullable = true)
        // |-- name: string (nullable = true)

        // Select only the "name" column
        df.select("name").show();
        // +-------+
        // |   name|
        // +-------+
        // |Michael|
        // |   Andy|
        // | Justin|
        // +-------+

        // Select everybody, but increment the age by 1
        df.select(col("name"), col("age").plus(1)).show();
        // +-------+---------+
        // |   name|(age + 1)|
        // +-------+---------+
        // |Michael|     null|
        // |   Andy|       31|
        // | Justin|       20|
        // +-------+---------+

        // Select people older than 21
        df.filter(col("age").gt(21)).show();
        // +---+----+
        // |age|name|
        // +---+----+
        // | 30|Andy|
        // +---+----+

        // Count people by age
        df.groupBy("age").count().show();
        // +----+-----+
        // | age|count|
        // +----+-----+
        // |  19|    1|
        // |null|    1|
        // |  30|    1|
        // +----+-----+
        // $example off:untyped_ops$

        // $example on:run_sql$
        // Register the DataFrame as a SQL temporary view 创建临时表people
        df.createOrReplaceTempView("people");

        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
        sqlDF.show();
        // +----+-------+
        // | age|   name|
        // +----+-------+
        // |null|Michael|
        // |  30|   Andy|
        // |  19| Justin|
        // +----+-------+
        // $example off:run_sql$

        // $example on:global_temp_view$
        // Register the DataFrame as a global temporary view   创建全局临时表people
        df.createGlobalTempView("people");

        // Global temporary view is tied to a system preserved database `global_temp`   全局临时表 在global_temp的命名空间下
        spark.sql("SELECT * FROM global_temp.people").show();
        // +----+-------+
        // | age|   name|
        // +----+-------+
        // |null|Michael|
        // |  30|   Andy|
        // |  19| Justin|
        // +----+-------+

        // Global temporary view is cross-session    全局临时表是跨session的
        spark.newSession().sql("SELECT * FROM global_temp.people").show();
        // +----+-------+
        // | age|   name|
        // +----+-------+
        // |null|Michael|
        // |  30|   Andy|
        // |  19| Justin|
        // +----+-------+
        // $example off:global_temp_view$
    }

    private static void runDatasetCreationExample(SparkSession spark) {
        // $example on:create_ds$
        // Create an instance of a Bean class
        Person person = new Person();
        person.setName("Andy");
        person.setAge(32);

        /**
         * instead of using Java serialization or Kryo they use a specialized Encoder to serialize the objects for processing or transmitting over the network.
         * While both encoders and standard serialization are responsible for turning an object into bytes, encoders are code generated dynamically and use a format that allows Spark to perform many operations like filtering, sorting and hashing without deserializing the bytes back into an object.
         * 使用Encoder来做序列化和网络传输，encoders可以动态生成代码并且使用一种转换能让spark执行许多操作，
         * 例如filtering、sorting、hashing，而不用将字节反序列化回对象。
         */
        // Encoders are created for Java beans
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = spark.createDataset(
                Collections.singletonList(person),
                personEncoder
        );
        javaBeanDS.show();
        // +---+----+
        // |age|name|
        // +---+----+
        // | 32|Andy|
        // +---+----+

        // Encoders for most common types are provided in class Encoders
        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
        Dataset<Integer> transformedDS = primitiveDS.map(
                (MapFunction<Integer, Integer>) value -> value + 1,
                integerEncoder);
        transformedDS.collect(); // Returns [2, 3, 4]

        // DataFrames can be converted to a Dataset by providing a class. Mapping based on name
        String path = "src/main/resources/people.json";
        Dataset<Person> peopleDS = spark.read().json(path).as(personEncoder);
        peopleDS.show();
        // +----+-------+
        // | age|   name|
        // +----+-------+
        // |null|Michael|
        // |  30|   Andy|
        // |  19| Justin|
        // +----+-------+
        // $example off:create_ds$
    }


    /**
     *
     * Spice SQL支持两种方法将RDDS转换成Datasets。
     * 第一种方法使用反射来推断包含特定类型对象的RDD的模式。
     * 在编写Spark应用程序时已经了解了模式时，这种基于反射的方法能很好的使用、使代码简洁。
     * The second method for creating Datasets is through a programmatic interface that allows you to construct a schema and then apply it to an existing RDD. While this method is more verbose, it allows you to construct Datasets when the columns and their types are not known until runtime.
     * 第二种方法是通过程序化接口 让你构建schema 然后应用到RDD上，这种方法复杂但是可以让你在不知道对象字段和类型的时候来构建datasets。
     * @param spark
     */
  private static void runInferSchemaExample(SparkSession spark) {

      /**
       *  Currently, Spark SQL does not support JavaBeans that contain Map field(s). Nested JavaBeans and List or Array fields are supported though.
       *  You can create a JavaBean by creating a class that implements Serializable and has getters and setters for all of its fields.
       *  当前，Spark SQL 支持嵌套javabean、list和array，但还不支持 javabean中包含 map字段。
       */
    // $example on:schema_inferring$
    // Create an RDD of Person objects from a text file
    JavaRDD<Person> peopleRDD = spark.read()
      .textFile("src/main/resources/people.txt")
      .javaRDD()
      .map(line -> {
        String[] parts = line.split(",");
        Person person = new Person();
        person.setName(parts[0]);
        person.setAge(Integer.parseInt(parts[1].trim()));
        return person;
      });

    // Apply a schema to an RDD of JavaBeans to get a DataFrame
    Dataset<Row> peopleDF = spark.createDataFrame(peopleRDD, Person.class);

    // Register the DataFrame as a temporary view
    peopleDF.createOrReplaceTempView("people");

    // SQL statements can be run by using the sql methods provided by spark
    Dataset<Row> teenagersDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");


    // The columns of a row in the result can be accessed by field index
    Encoder<String> stringEncoder = Encoders.STRING();
    Dataset<String> teenagerNamesByIndexDF = teenagersDF.map(
        (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
        stringEncoder);
    teenagerNamesByIndexDF.show();
    // +------------+
    // |       value|
    // +------------+
    // |Name: Justin|
    // +------------+

    // or by field name
    Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(
        (MapFunction<Row, String>) row -> "Name: " + row.<String>getAs("name"),
        stringEncoder);
    teenagerNamesByFieldDF.show();
    // +------------+
    // |       value|
    // +------------+
    // |Name: Justin|
    // +------------+
    // $example off:schema_inferring$
  }

    /**
     * 第二种方法将 RDDS转换成Datasets
     * When JavaBean classes cannot be defined ahead of time 当javabean还无法定义字段类型的时候
     * (for example, the structure of records is encoded in a string, or a text dataset will be parsed and fields will be projected differently for different users),
     * a Dataset<Row> can be created programmatically with three steps.
         Create an RDD of Rows from the original RDD;
         Create the schema represented by a StructType matching the structure of Rows in the RDD created in Step 1.
         Apply the schema to the RDD of Rows via createDataFrame method provided by SparkSession.
     * @param spark
     */
  private static void runProgrammaticSchemaExample(SparkSession spark) {
    // $example on:programmatic_schema$
    // Create an RDD
    JavaRDD<String> peopleRDD = spark.sparkContext()
      .textFile("src/main/resources/people.txt", 1)
      .toJavaRDD();

    // The schema is encoded in a string
    String schemaString = "name age";

    // Generate the schema based on the string of schema  !!!!!
    List<StructField> fields = new ArrayList<>();
    for (String fieldName : schemaString.split(" ")) {
      StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
      fields.add(field);
    }
    StructType schema = DataTypes.createStructType(fields);

    // Convert records of the RDD (people) to Rows
    JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) record -> {
      String[] attributes = record.split(",");
      return RowFactory.create(attributes[0], attributes[1].trim());
    });

    // Apply the schema to the RDD  !!!!
    Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);

    // Creates a temporary view using the DataFrame
    peopleDataFrame.createOrReplaceTempView("people");

    // SQL can be run over a temporary view created using DataFrames
    Dataset<Row> results = spark.sql("SELECT name FROM people");

    // The results of SQL queries are DataFrames and support all the normal RDD operations
    // The columns of a row in the result can be accessed by field index or by field name
    Dataset<String> namesDS = results.map(
        (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
        Encoders.STRING());
    namesDS.show();
    // +-------------+
    // |        value|
    // +-------------+
    // |Name: Michael|
    // |   Name: Andy|
    // | Name: Justin|
    // +-------------+
    // $example off:programmatic_schema$
  }
}

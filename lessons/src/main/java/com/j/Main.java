package com.j;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.split;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class Main {

  public static void main(String[] args) throws TimeoutException, StreamingQueryException, IOException {

    SparkSession spark = SparkSession
        .builder()
        .master("spark://mufaddal-OMEN-16:7077")
        .appName("Java Spark SQL basic example")
        .getOrCreate();
    spark.sparkContext().setLogLevel("WARN");

//    var dataset = spark.readStream().format("socket").option("host", "localhost").option("port", "12344").load();

//    var dataset = spark.read().option("header", true).csv("employees.csv");
    var dataset = spark.readStream().format("rate").option("rowsPerSecond", "1").load();

    var d1 = dataset.select(col("*"), functions.expr("value%3").as("div"))
        .withColumn("new_str", functions.expr("case when `div` = 0 then 'eagle owl peacock' when div = 1 then 'bear cat frog' when div = "
                                                  + "2 then 'cod salmon mackerel' end"))
        .withColumn("aword", explode(split(col("new_str"), " ")))
        ;

    d1.writeStream().format("console").outputMode("append").start();

//    d1.withWatermark(col("timestamp").toString(), "3 minutes")
//        .groupBy(functions.window(col("timestamp"), "10 seconds", "8 seconds"),
//                 col("aword")
//               //col("div")
//        )
//        .count()
//        .writeStream().format("parquet").option("path", "./wordcount.parquet")
//        .option("checkpointLocation", "/tmp/checkpointloc")
//        .outputMode("append").start().awaitTermination();

    d1.createOrReplaceTempView("myst");
    var d3 = spark.sql("select window, aword, count(2) from myst group by window(timestamp, '10 seconds'), aword");
    d3.printSchema();
    d3.writeStream().format("console").option("path", "./wordcount.parquet").option("truncate", false)
        .outputMode("complete").start().awaitTermination();

//    dataset.show(10, false);
//
//    dataset.groupBy("JOB_ID")
//        .agg(Map.of("SALARY", "count"))
//        .show();
//
//    dataset.select(functions.col("*"), functions.avg("SALARY").over(Window.partitionBy("JOB_ID")))
//        .show();

  }
}

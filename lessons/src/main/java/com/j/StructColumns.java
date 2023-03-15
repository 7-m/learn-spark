package com.j;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class StructColumns {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession session = SparkSession.builder().appName("srtuct terust").master("local[1]").getOrCreate();
        session.sparkContext().setLogLevel("warn");

        Dataset<Row> rate = session.readStream().format("rate").load();
        rate.printSchema();


        rate = rate.groupBy(window(col("timestamp"), "10 seconds")).agg(count("value"));
        rate.printSchema();


        rate.select("window.*", "*").printSchema();
//      rate.writeStream().outputMode("update").format("console").start().awaitTermination();

    }
}

package com.j;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.decode;

public class ReadSpark {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException, InterruptedException {
        SparkSession ss = SparkSession.builder().appName("Kafkfa Mysql").master("local[2]").getOrCreate();

        var stream = ss  .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:29092")
                .option("subscribe", "mydbv3.inventory.product")
                .option("startingOffsets", "earliest")
                .option("spark.sql.streaming.kafka.useDeprecatedOffsetFetching ", false)
                .load();
        ss.sparkContext().setLogLevel("warn");

        var schema = ss.read().json("data2.json").schema();


        stream = stream.select(functions.from_json(decode(col("value"), "UTF-8"),schema).as("parsed"));
        stream = stream.select("parsed.payload.id");


        stream.writeStream().format("console").outputMode("append").option("truncate", false).

                start().awaitTermination();




    }


    // {"st" : "val"}
    static String raw2 = "{\"st\" : \"val\"}";
    static String raw = "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"int32\",\"optional\":true,\"field\":\"id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"name\"},{\"type\":\"int32\",\"optional\":true,\"field\":\"price\"}],\"optional\":false,\"name\":\"mydbv3.inventory.product.Value\"},\"payload\":{\"id\":1,\"name\":\"shoes\",\"price\":30}}";
}

package com.j;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;


/**
 * Read a json and store in a dataframe.
 */
public class ReadJson {
    public static void main(String[] args) {
        var sess = SparkSession.builder().appName("json structs")
                .config("spark.sql.streaming.schemaInference", false)
                .master("local[1]")
                .getOrCreate();
        sess.sparkContext().setLogLevel("warn");

        /**
         * Multiline is required for json files extending multiple lines
         */
        StructType str = new StructType();
        str.add("name", new StringType());
        var df =  sess.read().option("multiline", true).json("data.json").cache();
        df.show();

    }
}

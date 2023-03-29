package com.j;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadParquet {

  public static void main(String[] args) {
    SparkSession s = SparkSession.builder().master("local[1]").getOrCreate();
    s.sparkContext().setLogLevel("WARN");

    final Dataset<Row> df = s.read().parquet("./wordcount.parquet");

    df.show(Integer.MAX_VALUE, false);
  }

}

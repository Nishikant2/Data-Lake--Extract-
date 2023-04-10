package com.datalake

import org.apache.spark.sql.{DataFrame, SparkSession}

class Main_datalake {

  def read_csv(full_path: String,spark : SparkSession): DataFrame = {
    spark.read.option("header","true").csv(full_path)
  }

  def read_mysql(table_name: String,spark : SparkSession): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/Apolo_db")
      .option("query", "select * from " + table_name)
      .option("user", "abhi")
      .option("password", "1234")
      .load()
  }

  def read_json(path: String,spark : SparkSession): DataFrame = {
    spark.read.json(path)
  }


//  ------------------------------------Write part ----------------------------------------------------
  def write_hdfs_json_df(df: DataFrame, path: String): Unit = {
    df.write.mode("overwrite").option("header","true").json("hdfs://localhost:9000" + path)
  }
  def write_hdfs_csv_df(df: DataFrame, path: String): Unit = {
    df.write.mode("overwrite").option("header","true").csv("hdfs://localhost:9000" + path)
  }

  def write_mysql(df: DataFrame,table_name :String): Unit ={
    df.write.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/Apolo_db")
      .option("dbtable", table_name)
      .option("user", "abhi")
      .option("password", "1234")
      .save()
  }
}

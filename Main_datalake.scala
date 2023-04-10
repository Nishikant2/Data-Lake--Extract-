package com.datalake

import org.apache.spark.sql.{DataFrame, SparkSession}

class Main_datalake {


  def read_mysql(table_name: String,spark : SparkSession): DataFrame = {
          spark.read
            .format("jdbc")
            .option("url", "jdbc:mysql://localhost:3306/Apolo_db")
            .option("query", "select * from " + table_name + " limit 1000000 ")
            .option("user", "abhi")
            .option("password", "password")
            .load()
  }

  def read_hdfs_json(path: String,spark : SparkSession): DataFrame = {
    spark.read
      .option("inferSchema", "false")
      .option("header", "true")
      .json("hdfs://localhost:9000" + path)
  }

  def read_hdfs_csv(path: String,spark : SparkSession): DataFrame = {
    spark.read.option("header","true").csv("hdfs://localhost:9000" + path)
  }


  def write_mysql_df(df: DataFrame, table_name: String): Unit = {
    df.write.format("jdbc")
      .mode("overwrite")
      .option("url", "jdbc:mysql://localhost:3306/Apolo_end_user")
      .option("dbtable", table_name)
      .option("user", "abhi1234")
      .option("password", "1212")
      .save()
  }

  def write_hdfs_json_df(df: DataFrame, path: String): Unit = {
    df.write.mode("overwrite").option("header","true").json("hdfs://localhost:9000" + path)
  }
  def write_hdfs_csv_df(df: DataFrame, path: String): Unit = {
    df.write.mode("overwrite").option("header","true").csv("hdfs://localhost:9000" + path)
  }
  def write_mongo_df(df: DataFrame,collection_name: String) : Unit = {
    df.write.format("mongo")
      .option("uri","mongodb://127.0.0.1/Apolo_db."+collection_name)
      .option("header","true")
      .mode("overwrite")
      .save()
  }
}

package com.datalake

import org.apache.spark.sql.SparkSession

object Driver {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("PR")
      .getOrCreate()

    // MAIN OBJ
    val main_obj = new Main_datalake()

    // -------------------Read csv from hdfs ------------------------------------
    val doctor_df = main_obj.read_hdfs_csv("/Apolo_datalake/rdbms_data/Doctor/*.csv",spark)
    val Hospital_df = main_obj.read_hdfs_csv("/Apolo_datalake/rdbms_data/Hospital/*.csv",spark)
    val Patient_df = main_obj.read_hdfs_csv("/Apolo_datalake/rdbms_data/Patient/*.csv",spark)

    //--------------------------Read Json from hdfs------------------------------
    val lab_df = main_obj.read_hdfs_json("/Apolo_datalake/lab_data/lab_json/", spark)

    //lab_df.show()


    //--------------------------Write into Mongo DB --------------------------------
    main_obj.write_mongo_df(lab_df,"lab_data")

    //--------------------------Write into Mysql --------------------------------

    main_obj.write_mysql_df(doctor_df,"Doctor_data")
    main_obj.write_mysql_df(Hospital_df,"Hospital_data")
    main_obj.write_mysql_df(Patient_df,"Patient_data")

  }

}

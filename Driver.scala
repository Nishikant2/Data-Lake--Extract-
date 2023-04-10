package com.datalake

import org.apache.spark.sql.{DataFrame, SparkSession}

object Driver {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("PR")
      .getOrCreate()

    // MAIN OBJ
    val main_obj = new Main_datalake()


    main_obj.write_mysql(main_obj.read_csv("/home/abhi/project_baba/Doct*.csv",spark),"Doctor")
    main_obj.write_mysql(main_obj.read_csv("/home/abhi/project_baba/Pat*.csv",spark),"Patient")
    main_obj.write_mysql(main_obj.read_csv("/home/abhi/project_baba/Hos*.csv",spark),"Hospital")


    //--------------------------Read Json------------------------------
    val lab_df=main_obj.read_json("/home/abhi/project_baba/la*.json",spark)

    //----------------------- Read Mysql Table ---------------------------------
    val doctor_df_mysql=main_obj.read_mysql("Doctor",spark)
    val Hospital_df_mysql=main_obj.read_mysql("Hospital",spark)
    val Patient_df_mysql=main_obj.read_mysql("Patient",spark)



    //------------------------------ HDFS write mysql data ------------------------
        main_obj.write_hdfs_csv_df(doctor_df_mysql,"/Apolo_datalake/rdbms_data/Doctor")
        main_obj.write_hdfs_csv_df(Hospital_df_mysql,"/Apolo_datalake/rdbms_data/Hospital")
        main_obj.write_hdfs_csv_df(Patient_df_mysql,"/Apolo_datalake/rdbms_data/Patient")

    // HDFS write json data
        main_obj.write_hdfs_json_df(lab_df,"/Apolo_datalake/lab_data/lab_json")

  }

}

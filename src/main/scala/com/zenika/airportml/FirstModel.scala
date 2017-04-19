package com.zenika.airportml

import org.apache.spark.sql.SparkSession

/**
  * Created by g.bernonville-ext on 19/04/2017.
  */
object FirstModel extends App {
  val spark = SparkSession.builder().appName("FirstModel").master("local[*]")
    .getOrCreate()

}

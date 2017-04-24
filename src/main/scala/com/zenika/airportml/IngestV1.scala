package com.zenika.airportml

import com.zenika.airportml.model.Flight
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by g.bernonville-ext on 24/04/2017.
  */
object IngestV1 extends App {
  val csvInputPath = System.getProperty("airport.csv.inputpath")
  val parquetOutputPath = System.getProperty("airport.parquet.outputpath")
  val spark = SparkSession.builder()
    .appName("FirstModel")
    .getOrCreate()

  val sc = spark.sparkContext
  import spark.implicits._
  val inputRdd = sc.textFile(csvInputPath)
    .filter(!_.startsWith("Year"))
    .flatMap(line => {
      try {
        Seq(Flight(line.split(",")))
      } catch {
        case e: Throwable => Seq.empty
      }
    })

//  inputRdd.as[Flight].show()
  inputRdd.toDF.as[Flight].write.mode(SaveMode.Overwrite).parquet(s"$parquetOutputPath/flights")
}

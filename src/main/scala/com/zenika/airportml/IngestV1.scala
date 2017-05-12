package com.zenika.airportml

import com.zenika.airportml.model.Flight
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by g.bernonville-ext on 24/04/2017.
  */
object IngestV1 extends App {
  val csvInputPath = System.getProperty("airport.csv.inputpath", args(0))
  val parquetOutputPath = System.getProperty("airport.parquet.outputpath", args(1))
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
        case e: Throwable => {
          println(e)
          Seq.empty
        }
      }
    })

//  inputRdd.as[Flight].show()
  inputRdd.toDF.as[Flight].write.mode(SaveMode.Overwrite).partitionBy("Year").parquet(s"$parquetOutputPath/flights")
}

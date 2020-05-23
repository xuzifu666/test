package com.unicom.proj

import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkStarter extends Logging{

//  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport().getOrCreate()
    val collect: Array[(String, Int)] = spark.sparkContext.textFile("/wc").flatMap(_.split(" "))
      .map((_, 1)).reduceByKey(_ + _).collect
    collect.foreach(x => logger.info(x._1 +  ":" + x._2))

  }

}

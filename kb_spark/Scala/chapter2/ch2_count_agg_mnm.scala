/**
 * Usage: MnMcount <mnm_file_dataset>
 */

package main.scala.chapter2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MnMcount {
  def main(args: Array[String]) {
    val spark = SparkSession
        .builder
        .appName("MnMCount")
        .getOrCreate()

    if (args.length < 1) {
        println("[Error] Usage: MnMcount <mnm_file_dataset>")
        sys.exit(1)
    }

    // Read the file into a Spark DataFrame
    val mnmFile = args(0)
    val mnmDF = spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(mnmFile)
    println("-- -----------\n-- Full file")
    mnmDF.show(3)

    // Aggregate colors count by state and color
    val countMnMDF = mnmDF
        .select("State", "Color", "Count")
        .groupBy("State", "Color")
        .sum("Count")
        .orderBy(desc("sum(Count)"))
    println("-- ---------------\n-- Filtered file")
    countMnMDF.show(60)
    println(s"Total rows = ${countMnMDF.count()}\n")

    // Find the aggregate counts for California by filtering
    val caCountMnMDF = mnmDF
        .select("State", "Color", "Count")
        .where(col("State") === "CA")
        .groupBy("State", "Color")
        .sum("Count")
        .orderBy(desc("sum(Count)"))
    println("-- -------------------\n-- File: California data")
    caCountMnMDF.show(10)
    println(s"Total Rows: ${caCountMnMDF.count()}")

    // Stop spark session
    spark.stop()

    println("Counting M&M data is complete now :sunglasses:")
  }
}
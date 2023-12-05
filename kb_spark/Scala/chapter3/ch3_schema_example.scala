/**
 * Topic: DataFrame API - Example 3.7
 */

package main.scala.chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Schema_Example3_7 {
    def main(args: Array[String]) {

        val spark = SparkSession
            .builder
            .appName("Schema: Example 3-7")
            .getOrCreate()
            
        if (args.length <= 0) {
            println("[Err] Usage Example3_7 <file path to blogs.json>")
            System.exit(1)
        }
        
        val jsonFile = args(0)
        println(">> Jsonfile: " + jsonFile)

        // Create schema 
        val schema = StructType(Array(
            StructField("Id", IntegerType, false),
            StructField("First", StringType, false),
            StructField("Last", StringType, false),
            StructField("Url", StringType, false),
            StructField("Published", StringType, false),
            StructField("Hits", IntegerType, false),
            StructField("Campaings", ArrayType(StringType), false)
        ))
       
        // Create dataframe
        val blogsDF = spark.read.schema(schema).json(jsonFile)  // #TODO
        println(">> DataFrame Schema:")
        blogsDF.printSchema()
        println(">> DataFrame Content:")
        blogsDF.show()

        // ######################### Playing with columns #########################
        println(">> Print columns")
        println(blogsDF.columns)

        println(">> Print 'Id' column")
        println(blogsDF.col("Id"))

        println(">> Computing a value")
        blogsDF.select(expr("Hits * 2")).show(2)
        blogsDF.select(col("Hits") * 2).show(3)

        println(">> Adding new column based on some criteria/condition")
        blogsDF
            .withColumn("Big hitter", (expr("Hits > 10568")))
            .withColumn("Big hitter II", col("Hits") > 10568).show()

        println(">> Concatenating multiple columns in a new one")
        blogsDF
            .withColumn("AuthorsId", (concat(expr("First"),expr("Last"),expr("Id"))))
            .select(col("AuthorsId"))
            .show(4)

        // println(">> Selecting specific column with any 3 diff statements")
        // blogsDF.select(expr("Hits")).show(2)
        // blogsDF.select(col("Hits")).show(2)
        // blogsDF.select("Hits").show(2)

        println(">> Sorting dataframe by Id in descending order")
        blogsDF.sort(col("Id").desc).show()
        println(">> Sorting dataframe by Hits in asc order")
        blogsDF.sort($("Hits")).show()


        println(">> Yay! Yay!")
   }
}

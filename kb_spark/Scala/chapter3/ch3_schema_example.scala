/**
 * Topic: DataFrame API - Example 3.7
 */

package main.scala.chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

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

        println(">> Yay! Yay!")
   }
}

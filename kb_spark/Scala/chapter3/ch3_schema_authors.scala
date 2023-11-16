/**
 * Topic: DataFrame API
 */

 package main.scala.chapter3

 import org.apache.spark.sql.types._

 object SchemaAuthors {
    def main(args: Array[String]) {
        
        val schema = StructType(Array(
            StructField("author", StringType, false),
            StructField("title", StringType, false),
            StructField("pages", IntegerType, false)
        ))
        println("> Defining schema programmatically:")
        println(schema)

        val schemaDDL = "author STRING, title STRING, pages INT"
        println("> Defining schema using DDL:")
        println(schemaDDL)
    }
 }

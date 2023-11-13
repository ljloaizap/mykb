/**
 * Topic: Structuring Spark: Key merits and benefits
    ~ Expressivity
    ~ Simplicity
    ~ Composiability
    ~ Uniformity

   >> Using high-level DataFrame API
 */

 package main.scala.chapter2

 import org.apache.spark.sql.SparkSession
 import org.apache.spark.sql.functions.avg

 object AuthorsAges {
    def main(args: Array[String]) {
        
        val spark = SparkSession
            .builder
            .appName("AuthorsAges")
            .getOrCreate()

        val dataDF = spark.createDataFrame(Seq(("Brooke", 20), ("Denny", 31), 
            ("Jules", 30), ("TD", 35), ("Brooke", 25))).toDF("name", "age")
        
        val avgDF = dataDF.groupBy("name").agg(avg("age"))
        avgDF.show()
    }
 }
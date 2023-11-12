/* 
    Run following commands:
        cd kb_spark/spark-3.2.4-bin-hadoop2.7/bin/
        ./spark-shell


object BasicCount {
  def main(args: Array[String]): Unit = {
    println("Hello, World!")

    // ----------------------------------------------

    val strings = spark.read.text("../README.md")
    strings.show(10, false)
    strings.count()

    // ----------------------------------------------

    val strings = spark.read.text("../README.md")
    val filtered = strings.filter(col("value").contains("Spark"))
    filtered.count()

    // ----------------------------------------------

  }
}
*/

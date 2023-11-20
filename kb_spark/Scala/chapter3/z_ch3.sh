sbt clean package

#$SPARK_HOME/bin/spark-submit --class main.scala.chapter3.SchemaAuthors target/scala-2.12/main-scala-chapter3_2.12-1.0.jar
$SPARK_HOME/bin/spark-submit --class main.scala.chapter3.Schema_Example3_7 target/scala-2.12/main-scala-chapter3_2.12-1.0.jar ../../data/ch3_schema_example.json

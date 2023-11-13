''' Topic: Structuring Spark: Key merits and benefits
        ~ Expressivity
        ~ Simplicity
        ~ Composiability
        ~ Uniformity
    
    >> Using high-level DataFrame API
'''

# Demonstrating expressivity and composability

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg


# Create a dataframe using SparkSession
spark = (SparkSession
         .builder
         .appName("AuthorsAges")
         .getOrCreate())

data_df = spark.createDataFrame([("Brooke", 20), ("Denny", 31), ("Jules", 30),
                                 ("TD", 35), ("Brooke", 25)], ["name", "age"])

avg_df = data_df.groupBy("name").agg(avg("age"))
avg_df.show()

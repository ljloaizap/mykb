'''
    Run following commands:
        cd kb_spark/spark-3.2.4-bin-hadoop2.7/bin/
        ./pyspark
'''

# ----------------------------------------------

strings = spark.read.text("../README.md")
strings.show(10, False)
strings.count()

# ----------------------------------------------

strings = spark.read.text('../README.md')
filtered = strings.filter(strings.value.contains("Spark"))
filtered.count()

# ----------------------------------------------

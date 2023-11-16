'''
    Target: > Script to be used in pyspark shell!
            > Run following commands:
                cd kb_spark/spark-3.2.4-bin-hadoop2.7/bin/
                ./pyspark

    Topic: Structuring Spark: Key merits and benefits
        ~ Expressivity
        ~ Simplicity
        ~ Composiability
        ~ Uniformity
    
    >> Using low-level RDD API
'''

# Demonstrating expressivity and composability

dataRDD = sc.parallelize(
    [("Brooke", 20), ("Denny", 31), ("Jules", 30), ("TD", 35),
     ("Brooke", 25)])
agesRDD = (dataRDD
           .map(lambda x: (x[0], (x[1], 1)))
           .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
           .map(lambda x: (x[0], x[1][0]/x[1][1])))

agesRDD.foreach(lambda x: print(x, "\n"))

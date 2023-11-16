''' 
    Topic: DataFrame API
'''

from pyspark.sql.types import *

schema = StructType([
    StructField("author", StringType(), False),
    StructField("title", StringType(), False),
    StructField("pages", IntegerType(), False)
])
print("> Defining schema programmatically:")
print(schema)

# --

schema_ddl = "author STRING, title STRING, pages INT"
print("> Defining schema using DDL:")
print(schema_ddl)

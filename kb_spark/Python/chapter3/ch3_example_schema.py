''' 
    Topic: Example DataFrame Schema, example 3.6
'''

from pyspark.sql import SparkSession

schema = '`Id` INT, `First` STRING, `Last` STRING, `Url` STRING, `Published` STRING, `Hits` INT, `Campaings` ARRAY<STRING>'


# Create static data
data = [
    [1, 'Jules', 'Damji', 'https://tinyurl.1',
        '1/4/2016', 4535, ['twitter', 'linkedin']],
    [2, 'Brooke', 'Wenig', 'https://tinyurl.2',
        '5/5/2018', 8908, ['twitter', 'linkedin']],
    [3, 'Denny', 'Lee', 'https://tinyurl.3',
        '6/7/2019', 7659, ['web', 'twitter', 'FB']],
    [4, 'Tathagata', 'Das', 'https://tinyurl.4',
        '5/12/2018', 10568, ['twitter', 'FB']],
    [5, 'Matei', 'Zaharia', 'https://tinyurl.5', '5/14/2014',
        40578, ['web', 'twitter', 'FB', 'linkedin']],
    [6, 'Raynold', 'Xin', 'https://tinyurl.6',
        '3/2/2015', 25568, ['twitter', 'linkedin']]
]

# Main program
if __name__ == '__main__':
    spark = (SparkSession
             .builder
             .appName('Schema: Example 3-6')
             .getOrCreate())

    # Create a dataframe using the schema defined above
    blogs_df = spark.createDataFrame(data, schema)

    blogs_df.show(10)

    # Print the schema (kind of "pretty")
    print(blogs_df.printSchema())

    # Print the schema with StructType
    print(blogs_df.schema)

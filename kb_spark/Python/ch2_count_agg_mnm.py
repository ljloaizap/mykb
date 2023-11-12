import sys

from pyspark.sql import SparkSession


if __name__ == "__main__":
    print('\n'*3)
    if len(sys.argv) != 2:
        print('Usage: mnmcount <file>', file=sys.stderr)
        sys.exit(-1)

    # Build a SparkSession using the SparkSession APIs.
    # If one does not exist, then create an instance.
    # There can only be one SparkSession per JVM.
    spark = (SparkSession
             .builder
             .appName('Python_MnM_count')
             .getOrCreate())

    # Get the M&M data set filename from the command-line arguments
    mnm_file = sys.argv[1]

    # Read the file into a Spark DataFrame using the .CSV
    # format by inferring the schema and specifying that the
    # file contains a header, which provides column names for
    # comma-separated fields.
    mnm_df = (spark.read.format('csv')
              .option('header', 'true')
              .option('inferSchema', 'true')
              .load(mnm_file))

    # We use the DataFrame high-level APIs. Note
    # that we don't use RDDs at all. Because some of Spark's
    # functions return the same object, we can chain function calls.
    print('\n-------------\n* Total data:')
    count_mnm_df = (mnm_df
                    .select('State', 'Color', 'Count')
                    .groupBy('State', 'Color')
                    .sum('Count')
                    .orderBy('sum(Count)', ascending=False)
                    )

    count_mnm_df.show(n=60, truncate=False)
    print(f'Total Rows = {count_mnm_df.count()}')

    # What if we just want to see the data for a single state, e.g., CA?
    print('\n-------------\n* CA data:')
    ca_count_mnm_df = (mnm_df
                       .select('State', 'Color', 'Count')
                       .where(mnm_df.State == 'CA')
                       .groupBy('State', 'Color')
                       .sum('Count')
                       .orderBy('sum(Count)', ascending=False))
    ca_count_mnm_df.show(n=10, truncate=False)

    # Stop the SparkSession
    spark.stop()

    print('Tomatoes!')

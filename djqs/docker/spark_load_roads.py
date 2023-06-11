"""
Load the roads database into a local spark warehouse
"""
from pyspark.sql import SparkSession  # pylint: disable=import-error

print("Starting spark session...")
spark = (
    SparkSession.builder.master("local[*]")
    .appName("djqs-load-roads")
    .enableHiveSupport()
    .getOrCreate()
)
with open("/code/docker/spark.roads.sql", encoding="UTF-8") as sql_file:
    queries = sql_file.read()
    for query in queries.split(";"):
        if query.strip():
            print("Submitting query...")
            spark.sql(query)
            print("Query completed...")

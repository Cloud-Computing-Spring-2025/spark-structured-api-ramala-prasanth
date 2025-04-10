from pyspark.sql import SparkSession
# Create Spark session
spark = SparkSession.builder.appName("MusicMetadata").getOrCreate()

# Load the CSV into a Spark DataFrame
df = spark.read.option("header", True).csv("songs_metadata.csv")

# Show all rows
df.show(truncate=False, n=200)  # Adjust n if needed

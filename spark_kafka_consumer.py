from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaMouseEventStreamingToHDFS") \
    .getOrCreate()

# Set the log level
spark.sparkContext.setLogLevel("WARN")

# Define the schema for the JSON data
schema = StructType([
    StructField("x", IntegerType(), True),
    StructField("y", IntegerType(), True),
    StructField("button", StringType(), True)
])

# Create a DataFrame representing the stream of input lines from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "mouse_events_topic") \
    .option("startingOffsets", "earliest") \
    .load()

# Cast the value column to string and parse JSON
parsed_df = df.selectExpr("CAST(value AS STRING) AS json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Write the streaming DataFrame to the console for visualization
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "hdfs://localhost:9000/user/son/mousedata2") \
    .option("checkpointLocation", "hdfs://localhost:9000/user/son/mousedata2") \
    .start()

# Await termination
query.awaitTermination()

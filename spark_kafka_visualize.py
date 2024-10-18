from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import matplotlib.pyplot as plt
import pandas as pd
import time

# Initialize Spark session (if not already done)
spark = SparkSession.builder \
    .appName("KafkaMouseEventStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("FATAL")

def plot_realtime_coordinates():
    plt.ion()  # Enable interactive mode
    plt.figure(figsize=(10, 6))  # Set figure size once

    x_data = []  # Initialize lists to store coordinates
    y_data = []
    clicked_x_data = []
    clicked_y_data = []

    # Set up the schema for the JSON data
    schema = StructType([
        StructField("x", IntegerType(), True),
        StructField("y", IntegerType(), True),
        StructField("button", StringType(), True)
    ])

    # Create a streaming DataFrame to read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "mouse_events_topic") \
        .option("startingOffsets", "earliest") \
        .load()

    # Define processing of the stream
    processed_df = df.selectExpr("CAST(value AS STRING) AS json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*") \
        .filter(col("x").isNotNull() & col("y").isNotNull())

    # Function to update the plot
    def update_plot(new_data):
        nonlocal x_data, y_data, clicked_x_data, clicked_y_data

        if not new_data.empty:
            clicked_data = new_data[new_data['button'] == '0']

            # Append new data to the lists
            x_data.extend(new_data['x'].tolist())
            y_data.extend(new_data['y'].tolist())
            clicked_x_data.extend(clicked_data['x'].tolist())
            clicked_y_data.extend(clicked_data['y'].tolist())

            plt.clf()  # Clear the current figure

            # Plot the data as a line plot
            plt.plot(x_data, y_data, color='skyblue', linewidth=0.5)  # Line plot
            plt.scatter(clicked_x_data, clicked_y_data, color='r', s=10)
            plt.title('Real-Time Mouse Coordinates')
            plt.xlabel('X Coordinate')
            plt.ylabel('Y Coordinate')
            plt.gca().invert_yaxis()
            plt.gca().xaxis.tick_top()
            plt.legend(['Mouse Path', 'Clicks'])

            # Draw the updated plot
            plt.draw()
            plt.pause(1)  # Shorter pause for responsiveness

    # Create a query that writes the stream to a memory sink
    query = processed_df.writeStream \
        .outputMode("append") \
        .format("memory") \
        .queryName("mouse_events") \
        .start()

    try:
        while True:
            # Fetch the latest data from the memory table
            pandas_df = spark.sql("SELECT * FROM mouse_events").toPandas()
            update_plot(pandas_df)

    except KeyboardInterrupt:
        print("Stopping the stream and closing the plot...")
    finally:
        query.stop()
        plt.close()

# Call the function to start plotting
plot_realtime_coordinates()

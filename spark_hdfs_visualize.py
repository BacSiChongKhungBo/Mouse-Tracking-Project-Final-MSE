from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import matplotlib.pyplot as plt
import pandas as pd
import time

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MouseEventPlotting") \
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
        StructField("clicked", StringType(), True)
    ])
    
    hdfs_path = "hdfs://localhost:9000/user/son/mousedata66"
    
    # Read the JSON data from HDFS as a streaming DataFrame
    df = spark.readStream.schema(schema).json(hdfs_path)

    # Process the DataFrame to filter null values
    processed_df = df.filter(col("x").isNotNull() & col("y").isNotNull())

    # Write the stream to a temporary sink
    query = processed_df.writeStream \
        .outputMode("append") \
        .format("memory") \
        .queryName("mouse_events") \
        .start()

    try:
        while True:
            # Fetch the latest data from the memory table
            pandas_df = spark.sql("SELECT * FROM mouse_events").toPandas()
            
            if not pandas_df.empty:
                new_data = pandas_df.iloc[len(x_data):]  # Only fetch new rows

                clicked_data = new_data[new_data['clicked'] == '0']

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

            time.sleep(1)  # Simulate real-time delay between updates

    except KeyboardInterrupt:
        print("Stopping the stream and closing the plot...")

    finally:
        query.stop()  # Stop the streaming query
        plt.close()  # Close the plot when finished

# Call the function to start plotting
plot_realtime_coordinates()

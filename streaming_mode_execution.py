import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from textblob import TextBlob
from pyspark.sql import functions as F
from pyspark.sql.streaming import DataStreamWriter

# Initialize Spark session for streaming mode
spark = SparkSession.builder \
    .appName("StreamingModeExecution") \
    .config("spark.jars", "/Users/bhargavbg/Desktop/dbt2/mysql-connector-java-8.0.27/mysql-connector-java-8.0.27.jar") \
    .getOrCreate()

# Define UDF for sentiment analysis using TextBlob
def get_sentiment(tweet):
    analysis = TextBlob(tweet)
    return analysis.sentiment.polarity

# Register the UDF
sentiment_udf = udf(get_sentiment, DoubleType())

# MySQL connection properties
url = "jdbc:mysql://localhost:3306/tweet_analysis"
properties = {
    "user": "root",
    "password": "your_password",  # Update with your MySQL password
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Function to process streaming data from Kafka
def process_streaming_data():
    # Read data from Kafka topic
    tweets_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "sentiment_results") \
        .load()

    # Convert Kafka binary data to string
    tweets_stream_df = tweets_stream_df.selectExpr("CAST(value AS STRING) AS tweet")

    # Apply sentiment analysis on the tweets using the UDF
    tweets_with_sentiment_stream = tweets_stream_df.withColumn("sentiment", sentiment_udf(col("tweet")))

    # Write the results back to MySQL
    query = tweets_with_sentiment_stream.writeStream \
        .outputMode("append") \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", "tweets") \
        .option("user", "root") \
        .option("password", "your_password") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .start()

    print("Streaming mode execution started...")

    # Wait for the streaming query to process data
    query.awaitTermination()

# Start streaming data processing
process_streaming_data()

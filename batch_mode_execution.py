import time
import mysql.connector
from textblob import TextBlob
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from sklearn.metrics import mean_squared_error

# Initialize Spark session with MySQL JDBC driver
spark = SparkSession.builder \
    .appName("BatchModeExecution") \
    .config("spark.jars", "/Users/bhargavbg/Desktop/dbt2/mysql-connector-java-8.0.27/mysql-connector-java-8.0.27.jar") \
    .getOrCreate()  # Proper line continuation here

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
    "password": "bbgdbms123",  # Update with your MySQL password
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Load data from MySQL table
start_time = time.time()  # Start time for batch processing

tweets_df = spark.read.jdbc(url=url, table="tweets", properties=properties)

# Apply sentiment analysis on tweets
tweets_with_sentiment = tweets_df.withColumn("sentiment", sentiment_udf(col("tweet")))

# Show the sentiment analysis results
tweets_with_sentiment.show()

# Measure the execution time for batch processing
batch_processing_time = time.time() - start_time
print(f"Batch Mode Execution Time: {batch_processing_time} seconds")

# Optional: Compute accuracy (if you have labeled sentiment data)
# You can compare the predicted sentiment with actual sentiment if you have labeled data.
actual_sentiment = [0.1, 0.5, -0.2]  # Example actual sentiments
predicted_sentiment = [0.05, 0.6, -0.3]  # Example predicted sentiments from your model

mse = mean_squared_error(actual_sentiment, predicted_sentiment)
rmse = mse ** 0.5

print(f"Mean Squared Error: {mse}")
print(f"Root Mean Squared Error: {rmse}")

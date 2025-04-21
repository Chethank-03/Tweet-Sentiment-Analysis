from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from textblob import TextBlob
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
import mysql.connector

# Initialize Spark session with MySQL JDBC driver
spark = SparkSession.builder \
    .appName("TweetSentimentAnalysis") \
    .config("spark.jars", "/Users/bhargavbg/Desktop/dbt2/mysql-connector-java-8.0.27/mysql-connector-java-8.0.27.jar") \
    .getOrCreate()

# Define UDF for sentiment analysis using TextBlob
def get_sentiment(tweet):
    if tweet is not None:
        analysis = TextBlob(tweet)
        return analysis.sentiment.polarity
    return None

# Register the UDF with DoubleType return type
sentiment_udf = udf(get_sentiment, DoubleType())

# MySQL connection properties
url = "jdbc:mysql://localhost:3306/tweet_analysis"
properties = {
    "user": "root",
    "password": "bbgdbms123",  # Update with your MySQL password
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Load data from MySQL table
tweets_df = spark.read.jdbc(url=url, table="tweets", properties=properties)

# Apply sentiment analysis on the 'tweet' column
tweets_with_sentiment = tweets_df.withColumn("sentiment", sentiment_udf(col("tweet")))

# Show sentiment analysis results
tweets_with_sentiment.show()

# Save the results back into the MySQL table (overwrite existing data)
tweets_with_sentiment.write.jdbc(url=url, table="tweets", mode="overwrite", properties=properties)

print("Sentiment analysis completed and results saved to the database.")

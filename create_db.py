import mysql.connector
from textblob import TextBlob

# Function to get sentiment score
def get_sentiment(tweet):
    analysis = TextBlob(tweet)
    return analysis.sentiment.polarity  # Returns sentiment score between -1 (negative) and 1 (positive)

# Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="bbgdbms123",  # Update with your MySQL password
    database="tweet_analysis"
)

cursor = conn.cursor()

# Create database if not exists
cursor.execute("CREATE DATABASE IF NOT EXISTS tweet_analysis;")
cursor.execute("USE tweet_analysis;")

# Create tweets table if it doesn't exist
cursor.execute("""
CREATE TABLE IF NOT EXISTS tweets (
    id INT AUTO_INCREMENT PRIMARY KEY,
    tweet VARCHAR(255),
    sentiment DOUBLE
);
""")

# Insert some sample tweets with sentiment values
sample_tweets = [
    "I love Python programming!",
    "AI is transforming the world.",
    "The stock market is down today.",
    "Can't wait for the weekend!",
    "Learning machine learning is fun!"
]

for tweet in sample_tweets:
    sentiment = get_sentiment(tweet)  # Calculate sentiment
    cursor.execute("INSERT INTO tweets (tweet, sentiment) VALUES (%s, %s);", (tweet, sentiment))

# Commit and close connection
conn.commit()
cursor.close()
conn.close()

print("Database setup and sample tweets with sentiment inserted successfully.")

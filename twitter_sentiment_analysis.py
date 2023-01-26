from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField, FloatType, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import re
from pyspark.ml.feature import RegexTokenizer
from textblob import *
import os

# fix UDF
os.environ['OBJC_DISABLE_INITIALIZE_FORK_SAFETY'] = 'YES'


def cleanTweet(tweet: str) -> str:
    tweet = re.sub(r'http\S+', '', str(tweet))
    tweet = re.sub(r'bit.ly/\S+', '', str(tweet))
    tweet = tweet.strip('[link]')

    # remove users
    tweet = re.sub('(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))
    tweet = re.sub('(@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    # remove puntuation
    my_punctuation = '!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~•@â'
    tweet = re.sub('[' + my_punctuation + ']+', ' ', str(tweet))

    # remove number
    tweet = re.sub('([0-9]+)', '', str(tweet))

    # remove hashtag
    tweet = re.sub('(#[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    return tweet


# Create a function to get the subjectivity
def getSubjectivity(tweet: str) -> float:
    return TextBlob(tweet).sentiment.subjectivity


# Create a function to get the polarity
def getPolarity(tweet: str) -> float:
    return TextBlob(tweet).sentiment.polarity


def getSentiment(polarityValue: int) -> str:
    if polarityValue < 0:
        return 'Negative'
    elif polarityValue == 0:
        return 'Neutral'
    else:
        return 'Positive'


def write_row_in_mongo(df, epoch_id):
    df.write.format("mongodb").mode("append").save()
    pass


mongoURL = "mongodb+srv://username:Password@cluster0.xv3s8sy.mongodb.net/test.demo_collection4?retryWrites" \
           "=true&w=majority"


# epoch
def write_row_in_mongo(df, epoch_id):
    df.write.format("mongodb").mode("append").option("uri", mongoURL).save()
    pass


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("TwitterSentimentAnalysis") \
        .config("spark.mongodb.read.connection.uri", mongoURL) \
        .config("spark.mongodb.write.connection.uri", mongoURL) \
        .config("spark.jars",
                "/Users/benminh1201/spark/spark-3.2.3-bin-hadoop2.7/jars/mongo-spark-connector_2.12-10.1.0.jar") \
        .config("spark.jars", "/Users/benminh1201/spark/spark-3.2.3-bin-hadoop2.7/jars/mongodb-driver-core-4.8.2.jar") \
        .config("spark.jars",
                "/Users/benminh1201/spark/spark-3.2.3-bin-hadoop2.7/jars/spark-sql-kafka-0-10_2.12-3.2.3.jar") \
        .config("spark.jars",
                "/Users/benminh1201/spark/spark-3.2.3-bin-hadoop2.7/jars/spark-streaming-kafka-0-10-assembly_2.12-3.2"
                ".3.jar") \
        .getOrCreate()

    # Set Kafka options
    kafka_options = {
        "kafka.bootstrap.servers": "",
        "subscribe": "twitter",
        "startingOffsets": "earliest",
        "kafka.group.id": "twitter.group.v1",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required "
                                  "username='' "
                                  "password='';", }

    df = spark \
        .readStream \
        .format("kafka") \
        .options(**kafka_options) \
        .load()

    mySchema = StructType([StructField("text", StringType(), True)])
    # Get only the "text" from the information we receive from Kafka. The text is the tweet produce by a user

    values = df.select(from_json(df.value.cast("string"), mySchema).alias("tweet"))

    df1 = values.select("tweet.*")
    clean_tweets = F.udf(cleanTweet, StringType())
    raw_tweets = df1.withColumn("processed_text", clean_tweets(col("text")))

    subjectivity = F.udf(getSubjectivity, FloatType())
    polarity = F.udf(getPolarity, FloatType())
    sentiment = F.udf(getSentiment, StringType())

    subjectivity_tweets = raw_tweets.withColumn('subjectivity', subjectivity(col("processed_text")))
    polarity_tweets = subjectivity_tweets.withColumn("polarity", polarity(subjectivity_tweets['processed_text']))
    sentiment_tweets = polarity_tweets.withColumn("sentiment", sentiment(polarity_tweets['polarity']))

    query = sentiment_tweets.writeStream \
        .foreachBatch(write_row_in_mongo) \
        .start()

    query.awaitTermination()

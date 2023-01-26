from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# create a SparkSession
spark = SparkSession.builder.appName("MongoDBSinkExample").getOrCreate()

# read stream from kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
    .option("subscribe", "topic1") \
    .load()

# select the value column
df = df.selectExpr("cast (value as string) as json")

# parse json
df = df.select(from_json(col("json"), schema).alias("parsed_json"))

# extract the desired field
df = df.select("parsed_json.*")

# write stream to MongoDB Atlas
query = df.writeStream.outputMode("append").format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://<username>:<password>@<host>:<port>/<database>.<collection>").start()
query.awaitTermination()

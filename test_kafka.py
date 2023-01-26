from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType, StructField, FloatType

mongoURL = "mongodb+srv://benminh1201:Benminhh1201@cluster0.xv3s8sy.mongodb.net/test.demo_collection?retryWrites=true&w=majority"

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("TwitterSentimentAnalysis") \
        .config("spark.mongodb.read.connection.uri", mongoURL) \
        .config("spark.mongodb.write.connection.uri", mongoURL) \
        .config("spark.jars",
                "/Users/benminh1201/spark/spark-3.2.3-bin-hadoop2.7/jars/mongo-spark-connector_2.12-10.1.0.jar") \
        .config("spark.jars", "/Users/benminh1201/spark/spark-3.2.3-bin-hadoop2.7/jars/mongodb-driver-core-4.8.2.jar") \
        .config("spark.jars", "/Users/benminh1201/spark/spark-3.2.3-bin-hadoop2.7/jars/spark-sql-kafka-0-10_2.12-3.2.3.jar") \
        .config("spark.jars", "/Users/benminh1201/spark/spark-3.2.3-bin-hadoop2.7/jars/spark-streaming-kafka-0-10-assembly_2.12-3.2.3.jar") \
        .getOrCreate()

    # Set Kafka options
    kafka_options = {
        "kafka.bootstrap.servers": "pkc-ldvr1.asia-southeast1.gcp.confluent.cloud:9092",
        "subscribe": "twitter",
        "startingOffsets": "earliest",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.group.id": "spark-streaming",
        "kafka.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required "
                                  "username='72MRJAH534PWMIW3' "
                                  "password='unlSc6dMs6zGbeT04wSOUuX6CvCeQx6vMxGDEzuro4Ar9FLAAlQKU8qcA4riCEtx';", }

    # df = spark \
    #     .readStream \
    #     .format("kafka") \
    #     .options(**kafka_options) \
    #     .load()
    #
    # mySchema = StructType([StructField("text", StringType(), True)])
    # json_df = df.selectExpr("CAST(value AS STRING)") \
    #     .select(from_json(col("value"), mySchema).alias("data")) \
    #     .select("data.*")
    #
    # query = json_df.writeStream \
    #     .format("mongodb") \
    #     .option("checkpointLocation", "/tmp/pyspark/")\
    #     .option("forceDeleteTempCheckpointLocation", "true")\
    #     .outputMode("append").start()

# Print the value column to the console
#     query = json_df.writeStream \
#         .outputMode("append") \
#         .format("console") \
#         .start()

    # query.awaitTermination()

    people = spark.createDataFrame([("Bilbo Baggins",  50), ("Gandalf", 1000), ("Thorin", 195), ("Balin", 178), ("Kili", 77),
                                    ("Dwalin", 169), ("Oin", 167), ("Gloin", 158), ("Fili", 82), ("Bombur", None)], ["name", "age"])

    people.write.format("mongodb").mode("append").save()
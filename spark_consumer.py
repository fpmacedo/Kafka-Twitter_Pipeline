import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import col


spark = SparkSession \
    .builder \
    .appName("Twitter-Streaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

sc = spark.sparkContext

# Create stream dataframe setting kafka server, topic and offset option
# Subscribe to 1 topic
df = (spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:29092") \
  .option("subscribe", "twitter") \
  .option("startingOffsets", "earliest") \
  .load())


# Convert binary to string key and value
df1 = (df
    .withColumn("key", df["key"].cast(StringType()))
    .withColumn("value", df["value"].cast(StringType())))

# Event data schema
schema_twitter = StructType(
    [StructField("id",LongType(),True),
     StructField("text",StringType(),True)])

# Create dataframe setting schema for event data
df_twitts = (df1
           # Sets schema for event data
           .withColumn("value", from_json("value", schema_twitter))
          )

df_twitts_formatted = df_twitts.select(col("key"), col("topic"), col("value.id"), col("value.text"))

df_twitts_formatted.printSchema()

# Start query stream over stream dataframe

queryStream =(
    df_twitts_formatted \
    .writeStream \
    .format("parquet") \
    .queryName("ingestion") \
    .option("checkpointLocation", "checkpoint_folder") \
    .option("path", "twitter_parquet") \
    .outputMode("append") \
    .start())

queryStream.awaitTermination()
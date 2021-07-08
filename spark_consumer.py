import pyspark
from time import sleep
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, IntegerType, DoubleType
from pyspark.sql.functions import col


def create_spark_session():
    
    """
    Create the spark session with the passed configs.

    Returns: 
        - spark: spark session
    """

    spark = SparkSession \
        .builder \
        .appName("Twitter-Streaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .getOrCreate()
    
    return spark


def process_twitter_streaming(spark, bootstrap_server, output_path):

    """
    Perform ETL on song_data to create the songs and artists dimensional tables: 
    - Read streaming tweets using Kafka; 
    - Format the the received data;
    - Write the data into parquet files.
    
    Parameters:
    - spark: spark session
    - bootstrap_server: the Kafka bootstrap server adress
    - output_data : path to output files
    """

    # Create stream dataframe setting kafka server, topic and offset option
    # Subscribe to 1 topic
    df = (spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers",bootstrap_server ) \
    .option("subscribe", "twitter") \
    .option("startingOffsets", "earliest") \
    .load())


    # Convert binary to string key and value
    df1 = (df
        .withColumn("key", df["key"].cast(StringType()))
        .withColumn("value", df["value"].cast(StringType())))

    # Event data schema
    schema_twitter = StructType(
        [StructField("user_id",LongType(),True),
        StructField("user_name",StringType(),True),
        StructField("verified",BooleanType(),True),
        StructField("followers_count",IntegerType(),True),
        StructField("friends_count",IntegerType(),True),
        StructField("user_created_at",DoubleType(),True),
        StructField("tweet_id",LongType(),True),
        StructField("tweet_message",StringType(),True),
        StructField("tweet_created_at",DoubleType(),True),
        ])

    # Create dataframe setting schema for event data
    df_twitts = (df1
            # Sets schema for event data
            .withColumn("value", from_json("value", schema_twitter))
            )

    #Select columns from the formatted dataset
    df_twitts_formatted = df_twitts.select(
                                            col("topic"),
                                            col("value.user_id"),
                                            col("value.user_name"),
                                            col("value.verified"),
                                            col("value.followers_count"),
                                            col("value.friends_count"),
                                            col("value.user_created_at").cast("timestamp"),
                                            col("value.tweet_id"),
                                            col("value.tweet_message"),
                                            col("value.tweet_created_at").cast("timestamp"),
                                        )

    # Start query stream over stream dataframe
    queryStream =(
        df_twitts_formatted \
        .writeStream \
        .format("parquet") \
        .queryName("ingestion") \
        .option("checkpointLocation", "checkpoint_folder") \
        .option("path", output_path) \
        .outputMode("append") \
        .start())

    queryStream.awaitTermination()
            

def main():

    #Create spark session
    spark = create_spark_session()
    #Proceses tweets using streaming
    process_twitter_streaming(spark, "localhost:29092", "twitter.parquet")

if __name__ == "__main__":
    main()
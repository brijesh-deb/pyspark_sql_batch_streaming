from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    print("Streaming Application Started ...")

    spark = SparkSession \
            .builder \
            .appName("Structured Streaming - JSON") \
            .master("local[*]") \
            .getOrCreate()

    # set log level to ERROR to suppress detailed logs
    spark.sparkContext.setLogLevel('ERROR')

    # For stream processing Schema needs to be provided, instead of expecting Spark to infer it automatically
    input_json_schema = StructType([
        StructField("registration_dttm", StringType(), True),
        StructField("id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("cc", StringType(), True),
        StructField("country", StringType(), True),
        StructField("birthdate", StringType(), True),
        StructField("salary", DoubleType(), True),
        StructField("title", StringType(), True),
        StructField("comments", StringType(), True)
    ])

    # Read Json files as streams as they are added to inout_data folder
    stream_df = spark.readStream.json("../input_data/json",schema=input_json_schema)

    print(stream_df.isStreaming)
    print(stream_df.printSchema())

    # Group incoming data into countries and count each group
    stream_count_df = stream_df \
        .groupBy("country") \
        .count() \
        .orderBy("count", ascending=False) \
        .limit(10)

    # use Checkpoint to ensure fault tolerance of streams
    write_stream_query = stream_count_df\
        .writeStream\
        .outputMode("complete")\
        .format("console")\
        .option("checkpointLocation", "streaming-checkpoint-loc-json")\
        .trigger(processingTime="10 second")\
        .start()

    write_stream_query.awaitTermination()
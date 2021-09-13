from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__=="__main__":
    print("Hello")
    sparksession = SparkSession.builder \
                    .appName("file_streaming") \
                    .master("local[*]") \
                    .getOrCreate()

    # registration_dttm,id,first_name,last_name,email,gender,ip_address,cc,country,birthdate,salary,title,comments
    input_csv_schema = StructType([
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

    stream_df = sparksession.readStream.csv("../input_data/csv", header=True,schema=input_csv_schema)

    stream_df.printSchema()

    stream_df_query = stream_df.writeStream.format("console").start()
    stream_df_query.awaitTermination()

    print("Application Completed.")

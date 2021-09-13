from pyspark.sql import SparkSession

if __name__ == "__main__":
    print("Application Started ...")

    sparksession = SparkSession \
            .builder \
            .appName("Read CSV Data - Batch") \
            .master("local[*]") \
            .getOrCreate()

    batch_df = sparksession \
                .read \
                .format("csv") \
                .option("header", "true") \
                .load(path="../input_data/csv")

    batch_df.printSchema()
    batch_df.show(10, False)
    sparksession.stop()
    print("Application Completed.")


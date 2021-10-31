from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf

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
                .option("inferSchema", value=True) \
                .load(path="../batch_data")\

    batch_df.show(5, False)
    batch_df.printSchema()
    print('Number of rows: ' + str(batch_df.count()))

    # different select operations
    batch_df.select('*').show(10)
    batch_df.select('Name','Age').show(10)
    batch_df.where(batch_df.Age>25).show(10)
    batch_df.where((batch_df.Age > 25) & (batch_df.Survived == 1)).show(5)

    # aggregate functions
    batch_df.agg({'Fare':'avg'}).show(1)
    batch_df.groupBy('Pclass').agg({'Fare':'avg'}).show(10)
    batch_df.filter(batch_df.Age > 25).agg({'Fare': 'avg'}).show(1)

    # create a user define function (UDF)
    def round_float(x):
        return int(x)

    round_float_udf = udf(round_float, IntegerType())
    batch_df.select('PassengerId', 'Fare', round_float_udf('Fare').alias('Fare Rounded Down')).show(5)

    # create a view
    batch_df.createOrReplaceTempView("Titanic")
    sparksession.sql('select * from Titanic').show(10)

    sparksession.stop()
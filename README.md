# pyspark_sql_batch_streaming
PySpark SQL sampes for batch processing and unstructured streaming. Uses Python 3.9 and PySpark 3.1.2
## Running the programs
- Open the project in PyCharm
- Create a virtual environment and add PySpark
- Run the files directly from PyCharm; make sure Spark folder is correct in Run > Edit Configurations > Environment Variables
## Batch processing
- Batch processing of CSV file using Spark SQL
- Select, filter, aggregate operations
- User defined function (UDF)
- Create temp view
## Structured streaming
- Processing of files in different formats. 
  - file_streaming_json.py: process JSON file. 
- Remove all files from input_data folder before running, and then add the files one by one to simulate incoming files. 
- Remove checkpoint folder for rerun

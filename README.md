# pyspark_sql_batch_streaming
PySpark SQL sampes for batch processing and unstructured streaming. Uses Python 3.9 and PySpark 3.1.2
## Structured streaming
- Processing of files in different formats. 
  - file_streaming_json.py: JSON file format. Shows checkpointing
- Remove all files from input_data folder before running, and then add the files one by one to simulate incoming files. 
- Remove checkpoint folder for rerun

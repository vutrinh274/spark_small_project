from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

SPARK_EXECUTOR_INSTANCES = 2
SPARK_EXECUTOR_CORES = 2
SPARK_EXECUTOR_MEMORY = "2g"
SPARK_CORES_MAX = SPARK_EXECUTOR_INSTANCES * SPARK_EXECUTOR_CORES
SPARK_SQL_FILES_MAXPARTITIONBYTES = 256 * 1024 * 1024

spark = SparkSession.builder \
    .appName("my_app") \
    .master("spark://spark-master:7077") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "/tmp/spark-events") \
    .config("spark.eventLog.logBlockUpdates.enabled", "true") \
    .config("spark.executor.instances", SPARK_EXECUTOR_INSTANCES) \
    .config("spark.executor.cores", SPARK_EXECUTOR_CORES) \
    .config("spark.cores.max", SPARK_CORES_MAX) \
    .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY) \
    .config("spark.sql.files.maxPartitionBytes", SPARK_SQL_FILES_MAXPARTITIONBYTES) \
    .getOrCreate()

try:

    input_path = "file:///opt/spark/work-dir/spark-warehouse/data/*.parquet"
    
    print(f"--- Reading data from {input_path} ---")
    
    df = spark.read.parquet(input_path)
    # df.cache()
    # df.count()
    
    result = df.groupBy("l_linenumber") \
           .agg(
               F.sum("l_quantity").alias("sum_l_quantity"),
               F.avg("l_extendedprice").alias("avg_price"),
               F.sum("l_discount").alias("sum_l_discount"),
               F.min("l_orderkey").alias("l_orderkey"),
               F.min("l_partkey").alias("l_partkey"),
               F.min("l_suppkey").alias("l_suppkey"),
               F.min("l_returnflag").alias("l_returnflag"),
               F.min("l_linestatus").alias("l_linestatus"),
               F.min("l_shipdate").alias("l_shipdate"),
               F.min("l_commitdate").alias("l_commitdate"),
               F.min("l_receiptdate").alias("l_receiptdate"),
               F.min("l_shipinstruct").alias("l_shipinstruct"),
               F.min("l_shipmode").alias("l_shipmode"),
               F.min("l_comment").alias("l_comment")
           )
    
    result.show(10)
    

    print("--- Job Completed Successfully ---")

finally:
    spark.stop()




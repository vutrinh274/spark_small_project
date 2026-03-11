from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("my_app") \
    .master("spark://spark-master:7077") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "/tmp/spark-events") \
    .config("spark.executor.instances", "2") \
    .config("spark.cores.max", "4") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.memory", "5g") \
    .config("spark.deploy.spreadOutApps", "true") \
    .config("spark.eventLog.logBlockUpdates.enabled", "true") \
    .config("spark.sql.files.maxPartitionBytes", 268435456) \
    .getOrCreate()

try:

    input_path = "file:///opt/spark/work-dir/spark-warehouse/data/0000.parquet"
    
    print(f"--- Reading data from {input_path} ---")
    
    df = spark.read.parquet(input_path)
    # df.persist(StorageLevel.MEMORY_ONLY)
    result = df.groupBy("l_linenumber") \
           .agg(
               F.sum("l_quantity").alias("sum_l_quantity"),
               F.avg("l_extendedprice").alias("avg_price"),
               F.sum("l_discount").alias("sum_l_discount"),
               F.first("l_orderkey").alias("l_orderkey"),
               F.first("l_partkey").alias("l_partkey"),
               F.first("l_suppkey").alias("l_suppkey"),
               F.first("l_returnflag").alias("l_returnflag"),
               F.first("l_linestatus").alias("l_linestatus"),
               F.first("l_shipdate").alias("l_shipdate"),
               F.first("l_commitdate").alias("l_commitdate"),
               F.first("l_receiptdate").alias("l_receiptdate"),
               F.first("l_shipinstruct").alias("l_shipinstruct"),
               F.first("l_shipmode").alias("l_shipmode"),
               F.first("l_comment").alias("l_comment")
           )
    

    result.show(10)
    

    print("--- Job Completed Successfully ---")

finally:
    spark.stop()

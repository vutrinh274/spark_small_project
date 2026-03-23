from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

# --- Spark cluster resource configuration ---
SPARK_EXECUTOR_INSTANCES = 2  # Number of executor JVMs to launch
SPARK_EXECUTOR_CORES = 2  # CPU cores per executor
SPARK_EXECUTOR_MEMORY = "2g"  # RAM per executor
SPARK_CORES_MAX = (
    SPARK_EXECUTOR_INSTANCES * SPARK_EXECUTOR_CORES
)  # Total cores across cluster
SPARK_SQL_FILES_MAXPARTITIONBYTES = (
    256 * 1024 * 1024
)  # Max size of each partition when reading files (256MB)

# --- Build SparkSession with cluster and event-logging settings ---
# Event log configs enable the Spark History Server to review past jobs
spark = (
    SparkSession.builder.appName("my_app")
    .master("spark://spark-master:7077")
    .config("spark.eventLog.enabled", "true")
    .config("spark.eventLog.dir", "/tmp/spark-events")
    .config("spark.eventLog.logBlockUpdates.enabled", "true")
    .config("spark.executor.instances", SPARK_EXECUTOR_INSTANCES)
    .config("spark.executor.cores", SPARK_EXECUTOR_CORES)
    .config("spark.cores.max", SPARK_CORES_MAX)
    .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
    .config("spark.sql.files.maxPartitionBytes", SPARK_SQL_FILES_MAXPARTITIONBYTES)
    .getOrCreate()
)

try:

    # Read all parquet files from the data directory (TPC-H lineitem table)
    input_path = "file:///opt/spark/work-dir/spark-warehouse/data/*.parquet"

    print(f"--- Reading data from {input_path} ---")

    df = spark.read.parquet(input_path)
    # df.cache()   # Optional: cache DataFrame in memory to speed up repeated access
    # df.count()   # Optional: trigger caching by forcing a full scan

    # Group by line number and compute aggregations across the lineitem table
    result = df.groupBy("l_linenumber").agg(
        F.sum("l_quantity").alias("sum_l_quantity"),  # Total quantity per line number
        F.avg("l_extendedprice").alias("avg_price"),  # Average price per line number
        F.sum("l_discount").alias("sum_l_discount"),  # Total discount per line number
        F.min("l_orderkey").alias(
            "l_orderkey"
        ),  # Grab one representative value for remaining columns
        F.min("l_partkey").alias("l_partkey"),
        F.min("l_suppkey").alias("l_suppkey"),
        F.min("l_returnflag").alias("l_returnflag"),
        F.min("l_linestatus").alias("l_linestatus"),
        F.min("l_shipdate").alias("l_shipdate"),
        F.min("l_commitdate").alias("l_commitdate"),
        F.min("l_receiptdate").alias("l_receiptdate"),
        F.min("l_shipinstruct").alias("l_shipinstruct"),
        F.min("l_shipmode").alias("l_shipmode"),
        F.min("l_comment").alias("l_comment"),
    )

    # Display first 10 rows of the aggregated result
    result.show(10)

    print("--- Job Completed Successfully ---")

# Ensure SparkSession is stopped even if an error occurs
finally:
    spark.stop()

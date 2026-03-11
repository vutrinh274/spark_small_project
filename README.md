# Run Spark cluster using Docker
```docker compose up -d```

# Generate 20GB of data

## Generate TCPH data
Note: If you running out of memory, you can change the -s to lower value, such as 5

```bench tpch gen -s 10```

## Copy into the folder data
```cp ./tpch_data/parquet/sf=10/n=1/lineitem/0000.parquet ./data/0000.parquet```

## Run the script to reach the 20GB data size
```chmod +x duplicate.sh && ./duplicate.sh 20```

# Run the Spark application
```docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/spark-warehouse/data/main.py```
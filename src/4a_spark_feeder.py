import time
import os
import sys
import shutil

os.environ['HADOOP_HOME'] = "C:\\winutils"
os.environ['JAVA_HOME'] = "C:\\Program Files\\Microsoft\\jdk-11.0.21.9-hotspot"
os.environ['HADOOP_USER_NAME'] = "administrator"

current_dir = os.path.dirname(os.path.abspath(__file__))
venv_python = os.path.abspath(os.path.join(current_dir, "..", "venv", "Scripts", "python.exe"))
os.environ['PYSPARK_PYTHON'] = venv_python
os.environ['PYSPARK_DRIVER_PYTHON'] = venv_python

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType

def start_spark_feeder():
    print(">>> [INIT] Initializing HPC-Sentinel Spark Ingestion Engine...", flush=True)

    SAFE_ROOT = r"C:\HPC_Sentinel_Live"
    INPUT_DIR = os.path.join(SAFE_ROOT, "input")
    HANDOFF_DIR = os.path.join(SAFE_ROOT, "handoff")
    CHECKPOINT_DIR = os.path.join(SAFE_ROOT, f"checkpoint_{int(time.time())}")
    
    os.makedirs(INPUT_DIR, exist_ok=True)
    os.makedirs(HANDOFF_DIR, exist_ok=True)
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)

    spark = SparkSession.builder \
        .appName("HPC-Sentinel-Feeder") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "5") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.sql.streaming.fileSource.logDeletion", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    input_uri = f"file:///{INPUT_DIR.replace(os.sep, '/')}"
    print(f">>> [LISTEN] Spark Watching: {input_uri}", flush=True)

    # STRICT POSITION SCHEMA
    # Since there is no header, Spark maps these names to columns 0, 1, 2... in order.
    schema = StructType([
        StructField("hash", StringType(), True), # Col 0
        StructField("f1", DoubleType(), True),   # Col 1
        StructField("f2", DoubleType(), True),
        StructField("f3", DoubleType(), True),
        StructField("f4", DoubleType(), True),
        StructField("f5", DoubleType(), True),
        StructField("f6", DoubleType(), True),
        StructField("f7", DoubleType(), True),
        StructField("f8", DoubleType(), True),
        StructField("f9", DoubleType(), True),
        StructField("f10", DoubleType(), True),
        StructField("f11", DoubleType(), True),
        StructField("f12", DoubleType(), True),
        StructField("label", IntegerType(), True)
    ])

    # READ (No Header)
    raw_stream = spark.readStream \
        .option("header", "false") \
        .option("maxFilesPerTrigger", 1) \
        .schema(schema) \
        .csv(input_uri)

    # WRITE
    # We select 'hash' first to guarantee the output order
    ordered_stream = raw_stream.select(
        "hash", 
        "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f11", "f12", 
        "label"
    )

    query = ordered_stream.writeStream \
        .outputMode("append") \
        .format("csv") \
        .option("path", HANDOFF_DIR) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .trigger(processingTime='1 second') \
        .start()

    print(">>> [SYSTEM] Spark Pipeline Active. Feeding data to AI layer...", flush=True)
    query.awaitTermination()

if __name__ == "__main__":
    start_spark_feeder()
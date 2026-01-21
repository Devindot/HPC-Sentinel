import os
import sys

# --- WINDOWS CONFIGURATION ---
os.environ['HADOOP_HOME'] = "C:\\winutils"
# FORCE JAVA 11 (Crucial for stability)
os.environ['JAVA_HOME'] = "C:\\Program Files\\Microsoft\\jdk-11.0.21.9-hotspot"
os.environ['HADOOP_USER_NAME'] = "administrator"

# Clear any conflicting Java 17 flags
if "JDK_JAVA_OPTIONS" in os.environ:
    del os.environ["JDK_JAVA_OPTIONS"]

from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType
from pyspark.sql.functions import col, when

print(">>> Initializing HPC-Sentinel Detector")

spark = SparkSession.builder \
    .appName("HPC-Sentinel-Detector") \
    .master("local[*]") \
    .config("spark.hadoop.security.authentication", "simple") \
    .config("spark.hadoop.hadoop.security.authentication", "simple") \
    .config("spark.ui.showConsoleProgress", "false") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 1. Load the Model
base_dir = os.path.dirname(os.path.abspath(__file__))
model_path = os.path.abspath(os.path.join(base_dir, "..", "models", "hpc_rf_model"))
stream_dir = os.path.abspath(os.path.join(base_dir, "..", "stream_stage"))

print(f">>> Loading Model from: {model_path}")

if not os.path.exists(model_path):
    print("CRITICAL ERROR: Model folder not found!")
    sys.exit(1)

try:
    model = RandomForestClassificationModel.load(model_path)
    print(">>>Model Loaded Successfully.")
except Exception as e:
    print(f"\n[ERROR] Model Load Failed: {e}")
    sys.exit(1)

# 2. Define Data Schema
schema = StructType([
    StructField("label", IntegerType(), True),
    StructField("hash", StringType(), True),
    StructField("f1", DoubleType(), True),
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
    StructField("f12", DoubleType(), True)
])

# 3. Start Monitoring
if not os.path.exists(stream_dir):
    os.makedirs(stream_dir)
    
print(f">>> Monitoring Network Traffic at: {stream_dir}")

raw_stream = spark.readStream \
    .option("header", "true") \
    .schema(schema) \
    .csv(stream_dir)

feature_cols = [f"f{i+1}" for i in range(12)]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")
vectorized_stream = assembler.transform(raw_stream)

# 4. Predict
predictions = model.transform(vectorized_stream)

# 5. Format Output
final_output = predictions.select(
    col("hash").alias("Process_ID"),
    col("prediction"),
    when(col("prediction") == 1.0, "!!! MALWARE DETECTED !!!")
    .otherwise("Normal Traffic").alias("STATUS")
)

print(">>> SYSTEM ARMED. Waiting for traffic... (Press Ctrl+C to stop)")

query = final_output.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
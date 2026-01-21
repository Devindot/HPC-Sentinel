import os
import sys
import shutil

# --- WINDOWS CONFIGURATION ---
os.environ['HADOOP_HOME'] = "C:\\winutils"
# FORCE JAVA 11 (Keeping the setting that worked)
os.environ['JAVA_HOME'] = "C:\\Program Files\\Microsoft\\jdk-11.0.21.9-hotspot"
os.environ['HADOOP_USER_NAME'] = "administrator"

if "JDK_JAVA_OPTIONS" in os.environ:
    del os.environ["JDK_JAVA_OPTIONS"]

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

print(">>> Initializing HPC-Sentinel Training")

spark = SparkSession.builder \
    .appName("HPC-Sentinel-Trainer") \
    .master("local[*]") \
    .config("spark.hadoop.security.authentication", "simple") \
    .config("spark.hadoop.hadoop.security.authentication", "simple") \
    .config("spark.ui.showConsoleProgress", "false") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 1. Load Data
data_path = "../data/hpc_dataset_flattened.csv"
print(f">>> Loading Data from: {data_path}")

if not os.path.exists(data_path):
    print("CRITICAL ERROR: Dataset not found!")
    sys.exit(1)

df = spark.read.csv(data_path, header=True, inferSchema=True)
print(f"    Loaded {df.count()} rows.")

# 2. Vectorize Features
feature_columns = [f"f{i+1}" for i in range(12)]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
df_vectorized = assembler.transform(df)
final_data = df_vectorized.select("features", "label")

# 3. Train (ENTERPRISE SETTINGS)
print(">>> Training Enterprise Random Forest...")
print("    (Allocated 4GB RAM. This will take 2-3 minutes...)")

train_data, test_data = final_data.randomSplit([0.8, 0.2], seed=42)

# Enterprise Hyperparameters
rf = RandomForestClassifier(
    labelCol="label", 
    featuresCol="features", 
    numTrees=300, 
    maxDepth=22, 
    maxBins=128,
    seed=42
)
model = rf.fit(train_data)

# 4. Evaluate
print(">>> Evaluating Performance...")
predictions = model.transform(test_data)

# Accuracy
evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
accuracy = evaluator.evaluate(predictions)

# Weighted F1 Score
f1_evaluator = MulticlassClassificationEvaluator(metricName="f1")
f1_score = f1_evaluator.evaluate(predictions)

print("\n" + "="*40)
print(f"FINAL ACCURACY:  {accuracy*100:.2f}%")
print(f"WEIGHTED F1:     {f1_score*100:.2f}%")
print("="*40)

# 5. Save Model
model_dir = "../models"
model_path = os.path.join(model_dir, "hpc_rf_model")

if os.path.exists(model_path):
    shutil.rmtree(model_path)

print(f">>> Saving Model to: {model_path}")
model.write().save(model_path)

print("SUCCESS! Model Saved.")
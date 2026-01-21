# HPC-Sentinel: Real-Time Malware Detection Engine

**HPC-Sentinel** is an end-to-end cybersecurity threat detection system built with **Apache Spark** and **Python**. It simulates a live high-performance computing (HPC) network environment, ingests real-time telemetry data, and uses a machine learning model to flag malicious activity with **84% accuracy**.

![Status](https://img.shields.io/badge/Status-Active-success)
![Tech](https://img.shields.io/badge/Spark-Structured%20Streaming-orange)
![Python](https://img.shields.io/badge/Python-3.x-blue)

 Key Features
* Real-Time Streaming Pipeline: Ingests live data packets instantly using Spark Structured Streaming.
* Enterprise-Grade AI: A Random Forest Classifier (300 trees, depth 18) trained on HPC telemetry data to distinguish between benign and malicious traffic.
* Attack Simulation: Includes a custom "Hacker" script (`stream_producer.py`) that generates realistic, randomized attack patterns to test the system.
* Low Latency: Processes and flags threats in sub-second timeframes.

Tech Stack
* Core Engine: Apache Spark 3.4.1 (PySpark)
* Language: Python 3.10+
* Machine Learning: Spark MLlib (Random Forest Classification)
* Environment: Windows 11 (Custom Hadoop/Winutils configuration)
* Dependencies: Java 11 (OpenJDK)

## 📂 Project Structure
```text
HPC-Sentinel/
├── data/                  # Raw training dataset (CSV)
├── models/                # Saved Spark ML models
├── src/
│   ├── 2_train_model.py       # Trains the Random Forest model
│   ├── 3_stream_producer.py   # Simulates live network traffic (The Hacker)
│   └── 4_stream_detector.py   # Real-time detection engine (The Sentinel)
├── stream_stage/          # Staging area for live data packets
└── requirements.txt       # Python dependencies

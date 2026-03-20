# 🛡️ HPC-Sentinel: Real-Time XGBoost Malware Detection Engine

**HPC-Sentinel** is an end-to-end cybersecurity threat detection system built with **Apache Spark** and **Python**. It simulates a live high-performance computing (HPC) network environment, ingests zero-disk telemetry data in real-time, and utilizes an edge-deployed **XGBoost** machine learning model to flag malicious activity. 

Going beyond standard classification, HPC-Sentinel features a deterministic logic gate that automatically enriches detected threats with live **CVE identifiers and CVSS severity scores**, translating raw hardware anomalies into actionable SOC metrics in real-time.

### 🚀 Key Features

* **Zero-Disk Telemetry Streaming:** Ingests live data packets instantly using a custom Atomic Producer and the Spark Distributed Stream Engine.
* **Champion AI Inference:** Powered by an optimized **XGBoost Classifier** trained on HPC telemetry data, achieving **84.76% accuracy** in edge-deployment with sub-second latency.
* **Conditional NVD Enrichment:** A deterministic logic gate that bypasses benign traffic but automatically assigns CVE identifiers and CVSS severity scores (e.g., CVSS 9.7) to payloads that cross the probabilistic threat threshold.
* **Asynchronous Fused Triage Visualization:** Correlates abstract machine learning probabilities with deterministic CVSS severity scores into a single live relational index for human-readable SOC evaluation.

### 🛠️ Tech Stack

* **Core Engine:** Apache Spark 3.4.1 (PySpark)
* **Language:** Python 3.10+
* **Machine Learning:** XGBoost Engine
* **Environment:** Windows 11 (Custom Hadoop/Winutils configuration)
* **Dependencies:** Java 11 (OpenJDK)

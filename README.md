# ⚽ Soccer Match Analytics — Big Data Pipeline with Apache Spark & Kafka

<div align="center">

![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.x-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.x-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.x-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-ML-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Jupyter](https://img.shields.io/badge/Jupyter-Notebook-F37626?style=for-the-badge&logo=jupyter&logoColor=white)

*An end-to-end Big Data pipeline combining real-time stream processing and machine learning for football championship analytics.*

</div>

---

##  Overview

This project implements a **production-grade Big Data pipeline** for football match analytics, leveraging **Apache Kafka** for real-time event streaming and **Apache Spark Structured Streaming** for continuous, stateful data processing.

The system ingests historical Mauritanian championship match data (2007–2025), simulates live match event streams, computes dynamic standings, and trains a **Random Forest** predictive model using PySpark MLlib.

**Key capabilities:**
-  Real-time match event simulation via a Kafka Producer
-  Live championship rankings and goal statistics with Spark Streaming
-  Match outcome prediction using PySpark ML (Random Forest)
-  Fault-tolerant processing with Spark checkpointing and watermarking

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      DATA SOURCES                           │
│   CSV Dataset (2007–2025)  ──►  Parquet (Exported Dataset)  │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                   KAFKA PRODUCER                            │
│         src/producer.py  ──►  Kafka Topic (JSON events)     │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│              SPARK STRUCTURED STREAMING                     │
│   src/stream_job.py  ──►  Rankings  /  Goal Statistics      │
│   (Watermarking + Tumbling Windows + Checkpointing)         │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                      OUTPUTS                                │
│   outputs/stream_results/  ──►  Live championship results   │
│   outputs/model_exports/   ──►  Trained ML model            │
└─────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
projet-bigdata-spark-kafka/
│
├── 📁 data/                            # Raw and processed data assets
│   ├── ready_data/                     # Cleaned, analysis-ready datasets
│   ├── kafka_data/                     # Kafka ingestion artifacts
│   ├── saved_models/                   # Persisted ML model files
│   └── rim_championnat_results_2007-2025.csv  # Primary raw dataset
│
├── 📁 notebooks/                       # Jupyter Notebooks (EDA, preprocessing, ML)
│   ├── prepare_data.ipynb              # Data cleaning, merging, and feature engineering
│   └── train_model.ipynb               # Random Forest model training & evaluation
│
├── 📁 src/                             # Core pipeline source code
│   ├── producer.py                     # Kafka Producer – simulates real-time match events
│   └── stream_job.py                   # Spark Streaming job – computes live statistics
│
├── 📁 outputs/                         # Pipeline output artifacts
│   ├── exported_dataset/               # Merged Parquet dataset (batch + scraped data)
│   ├── processed_output/               # Intermediate processing results
│   ├── model_exports/                  # Exported trained ML models
│   └── stream_results/                 # Real-time streaming output (rankings, goals)
│
├── 📁 checkpoints/                     # Spark Structured Streaming checkpoint directory
│
└── README.md                           # Project documentation (this file)
```

---

## 🛠️ Technology Stack

| Layer | Technology | Version |
|---|---|---|
| Stream Ingestion | Apache Kafka | 3.x |
| Stream Processing | Apache Spark Structured Streaming | 3.5.x |
| Machine Learning | PySpark MLlib (Random Forest) | 3.5.x |
| Data Exploration | Jupyter Notebook / JupyterLab | — |
| Language | Python | 3.x |
| Data Format | Apache Parquet, JSON, CSV | — |

---

## ⚙️ Prerequisites

Ensure the following are installed and properly configured on your system:

- **Apache Spark** ≥ 3.5.x
- **Apache Kafka** ≥ 3.x (broker running locally or accessible)
- **Python** ≥ 3.8 with the following packages:

```bash
pip install pyspark pandas confluent-kafka
```

- **Jupyter Notebook** or JupyterLab

---

##  Getting Started

Follow these steps in order to run the full pipeline end-to-end.

### Step 1 — Data Preparation

Open and execute the data preparation notebook. This notebook cleans the raw CSV dataset, applies feature engineering, and exports the result as a merged Parquet dataset.

```bash
jupyter notebook notebooks/prepare_data.ipynb
```

**Output:** `outputs/exported_dataset/`

---

### Step 2 — Start the Kafka Producer

Launch the producer script to simulate a real-time stream of match events. Each event is published to a Kafka topic as a JSON message.

```bash
python src/producer.py
```

**Environment variables (optional):**

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `soccer-matches` | Target Kafka topic name |
| `MESSAGE_DELAY` | `1.0` | Delay (seconds) between messages |

---

### Step 3 — Launch the Spark Streaming Job

Submit the Spark job to consume from Kafka and compute live championship statistics in real time.

```bash
spark-submit \
    --master spark://<your-spark-master>:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.kafka:kafka-clients:3.5.1 \
    src/stream_job.py
```

> **Note:** Replace `<your-spark-master>` with your actual Spark master address, or use `local[*]` for local mode.

**Output:** Results are continuously written to `outputs/stream_results/` and include:
- **Points Table** — live championship standings (wins, draws, losses, points)
- **Goal Statistics** — goals scored and conceded per team

---

### Step 4 — Train the Machine Learning Model

Run the training notebook to fit a Random Forest classifier on the prepared dataset and export the model.

```bash
jupyter notebook notebooks/train_model.ipynb
```

**Output:** `outputs/model_exports/`

---

##  Key Features

| Feature | Description |
|---|---|
| **Batch Preprocessing** | Cleans and merges historical data (2007–2025) into a unified Parquet dataset |
| **Stream Simulation** | Kafka Producer replays match data with configurable delay to simulate live events |
| **Real-Time Rankings** | Spark Streaming dynamically calculates championship tables using Tumbling Windows |
| **Watermarking** | Handles late-arriving data gracefully in the streaming pipeline |
| **Fault Tolerance** | Spark checkpointing ensures exactly-once semantics and recovery on failure |
| **ML Prediction** | Random Forest model predicts match outcomes based on historical team performance |



<div align="center">
  <i>Built with Apache Spark, Apache Kafka, and PySpark MLlib — demonstrating a complete Big Data pipeline for real-time sports analytics.</i>
</div>

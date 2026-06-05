# E-Commerce Real-Time Recommendation & Dynamic Pricing Platform

> **Z5008 Big Data Lab — IIT Madras Zanzibar**
> Student: Gaurav Jha (ZDA25M005)

A production-grade 8-layer big data platform for real-time product recommendations and dynamic pricing, built on Apache Kafka, Apache Spark, Delta Lake, MLflow, and BentoML.

---

## Architecture

```
Kafka (Layer 1)
    → Delta Lake Bronze/Silver/Gold on MinIO (Layer 2/3)
        → Apache Spark Batch + Streaming (Layer 3)
            → Airflow Orchestration (Layer 4)
                → ALS Recommender + GBT Pricing ML (Layer 5)
                    → MLflow Experiment Tracking (Layer 6)
                        → BentoML REST API Serving (Layer 7)
                            → Grafana + Prometheus Monitoring (Layer 8)
```

### Key Metrics
- **1.19M+** real-time events processed via Kafka
- **ALS Recommender**: NDCG@10 = 0.996, MAP@10 = 0.036
- **GBT Dynamic Pricing**: R² = 0.512, RMSE = 125
- **BentoML API**: 158 req/s, p99 latency = 15ms, 0% failure rate
- **500K** user profiles, **164K** product features in Gold feature store

---

## Repository Structure

```
├── spark_jobs/          # Production Spark batch and streaming jobs
│   ├── kafka_to_bronze.py
│   ├── bronze_to_silver.py
│   ├── silver_to_gold.py
│   └── rolling_features.py
├── dags/                # Airflow pipeline DAGs
│   ├── nightly_pipeline_dag.py
│   └── drift_sensor_dag.py
├── ml/                  # ML training scripts + MLflow tracking
│   ├── recommender/als_model.py
│   └── pricing/gbt_pricing_model.py
├── api/                 # BentoML service definition + Dockerfile
│   ├── recommendation_service.py
│   └── Dockerfile
├── dashboards/          # Grafana dashboard JSON exports
│   └── ecommerce_platform.json
├── notebooks/           # Jupyter EDA and analysis notebooks
├── data/                # Sample data and generation scripts
│   └── scripts/
├── kafka/               # Kafka producers
├── spark/               # Spark config and full job source
├── monitoring/          # Prometheus + Grafana config
├── ollama/              # Natural language query interface (Bonus)
├── docker-compose.yml   # All 14 services
├── .env.example         # Environment variables template
└── requirements.txt
```

---

## Prerequisites

- Docker Desktop (>= 4.x) with at least **8GB RAM** allocated
- Docker Compose v2
- Python 3.10+ (for running load tests locally)

---

## Quick Start

### 1. Clone the repository
```bash
git clone https://github.com/zda25m005-netizen/ecommerce-realtime-intelligence-platform.git
cd ecommerce-realtime-intelligence-platform
```

### 2. Set up environment variables
```bash
cp .env.example .env
```

### 3. Start all services
```bash
docker compose up -d
```
Wait ~60 seconds for all 14 containers to initialize.

### 4. Verify all containers are running
```bash
docker ps --format "table {{.Names}}\t{{.Status}}" | sort
```
You should see 14 containers all showing `Up`.

### 5. Download dataset
```bash
bash data/scripts/download_datasets.sh
```

### 6. Run the data pipeline

**Start Bronze streaming (keep running in background):**
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-java-options "-Divy.home=/tmp/.ivy2" \
  --packages "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4" \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" \
  --conf "spark.hadoop.fs.s3a.access.key=minio_admin" \
  --conf "spark.hadoop.fs.s3a.secret.key=minio_password" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  /opt/spark-apps/streaming/kafka_to_bronze.py
```

**Run Silver/Gold batch pipeline:**
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-java-options "-Divy.home=/tmp/.ivy2" \
  --packages "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4" \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" \
  --conf "spark.hadoop.fs.s3a.access.key=minio_admin" \
  --conf "spark.hadoop.fs.s3a.secret.key=minio_password" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  /opt/spark-apps/batch/silver_to_gold.py
```

### 7. Train ML models

**ALS Recommender:**
```bash
docker exec \
  -e AWS_ACCESS_KEY_ID=minio_admin \
  -e AWS_SECRET_ACCESS_KEY=minio_password \
  -e MLFLOW_S3_ENDPOINT_URL=http://minio:9000 \
  spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-java-options "-Divy.home=/tmp/.ivy2" \
  --packages "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4" \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" \
  --conf "spark.hadoop.fs.s3a.access.key=minio_admin" \
  --conf "spark.hadoop.fs.s3a.secret.key=minio_password" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  /opt/ml/recommender/als_model.py
```

**GBT Pricing Model:**
```bash
docker exec \
  -e AWS_ACCESS_KEY_ID=minio_admin \
  -e AWS_SECRET_ACCESS_KEY=minio_password \
  -e MLFLOW_S3_ENDPOINT_URL=http://minio:9000 \
  spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-java-options "-Divy.home=/tmp/.ivy2" \
  --packages "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4" \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.hadoop.fs.s3a.endpoint=http://minio:9000" \
  --conf "spark.hadoop.fs.s3a.access.key=minio_admin" \
  --conf "spark.hadoop.fs.s3a.secret.key=minio_password" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  /opt/ml/pricing/gbt_pricing_model.py
```

### 8. Test the API
```bash
# Recommendations
curl -X POST http://localhost:3001/recommend \
  -H "Content-Type: application/json" \
  -d '{"user_id": 42, "n_items": 10}'

# Dynamic Pricing
curl -X POST http://localhost:3001/price \
  -H "Content-Type: application/json" \
  -d '{"item_id": 100, "current_stock": 50, "demand_score": 1.5}'
```

### 9. Run Load Test
```bash
pip install locust
locust -f tests/locustfile.py --headless --users 50 --spawn-rate 10 \
  --run-time 60s --host http://localhost:3001
```

---

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana | http://localhost:3000 | admin / admin |
| Spark UI | http://localhost:8080 | — |
| MLflow | http://localhost:5001 | — |
| Airflow | http://localhost:8082 | admin / admin |
| MinIO | http://localhost:9001 | minio_admin / minio_password |
| BentoML API | http://localhost:3001 | — |
| Ollama | http://localhost:11434 | — |

---

## Bonus: Natural Language Query Interface

Uses Ollama (LLaMA 3.2) to translate plain English to Spark SQL:

```bash
# Pull the model first
docker exec ollama ollama pull llama3.2:1b

# Run interactive CLI
docker cp ollama/nl_query_interface.py spark-master:/tmp/nl_query_interface.py
docker exec -it \
  -e OLLAMA_HOST=http://ollama:11434 \
  -e OLLAMA_MODEL=llama3.2:1b \
  spark-master python3 /tmp/nl_query_interface.py
```

Example queries:
- *"Show me top 10 users by engagement score"*
- *"Which products were viewed over 1000 times but never purchased?"*
- *"What is the average conversion rate by device type?"*

---

## Stopping the Platform
```bash
docker compose down
```

To also remove all data volumes:
```bash
docker compose down -v
```

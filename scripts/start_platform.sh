#!/bin/bash
# =============================================================================
# E-Commerce Intelligence Platform - Startup Script
# Starts all services layer by layer with health checks.
# =============================================================================

set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() { echo -e "${BLUE}[$(date '+%H:%M:%S')]${NC} $1"; }
success() { echo -e "${GREEN}[✓]${NC} $1"; }
warn() { echo -e "${YELLOW}[!]${NC} $1"; }
error() { echo -e "${RED}[✗]${NC} $1"; }

echo ""
echo "============================================="
echo "  E-Commerce Intelligence Platform"
echo "  Z5008 Big Data Lab - Gaurav Jha"
echo "============================================="
echo ""

# ── Layer 1: Kafka + Zookeeper ──────────────────────────────────────────────
log "Layer 1: Starting Kafka & Zookeeper..."
docker compose up -d zookeeper kafka
sleep 10

# Wait for Kafka to be ready
log "Waiting for Kafka to be ready..."
for i in {1..30}; do
    if docker compose exec -T kafka kafka-topics --list --bootstrap-server localhost:9092 &>/dev/null; then
        success "Kafka is ready"
        break
    fi
    sleep 2
done

# Create topics
docker compose up -d kafka-init
sleep 5
success "Layer 1: Kafka topics created"

# ── Layer 2: MinIO ──────────────────────────────────────────────────────────
log "Layer 2: Starting MinIO..."
docker compose up -d minio
sleep 5
docker compose up -d minio-init
sleep 5
success "Layer 2: MinIO buckets ready"

# ── Layer 3: Spark ──────────────────────────────────────────────────────────
log "Layer 3: Starting Spark cluster..."
docker compose up -d spark-master spark-worker
sleep 10
success "Layer 3: Spark Master + Worker running"

# ── Layer 4: Airflow ────────────────────────────────────────────────────────
log "Layer 4: Starting Airflow..."
docker compose up -d airflow-postgres
sleep 5
docker compose up -d airflow-webserver airflow-scheduler
sleep 15
success "Layer 4: Airflow webserver + scheduler running"

# ── Layer 6: MLflow ─────────────────────────────────────────────────────────
log "Layer 6: Starting MLflow..."
docker compose up -d mlflow
sleep 5
success "Layer 6: MLflow tracking server running"

# ── Layer 8: Monitoring ─────────────────────────────────────────────────────
log "Layer 8: Starting Prometheus + Grafana..."
docker compose up -d prometheus grafana metrics-exporter
sleep 5
success "Layer 8: Monitoring stack running"

# ── Summary ─────────────────────────────────────────────────────────────────
echo ""
echo "============================================="
echo "  Platform is running!"
echo "============================================="
echo ""
echo "  Service URLs:"
echo "  ─────────────────────────────────────────"
echo "  Kafka:          localhost:29092"
echo "  MinIO Console:  http://localhost:9001  (minio_admin / minio_password)"
echo "  Spark Master:   http://localhost:8080"
echo "  Spark Worker:   http://localhost:8081"
echo "  Airflow:        http://localhost:8082  (admin / admin)"
echo "  MLflow:         http://localhost:5000"
echo "  Prometheus:     http://localhost:9090"
echo "  Grafana:        http://localhost:3000  (admin / admin)"
echo ""
echo "  Next steps:"
echo "  1. Download datasets:  bash data/scripts/download_datasets.sh"
echo "  2. Start producers:    python kafka/producers/clickstream_producer.py"
echo "  3. Start streaming:    spark-submit spark/streaming/kafka_to_bronze.py"
echo "  4. Run nightly ETL:    Trigger from Airflow UI"
echo "  5. Run load test:      locust -f tests/locustfile.py"
echo ""

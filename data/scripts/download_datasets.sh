#!/bin/bash
# =============================================================================
# Dataset Download Script
# Downloads Retailrocket and Olist datasets from Kaggle
# Requires: kaggle CLI (pip install kaggle) + API key in ~/.kaggle/kaggle.json
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$SCRIPT_DIR/../raw"

echo "============================================="
echo "E-Commerce Intelligence Platform"
echo "Dataset Download Script"
echo "============================================="
echo ""

# Find kaggle CLI - try direct command first, then python3 -m kaggle
KAGGLE_CMD=""
if command -v kaggle &> /dev/null; then
    KAGGLE_CMD="kaggle"
elif python3 -m kaggle --version &> /dev/null; then
    KAGGLE_CMD="python3 -m kaggle"
else
    echo "ERROR: kaggle CLI not found."
    echo "Install it with: pip3 install kaggle"
    exit 1
fi
echo "Using kaggle via: $KAGGLE_CMD"

mkdir -p "$DATA_DIR/retailrocket"
mkdir -p "$DATA_DIR/olist"

# --- Dataset 1: Retailrocket E-Commerce Dataset ---
echo "[1/2] Downloading Retailrocket E-Commerce Dataset..."
echo "      (2.7M clickstream events, 1.4M users)"
if [ -f "$DATA_DIR/retailrocket/events.csv" ]; then
    echo "      Already exists, skipping."
else
    $KAGGLE_CMD datasets download -d retailrocket/ecommerce-dataset \
        -p "$DATA_DIR/retailrocket" --unzip
    echo "      Done!"
fi

# --- Dataset 2: Brazilian E-Commerce (Olist) ---
echo "[2/2] Downloading Brazilian E-Commerce (Olist) Dataset..."
echo "      (100K orders with prices, reviews)"
if [ -f "$DATA_DIR/olist/olist_orders_dataset.csv" ]; then
    echo "      Already exists, skipping."
else
    $KAGGLE_CMD datasets download -d olistbr/brazilian-ecommerce \
        -p "$DATA_DIR/olist" --unzip
    echo "      Done!"
fi

echo ""
echo "============================================="
echo "All datasets downloaded to: $DATA_DIR"
echo "============================================="
echo ""
echo "Contents:"
ls -lh "$DATA_DIR/retailrocket/"
echo ""
ls -lh "$DATA_DIR/olist/"

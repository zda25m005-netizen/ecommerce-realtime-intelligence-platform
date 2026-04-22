"""
Inventory Producer - Layer 1
Generates synthetic inventory events (stock levels, price changes, restocks).
Topic: inventory
Events: stock_level, price_change, restock
"""

import json
import time
import random
import os
import logging
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('inventory_producer')

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
TOPIC = 'inventory'
EVENTS_PER_SECOND = int(os.getenv('INV_EVENTS_PER_SECOND', '20'))

fake = Faker()

# Simulated product catalog
NUM_PRODUCTS = 2000
CATEGORIES = [
    'Electronics', 'Clothing', 'Home & Kitchen', 'Sports', 'Books',
    'Beauty', 'Toys', 'Automotive', 'Health', 'Garden'
]

# Initialize product inventory state
product_inventory = {}
for pid in range(1, NUM_PRODUCTS + 1):
    product_inventory[pid] = {
        'stock': random.randint(5, 500),
        'base_price': round(random.uniform(9.99, 299.99), 2),
        'current_price': None,
        'category': random.choice(CATEGORIES),
    }
    product_inventory[pid]['current_price'] = product_inventory[pid]['base_price']


def create_producer(retries=10, delay=5):
    """Create Kafka producer with retry logic."""
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
            )
            logger.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP}")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"Kafka not ready (attempt {attempt+1}/{retries}), retrying in {delay}s...")
            time.sleep(delay)
    raise ConnectionError("Could not connect to Kafka")


def generate_stock_level_event(item_id):
    """Simulate a stock level change (purchase reduces stock)."""
    product = product_inventory[item_id]
    units_sold = random.randint(1, 5)
    product['stock'] = max(0, product['stock'] - units_sold)

    return {
        'event_type': 'stock_level',
        'item_id': item_id,
        'stock_level': product['stock'],
        'units_sold': units_sold,
        'category': product['category'],
        'warehouse': random.choice(['warehouse_A', 'warehouse_B', 'warehouse_C']),
        'timestamp': datetime.utcnow().isoformat(),
    }


def generate_price_change_event(item_id):
    """Simulate a price change based on demand signals."""
    product = product_inventory[item_id]
    # Price fluctuates +/- 15% from base
    change_pct = random.uniform(-0.15, 0.15)
    new_price = round(product['base_price'] * (1 + change_pct), 2)
    old_price = product['current_price']
    product['current_price'] = new_price

    return {
        'event_type': 'price_change',
        'item_id': item_id,
        'old_price': old_price,
        'new_price': new_price,
        'change_pct': round(change_pct * 100, 2),
        'category': product['category'],
        'reason': random.choice(['demand_spike', 'competitor_match', 'clearance', 'seasonal', 'algorithm']),
        'timestamp': datetime.utcnow().isoformat(),
    }


def generate_restock_event(item_id):
    """Simulate a restock event for low-inventory items."""
    product = product_inventory[item_id]
    restock_qty = random.randint(50, 300)
    product['stock'] += restock_qty

    return {
        'event_type': 'restock',
        'item_id': item_id,
        'restock_quantity': restock_qty,
        'new_stock_level': product['stock'],
        'category': product['category'],
        'supplier': fake.company(),
        'estimated_arrival': fake.date_between(start_date='today', end_date='+7d').isoformat(),
        'timestamp': datetime.utcnow().isoformat(),
    }


def run_producer(producer, duration_seconds=3600):
    """Run continuous inventory event generation."""
    logger.info(f"Starting inventory event generation for {duration_seconds}s")
    start_time = time.time()
    event_count = 0
    event_generators = [
        (generate_stock_level_event, 0.50),   # 50% stock updates
        (generate_price_change_event, 0.30),  # 30% price changes
        (generate_restock_event, 0.20),       # 20% restocks
    ]

    while time.time() - start_time < duration_seconds:
        for _ in range(EVENTS_PER_SECOND):
            item_id = random.randint(1, NUM_PRODUCTS)

            # Weighted random event type selection
            gen_func = random.choices(
                [g[0] for g in event_generators],
                weights=[g[1] for g in event_generators],
                k=1
            )[0]

            event = gen_func(item_id)
            key = str(item_id)
            producer.send(TOPIC, key=key, value=event)
            event_count += 1

        producer.flush()

        if event_count % 100 == 0:
            logger.info(f"Sent {event_count} inventory events")

        time.sleep(1)

    logger.info(f"Generation complete. Total events: {event_count}")


if __name__ == '__main__':
    producer = create_producer()
    run_producer(producer, duration_seconds=int(os.getenv('RUN_DURATION', '3600')))
    producer.close()

"""
Clickstream Producer - Layer 1
Replays Retailrocket events via Kafka at a defined streaming rate.
Topic: clickstream
Events: view, addtocart, transaction

STREAMING RATE (defined per professor's requirement):
  - Clickstream producer : 100 events/sec
  - Inventory producer   :  20 events/sec
  - User events producer :  50 events/sec
  - TOTAL platform rate  : 170 events/sec  (~10,200 events/min)

USER SUBSET:
  - Dataset: Retailrocket (1.4M visitors total)
  - Active subset: first 500,000 unique visitors (events_500k.csv)
  - Run data/scripts/prepare_subset.py to generate the subset file.
"""

import json
import time
import random
import csv
import os
import logging
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('clickstream_producer')

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
TOPIC = 'clickstream'

# Default to the 500k subset as required by the project spec.
# Falls back to full dataset if subset hasn't been prepared yet.
_SUBSET   = 'data/raw/retailrocket/events_500k.csv'
_FULL     = 'data/raw/retailrocket/events.csv'
DATA_FILE = os.getenv('CLICKSTREAM_DATA', _SUBSET if os.path.exists(_SUBSET) else _FULL)

# Defined streaming rate: 100 events/sec for clickstream topic
EVENTS_PER_SECOND = int(os.getenv('EVENTS_PER_SECOND', '100'))


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
                batch_size=16384,
                linger_ms=10,
                buffer_memory=33554432,
            )
            logger.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP}")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"Kafka not ready (attempt {attempt+1}/{retries}), retrying in {delay}s...")
            time.sleep(delay)
    raise ConnectionError(f"Could not connect to Kafka after {retries} attempts")


def replay_from_csv(producer, filepath):
    """
    Replay Retailrocket events from CSV.
    CSV columns: timestamp, visitorid, event, itemid, transactionid
    """
    logger.info(f"Replaying events from {filepath}")
    event_count = 0

    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        batch = []

        for row in reader:
            event = {
                'event_type': row['event'],
                'user_id': int(row['visitorid']),
                'item_id': int(row['itemid']),
                'timestamp': datetime.utcnow().isoformat(),
                'original_timestamp': int(row['timestamp']),
                'session_id': f"session_{row['visitorid']}_{random.randint(1000, 9999)}",
                'transaction_id': row.get('transactionid', None),
            }

            # Use user_id as partition key for ordering
            key = str(event['user_id'])
            producer.send(TOPIC, key=key, value=event)
            event_count += 1

            # Rate limiting
            if event_count % EVENTS_PER_SECOND == 0:
                producer.flush()
                logger.info(f"Sent {event_count} events")
                time.sleep(1)

    producer.flush()
    logger.info(f"Replay complete. Total events sent: {event_count}")
    return event_count


def generate_synthetic(producer, num_events=10000):
    """
    Generate synthetic clickstream events if CSV not available.
    Useful for testing without the full dataset.
    """
    logger.info(f"Generating {num_events} synthetic clickstream events")
    event_types = ['view', 'addtocart', 'transaction']
    event_weights = [0.75, 0.18, 0.07]  # Realistic funnel ratios

    num_users = 500
    num_items = 2000

    for i in range(num_events):
        user_id = random.randint(1, num_users)
        event_type = random.choices(event_types, weights=event_weights, k=1)[0]

        event = {
            'event_type': event_type,
            'user_id': user_id,
            'item_id': random.randint(1, num_items),
            'timestamp': datetime.utcnow().isoformat(),
            'session_id': f"session_{user_id}_{random.randint(1000, 9999)}",
            'transaction_id': f"txn_{random.randint(100000, 999999)}" if event_type == 'transaction' else None,
        }

        key = str(event['user_id'])
        producer.send(TOPIC, key=key, value=event)

        if (i + 1) % EVENTS_PER_SECOND == 0:
            producer.flush()
            logger.info(f"Generated {i + 1}/{num_events} events")
            time.sleep(1)

    producer.flush()
    logger.info(f"Synthetic generation complete: {num_events} events")


if __name__ == '__main__':
    producer = create_producer()

    if os.path.exists(DATA_FILE):
        replay_from_csv(producer, DATA_FILE)
    else:
        logger.warning(f"Data file not found at {DATA_FILE}, using synthetic generation")
        generate_synthetic(producer)

    producer.close()

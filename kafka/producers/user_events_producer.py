"""
User Events Producer - Layer 1
Generates synthetic user session events (login, session_start, device, region).
Topic: user_events
Events: login, session_start, session_end, page_view
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
logger = logging.getLogger('user_events_producer')

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
TOPIC = 'user_events'
EVENTS_PER_SECOND = int(os.getenv('USER_EVENTS_PER_SECOND', '50'))
NUM_USERS = 500

fake = Faker()

DEVICES = ['mobile_ios', 'mobile_android', 'desktop_windows', 'desktop_mac', 'tablet_ipad']
DEVICE_WEIGHTS = [0.30, 0.35, 0.20, 0.10, 0.05]

REGIONS = [
    'East Africa', 'West Africa', 'South Asia', 'Southeast Asia',
    'North America', 'Europe', 'South America', 'Middle East'
]
REGION_WEIGHTS = [0.25, 0.15, 0.20, 0.10, 0.10, 0.10, 0.05, 0.05]

PAGES = [
    'home', 'category_listing', 'product_detail', 'search_results',
    'cart', 'checkout', 'account', 'wishlist', 'deals', 'reviews'
]

# Track active sessions
active_sessions = {}


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


def generate_login_event(user_id):
    """User logs in."""
    device = random.choices(DEVICES, weights=DEVICE_WEIGHTS, k=1)[0]
    region = random.choices(REGIONS, weights=REGION_WEIGHTS, k=1)[0]
    session_id = f"session_{user_id}_{int(time.time())}_{random.randint(1000, 9999)}"

    active_sessions[user_id] = {
        'session_id': session_id,
        'device': device,
        'region': region,
        'started_at': datetime.utcnow().isoformat(),
    }

    return {
        'event_type': 'login',
        'user_id': user_id,
        'session_id': session_id,
        'device_type': device,
        'region': region,
        'ip_hash': fake.md5()[:16],
        'user_agent': fake.user_agent(),
        'is_returning': random.random() < 0.65,
        'timestamp': datetime.utcnow().isoformat(),
    }


def generate_session_start_event(user_id):
    """Session begins (can happen without login for guests)."""
    session_id = f"session_{user_id}_{int(time.time())}_{random.randint(1000, 9999)}"
    device = random.choices(DEVICES, weights=DEVICE_WEIGHTS, k=1)[0]
    region = random.choices(REGIONS, weights=REGION_WEIGHTS, k=1)[0]

    active_sessions[user_id] = {
        'session_id': session_id,
        'device': device,
        'region': region,
        'started_at': datetime.utcnow().isoformat(),
    }

    return {
        'event_type': 'session_start',
        'user_id': user_id,
        'session_id': session_id,
        'device_type': device,
        'region': region,
        'referrer': random.choice(['direct', 'google', 'instagram', 'facebook', 'email', 'affiliate']),
        'landing_page': random.choice(['home', 'product_detail', 'deals', 'category_listing']),
        'timestamp': datetime.utcnow().isoformat(),
    }


def generate_session_end_event(user_id):
    """Session ends."""
    session = active_sessions.pop(user_id, None)
    if not session:
        return None

    return {
        'event_type': 'session_end',
        'user_id': user_id,
        'session_id': session['session_id'],
        'device_type': session['device'],
        'region': session['region'],
        'pages_viewed': random.randint(1, 25),
        'duration_seconds': random.randint(10, 1800),
        'items_viewed': random.randint(0, 15),
        'items_carted': random.randint(0, 5),
        'purchased': random.random() < 0.03,  # 3% conversion rate
        'timestamp': datetime.utcnow().isoformat(),
    }


def generate_page_view_event(user_id):
    """User views a page during active session."""
    session = active_sessions.get(user_id)
    if not session:
        # Start a session first
        return generate_session_start_event(user_id)

    return {
        'event_type': 'page_view',
        'user_id': user_id,
        'session_id': session['session_id'],
        'device_type': session['device'],
        'region': session['region'],
        'page': random.choice(PAGES),
        'time_on_page_seconds': random.randint(2, 120),
        'scroll_depth_pct': random.randint(10, 100),
        'timestamp': datetime.utcnow().isoformat(),
    }


def run_producer(producer, duration_seconds=3600):
    """Run continuous user event generation."""
    logger.info(f"Starting user event generation for {duration_seconds}s")
    start_time = time.time()
    event_count = 0

    event_generators = [
        (generate_login_event, 0.15),
        (generate_session_start_event, 0.20),
        (generate_session_end_event, 0.15),
        (generate_page_view_event, 0.50),
    ]

    while time.time() - start_time < duration_seconds:
        for _ in range(EVENTS_PER_SECOND):
            user_id = random.randint(1, NUM_USERS)

            gen_func = random.choices(
                [g[0] for g in event_generators],
                weights=[g[1] for g in event_generators],
                k=1
            )[0]

            event = gen_func(user_id)
            if event is None:
                continue

            key = str(user_id)
            producer.send(TOPIC, key=key, value=event)
            event_count += 1

        producer.flush()

        if event_count % 200 == 0:
            logger.info(f"Sent {event_count} user events")

        time.sleep(1)

    logger.info(f"Generation complete. Total events: {event_count}")


if __name__ == '__main__':
    producer = create_producer()
    run_producer(producer, duration_seconds=int(os.getenv('RUN_DURATION', '3600')))
    producer.close()

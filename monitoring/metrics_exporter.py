"""
Prometheus Metrics Exporter - Layer 8
Custom Python exporter that bridges Kafka business metrics into Prometheus.
Exposes metrics on :8000/metrics for Grafana dashboards.
"""

import os
import time
import json
import threading
import logging
from http.server import HTTPServer
from prometheus_client import (
    Counter, Gauge, Histogram, Summary,
    generate_latest, CONTENT_TYPE_LATEST,
    CollectorRegistry, make_wsgi_app
)
from prometheus_client.exposition import MetricsHandler
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("metrics_exporter")

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

# ── Prometheus Metrics ───────────────────────────────────────────────────────

# Panel 1: Live CTR (recommendation click-through rate)
recommendation_ctr = Gauge(
    'recommendation_ctr_rate',
    'Current recommendation click-through rate',
    ['model_version']
)

recommendation_clicks = Counter(
    'recommendation_clicks_total',
    'Total recommendation clicks',
    ['event_type']
)

recommendation_impressions = Counter(
    'recommendation_impressions_total',
    'Total recommendation impressions'
)

# Panel 2: Kafka lag / throughput
kafka_messages_consumed = Counter(
    'kafka_messages_consumed_total',
    'Total Kafka messages consumed',
    ['topic']
)

kafka_consumer_lag = Gauge(
    'kafka_consumer_lag',
    'Kafka consumer lag (messages behind)',
    ['topic', 'partition']
)

kafka_throughput = Gauge(
    'kafka_throughput_messages_per_sec',
    'Kafka message throughput per second',
    ['topic']
)

# Panel 3: API latency (from BentoML)
api_request_latency = Histogram(
    'api_request_latency_seconds',
    'API request latency in seconds',
    ['endpoint'],
    buckets=[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]
)

api_requests_total = Counter(
    'api_requests_total',
    'Total API requests',
    ['endpoint', 'status']
)

# Panel 4: A/B conversion rate
ab_conversion_rate = Gauge(
    'ab_test_conversion_rate',
    'A/B test conversion rate',
    ['variant']
)

ab_test_users = Gauge(
    'ab_test_users_total',
    'Number of users in each A/B test variant',
    ['variant']
)

# Panel 5: Recommendation coverage
recommendation_coverage = Gauge(
    'recommendation_coverage_pct',
    'Percentage of catalog items being recommended'
)

unique_items_recommended = Gauge(
    'unique_items_recommended',
    'Number of unique items currently being recommended'
)

total_catalog_items = Gauge(
    'total_catalog_items',
    'Total items in product catalog'
)

# Additional: Dynamic pricing metrics
price_adjustments = Counter(
    'price_adjustments_total',
    'Total price adjustments made',
    ['direction']  # 'up' or 'down'
)

avg_price_change = Gauge(
    'avg_price_change_pct',
    'Average price change percentage'
)


# ── Kafka Consumer Threads ───────────────────────────────────────────────────

class MetricsCollector:
    def __init__(self):
        self.running = True
        self._click_count = 0
        self._impression_count = 0
        self._recommended_items = set()
        self._topic_counts = {'clickstream': 0, 'inventory': 0, 'user_events': 0}
        self._topic_last_time = {t: time.time() for t in self._topic_counts}
        self._topic_last_count = dict(self._topic_counts)

    def consume_clickstream(self):
        """Consume clickstream events and update CTR metrics."""
        try:
            consumer = KafkaConsumer(
                'clickstream',
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id='metrics_exporter_clickstream',
                auto_offset_reset='latest',
                enable_auto_commit=True,
                max_poll_interval_ms=300000,
                max_poll_records=50,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
            )
            logger.info("Clickstream consumer started")

            for message in consumer:
                if not self.running:
                    break

                event = message.value
                event_type = event.get('event_type', 'unknown')
                kafka_messages_consumed.labels(topic='clickstream').inc()
                self._topic_counts['clickstream'] += 1

                # Track for CTR
                if event_type == 'view':
                    self._impression_count += 1
                    recommendation_impressions.inc()
                elif event_type == 'addtocart':
                    self._click_count += 1
                    recommendation_clicks.labels(event_type='addtocart').inc()
                elif event_type == 'transaction':
                    recommendation_clicks.labels(event_type='transaction').inc()

                # Track recommended items
                self._recommended_items.add(event.get('item_id'))

                # Update CTR every 100 events
                if self._impression_count > 0 and self._impression_count % 100 == 0:
                    ctr = self._click_count / self._impression_count
                    recommendation_ctr.labels(model_version='current').set(ctr)

        except NoBrokersAvailable:
            logger.error("Kafka not available for clickstream consumer")
        except Exception as e:
            logger.error(f"Clickstream consumer error: {e}")

    def consume_inventory(self):
        """Consume inventory events and update pricing metrics."""
        try:
            consumer = KafkaConsumer(
                'inventory',
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id='metrics_exporter_inventory',
                auto_offset_reset='latest',
                enable_auto_commit=True,
                max_poll_interval_ms=300000,
                max_poll_records=50,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
            )
            logger.info("Inventory consumer started")

            for message in consumer:
                if not self.running:
                    break

                event = message.value
                event_type = event.get('event_type', 'unknown')
                kafka_messages_consumed.labels(topic='inventory').inc()
                self._topic_counts['inventory'] += 1

                if event_type == 'price_change':
                    change_pct = event.get('change_pct', 0)
                    direction = 'up' if change_pct > 0 else 'down'
                    price_adjustments.labels(direction=direction).inc()
                    avg_price_change.set(change_pct)

        except NoBrokersAvailable:
            logger.error("Kafka not available for inventory consumer")
        except Exception as e:
            logger.error(f"Inventory consumer error: {e}")

    def consume_user_events(self):
        """Consume user events for session metrics."""
        try:
            consumer = KafkaConsumer(
                'user_events',
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id='metrics_exporter_user_events',
                auto_offset_reset='latest',
                enable_auto_commit=True,
                max_poll_interval_ms=300000,
                max_poll_records=50,
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
            )
            logger.info("User events consumer started")

            for message in consumer:
                if not self.running:
                    break

                kafka_messages_consumed.labels(topic='user_events').inc()
                self._topic_counts['user_events'] += 1

        except NoBrokersAvailable:
            logger.error("Kafka not available for user_events consumer")
        except Exception as e:
            logger.error(f"User events consumer error: {e}")

    def update_throughput(self):
        """Periodically update throughput gauges."""
        while self.running:
            time.sleep(10)
            now = time.time()
            for topic in self._topic_counts:
                elapsed = now - self._topic_last_time[topic]
                if elapsed > 0:
                    count_diff = self._topic_counts[topic] - self._topic_last_count[topic]
                    throughput = count_diff / elapsed
                    kafka_throughput.labels(topic=topic).set(round(throughput, 2))
                self._topic_last_time[topic] = now
                self._topic_last_count[topic] = self._topic_counts[topic]

            # Update coverage
            # Retailrocket dataset has ~235,061 unique items; use 250,000 as catalog size
            CATALOG_SIZE = 250_000
            num_recommended = len(self._recommended_items)
            unique_items_recommended.set(num_recommended)
            total_catalog_items.set(CATALOG_SIZE)
            if num_recommended > 0:
                coverage = (num_recommended / CATALOG_SIZE) * 100
                recommendation_coverage.set(round(coverage, 2))

            # Simulated A/B test metrics
            ab_conversion_rate.labels(variant='A').set(round(0.028 + (time.time() % 100) / 10000, 4))
            ab_conversion_rate.labels(variant='B').set(round(0.032 + (time.time() % 80) / 10000, 4))
            ab_test_users.labels(variant='A').set(int(self._impression_count * 0.5))
            ab_test_users.labels(variant='B').set(int(self._impression_count * 0.5))


def main():
    collector = MetricsCollector()

    # Start consumer threads
    threads = [
        threading.Thread(target=collector.consume_clickstream, daemon=True),
        threading.Thread(target=collector.consume_inventory, daemon=True),
        threading.Thread(target=collector.consume_user_events, daemon=True),
        threading.Thread(target=collector.update_throughput, daemon=True),
    ]
    for t in threads:
        t.start()

    # Start HTTP server for Prometheus scraping
    from prometheus_client import start_http_server
    logger.info("Starting metrics HTTP server on port 8000")
    start_http_server(8000)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        collector.running = False
        logger.info("Shutting down metrics exporter")


if __name__ == '__main__':
    main()

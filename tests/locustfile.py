"""
Locust Load Test - Layer 7
Tests BentoML API endpoints under 10x normal traffic.
Validates the system doesn't collapse under pressure.
"""

import random
from locust import HttpUser, task, between


class EcommerceAPIUser(HttpUser):
    """Simulates a user hitting the recommendation and pricing APIs."""

    wait_time = between(0.1, 0.5)  # Aggressive wait time for load testing
    host = "http://localhost:3001"

    @task(3)
    def get_recommendations(self):
        """Test POST /recommend endpoint (most frequent)."""
        user_id = random.randint(1, 500)
        payload = {
            "user_id": user_id,
            "n_items": 10,
            "exclude_items": [random.randint(1, 2000) for _ in range(3)],
        }
        with self.client.post(
            "/recommend",
            json=payload,
            catch_response=True,
            name="/recommend"
        ) as response:
            if response.status_code == 200:
                data = response.json()
                if len(data.get("recommendations", [])) == 0:
                    response.failure("No recommendations returned")
                elif data.get("latency_ms", 0) > 1000:
                    response.failure(f"Latency too high: {data['latency_ms']}ms")
                else:
                    response.success()
            else:
                response.failure(f"Status {response.status_code}")

    @task(2)
    def get_price(self):
        """Test POST /price endpoint."""
        item_id = random.randint(1, 2000)
        payload = {
            "item_id": item_id,
            "current_stock": random.randint(1, 500),
            "demand_score": round(random.uniform(0.5, 3.0), 2),
            "competitor_price": round(random.uniform(20, 200), 2),
            "view_count_1h": random.randint(0, 500),
            "cart_count_1h": random.randint(0, 50),
        }
        with self.client.post(
            "/price",
            json=payload,
            catch_response=True,
            name="/price"
        ) as response:
            if response.status_code == 200:
                data = response.json()
                if data.get("optimal_price", 0) <= 0:
                    response.failure("Invalid price returned")
                elif data.get("latency_ms", 0) > 1000:
                    response.failure(f"Latency too high: {data['latency_ms']}ms")
                else:
                    response.success()
            else:
                response.failure(f"Status {response.status_code}")

    @task(1)
    def health_check(self):
        """Test /health_check endpoint."""
        with self.client.post(
            "/health_check",
            json={},
            catch_response=True,
            name="/health_check"
        ) as response:
            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "healthy":
                    response.success()
                else:
                    response.failure(f"Unhealthy: {data}")
            else:
                response.failure(f"Status {response.status_code}")

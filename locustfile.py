# locust
from locust import HttpUser, task, between

class ProducerUser(HttpUser):
    host = "http://127.0.0.1:5000"
    wait_time = between(1, 2)

    @task
    def publish_internal(self):
        self.client.post("/publish", json={
            "message": "test message",
            "topic": "internal",
            "events": ["event1", "event2"]
        })

    @task
    def publish_external(self):
        self.client.post("/publish", json={
            "topic": "external"
        })

    @task
    def broadcast_message(self):
        self.client.post("/broadcast", json={"message": "urgent broadcast"})

class ConsumerUser(HttpUser):
    host = "http://127.0.0.1:5001"
    wait_time = between(1, 2)

    @task
    def subscribe_topic(self):
        self.client.post("/subscribe", json={"username": "user1", "topic": "internal"})
    @task
    def unsubscribe_topic(self):
        self.client.post("/unsubscribe", json={"username": "user1", "topic": "internal"})

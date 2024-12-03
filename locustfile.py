from locust import HttpUser, task, between

class MessageProducerSimulation(HttpUser):
    # Configuration for the producer service endpoint
    host = "http://127.0.0.1:5000"
    
    # Randomize wait time between 1-2 seconds to simulate realistic user behavior
    wait_time = between(1, 2)

    @task
    def send_internal_message(self):
        """
        Simulate sending an internal message with multiple events
        Mimics a typical internal communication scenario
        """
        self.client.post("/publish", json={
            "message": "system notification",
            "topic": "internal",
            "events": ["quarterly_review", "team_meeting"]
        })

    @task
    def send_external_message(self):
        """
        Simulate sending an external communication
        With minimal payload to test basic publishing
        """
        self.client.post("/publish", json={
            "topic": "external",
        })

    @task
    def send_urgent_broadcast(self):
        """
        Simulate sending an urgent broadcast message
        Simulates emergency or high-priority communication
        """
        self.client.post("/broadcast", json={"message": "critical system alert"})

class MessageConsumerSimulation(HttpUser):
    # Configuration for the consumer service endpoint
    host = "http://127.0.0.1:5001"
    
    # Randomize wait time between 1-2 seconds to simulate realistic user behavior
    wait_time = between(1, 2)

    @task
    def register_topic_subscription(self):
        """
        Simulate a user subscribing to a specific topic
        Demonstrates topic-based message filtering
        """
        self.client.post("/subscribe", json={
            "username": "operator", 
            "topic": "company_updates"
        })

    @task
    def remove_topic_subscription(self):
        """
        Simulate a user unsubscribing from a topic
        Tests subscription management functionality
        """
        self.client.post("/unsubscribe", json={
            "username": "operator", 
            "topic": "company_updates"
        })
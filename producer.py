from flask import Flask, request, jsonify
from pika.exchange_type import ExchangeType
import requests
import pika
import json
import logging

# Configure logging for better error tracking and monitoring
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# Initialize Flask application
message_routing_app = Flask(__name__)

# RabbitMQ Connection Configuration
# Using environment variables or configuration management is recommended in production
rabbitmq_credentials = pika.PlainCredentials('guest', 'guest')
rabbitmq_connection_params = pika.ConnectionParameters(
    host='localhost',
    port=5673,
    credentials=rabbitmq_credentials,
    heartbeat=10
)

# Messaging Constants
MESSAGE_EXCHANGE = 'event_routing'
GLOBAL_BROADCAST_CHANNEL = 'broadcast'

# Predefined Topic Whitelist
AUTHORIZED_MESSAGE_TOPICS = ['internal', 'external']

def retrieve_job_listings():
    """
    Fetch job listings from an external job board API.
    
    Returns:
        dict: Job listing data or error information
    """
    try:
        job_board_endpoint = "https://boards-api.greenhouse.io/v1/boards/discord/jobs/"
        api_response = requests.get(job_board_endpoint, timeout=10)
        
        if api_response.status_code == 200:
            return api_response.json()
        
        logging.error(f"Job listing API request failed with status code: {api_response.status_code}")
        return {"error": "Failed to retrieve job listings"}
    
    except requests.RequestException as request_error:
        logging.error(f"Network error during job listing retrieval: {request_error}")
        return {"error": "Network error occurred"}

@message_routing_app.route('/publish', methods=['POST'])
def publish_message():
    """
    Publish messages to specific topics with routing capabilities.
    
    Handles:
    - Validation of message topics
    - Publishing internal events
    - Publishing external data
    
    Returns:
        JSON response with publishing status
    """
    # Validate and extract request payload
    request_payload = request.json
    message_content = request_payload.get("message", "")
    message_topic = request_payload.get("topic", "")
    event_list = request_payload.get("events", [])

    # Strict topic validation
    if message_topic not in AUTHORIZED_MESSAGE_TOPICS:
        error_details = f"Invalid message routing topic: {message_topic}"
        logging.warning(error_details)
        return jsonify(status="error", message=error_details), 400

    try:
        # Establish RabbitMQ connection
        rabbitmq_connection = pika.BlockingConnection(rabbitmq_connection_params)
        message_channel = rabbitmq_connection.channel()

        # Declare a durable direct exchange for precise message routing
        message_channel.exchange_declare(
            exchange=MESSAGE_EXCHANGE, 
            exchange_type=ExchangeType.direct, 
            durable=True
        )

        # Handle internal topic messages
        if message_topic == "internal":
            for individual_event in event_list:
                message_channel.basic_publish(
                    exchange=MESSAGE_EXCHANGE,
                    routing_key=message_topic,
                    body=individual_event,
                    properties=pika.BasicProperties(delivery_mode=2)  # Ensure message persistence
                )

        # Handle external topic messages
        elif message_topic == "external":
            external_job_data = retrieve_job_listings()
            
            if isinstance(external_job_data, dict) and "error" not in external_job_data:
                serialized_job_data = json.dumps(external_job_data).encode('utf-8')
                message_channel.basic_publish(
                    exchange=MESSAGE_EXCHANGE,
                    routing_key=message_topic,
                    body=serialized_job_data,
                    properties=pika.BasicProperties(delivery_mode=2)
                )

        # Close RabbitMQ connection
        rabbitmq_connection.close()

        return jsonify(status="success", message="Messages routed successfully!")

    except pika.exceptions.AMQPError as messaging_error:
        logging.error(f"RabbitMQ messaging error: {messaging_error}")
        return jsonify(status="error", message="Message routing failed"), 500

@message_routing_app.route('/broadcast', methods=['POST'])
def send_global_broadcast():
    """
    Send urgent broadcast messages across all channels.
    
    Returns:
        JSON response with broadcast status
    """
    request_payload = request.json
    urgent_message = request_payload.get("message", "")

    try:
        # Establish RabbitMQ connection
        rabbitmq_connection = pika.BlockingConnection(rabbitmq_connection_params)
        broadcast_channel = rabbitmq_connection.channel()

        # Publish urgent message to broadcast channel
        broadcast_channel.basic_publish(
            exchange=MESSAGE_EXCHANGE,
            routing_key=GLOBAL_BROADCAST_CHANNEL,
            body=urgent_message,
            properties=pika.BasicProperties(delivery_mode=2)  # Ensure message persistence
        )

        # Close RabbitMQ connection
        rabbitmq_connection.close()

        return jsonify(status="success", message="Urgent broadcast transmitted successfully!")

    except pika.exceptions.AMQPError as messaging_error:
        logging.error(f"Broadcast transmission error: {messaging_error}")
        return jsonify(status="error", message="Broadcast transmission failed"), 500

if __name__ == "__main__":
    message_routing_app.run(debug=True)
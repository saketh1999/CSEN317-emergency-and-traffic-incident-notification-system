from flask import Flask, request, jsonify
from threading import Thread, Lock
from requests.auth import HTTPBasicAuth
from datetime import datetime
import pika
import sys
import logging
import time
import requests
import pytz
import json

# Synchronization primitive for thread-safe operations
subscription_synchronization_lock = Lock()

# Messaging Constants
GLOBAL_BROADCAST_CHANNEL = 'broadcast'
AUTHORIZED_MESSAGE_TOPICS = ['internal', 'external']

def discover_active_message_queues():
    """
    Retrieve list of active message queues from RabbitMQ management API.
    
    Returns:
        list: Names of active queues
    """
    try:
        queue_discovery_endpoint = "http://localhost:15673/api/queues"
        api_response = requests.get(
            queue_discovery_endpoint, 
            auth=HTTPBasicAuth('guest', 'guest')
        )
        
        if api_response.status_code == 200:
            queues = api_response.json()
            return [queue['name'] for queue in queues if queue['name']]
        
        logging.error(f"Queue discovery failed: {api_response.text}")
        return []
    
    except requests.RequestException as network_error:
        logging.error(f"Network error during queue discovery: {network_error}")
        return []

class PacificTimeFormatter(logging.Formatter):
    """
    Custom logging formatter to display timestamps in Pacific Time Zone.
    """
    def formatTime(self, record, datefmt=None):
        pacific_timezone = pytz.timezone('America/Los_Angeles')
        converted_time = datetime.fromtimestamp(record.created, pacific_timezone)
        return converted_time.strftime('%Y-%m-%d %H:%M:%S')

def get_current_pacific_timestamp():
    """
    Generate current timestamp in Pacific Time Zone.
    
    Returns:
        str: Formatted timestamp
    """
    pacific_timezone = pytz.timezone('America/Los_Angeles')
    return datetime.now(pacific_timezone).strftime('%Y-%m-%d %H:%M:%S')

# Configure logging based on verbosity
log_level = logging.INFO if '--verbose' in sys.argv else logging.ERROR
logging.basicConfig(
    level=log_level, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
for log_handler in logging.root.handlers:
    log_handler.setFormatter(PacificTimeFormatter())

# Suppress werkzeug logs
logging.getLogger('werkzeug').setLevel(logging.ERROR)

message_routing_app = Flask(__name__)

# RabbitMQ connection configuration
rabbitmq_credentials = pika.PlainCredentials('guest', 'guest')
rabbitmq_connection_params = pika.ConnectionParameters(
    host='localhost',
    port=5673,
    credentials=rabbitmq_credentials,
    heartbeat=10
)

# Tracking active consumer threads
active_consumer_threads = {}

def process_message(queue_name):
    """
    Continuous message consumption thread for a specific queue.
    
    Args:
        queue_name (str): Name of the queue to consume messages from
    """
    while True:
        try:
            # Establish RabbitMQ connection
            rabbitmq_connection = pika.BlockingConnection(rabbitmq_connection_params)
            message_channel = rabbitmq_connection.channel()
            message_channel.basic_qos(prefetch_count=1)

            def message_handler(channel, method, properties, message_body):
                """
                Process and log incoming messages with appropriate formatting.
                
                Args:
                    channel: RabbitMQ channel
                    method: Delivery method
                    properties: Message properties
                    message_body: Raw message content
                """
                decoded_message = message_body.decode('utf-8')
                current_timestamp = get_current_pacific_timestamp()

                try:
                    # Attempt to parse job listing messages
                    json_message = json.loads(decoded_message)
                    job_listings = json_message.get("jobs", [])
                    
                    # Extract relevant job details
                    processed_job_listings = [{
                        "title": job.get("title", ""),
                        "location": job.get("location", {}).get("name", ""),
                        "job_url": job.get("absolute_url", ""),
                        "education_requirement": job.get("education", ""),
                        "job_id": job.get("id", ""),
                        "last_updated": job.get("updated_at", "")
                    } for job in job_listings]

                    formatted_job_listings = json.dumps(processed_job_listings, indent=4)
                    print(f'{current_timestamp} - QUEUE -> {queue_name} new message:\n{formatted_job_listings}')
                
                except json.JSONDecodeError:
                    # Fallback for non-JSON messages
                    print(f'{current_timestamp} - QUEUE -> {queue_name} new message: {decoded_message}')
                
                # Acknowledge message processing
                channel.basic_ack(delivery_tag=method.delivery_tag)

            # Consume messages from the specified queue
            message_channel.basic_consume(
                queue=queue_name, 
                on_message_callback=message_handler
            )
            message_channel.start_consuming()
        
        except pika.exceptions.AMQPConnectionError as connection_error:
            logging.error(f'Connection lost, reconnecting in 5 seconds: {connection_error}')
            time.sleep(5)
        
        except Exception as unexpected_error:
            logging.error(f'Unexpected error in consumer thread for {queue_name}: {unexpected_error}')
            break
        
        finally:
            # Ensure connection closure
            if 'rabbitmq_connection' in locals() and rabbitmq_connection.is_open:
                rabbitmq_connection.close()

@message_routing_app.route('/subscribe', methods=['POST'])
def manage_topic_subscription():
    """
    Handle user topic subscription requests.
    
    Returns:
        JSON response with subscription status
    """
    try:
        request_data = request.json
        username = request_data.get('username')
        message_topic = request_data.get('topic')

        # Validate request parameters
        if not username or not message_topic:
            logging.error("Subscription request: Missing username or topic")
            return jsonify({'error': 'Missing username or topic'}), 400

        # Validate message topic
        if message_topic not in AUTHORIZED_MESSAGE_TOPICS:
            logging.error(f"Invalid topic subscription attempt: {message_topic}")
            return jsonify({'error': 'Topic subscription not allowed'}), 400

        # Establish RabbitMQ connection
        rabbitmq_connection = pika.BlockingConnection(rabbitmq_connection_params)
        message_channel = rabbitmq_connection.channel()

        # Create and bind queue
        queue_name = username
        message_channel.queue_declare(queue=queue_name, durable=True, exclusive=False)
        message_channel.queue_bind(
            exchange='routing', 
            queue=queue_name, 
            routing_key=GLOBAL_BROADCAST_CHANNEL
        )

        # Thread-safe topic binding
        with subscription_synchronization_lock:
            message_channel.queue_bind(
                exchange='routing', 
                queue=queue_name, 
                routing_key=message_topic
            )

        message_channel.close()
        rabbitmq_connection.close()

        # Start consumer thread if not already running
        if username not in active_consumer_threads:
            consumer_thread = Thread(target=process_message, args=(username,))
            active_consumer_threads[username] = consumer_thread
            consumer_thread.start()

        current_timestamp = get_current_pacific_timestamp()
        print(f'{current_timestamp} - USER -> {queue_name} subscribed to topic: {message_topic}.')
        
        return jsonify({
            'status': 'subscribed', 
            'queue': queue_name, 
            'topic': message_topic
        }), 200

    except Exception as subscription_error:
        logging.error(f"Subscription processing error: {subscription_error}")
        return jsonify({
            'error': 'Subscription failed', 
            'details': str(subscription_error)
        }), 500

@message_routing_app.route('/unsubscribe', methods=['POST'])
def manage_topic_unsubscription():
    """
    Handle user topic unsubscription requests.
    
    Returns:
        JSON response with unsubscription status
    """
    request_data = request.json
    username = request_data.get('username')
    message_topic = request_data.get('topic')

    # Validate request parameters
    if not username or not message_topic:
        logging.error("Unsubscription request: Missing username or topic")
        return jsonify({'error': 'Missing username or topic'}), 400

    # Validate message topic
    if message_topic not in AUTHORIZED_MESSAGE_TOPICS:
        logging.error(f"Invalid topic unsubscription attempt: {message_topic}")
        return jsonify({'error': 'Topic unsubscription not allowed'}), 400

    # Prevent unsubscribing from broadcast channel
    if message_topic == GLOBAL_BROADCAST_CHANNEL:
        current_timestamp = get_current_pacific_timestamp()
        print(f"{current_timestamp} - USER -> {username} cannot unsubscribe from broadcast topic.")
        return jsonify({'error': 'Broadcast topic unsubscription is forbidden'}), 400

    try:
        # Establish RabbitMQ connection
        rabbitmq_connection = pika.BlockingConnection(rabbitmq_connection_params)
        message_channel = rabbitmq_connection.channel()

        # Thread-safe topic unbinding
        with subscription_synchronization_lock:
            message_channel.queue_unbind(
                queue=username, 
                exchange='routing', 
                routing_key=message_topic
            )

        message_channel.close()
        rabbitmq_connection.close()

        current_timestamp = get_current_pacific_timestamp()
        print(f'{current_timestamp} - USER -> {username} unsubscribed from topic: {message_topic}.')
        
        return jsonify({
            'status': 'unsubscribed', 
            'queue': username, 
            'topic': message_topic,
            'message': 'Topic successfully unbound'
        }), 200

    except Exception as unsubscription_error:
        logging.error(f"Unsubscription processing error: {unsubscription_error}")
        return jsonify({
            'error': 'Unsubscription failed', 
            'details': str(unsubscription_error)
        }), 500

def initialize_message_consumers_on_startup():
    """
    Start consumer threads for existing queues during application startup.
    """
    active_queue_names = discover_active_message_queues()
    for queue_name in active_queue_names:
        if queue_name not in active_consumer_threads:
            consumer_thread = Thread(target=process_message, args=(queue_name,))
            active_consumer_threads[queue_name] = consumer_thread
            consumer_thread.start()
            
            current_timestamp = get_current_pacific_timestamp()
            print(f"{current_timestamp} - QUEUE -> {queue_name} consumer activated.")

if __name__ == '__main__':
    initialize_message_consumers_on_startup()
    message_routing_app.run(debug=True, port=5001)
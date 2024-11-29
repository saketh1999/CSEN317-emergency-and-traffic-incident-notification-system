from flask import Flask, request, jsonify
from threading import Thread
from requests.auth import HTTPBasicAuth
from threading import Lock
from datetime import datetime
import pika
import sys
import logging
import time
import requests
import pytz
import json

subscribe_lock = Lock()
# broadcast routing key
BROADCAST_ROUTING_KEY = 'broadcast'
# Allowed routing keys
VALID_ROUTING_KEYS = ['internal', 'external']

def retrieve_queue_list():
    url = "http://localhost:15673/api/queues"
    response = requests.get(url, auth=HTTPBasicAuth('guest', 'guest'))
    if response.status_code == 200:
        queues = response.json()
        return [q['name'] for q in queues if q['name']]  # Filter out any queues without names
    else:
        logging.error(f"Failed to fetch queues: {response.text}")
        return []

class PSTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        pst_timezone = pytz.timezone('America/Los_Angeles')
        converted_time = datetime.fromtimestamp(record.created, pst_timezone)
        return converted_time.strftime('%Y-%m-%d %H:%M:%S')

def get_current_time_in_pst():
    pst_timezone = pytz.timezone('America/Los_Angeles')
    return datetime.now(pst_timezone).strftime('%Y-%m-%d %H:%M:%S')

# Checking for '--verbose' argument; info logs will be displayed only if verbose is passed
log_level = logging.INFO if '--verbose' in sys.argv else logging.ERROR
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

# Configure logging
logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')
for handler in logging.root.handlers:
    handler.setFormatter(PSTFormatter())

app = Flask(__name__)

# RabbitMQ setup parameters
credentials = pika.PlainCredentials('guest', 'guest')
connection_config = pika.ConnectionParameters(
    host='localhost',
    port=5673,
    credentials=credentials,
    heartbeat=10
)

# Dictionary to store consumer threads
consumer_threads = {}

def message_consumer(queue_name):
    while True:
        try:
            connection = pika.BlockingConnection(connection_config)
            channel = connection.channel()
            channel.basic_qos(prefetch_count=1)

            def callback(ch, method, properties, body):
                message = body.decode('utf-8')
                try:
                    # Parsing messages as JSON string from greenhouse.io
                    json_message = json.loads(message)
                    # disaster_data = json_message.get("data", {})
                    jobs = json_message.get("data", [])
                    # filtering response
                    filtered_jobs = [
                    {
                        "id": str(job.get("id", "")),
                        "score": job.get("score"),  # Adding a default score as in the example
                        "name": job.get('fields', {}).get('name'),
                        "href": job.get("href", "")
                    } for job in jobs
                    ]

                    beautified_json = json.dumps(filtered_jobs, indent=4)
                    current_time = get_current_time_in_pst()
                    print(f'{current_time} - USERre -> {queue_name} new message:\n{beautified_json}')
                except json.JSONDecodeError:
                    current_time = get_current_time_in_pst()
                    print(f'{current_time} - USER -> {queue_name} new message: {message}')
                ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(queue=queue_name, on_message_callback=callback)
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            logging.error(f'Connection was closed, retrying in 5 seconds: {e}')
            time.sleep(5)
        except Exception as e:
            logging.error(f'Unexpected error: {e}, exiting consumer thread for {queue_name}')
            break
        finally:
            if connection.is_open:
                connection.close()


@app.route('/subscribe', methods=['POST'])
def create_subscription():
    try:
        data = request.json
        username = data.get('username')
        routing_key = data.get('topic')
        # Validate username and routing key
        if not username or not routing_key:
            logging.error(f"Subscribe error: Missing username or routing key in request")
            return jsonify({'error': 'Missing username or routing key'}), 400

        # Check if the routing key is allowed
        if routing_key not in VALID_ROUTING_KEYS:
            logging.error(f"Subscribe error: Attempt to subscribe to invalid routing key '{routing_key}' by user '{username}'")
            return jsonify({'error': 'Subscription to this routing key is not allowed'}), 400

        connection = pika.BlockingConnection(connection_config)
        channel = connection.channel()

        queue_name = username
        channel.queue_declare(queue=queue_name, durable=True, exclusive=False)
        channel.queue_bind(exchange='routing', queue=queue_name, routing_key=BROADCAST_ROUTING_KEY)
        # locking here when binding; unlocking will be done after this execution.
        with subscribe_lock:
            channel.queue_bind(exchange='routing', queue=queue_name, routing_key=routing_key)

        channel.close()
        connection.close()

        if username not in consumer_threads:
            consumer_thread = Thread(target=message_consumer, args=(username,))
            consumer_threads[username] = consumer_thread
            consumer_thread.start()

        current_time = get_current_time_in_pst()
        print(f'{current_time} - USER -> {queue_name} subscribed for routing key: {routing_key}.')
        return jsonify({'status': 'subscribed', 'queue': queue_name, 'routing_key': routing_key}), 200
    except Exception as e:
        logging.error(f"Subscription error: {e}")
        return jsonify({'error': 'Failed to subscribe', 'details': str(e)}), 500


@app.route('/unsubscribe', methods=['POST'])
def remove_subscription():
    data = request.json
    username = data.get('username')
    routing_key = data.get('topic')

    # Validate username and routing key
    if not username or not routing_key:
        logging.error(f"Subscribe error: Missing username or routing key in request")
        return jsonify({'error': 'Missing username or routing key'}), 400

    # Check if the routing key is allowed
    if routing_key not in VALID_ROUTING_KEYS:
        logging.error(f"Subscribe error: Attempt to unsubscribe to invalid routing key '{routing_key}' by user '{username}'")
        return jsonify({'error': 'Invalid routing key unsubscription is not allowed'}), 400

    # Check if the routing key is 'broadcast'
    if routing_key == 'broadcast':
        current_time = get_current_time_in_pst()
        print(f"{current_time} - USER -> {username} unsubscribing from the broadcast routing key is not allowed.")
        return jsonify({'error': 'Unsubscribing from the broadcast routing key is not allowed'}), 400

    try:
        connection = pika.BlockingConnection(connection_config)
        channel = connection.channel()

        # Unbind the routing key from the user's queue
        # locking here when unbinding; unlocking will be done after this execution.
        with subscribe_lock:
            channel.queue_unbind(queue=username, exchange='routing', routing_key=routing_key)

        channel.close()
        connection.close()
        current_time = get_current_time_in_pst()
        print(f'{current_time} - USER -> {username} unsubscribed from routing key: {routing_key}.')
        return jsonify({'status': 'unsubscribed', 'queue': username, 'routing_key': routing_key,
                        'message': 'Routing key unbound from queue successfully'}), 200
    except Exception as e:
        return jsonify({'error': 'Failed to unbind routing key from queue', 'details': str(e)}), 500

def start_consumers_on_startup():
    queue_names = retrieve_queue_list()
    for queue_name in queue_names:
        if queue_name not in consumer_threads:
            consumer_thread = Thread(target=message_consumer, args=(queue_name,))
            consumer_threads[queue_name] = consumer_thread
            consumer_thread.start()
            current_time = get_current_time_in_pst()
            print(f"{current_time} - USER -> {queue_name} is active.")

if __name__ == '__main__':
    start_consumers_on_startup()
    app.run(debug=True, port=5001)
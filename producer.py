from flask import Flask, request, jsonify
from pika.exchange_type import ExchangeType
import requests
import pika
import json
import logging

app = Flask(__name__)

credentials = pika.PlainCredentials('guest', 'guest')
connection_config = pika.ConnectionParameters(
    host='localhost',
    port=5673,
    credentials=credentials,
    heartbeat=10
)
ROUTING_EXCHANGE = 'routing'
BROADCAST_ROUTING_KEY = 'broadcast'
# Allowed routing keys
VALID_ROUTING_KEYS = ['internal', 'external']

def retrieve_external_data():
    url = "https://api.reliefweb.int/v1/disasters"
    response = requests.get(url)
    if response.status_code == 200:
        # print(response.json())
        return response.json()
    else:
        return {"error": "Failed to fetch data"}

@app.route('/publish', methods=['POST'])
def send_message():
    # Get message and routing key from POST data
    data = request.json
    message = data.get("message", "")
    routing_key = data.get("topic", "")
    events = data.get("events", [])
    
    # Check if the routing key is neither 'internal' nor 'external'
    if routing_key not in VALID_ROUTING_KEYS:
        error_message = f"Invalid routing key specified: {routing_key}"
        logging.error(error_message)
        return jsonify(status="error", message="Invalid routing key specified"), 400
    
    # Create a connection to the RabbitMQ server
    connection = pika.BlockingConnection(connection_config)
    channel = connection.channel()
    
    # Exchange Declaration
    channel.exchange_declare(exchange=ROUTING_EXCHANGE, exchange_type=ExchangeType.direct, durable=True)
    
    if routing_key == "internal":
        # make message persistent delivery_mode=2
        for event in events:  # Process events from the request
            channel.basic_publish(
                exchange=ROUTING_EXCHANGE,
                routing_key=routing_key,
                body=event,
                properties=pika.BasicProperties(delivery_mode=2)  # make message persistent
            )
    
    elif routing_key == "external":
        external_data = retrieve_external_data()
        if isinstance(external_data, dict) and "error" not in external_data:
            # Converting the dictionary of external events to a JSON-formatted string
            json_data = json.dumps(external_data).encode('utf-8')
            channel.basic_publish(
                exchange=ROUTING_EXCHANGE,
                routing_key=routing_key,
                body=json_data,
                properties=pika.BasicProperties(delivery_mode=2)
            )
    
    connection.close()
    return jsonify(status="success", message="Message(s) published successfully!")

@app.route('/broadcast', methods=['POST'])
def send_broadcast():
    data = request.json
    urgent_message = data.get("message", "")
    
    connection = pika.BlockingConnection(connection_config)
    channel = connection.channel()
    
    channel.basic_publish(
        exchange=ROUTING_EXCHANGE,
        routing_key=BROADCAST_ROUTING_KEY,
        body=urgent_message,
        properties=pika.BasicProperties(delivery_mode=2)
    )
    
    connection.close()
    return jsonify(status="success", message="Broadcast message sent successfully!")

if __name__ == "__main__":
    app.run(debug=True)
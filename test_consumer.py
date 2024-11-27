import unittest
from unittest.mock import patch, MagicMock
import consumer  # Renamed from 'consumer'

class MessageSubscriptionTestSuite(unittest.TestCase):
    """
    Test suite for the Message Subscription Management Application.
    Covers subscription, unsubscription, and consumer startup scenarios.
    """
    
    def setUp(self):
        """
        Prepare test client before each test method.
        Sets up a test environment for the Flask application.
        """
        self.test_client = consumer.app.test_client()
        self.test_client.testing = True

    @patch('consumer.message_processor')  # Renamed from message_consumer
    @patch('consumer.pika.BlockingConnection')
    def test_topic_subscription_success(self, mock_rabbitmq_connection, mock_message_processor):
        """
        Test successful topic subscription for a user.
        Verifies that a user can subscribe to an allowed topic.
        """
        # Create a mock channel for RabbitMQ connection
        mock_channel = MagicMock()
        mock_rabbitmq_connection.return_value.channel.return_value = mock_channel
        
        # Attempt to subscribe to an internal topic
        response = self.test_client.post('/subscribe', json={
            'username': 'system_analyst',
            'topic': 'internal'  # Predefined allowed topic
        })
        
        # Validate subscription response
        self.assertEqual(response.status_code, 200)
        self.assertIn('subscribed', response.json['status'])

    @patch('consumer.message_processor')
    @patch('consumer.pika.BlockingConnection')
    def test_topic_unsubscription_success(self, mock_rabbitmq_connection, mock_message_processor):
        """
        Test successful topic unsubscription for a user.
        Verifies that a user can unsubscribe from a previously subscribed topic.
        """
        # Create a mock channel for RabbitMQ connection
        mock_channel = MagicMock()
        mock_rabbitmq_connection.return_value.channel.return_value = mock_channel
        
        # Attempt to unsubscribe from an internal topic
        response = self.test_client.post('/unsubscribe', json={
            'username': 'system_analyst',
            'topic': 'internal'  # Predefined allowed topic
        })
        
        # Validate unsubscription response
        self.assertEqual(response.status_code, 200)
        self.assertIn('unsubscribed', response.json['status'])

    @patch('consumer.retrieve_queue_list')  # Renamed from fetch_queues
    @patch('consumer.message_processor')
    def test_initialize_message_consumers_on_startup(self, mock_message_processor, mock_retrieve_queue_list):
        """
        Test consumer initialization during application startup.
        Verifies that message processors are started for all available queues.
        """
        # Simulate available queues during startup
        mock_retrieve_queue_list.return_value = ['notification_queue', 'event_queue']
        
        # Trigger consumers startup
        consumer.start_consumers_on_startup()
        
        # Validate that message processors are called for each queue
        mock_message_processor.assert_has_calls([
            unittest.mock.call('notification_queue'),
            unittest.mock.call('event_queue')
        ], any_order=True)

if __name__ == '__main__':
    unittest.main()
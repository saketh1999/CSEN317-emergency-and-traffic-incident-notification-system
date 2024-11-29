import unittest
from unittest.mock import patch, MagicMock
import consumer

class TestConsumerApp(unittest.TestCase):
    def setUp(self):
        self.app = consumer.app.test_client()
        self.app.testing = True

    # Mock the message_consumer function
    @patch('consumer.message_consumer')
    @patch('consumer.pika.BlockingConnection')
    def test_create_subscription_success(self, mock_connection, mock_message_consumer):
        mock_channel = MagicMock()
        mock_connection.return_value.channel.return_value = mock_channel
        response = self.app.post('/subscribe', json={
            'username': 'testuser',
            'topic': 'internal' # Allowed routing key
        })
        self.assertEqual(response.status_code, 200)
        self.assertIn('subscribed', response.json['status'])

    # Mock the message_consumer function
    @patch('consumer.message_consumer')
    @patch('consumer.pika.BlockingConnection')
    def test_remove_subscription_success(self, mock_connection, mock_message_consumer):
        mock_channel = MagicMock()
        mock_connection.return_value.channel.return_value = mock_channel
        response = self.app.post('/unsubscribe', json={
            'username': 'testuser',
            'topic': 'internal' # Allowed routing key
        })
        self.assertEqual(response.status_code, 200)
        self.assertIn('unsubscribed', response.json['status'])

    # Mock the message_consumer function
    @patch('consumer.message_consumer')
    @patch('consumer.retrieve_queue_list')
    def test_start_consumers_on_startup(self, mock_retrieve_queue_list, mock_message_consumer):
        mock_retrieve_queue_list.return_value = ['queue1', 'queue2']
        consumer.start_consumers_on_startup()
        mock_message_consumer.assert_has_calls([
            unittest.mock.call('queue1'),
            unittest.mock.call('queue2')
        ], any_order=True)

if __name__ == '__main__':
    unittest.main()
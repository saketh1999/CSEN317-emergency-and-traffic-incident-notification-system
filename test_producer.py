import unittest
from unittest.mock import patch, MagicMock
import producer  # Renamed from 'producer'

class JobPublisherTestSuite(unittest.TestCase):
    """
    Test suite for the Job Publisher application.
    Covers various scenarios of message publishing and API interactions.
    """
    
    def setUp(self):
        """
        Prepare test client before each test method.
        Sets up a test environment for the Flask application.
        """
        self.test_client = producer.app.test_client()
        self.test_client.testing = True

    @patch('producer.requests.get')  # Mock external API request
    @patch('producer.pika.BlockingConnection')  # Mock RabbitMQ connection
    def test_fetch_external_job_data_success(self, mock_rabbitmq_connection, mock_api_request):
        """
        Test successful retrieval and processing of external job data.
        Verifies API call mocking and message publishing for external topics.
        """
        # Simulate successful Greenhouse API response with job data
        mock_api_request.return_value = MagicMock(
            status_code=200, 
            json=lambda: {
                "jobs": [
                    {
                        "absolute_url": "https://careers.example.com/job/data-science-intern",
                        "internal_job_id": 5672455002,
                        "location": {"name": "Remote"},
                        "title": "Machine Learning Research Intern"
                    }
                ]
            }
        )
        
        # Test publishing message for external job topic
        response = self.test_client.post('/publish', json={
            'message': 'New Job Listing Discovered',
            'topic': 'external'
        })
        
        # Validate response
        self.assertEqual(response.status_code, 200)
        self.assertIn('success', response.json['status'])

    @patch('producer.pika.BlockingConnection')  # Mock RabbitMQ connection
    def test_publish_internal_event_success(self, mock_rabbitmq_connection):
        """
        Test successful publishing of internal events.
        Verifies message publishing for internal topics with multiple events.
        """
        # Test internal topic publishing
        response = self.test_client.post('/publish', json={
            'message': 'Internal Team Update',
            'topic': 'internal',
            'events': ['recruitment_cycle', 'candidate_review']
        })
        
        # Validate response
        self.assertEqual(response.status_code, 200)
        self.assertIn('success', response.json['status'])

    @patch('producer.pika.BlockingConnection')
    def test_emergency_broadcast_success(self, mock_rabbitmq_connection):
        """
        Test emergency broadcast functionality.
        Ensures urgent messages can be sent across all channels.
        """
        # Test broadcast endpoint for critical messages
        response = self.test_client.post('/broadcast', json={
            'message': 'Critical System Notification'
        })
        
        # Validate response
        self.assertEqual(response.status_code, 200)
        self.assertIn('success', response.json['status'])

    def tearDown(self):
        """
        Clean up resources after each test method.
        Currently a no-op, but can be extended for complex teardown scenarios.
        """
        pass

if __name__ == '__main__':
    unittest.main()
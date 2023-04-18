import unittest
from unittest.mock import patch, Mock
from aind_data_access_api.secrets_access import get_secret, get_parameter


class TestMyModule(unittest.TestCase):

    @patch('boto3.client')
    def test_get_secret_success(self, mock_boto3_client):
        # Mock the Secrets Manager client and response
        mock_client = Mock()
        mock_boto3_client.return_value = mock_client
        mock_secret_string = '{"username": "admin", "password": "secret_password"}'
        mock_response = {'SecretString': mock_secret_string}
        mock_client.get_secret_value.return_value = mock_response

        # Call the get_secret method with a mock secret name
        secret_name = 'my_secret'
        secret_value = get_secret(secret_name)

        # Assert that the Secrets Manager client was called with the correct arguments
        mock_boto3_client.assert_called_with('secretsmanager')
        mock_client.get_secret_value.assert_called_with(SecretId=secret_name)

        # Assert that the secret value returned matches the expected value
        expected_value = 'secret_password'
        self.assertEqual(secret_value, expected_value)

    @patch('boto3.client')
    def test_get_secret_permission_denied(self, mock_boto3_client):
        # Set up mock ClientError with 403 status code for get_secret_value method
        mock_client = Mock()
        mock_secret_string = '{"username": "admin", "password": "secret_password"}'
        mock_boto3_client.return_value.get_secret_value.side_effect = \
            mock_client.exceptions.ClientError(
                {'Error': {'Code': 'AccessDeniedException', 'HTTPStatusCode': 403}},
                'operation_name'
            )
        # Call the method to get the secret
        result = get_secret('my_secret')

        # Check that the method returns None
        self.assertIsNone(result)

    @patch('boto3.client')
    def test_get_parameter_success(self, mock_boto3_client):
        # Mock the Systems Manager client and response
        mock_client = Mock()
        mock_boto3_client.return_value = mock_client
        mock_parameter_value = 'my_parameter_value'
        mock_response = {'Parameter': {'Value': mock_parameter_value}}
        mock_client.get_parameter.return_value = mock_response

        # Call the get_parameter method with a mock parameter name
        parameter_name = '/my/parameter/name'
        parameter_value = get_parameter(parameter_name)

        # Assert that the Systems Manager client was called with the correct arguments
        mock_boto3_client.assert_called_with('ssm')
        mock_client.get_parameter.assert_called_with(Name=parameter_name)

        # Assert that the parameter value returned matches the expected value
        expected_value = 'my_parameter_value'
        self.assertEqual(parameter_value, expected_value)


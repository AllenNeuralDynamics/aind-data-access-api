"""Test secrets_access module."""
import unittest
from unittest.mock import Mock, patch

from botocore.exceptions import ClientError

from aind_data_access_api.secrets import get_parameter, get_secret


class TestSecretAccess(unittest.TestCase):
    """Test methods in secrets_access module"""

    @patch("boto3.client")
    def test_get_secret_success(self, mock_boto3_client):
        """Tests that secret is retrieved as expected"""
        # Mock the Secrets Manager client and response
        mock_client = Mock()
        mock_boto3_client.return_value = mock_client
        mock_secret_string = (
            '{"username": "admin",'
            ' "password": "secret_password",'
            ' "host": "some host",'
            ' "database": "some database"}'
        )
        mock_response = {"SecretString": mock_secret_string}
        mock_client.get_secret_value.return_value = mock_response

        # Call the get_secret method with a mock secret name
        secret_name = "my_secret"
        secret_value = get_secret(secret_name)

        # Assert that the client was called with the correct arguments
        mock_boto3_client.assert_called_with("secretsmanager")
        mock_client.get_secret_value.assert_called_with(SecretId=secret_name)

        # Assert that the secret value returned matches the expected value
        expected_value = {
            "username": "admin",
            "password": "secret_password",
            "host": "some host",
            "database": "some database",
        }
        self.assertEqual(secret_value, expected_value)

    @patch("boto3.client")
    def test_get_secret_permission_denied(self, mock_boto3_client):
        """Tests  secret retrieval fails with incorrect aws permissions"""
        mock_boto3_client.return_value.get_secret_value.side_effect = (
            ClientError(
                {
                    "Error": {
                        "Code": "AccessDeniedException",
                        "HTTPStatusCode": 403,
                    }
                },
                "get_secret_value",
            )
        )
        # Assert that ClientError is raised
        with self.assertRaises(ClientError):
            get_secret("my_secret")

    @patch("boto3.client")
    def test_get_parameter_success(self, mock_boto3_client):
        """ "Tests that parameter is retrieved as expected"""
        # Mock the Systems Manager client and response
        mock_client = Mock()
        mock_boto3_client.return_value = mock_client
        mock_parameter_value = "my_parameter_value"
        mock_response = {"Parameter": {"Value": mock_parameter_value}}
        mock_client.get_parameter.return_value = mock_response

        # Call the get_parameter method with a mock parameter name
        parameter_name = "/my/parameter/name"
        parameter_value = get_parameter(parameter_name)

        # Assert that the client was called with the correct arguments
        mock_boto3_client.assert_called_with("ssm")
        mock_client.get_parameter.assert_called_with(
            Name=parameter_name, WithDecryption=False
        )

        # Assert that the parameter value returned matches the expected value
        expected_value = "my_parameter_value"
        self.assertEqual(parameter_value, expected_value)

    @patch("boto3.client")
    def test_get_parameter_permission_denied(self, mock_boto3_client):
        """Tests parameter retrieval fails with incorrect aws permissions"""
        mock_boto3_client.return_value.get_parameter.side_effect = ClientError(
            {
                "Error": {
                    "Code": "AccessDeniedException",
                    "HTTPStatusCode": 403,
                }
            },
            "get_parameter",
        )
        # Assert that ClientError is raised
        with self.assertRaises(ClientError):
            get_parameter("my_parameter")

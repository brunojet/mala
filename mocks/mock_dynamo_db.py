import boto3
import pytest
from unittest.mock import MagicMock, patch
from moto import mock_aws


DESCRIBLE_TABLE_MOCK = {
    "Table": {
        "TableStatus": "ACTIVE",
        "ProvisionedThroughput": {
            "ReadCapacityUnits": 5,
            "WriteCapacityUnits": 5,
        },
    }
}

QUERY_IDS_MOCK = {
    "Items": [
        {"id": {"S": "test app 1"}, "id_range": {"S": "SF01#1.0.0"}},
        {"id": {"S": "test app 2"}, "id_range": {"S": "SF01#1.1.0"}},
    ],
    "Count": 2,
    "ScannedCount": 2,
    "LastEvaluatedKey": None
}


@pytest.fixture
def dynamodb():
    with mock_aws():
        resource = boto3.resource("dynamodb", region_name="us-east-1")
        client = boto3.client("dynamodb", region_name="us-east-1")
        table = resource.create_table(
            TableName="test_table",
            KeySchema=[
                {"AttributeName": "id", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "id", "AttributeType": "S"},
            ],
            ProvisionedThroughput={
                "ReadCapacityUnits": 5,
                "WriteCapacityUnits": 5,
            },
        )
        table.wait_until_exists()
        yield resource, client, table


@pytest.fixture
def dynamodb_with_range_key():
    with mock_aws():
        resource = boto3.resource("dynamodb", region_name="us-east-1")
        client = boto3.client("dynamodb", region_name="us-east-1")
        table = resource.create_table(
            TableName="test_table",
            KeySchema=[
                {"AttributeName": "id", "KeyType": "HASH"},
                {"AttributeName": "id_range", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "id", "AttributeType": "S"},
                {"AttributeName": "id_range", "AttributeType": "S"},
            ],
            ProvisionedThroughput={
                "ReadCapacityUnits": 5,
                "WriteCapacityUnits": 5,
            },
        )
        table.wait_until_exists()
        yield resource, client, table

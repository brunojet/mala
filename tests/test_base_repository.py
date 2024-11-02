import pytest
from moto import mock_aws
import boto3
from base_repository import BaseRepository


@pytest.fixture
def dynamodb():
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
        table = dynamodb.create_table(
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
        yield dynamodb


@pytest.fixture
def base_repository(dynamodb):
    return BaseRepository(
        table_name="test_table",
        max_item_size=256,
        has_range_key=False,
        range_key_items=[],
        gsi_key_schemas=[],
    )


def test_init_table(base_repository):
    assert base_repository.table_name == "test_table"
    assert base_repository.max_read_items > 0
    assert base_repository.max_write_items > 0


def test_insert(base_repository):
    item = {"id": "test_id", "name": "test_name"}
    result = base_repository.insert(item)
    assert result == "test_id"


def test_query(base_repository):
    key_condition = {"id": "test_id"}
    items, last_evaluated_key = base_repository.query(key_condition)
    assert isinstance(items, list)
    assert last_evaluated_key is None

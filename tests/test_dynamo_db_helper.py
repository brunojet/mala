import boto3
import pytest
from typing import Any, Tuple
from moto import mock_aws
from unittest.mock import patch
from dynamo_db_utils import create_table, DESCRIBLE_TABLE
from dynamo_db_helper import DynamoDBHelper, PRIMARY_HASH_KEY, PRIMARY_RANGE_KEY


@pytest.fixture
def dynamodb():
    with mock_aws():
        resource = boto3.resource("dynamodb", region_name="us-east-1")
        client = boto3.client("dynamodb", region_name="us-east-1")

        key_schema = {"HASH": "id", "RANGE": "id_range"}
        gsi_key_schemas = [
            {
                "index_name": "gsi_hash_key-gsi_range_key-index",
                "HASH": "gsi_hash_key",
                "RANGE": "gsi_range_key",
            },
        ]
        table = create_table(resource, key_schema, gsi_key_schemas)

        yield client, table


@pytest.fixture
def dynamo_db_helper(dynamodb: Tuple[boto3.client, Any]):
    client, table = dynamodb

    with patch.object(
        client,
        "describe_table",
        return_value=DESCRIBLE_TABLE,
    ):
        repo = DynamoDBHelper(
            table_name="test_table",
            max_item_size=256,
            has_range_key=True,
            range_key_items=["range_key1", "range_key2"],
            gsi_key_schemas=[
                {"index_name": "GSI1", "HASH": "gsi_hash_key", "RANGE": "gsi_range_key"}
            ],
        )

        yield repo, table


def test_init(dynamo_db_helper: Tuple[DynamoDBHelper, Any]):
    repo = dynamo_db_helper[0]
    assert repo.primary_keys == [PRIMARY_HASH_KEY, PRIMARY_RANGE_KEY]
    assert repo.has_range_key is True
    assert repo.range_key_items == ["range_key1", "range_key2"]
    assert len(repo.gsi_key_schemas) == 1


def test_is_primary_key(dynamo_db_helper: Tuple[DynamoDBHelper, Any]):
    repo = dynamo_db_helper[0]

    key_condition = {PRIMARY_HASH_KEY: "hash_value", PRIMARY_RANGE_KEY: "range_value"}
    assert repo.is_primary_key(key_condition) is True

    key_condition = {PRIMARY_HASH_KEY: "hash_value"}
    assert repo.is_primary_key(key_condition) is False


def test_add_range_key(dynamo_db_helper: Tuple[DynamoDBHelper, Any]):
    repo = dynamo_db_helper[0]
    item = {
        PRIMARY_HASH_KEY: "hash_value",
        "range_key1": "value1",
        "range_key2": "value2",
    }
    repo.add_range_key(item)
    assert item[PRIMARY_RANGE_KEY] == "value1#value2"


def test_build_insert_condition_expression(
    dynamo_db_helper: Tuple[DynamoDBHelper, Any]
):
    repo = dynamo_db_helper[0]
    condition_expression = repo._build_insert_condition_expression()
    assert condition_expression is not None


def test_build_key_expression(dynamo_db_helper: Tuple[DynamoDBHelper, Any]):
    repo = dynamo_db_helper[0]
    key_condition = {PRIMARY_HASH_KEY: "hash_value", PRIMARY_RANGE_KEY: "range_value"}
    key_expression = repo._build_key_expression(key_condition)
    assert key_expression is not None


def test_get_gsi_key_schema(dynamo_db_helper: Tuple[DynamoDBHelper, Any]):
    repo = dynamo_db_helper[0]
    key_set = {"gsi_hash_key", "gsi_range_key"}
    gsi_key_schema = repo._get_gsi_key_schema(key_set)
    assert gsi_key_schema is not None


def test_get_gsi_key_expression(dynamo_db_helper: Tuple[DynamoDBHelper, Any]):
    repo = dynamo_db_helper[0]
    key_condition = {"gsi_hash_key": "hash_value", "gsi_range_key": "range_value"}
    index_name, key_expression = repo._get_gsi_key_expression(key_condition)
    assert index_name == "GSI1"
    assert key_expression is not None


def test_build_projection_expression():
    projection_expression = ["id", "name", "status"]
    expression, attribute_names = DynamoDBHelper.build_projection_expression(
        projection_expression
    )
    assert expression == "id, name, #status"
    assert attribute_names == {"#status": "status"}

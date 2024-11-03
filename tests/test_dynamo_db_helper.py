import boto3
import pytest
from datetime import datetime
from typing import Any, Tuple
from moto import mock_aws
from unittest.mock import patch
from botocore.exceptions import ClientError
from test_dynamo_db_utils import create_table
from dynamo_db_helper import DynamoDBHelper, PRIMARY_HASH_KEY, PRIMARY_RANGE_KEY
from boto3.dynamodb.conditions import Attr


@pytest.fixture
def dynamodb():
    with mock_aws():
        table_name = "test_table"
        resource = boto3.resource("dynamodb", region_name="us-east-1")
        key_schema = {"HASH": "id", "RANGE": "id_range"}
        gsi_key_schemas = [
            {
                "index_name": "gsi_hash_key-gsi_range_key-index",
                "HASH": "gsi_hash_key",
                "RANGE": "gsi_range_key",
            },
        ]

        yield create_table(table_name, resource, key_schema, gsi_key_schemas)


@pytest.fixture
def dynamo_db_helper(dynamodb: Tuple[boto3.client, Any]):
    client, table, describle_table = dynamodb

    with patch.object(
        client,
        "describe_table",
        return_value=describle_table,
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


def test_init_key_schemas(dynamo_db_helper: Tuple[DynamoDBHelper, Any]):
    helper, _ = dynamo_db_helper
    helper._init_key_schemas(
        False,
        [],
        [{"index_name": "GSI1", "HASH": "gsi_hash_key", "RANGE": "gsi_range_key"}],
    )
    assert helper.primary_keys == [PRIMARY_HASH_KEY]
    assert len(helper.gsi_key_schemas) == 1
    assert helper.gsi_key_schemas[0]["index_name"] == "GSI1"


def test_init_key_schemas_value_error(dynamo_db_helper: Tuple[DynamoDBHelper, Any]):
    helper = dynamo_db_helper[0]
    with pytest.raises(ValueError):
        helper, _ = dynamo_db_helper
        helper._init_key_schemas(
            False, [], [{"index_name": "GSI1", "RANGE": "gsi_range_key"}]
        )


def test_init_table(dynamo_db_helper: Tuple[DynamoDBHelper, Any]):
    helper, _ = dynamo_db_helper
    helper._init_table("test_table", 1024)
    assert helper.max_read_items == 4
    assert helper.max_write_items == 1


def test_init_table_value_error(dynamo_db_helper: Tuple[DynamoDBHelper, Any]):
    helper = dynamo_db_helper[0]
    with pytest.raises(ValueError):
        helper._init_table("test_table", 2048)


def test_execute_tries_with_provisioned_throughput_exceeded(
    dynamo_db_helper: Tuple[DynamoDBHelper, Any]
):
    helper, table = dynamo_db_helper

    def mock_put_item(*args, **kwargs):
        raise ClientError(
            {
                "Error": {
                    "Code": "ProvisionedThroughputExceededException",
                    "Message": "Provisioned throughput exceeded",
                }
            },
            "PutItem",
        )

    with patch.object(table, "put_item", side_effect=mock_put_item):
        with patch(
            "time.sleep", return_value=None
        ):  # Mockar time.sleep para evitar atrasos nos testes
            with pytest.raises(ClientError) as excinfo:
                helper.execute_tries(
                    table.put_item,
                    {"Item": {"id": "test_id_1", "id_range": "range_value"}},
                )
            assert (
                excinfo.value.response["Error"]["Code"]
                == "ProvisionedThroughputExceededException"
            )


def test_execute_tries_with_other_exception(
    dynamo_db_helper: Tuple[DynamoDBHelper, Any]
):
    helper, table = dynamo_db_helper

    def mock_put_item(*args, **kwargs):
        raise ClientError(
            {
                "Error": {
                    "Code": "Internal error",
                    "Message": "Internal error",
                }
            },
            "PutItem",
        )

    with patch.object(table, "put_item", side_effect=mock_put_item):
        with patch(
            "time.sleep", return_value=None
        ):  # Mockar time.sleep para evitar atrasos nos testes
            with pytest.raises(ClientError) as excinfo:
                helper.execute_tries(
                    table.put_item,
                    {"Item": {"id": "test_id_1", "id_range": "range_value"}},
                )
            assert excinfo.value.response["Error"]["Code"] == "Internal error"

import boto3
import pytest
from datetime import datetime
from typing import Any, Tuple
from moto import mock_aws
from unittest.mock import patch
from botocore.exceptions import ClientError
from dynamo_db_utils import create_table
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
    index_name, key_expression = repo.build_key_expression(key_condition)
    assert index_name is None
    assert key_expression is not None


def test_get_gsi_key_expression(dynamo_db_helper: Tuple[DynamoDBHelper, Any]):
    helper = dynamo_db_helper[0]
    key_condition = {"gsi_hash_key": "hash_value", "gsi_range_key": "range_value"}
    index_name, key_expression = helper.build_key_expression(key_condition)
    assert index_name == "GSI1"
    assert key_expression is not None


def test_get_gsi_key_expression_value_error(
    dynamo_db_helper: Tuple[DynamoDBHelper, Any]
):
    helper = dynamo_db_helper[0]
    key_condition_invalid = {"invalid_key": "value"}
    with pytest.raises(ValueError):
        helper._get_gsi_key_expression(key_condition_invalid)


def test_build_projection_expression():
    projection_expression = ["id", "name", "status"]
    expression, attribute_names = DynamoDBHelper.build_projection_expression(
        projection_expression
    )
    assert expression == "id, #name, #status"
    assert attribute_names == {"#name": "name", "#status": "status"}


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


def test_datetime_serializer():
    # Teste para datetime
    dt = datetime(2023, 1, 1, 12, 0, 0)
    assert DynamoDBHelper.datetime_serializer(dt) == "2023-01-01T12:00:00"

    # Teste para TypeError
    with pytest.raises(TypeError) as excinfo:
        DynamoDBHelper.datetime_serializer("not a datetime")
    assert str(excinfo.value) == "Type <class 'str'> not serializable"


def test_build_filter_expression_eq(dynamo_db_helper: Tuple[DynamoDBHelper, Any]):
    helper, _ = dynamo_db_helper
    filter_condition = {"status#eq": "active"}
    filter_expression = helper.build_filter_expression(filter_condition)
    from boto3.dynamodb.conditions import Attr

    assert filter_expression == Attr("status").eq("active")


def test_build_filter_expression_ne(dynamo_db_helper: Tuple[DynamoDBHelper, Any]):
    helper, _ = dynamo_db_helper
    filter_condition = {"status#ne": "inactive"}
    filter_expression = helper.build_filter_expression(filter_condition)
    assert filter_expression == Attr("status").ne("inactive")


def test_build_filter_expression_in(dynamo_db_helper: Tuple[DynamoDBHelper, Any]):
    helper, _ = dynamo_db_helper
    filter_condition = {"status#in": ["active", "pending"]}
    filter_expression = helper.build_filter_expression(filter_condition)
    assert filter_expression == Attr("status").is_in(["active", "pending"])


def test_build_filter_expression_lt(dynamo_db_helper: Tuple[DynamoDBHelper, Any]):
    helper, _ = dynamo_db_helper
    filter_condition = {"age#lt": 30}
    filter_expression = helper.build_filter_expression(filter_condition)
    assert filter_expression == Attr("age").lt(30)


def test_build_filter_expression_lte(dynamo_db_helper: Tuple[DynamoDBHelper, Any]):
    helper, _ = dynamo_db_helper
    filter_condition = {"age#lte": 30}
    filter_expression = helper.build_filter_expression(filter_condition)
    assert filter_expression == Attr("age").lte(30)


def test_build_filter_expression_gt(dynamo_db_helper: Tuple[DynamoDBHelper, Any]):
    helper, _ = dynamo_db_helper
    filter_condition = {"age#gt": 30}
    filter_expression = helper.build_filter_expression(filter_condition)
    assert filter_expression == Attr("age").gt(30)


def test_build_filter_expression_gte(dynamo_db_helper: Tuple[DynamoDBHelper, Any]):
    helper, _ = dynamo_db_helper
    filter_condition = {"age#gte": 30}
    filter_expression = helper.build_filter_expression(filter_condition)
    assert filter_expression == Attr("age").gte(30)


def test_build_filter_expression_between(dynamo_db_helper: Tuple[DynamoDBHelper, Any]):
    helper, _ = dynamo_db_helper
    filter_condition = {"age#between": [20, 30]}
    filter_expression = helper.build_filter_expression(filter_condition)
    assert filter_expression == Attr("age").between(20, 30)


def test_build_filter_expression_begins_with(
    dynamo_db_helper: Tuple[DynamoDBHelper, Any]
):
    helper, _ = dynamo_db_helper
    filter_condition = {"name#begins_with": "John"}
    filter_expression = helper.build_filter_expression(filter_condition)
    assert filter_expression == Attr("name").begins_with("John")

import pytest
from dynamo_db_utils import DynamoDBUtils, PRIMARY_HASH_KEY, PRIMARY_RANGE_KEY
from boto3.dynamodb.conditions import Attr, Key
from datetime import datetime
from typing import Dict


def test_is_primary_key():
    assert not DynamoDBUtils.is_primary_key(True, {"x": "123"})
    assert DynamoDBUtils.is_primary_key(True, {"id": "123", "id_range": "456"})
    assert DynamoDBUtils.is_primary_key(False, {"id": "123"})
    assert not DynamoDBUtils.is_primary_key(True, {"id": "123"})


def test_build_primary_key():
    primary_key = {"id": "123", "id_range": "456"}
    assert DynamoDBUtils.build_primary_key(True, primary_key) == {
        "id": "123",
        "id_range": "456",
    }
    assert DynamoDBUtils.build_primary_key(False, primary_key) == {"id": "123"}


def test_build_insert_condition_expression():
    condition_expression = DynamoDBUtils.build_insert_condition_expression(True)
    assert (
        condition_expression
        == Attr(PRIMARY_HASH_KEY).not_exists() & Attr(PRIMARY_RANGE_KEY).not_exists()
    )

    condition_expression = DynamoDBUtils.build_insert_condition_expression(False)
    assert condition_expression == Attr(PRIMARY_HASH_KEY).not_exists()


def test_build_filter_expression():
    params: Dict[str, str] = {}

    DynamoDBUtils.build_filter_expression(params, None)
    assert "FilterExpression" not in params

    filter_condition = {"status#eq": "active"}
    DynamoDBUtils.build_filter_expression(params, filter_condition)
    assert params["FilterExpression"] == Attr("status").eq("active")

    filter_condition = {"status#ne": "inactive"}
    DynamoDBUtils.build_filter_expression(params, filter_condition)
    assert params["FilterExpression"] == Attr("status").ne("inactive")

    filter_condition = {"status#in": ["active", "pending"]}
    DynamoDBUtils.build_filter_expression(params, filter_condition)
    assert params["FilterExpression"] == Attr("status").is_in(["active", "pending"])

    filter_condition = {"age#lt": 30}
    DynamoDBUtils.build_filter_expression(params, filter_condition)
    assert params["FilterExpression"] == Attr("age").lt(30)

    filter_condition = {"age#lte": 30}
    DynamoDBUtils.build_filter_expression(params, filter_condition)
    assert params["FilterExpression"] == Attr("age").lte(30)

    filter_condition = {"age#gt": 30}
    DynamoDBUtils.build_filter_expression(params, filter_condition)
    assert params["FilterExpression"] == Attr("age").gt(30)

    filter_condition = {"age#gte": 30}
    DynamoDBUtils.build_filter_expression(params, filter_condition)
    assert params["FilterExpression"] == Attr("age").gte(30)

    filter_condition = {"age#between": [20, 30]}
    DynamoDBUtils.build_filter_expression(params, filter_condition)
    assert params["FilterExpression"] == Attr("age").between(20, 30)

    filter_condition = {"name#begins_with": "John"}
    DynamoDBUtils.build_filter_expression(params, filter_condition)
    assert params["FilterExpression"] == Attr("name").begins_with("John")

    filter_condition = {"status": "active", "name#begins_with": "John"}
    DynamoDBUtils.build_filter_expression(params, filter_condition)
    assert params["FilterExpression"] == Attr("status").eq("active") & Attr(
        "name"
    ).begins_with("John")

    with pytest.raises(AssertionError):
        DynamoDBUtils.build_filter_expression(params, {"status#invalid": "active"})


def test_build_projection_expression():
    params: Dict[str, str] = {}
    projection_expression = [PRIMARY_HASH_KEY, "name", "status"]
    DynamoDBUtils.build_projection_expression(params, projection_expression)
    assert params["ProjectionExpression"] == "#id, #name, #status"
    assert params["ExpressionAttributeNames"] == {
        "#id": PRIMARY_HASH_KEY,
        "#name": "name",
        "#status": "status",
    }

    params: Dict[str, str] = {}
    DynamoDBUtils.build_projection_expression(params, None)
    assert "ProjectionExpression" not in params
    assert "ExpressionAttributeNames" not in params


def test_build_update_expression():
    params: Dict[str, str] = {}
    update_items = {"status": "active", "age": 30}
    DynamoDBUtils.build_update_expression(params, update_items)
    assert (
        update_expression
        == "SET #status = :status, #age = :age, #updated_at = :updated_at"
    )
    assert expression_attribute_names == {
        "#status": "status",
        "#age": "age",
        "#updated_at": "updated_at",
    }
    assert expression_attribute_values == {
        ":status": "active",
        ":age": 30,
        ":updated_at": expression_attribute_values[":updated_at"],
    }


def test_build_put_item_params():
    item = {PRIMARY_HASH_KEY: "123", "mdm": "123", "version_name": "v1"}
    params = DynamoDBUtils.build_put_item_params(
        item, ["mdm", "version_name"], overwrite=False
    )
    item["id_range"] = "123#v1"
    item["created_at"] = params["Item"]["created_at"]
    item["updated_at"] = params["Item"]["updated_at"]
    assert params["Item"] == item
    assert (
        params["ConditionExpression"]
        == Attr(PRIMARY_HASH_KEY).not_exists() & Attr(PRIMARY_RANGE_KEY).not_exists()
    )

    params = DynamoDBUtils.build_put_item_params(item, [], overwrite=True)
    item["created_at"] = params["Item"]["created_at"]
    item["updated_at"] = params["Item"]["updated_at"]
    assert params["Item"] == item
    assert "ConditionExpression" not in params


def test_build_get_item_params():
    key_condition = {"id": "123", "id_range": "456"}
    filter_condition = {"status#eq": "active"}
    projection_expression = ["id", "name", "status"]
    last_evaluated_key = {"id": "123"}
    limit = 10

    params = DynamoDBUtils.build_get_item_params(
        key_condition,
        filter_condition,
        projection_expression,
        last_evaluated_key,
        limit,
    )
    assert params["KeyConditionExpression"] == Key("id").eq("123") & Key("id_range").eq(
        "456"
    )
    assert params["FilterExpression"] == Attr("status").eq("active")
    assert params["ProjectionExpression"] == "#id, #name, #status"
    assert params["ExpressionAttributeNames"] == {
        "#id": "id",
        "#name": "name",
        "#status": "status",
    }
    assert params["ExclusiveStartKey"] == last_evaluated_key
    assert params["Limit"] == limit


def test_build_get_item_params_gsi_key_schema():
    gsi_key_schemas = [
        {"index_name": "GSI1", "HASH": "gsi_hash_key", "RANGE": "gsi_range_key"}
    ]
    key_condition = {"gsi_hash_key": "123", "gsi_range_key": "456"}
    projection_expression = ["id", "name", "status"]
    last_evaluated_key = {"gsi_hash_key": "123"}
    limit = 10

    params = DynamoDBUtils.build_get_item_params_gsi_key_schema(
        gsi_key_schemas, key_condition, projection_expression, last_evaluated_key, limit
    )
    assert params["KeyConditionExpression"] == Key("gsi_hash_key").eq("123") & Key(
        "gsi_range_key"
    ).eq("456")
    assert params["ProjectionExpression"] == "#id, #name, #status"
    assert params["ExpressionAttributeNames"] == {
        "#id": "id",
        "#name": "name",
        "#status": "status",
    }
    assert params["ExclusiveStartKey"] == last_evaluated_key
    assert params["Limit"] == limit
    assert params["IndexName"] == "GSI1"

    key_condition_invalid = {"invalid_key": "value"}
    with pytest.raises(ValueError):
        DynamoDBUtils.build_get_item_params_gsi_key_schema(
            gsi_key_schemas,
            key_condition_invalid,
            projection_expression,
            last_evaluated_key,
            limit,
        )


def test_build_get_item_params_gsi_key_schema_no_range():
    gsi_key_schemas = [
        {"index_name": "GSI1", "HASH": "gsi_hash_key", "RANGE": "gsi_range_key"}
    ]
    key_condition = {"gsi_hash_key": "123"}
    projection_expression = ["id", "name", "status"]
    last_evaluated_key = {"gsi_hash_key": "123"}
    limit = 10

    params = DynamoDBUtils.build_get_item_params_gsi_key_schema(
        gsi_key_schemas, key_condition, projection_expression, last_evaluated_key, limit
    )
    assert params["KeyConditionExpression"] == Key("gsi_hash_key").eq("123")
    assert params["ProjectionExpression"] == "#id, #name, #status"
    assert params["ExpressionAttributeNames"] == {
        "#id": "id",
        "#name": "name",
        "#status": "status",
    }
    assert params["ExclusiveStartKey"] == last_evaluated_key
    assert params["Limit"] == limit
    assert params["IndexName"] == "GSI1"

    key_condition_invalid = {"invalid_key": "value"}
    with pytest.raises(ValueError):
        DynamoDBUtils.build_get_item_params_gsi_key_schema(
            gsi_key_schemas,
            key_condition_invalid,
            projection_expression,
            last_evaluated_key,
            limit,
        )


def test_build_update_item_params():
    key = {PRIMARY_HASH_KEY: "123", PRIMARY_RANGE_KEY: "456"}
    update_items = {"status": "active", "age": 30}
    params = DynamoDBUtils.build_update_item_params(key, update_items)
    assert params["Key"] == key
    assert (
        params["UpdateExpression"]
        == "SET #status = :status, #age = :age, #updated_at = :updated_at"
    )
    assert params["ExpressionAttributeNames"] == {
        "#status": "status",
        "#age": "age",
        "#updated_at": "updated_at",
    }
    assert params["ExpressionAttributeValues"] == {
        ":status": "active",
        ":age": 30,
        ":updated_at": params["ExpressionAttributeValues"][":updated_at"],
    }

    condition_expression = "attribute_not_exists(id)"
    params = DynamoDBUtils.build_update_item_params(
        key, update_items, condition_expression
    )
    assert params["ConditionExpression"] == condition_expression


def test_datetime_serializer():
    # Teste para datetime
    dt = datetime(2023, 1, 1, 12, 0, 0)
    assert DynamoDBUtils.datetime_serializer(dt) == "2023-01-01T12:00:00"

    # Teste para TypeError
    with pytest.raises(TypeError) as excinfo:
        DynamoDBUtils.datetime_serializer("not a datetime")
    assert str(excinfo.value) == "Type <class 'str'> not serializable"

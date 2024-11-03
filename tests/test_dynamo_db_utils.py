import pytest
from dynamo_db_utils import DynamoDBUtils
from boto3.dynamodb.conditions import Attr, Key
from datetime import datetime


def test_build_insert_condition_expression():
    item = {"id": "123", "id_range": "456"}
    condition_expression = DynamoDBUtils.build_insert_condition_expression(item)
    assert (
        condition_expression == Attr("id").not_exists() & Attr("id_range").not_exists()
    )

    item = {"id": "123"}
    condition_expression = DynamoDBUtils.build_insert_condition_expression(item)
    assert condition_expression == Attr("id").not_exists()

    with pytest.raises(AssertionError):
        DynamoDBUtils.build_insert_condition_expression({})


def test_build_key_expression():
    key_condition = {"id": "123", "id_range": "456"}
    key_expression = DynamoDBUtils.build_key_expression(key_condition)
    assert key_expression == Key("id").eq("123") & Key("id_range").eq("456")

    with pytest.raises(AssertionError):
        DynamoDBUtils.build_key_expression({})


def test_build_filter_expression():
    filter_condition = {"status#eq": "active"}
    filter_expression = DynamoDBUtils.build_filter_expression(filter_condition)
    assert filter_expression == Attr("status").eq("active")

    filter_condition = {"status#ne": "inactive"}
    filter_expression = DynamoDBUtils.build_filter_expression(filter_condition)
    assert filter_expression == Attr("status").ne("inactive")

    filter_condition = {"status#in": ["active", "pending"]}
    filter_expression = DynamoDBUtils.build_filter_expression(filter_condition)
    assert filter_expression == Attr("status").is_in(["active", "pending"])

    filter_condition = {"age#lt": 30}
    filter_expression = DynamoDBUtils.build_filter_expression(filter_condition)
    assert filter_expression == Attr("age").lt(30)

    filter_condition = {"age#lte": 30}
    filter_expression = DynamoDBUtils.build_filter_expression(filter_condition)
    assert filter_expression == Attr("age").lte(30)

    filter_condition = {"age#gt": 30}
    filter_expression = DynamoDBUtils.build_filter_expression(filter_condition)
    assert filter_expression == Attr("age").gt(30)

    filter_condition = {"age#gte": 30}
    filter_expression = DynamoDBUtils.build_filter_expression(filter_condition)
    assert filter_expression == Attr("age").gte(30)

    filter_condition = {"age#between": [20, 30]}
    filter_expression = DynamoDBUtils.build_filter_expression(filter_condition)
    assert filter_expression == Attr("age").between(20, 30)

    filter_condition = {"name#begins_with": "John"}
    filter_expression = DynamoDBUtils.build_filter_expression(filter_condition)
    assert filter_expression == Attr("name").begins_with("John")

    filter_condition = {"status": "active", "name#begins_with": "John"}
    filter_expression = DynamoDBUtils.build_filter_expression(filter_condition)
    assert filter_expression == Attr("status").eq("active") & Attr("name").begins_with(
        "John"
    )

    with pytest.raises(AssertionError):
        DynamoDBUtils.build_filter_expression({"status#invalid": "active"})


def test_build_projection_expression():
    projection_expression = ["id", "name", "status"]
    expression, attribute_names = DynamoDBUtils.build_projection_expression(
        projection_expression
    )
    assert expression == "#id, #name, #status"
    assert attribute_names == {"#id": "id", "#name": "name", "#status": "status"}

    expression, attribute_names = DynamoDBUtils.build_projection_expression(None)
    assert expression is None
    assert attribute_names is None


def test_build_update_expression():
    update_items = {"status": "active", "age": 30}
    update_expression, expression_attribute_names, expression_attribute_values = (
        DynamoDBUtils.build_update_expression(update_items)
    )
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
    item = {"id": "123", "id_range": "456"}
    params = DynamoDBUtils.build_put_item_params(item, [], overwrite=False)
    item["created_at"] = params["Item"]["created_at"]
    item["updated_at"] = params["Item"]["updated_at"]
    assert params["Item"] == item
    assert (
        params["ConditionExpression"]
        == Attr("id").not_exists() & Attr("id_range").not_exists()
    )

    params = DynamoDBUtils.build_put_item_params(item, [], overwrite=True)
    item["created_at"] = params["Item"]["created_at"]
    item["updated_at"] = params["Item"]["updated_at"]
    assert params["Item"] == item
    assert "ConditionExpression" not in params


def test_build_update_item_params():
    key = {"id": "123", "id_range": "456"}
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


def test_build_get_item_params():
    key = {"id": "123", "id_range": "456"}
    projection_expression = ["id", "name", "status"]
    params = DynamoDBUtils.build_get_item_params(key, projection_expression)
    assert params["Key"] == key
    assert params["ProjectionExpression"] == "#id, #name, #status"
    assert params["ExpressionAttributeNames"] == {
        "#id": "id",
        "#name": "name",
        "#status": "status",
    }

    params = DynamoDBUtils.build_get_item_params(key)
    assert params["Key"] == key
    assert "ProjectionExpression" not in params
    assert "ExpressionAttributeNames" not in params


def test_datetime_serializer():
    # Teste para datetime
    dt = datetime(2023, 1, 1, 12, 0, 0)
    assert DynamoDBUtils.datetime_serializer(dt) == "2023-01-01T12:00:00"

    # Teste para TypeError
    with pytest.raises(TypeError) as excinfo:
        DynamoDBUtils.datetime_serializer("not a datetime")
    assert str(excinfo.value) == "Type <class 'str'> not serializable"

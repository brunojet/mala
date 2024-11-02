import pytest
from dynamo_db_helper import DynamoDBHelper, PRIMARY_HASH_KEY, PRIMARY_RANGE_KEY


class TestDynamoDBHelper(DynamoDBHelper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


@pytest.fixture
def dynamo_db_helper():
    return TestDynamoDBHelper(
        has_range_key=True,
        range_key_items=["range_key1", "range_key2"],
        gsi_key_schemas=[
            {"index_name": "GSI1", "HASH": "gsi_hash_key", "RANGE": "gsi_range_key"}
        ],
    )


def test_init(dynamo_db_helper):
    assert dynamo_db_helper.primary_keys == [PRIMARY_HASH_KEY, PRIMARY_RANGE_KEY]
    assert dynamo_db_helper.has_range_key is True
    assert dynamo_db_helper.range_key_items == ["range_key1", "range_key2"]
    assert len(dynamo_db_helper.gsi_key_schemas) == 1


def test_is_primary_key(dynamo_db_helper):
    key_condition = {PRIMARY_HASH_KEY: "hash_value", PRIMARY_RANGE_KEY: "range_value"}
    assert dynamo_db_helper.is_primary_key(key_condition) is True

    key_condition = {PRIMARY_HASH_KEY: "hash_value"}
    assert dynamo_db_helper.is_primary_key(key_condition) is False


def test_add_range_key(dynamo_db_helper):
    item = {
        PRIMARY_HASH_KEY: "hash_value",
        "range_key1": "value1",
        "range_key2": "value2",
    }
    dynamo_db_helper.add_range_key(item)
    assert item[PRIMARY_RANGE_KEY] == "value1#value2"


def test_build_insert_condition_expression(dynamo_db_helper):
    condition_expression = dynamo_db_helper._build_insert_condition_expression()
    assert condition_expression is not None


def test_build_key_expression(dynamo_db_helper):
    key_condition = {PRIMARY_HASH_KEY: "hash_value", PRIMARY_RANGE_KEY: "range_value"}
    key_expression = dynamo_db_helper._build_key_expression(key_condition)
    assert key_expression is not None


def test_get_gsi_key_schema(dynamo_db_helper):
    key_set = {"gsi_hash_key", "gsi_range_key"}
    gsi_key_schema = dynamo_db_helper._get_gsi_key_schema(key_set)
    assert gsi_key_schema is not None


def test_get_gsi_key_expression(dynamo_db_helper):
    key_condition = {"gsi_hash_key": "hash_value", "gsi_range_key": "range_value"}
    index_name, key_expression = dynamo_db_helper._get_gsi_key_expression(key_condition)
    assert index_name == "GSI1"
    assert key_expression is not None


def test_build_projection_expression():
    projection_expression = ["id", "name", "status"]
    expression, attribute_names = TestDynamoDBHelper.build_projection_expression(
        projection_expression
    )
    assert expression == "id, name, #status"
    assert attribute_names == {"#status": "status"}

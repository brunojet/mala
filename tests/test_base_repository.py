import boto3
import pytest
from typing import Tuple, Any
from moto import mock_aws
from unittest.mock import patch
from dynamo_db_utils import create_table
from base_repository import BaseRepository
from unittest import TestCase

MOCK_DATA = [
    {
        "id": "test_id_1",
        "name": "test_name_1",
        "stage": "production",
        "status": "pending",
    },
    {
        "id": "test_id_2",
        "stage": "production",
        "name": "test_name_2",
        "status": "approved",
    },
    {
        "id": "test_id_3",
        "stage": "production",
        "name": "test_name_3",
        "status": "rollout",
    },
    {
        "id": "test_id_4",
        "stage": "production",
        "name": "test_name_4",
        "status": "pending",
    },
    {
        "id": "test_id_5",
        "stage": "production",
        "name": "test_name_5",
        "status": "approved",
    },
    {
        "id": "test_id_6",
        "stage": "production",
        "name": "test_name_6",
        "status": "rollout",
    },
    {
        "id": "test_id_7",
        "stage": "production",
        "name": "test_name_7",
        "status": "pending",
    },
    {
        "id": "test_id_8",
        "stage": "production",
        "name": "test_name_8",
        "status": "approved",
    },
    {
        "id": "test_id_9",
        "stage": "production",
        "name": "test_name_9",
        "status": "rollout",
    },
    {
        "id": "test_id_10",
        "stage": "production",
        "name": "test_name_10",
        "status": "pending",
    },
    {
        "id": "test_id_11",
        "stage": "production",
        "name": "test_name_11",
        "status": "approved",
    },
    {
        "id": "test_id_12",
        "stage": "production",
        "name": "test_name_12",
        "status": "rollout",
    },
]

GSI_KEY_SCHEMAS = [{"index_name": "stage-index", "HASH": "stage"}]


@pytest.fixture
def dynamodb():
    with mock_aws():
        table_name = "test_table"
        resource = boto3.resource("dynamodb", region_name="us-east-1")
        key_schema = {"HASH": "id"}
        gsi_key_schemas = GSI_KEY_SCHEMAS

        yield create_table(table_name, resource, key_schema, gsi_key_schemas)


@pytest.fixture
def base_repository(dynamodb: Tuple[boto3.client, Any]):
    client, table, describle_table = dynamodb
    with patch.object(client, "describe_table", return_value=describle_table):
        repo = BaseRepository(
            table_name="test_table", max_item_size=1024, gsi_key_schemas=GSI_KEY_SCHEMAS
        )
        yield repo, table


def test_init_table(base_repository: Tuple[BaseRepository, Any]):
    repo = base_repository[0]
    assert repo.table_name == "test_table"
    assert repo.max_read_items > 0
    assert repo.max_write_items > 0


def test_insert(base_repository: Tuple[BaseRepository, Any]):
    repo = base_repository[0]

    for item in MOCK_DATA:
        result = repo.insert(item, overwrite=True)
        TestCase().assertDictEqual(result, repo.build_primary_key(item))


def test_query(base_repository: Tuple[BaseRepository, Any]):
    repo = base_repository[0]

    for item in MOCK_DATA:
        last_evaluated_key = None
        primary_key = repo.insert(item)
        results = repo.query(primary_key, last_evaluated_key=last_evaluated_key)[0]
        for result in results:
            for key, value in item.items():
                assert result[key] == value


def test_query_many(base_repository: Tuple[BaseRepository, Any]):
    repo = base_repository[0]
    expected_items = len([item for item in MOCK_DATA if item["stage"] == "production"])
    resulted_items = 0

    for item in MOCK_DATA:
        repo.insert(item)

    key_condition = {"stage": "production"}
    filter_condition = {"status#in": ["pending", "approved", "rollout"]}
    projection_expression = ["id", "name", "status"]

    last_evaluated_key = None

    while True:
        results, last_evaluated_key = repo.query(
            key_condition=key_condition,
            filter_condition=filter_condition,
            projection_expression=projection_expression,
            last_evaluated_key=last_evaluated_key,
        )

        resulted_items += len(results)

        if not last_evaluated_key:
            break

    assert expected_items == resulted_items

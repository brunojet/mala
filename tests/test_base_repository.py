import boto3
import pytest
from typing import Tuple, Any
from moto import mock_aws
from unittest.mock import patch
from dynamo_db_utils import create_table, DESCRIBLE_TABLE
from base_repository import BaseRepository
from unittest import TestCase

MOCK_DATA = [
    {
        "id": "test_id_1",
        "name": "test_name_1",
    },
    {
        "id": "test_id_2",
        "name": "test_name_2",
    },
    {
        "id": "test_id_3",
        "name": "test_name_3",
    },
]


@pytest.fixture
def dynamodb():
    with mock_aws():
        resource = boto3.resource("dynamodb", region_name="us-east-1")
        client = boto3.client("dynamodb", region_name="us-east-1")

        key_schema = {"HASH": "id"}
        gsi_key_schemas = []
        table = create_table(resource, key_schema, gsi_key_schemas)

        yield client, table


@pytest.fixture
def base_repository(dynamodb: Tuple[boto3.client, Any]):
    client, table = dynamodb
    with patch.object(client, "describe_table", return_value=DESCRIBLE_TABLE):
        repo = BaseRepository(table_name="test_table")
        yield repo, table


def test_init_table(base_repository: Tuple[BaseRepository, Any]):
    repo = base_repository[0]
    assert repo.table_name == "test_table"
    assert repo.max_read_items > 0
    assert repo.max_write_items > 0


def test_insert(base_repository: Tuple[BaseRepository, Any]):
    repo = base_repository[0]

    for item in MOCK_DATA:
        result = repo.insert(item)
        TestCase().assertDictEqual(result, repo.build_primary_key(item))


def test_query(base_repository: Tuple[BaseRepository, Any]):
    repo = base_repository[0]

    for item in MOCK_DATA:
        primary_key = repo.insert(item)
        results = repo.query(primary_key)[0]
        for result in results:
            for key, value in primary_key.items():
                assert result[key] == value

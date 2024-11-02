import boto3
import pytest
from mock_dynamo_db import DESCRIBLE_TABLE_MOCK
from moto import mock_aws
from unittest.mock import MagicMock, patch
from app_release_repository import (
    AppReleaseRepository,
    STAGE_PILOT,
    STATUS_PENDING,
    STATUS_APPROVED,
    STATUS_CANCELED,
)

MOCK_DATA = [
    {
        "id": "teste app 1",
        "id_range": "SF01#1.0.0",
        "mdm": "SF01",
        "mdm_key": {"release_id": 1},
        "version_name": "1.0.0",
        "stage": STAGE_PILOT,
        "status": STATUS_PENDING,
    },
    {
        "id": "teste app 2",
        "id_range": "SF01#1.1.0",
        "mdm": "SF01",
        "mdm_key": {"release_id": 2},
        "version_name": "1.1.0",
        "stage": STAGE_PILOT,
        "status": STATUS_PENDING,
    },
    {
        "id": "teste app 3",
        "id_range": "SF01#1.2.0",
        "mdm": "SF01",
        "mdm_key": {"release_id": 3},
        "version_name": "1.2.0",
        "stage": STAGE_PILOT,
        "status": STATUS_APPROVED,
    },
]


@pytest.fixture
def dynamodb():
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
                {"AttributeName": "mdm", "AttributeType": "S"},
                {"AttributeName": "version_name", "AttributeType": "S"},
                {"AttributeName": "stage", "AttributeType": "S"},
            ],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "mdm-version_name-index",
                    "KeySchema": [
                        {"AttributeName": "mdm", "KeyType": "HASH"},
                        {"AttributeName": "version_name", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                    "ProvisionedThroughput": {
                        "ReadCapacityUnits": 5,
                        "WriteCapacityUnits": 5,
                    },
                },
                {
                    "IndexName": "id-mdm-index",
                    "KeySchema": [
                        {"AttributeName": "id", "KeyType": "HASH"},
                        {"AttributeName": "mdm", "KeyType": "RANGE"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                    "ProvisionedThroughput": {
                        "ReadCapacityUnits": 5,
                        "WriteCapacityUnits": 5,
                    },
                },
                {
                    "IndexName": "stage-index",
                    "KeySchema": [
                        {"AttributeName": "stage", "KeyType": "HASH"},
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                    "ProvisionedThroughput": {
                        "ReadCapacityUnits": 5,
                        "WriteCapacityUnits": 5,
                    },
                },
            ],
            ProvisionedThroughput={
                "ReadCapacityUnits": 5,
                "WriteCapacityUnits": 5,
            },
        )
        table.wait_until_exists()
        yield client, table


class TestAppReleaseRepository(AppReleaseRepository):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


@pytest.fixture
def app_release_repository(dynamodb):
    client, table = dynamodb

    with patch.object(client, "describe_table", return_value=DESCRIBLE_TABLE_MOCK):
        repo = TestAppReleaseRepository(table_name="test_table")

        yield repo, table


def test_init(app_release_repository):
    repo = app_release_repository[0]

    assert repo.has_range_key is True
    assert repo.range_key_items == ["mdm", "version_name"]
    assert len(repo.gsi_key_schemas) == 3


def test_cancel_previous_versions(app_release_repository):
    repo, table = app_release_repository

    for data in MOCK_DATA:
        table.put_item(Item=data)

    repo._AppReleaseRepository__cancel_previous_versions(
        "teste app 3", "SF01", "2.0.0", STAGE_PILOT, STATUS_PENDING, STATUS_CANCELED
    )


def test_pilot_app(app_release_repository):
    repo, table = app_release_repository

    for data in MOCK_DATA:
        table.put_item(Item=data)

    result = repo.pilot_app("teste app 3", "SF01", {"release_id": 1}, "2.0.0")

    assert result == {"id": "teste app 3", "id_range": "SF01#2.0.0"}


def test_pilot_approve_app(app_release_repository):
    repo, table = app_release_repository

    for data in MOCK_DATA:
        table.put_item(Item=data)

    repo.pilot_approve_app("teste app 2", "SF01", "1.1.0")


def test_pilot_reprove_app(app_release_repository):
    repo, table = app_release_repository

    for data in MOCK_DATA:
        table.put_item(Item=data)

    repo.pilot_reprove_app("teste app 1", "SF01", "1.0.0")


def test_rollout_app(app_release_repository):
    repo, table = app_release_repository

    for data in MOCK_DATA:
        table.put_item(Item=data)

    repo.rollout_app("teste app 3", "SF01", "1.2.0")

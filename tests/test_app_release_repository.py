import boto3
import pytest
from typing import Tuple, Any
from test_dynamo_db_utils import create_table
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
        "id_range": "SF01#1.1.0",
        "mdm": "SF01",
        "mdm_key": {"release_id": 3},
        "version_name": "1.1.0",
        "stage": STAGE_PILOT,
        "status": STATUS_APPROVED,
    },
    {
        "id": "teste app 3",
        "id_range": "SF01#1.2.0",
        "mdm": "SF01",
        "mdm_key": {"release_id": 4},
        "version_name": "1.2.0",
        "stage": STAGE_PILOT,
        "status": STATUS_PENDING,
    },
]


@pytest.fixture
def dynamodb():
    with mock_aws():
        table_name = "test_table"
        resource = boto3.resource("dynamodb", region_name="us-east-1")
        key_schema = {"HASH": "id", "RANGE": "id_range"}
        gsi_key_schemas = [
            {
                "index_name": "mdm-version_name-index",
                "HASH": "mdm",
                "RANGE": "version_name",
            },
            {"index_name": "id-mdm-index", "HASH": "id", "RANGE": "mdm"},
            {"index_name": "stage-index", "HASH": "stage"},
        ]

        yield create_table(table_name, resource, key_schema, gsi_key_schemas)


@pytest.fixture
def app_release_repository(dynamodb: Tuple[boto3.client, Any]):
    client, table, describle_table = dynamodb

    with patch.object(client, "describe_table", return_value=describle_table):
        repo = AppReleaseRepository(table_name="test_table")

        yield repo, table


def test_init(app_release_repository: Tuple[AppReleaseRepository, Any]):
    repo = app_release_repository[0]

    assert repo.has_range_key is True
    assert repo.range_key_items == ["mdm", "version_name"]
    assert len(repo.gsi_key_schemas) == 3


def test_cancel_previous_versions(
    app_release_repository: Tuple[AppReleaseRepository, Any]
):
    repo, table = app_release_repository

    for data in MOCK_DATA:
        table.put_item(Item=data)

    repo._AppReleaseRepository__cancel_previous_versions(
        "teste app 3", "SF01", "2.0.0", STAGE_PILOT, STATUS_PENDING, STATUS_CANCELED
    )


def test_pilot_app(app_release_repository: Tuple[AppReleaseRepository, Any]):
    repo, table = app_release_repository

    for data in MOCK_DATA:
        table.put_item(Item=data)

    result = repo.pilot_app("teste app 3", "SF01", {"release_id": 1}, "2.0.0")

    assert result == {"id": "teste app 3", "id_range": "SF01#2.0.0"}


def test_pilot_approve_app(app_release_repository: Tuple[AppReleaseRepository, Any]):
    repo, table = app_release_repository

    for data in MOCK_DATA:
        table.put_item(Item=data)

    repo.pilot_approve_app("teste app 3", "SF01", "1.2.0")


def test_pilot_reprove_app(app_release_repository: Tuple[AppReleaseRepository, Any]):
    repo, table = app_release_repository

    for data in MOCK_DATA:
        table.put_item(Item=data)

    repo.pilot_reprove_app("teste app 1", "SF01", "1.0.0")


def test_rollout_app(app_release_repository: Tuple[AppReleaseRepository, Any]):
    repo, table = app_release_repository

    for data in MOCK_DATA:
        table.put_item(Item=data)

    repo.rollout_app("teste app 3", "SF01", "1.1.0")


def test_get_app(app_release_repository: Tuple[AppReleaseRepository, Any]):
    repo, table = app_release_repository

    for data in MOCK_DATA:
        table.put_item(Item=data)

    result = repo.get_app("teste app 3")

    assert len(result) == 2
    assert result[0]["id_range"] == "SF01#1.1.0"
    assert result[1]["id_range"] == "SF01#1.2.0"


def test_get_all_apps(app_release_repository: Tuple[AppReleaseRepository, Any]):
    repo, table = app_release_repository
    expected_result = [
        data
        for data in MOCK_DATA
        if data["stage"] == STAGE_PILOT
        and data["status"] in [STATUS_PENDING, STATUS_APPROVED]
    ]

    for data in expected_result:
        table.put_item(Item=data)

    result = repo.get_all_apps(
        stage=STAGE_PILOT, status=[STATUS_PENDING, STATUS_APPROVED]
    )

    assert len(result) == len(expected_result)

    for idx in range(len(result)):
        assert result[idx]["version_name"] == expected_result[idx]["version_name"]

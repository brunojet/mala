import pytest
from unittest.mock import MagicMock, patch
from app_release_repository import (
    AppReleaseRepository,
    STAGE_PILOT,
    STATUS_PENDING,
    STATUS_CANCELED,
)


@pytest.fixture
def app_release_repository():
    r = AppReleaseRepository(table_name="test_table")
    r_i_n = "_BaseRepository__insert"
    r_i = r._BaseRepository__insert
    r_q_n = "_BaseRepository__query"
    r_q = r._BaseRepository__query
    r_u_n = "_BaseRepository__update"
    r_u = r._BaseRepository__update
    r_bu_name = "_BaseRepository__batch_update"
    r_bu = r._BaseRepository__batch_update
    with patch.object(r, r_i_n, wraps=r_i) as m_i, patch.object(
        r, r_q_n, wraps=r_q
    ) as m_q, patch.object(r, r_u_n, wraps=r_u) as m_u, patch.object(
        r, r_bu_name, wraps=r_bu
    ) as m_bu:
        yield r, m_i, m_u, m_q, m_bu


def test_cancel_previous_versions(app_release_repository):
    repo, _, mock_query, _, mock_batch_update = app_release_repository

    mock_query.return_value = (
        [
            {"id": "teste app 1", "id_range": "SF01#1.0.0"},
            {"id": "teste app 2", "id_range": "SF01#1.1.0"},
        ],
        None,
    )

    mock_batch_update.return_value = None

    repo._AppReleaseRepository__cancel_previous_versions(
        "teste app 3", "SF01", "2.0.0", STAGE_PILOT, STATUS_PENDING, STATUS_CANCELED
    )


def test_pilot_app(app_release_repository):
    repo, mock_insert, mock_query, _, mock_batch_update = app_release_repository

    mock_query.return_value = (
        [
            {"id": "teste app 1", "id_range": "SF01#1.0.0"},
            {"id": "teste app 2", "id_range": "SF01#1.1.0"},
        ],
        None,
    )

    mock_batch_update.return_value = None

    mock_insert.return_value = {"id": "teste app 3", "id_range": "SF01#2.0.0"}

    result = repo.pilot_app("teste app 3", "SF01", {"release_id": 1}, "2.0.0")

    assert result == {"id": "teste app 3", "id_range": "SF01#2.0.0"}


def test_pilot_approve_app(app_release_repository):
    repo, mock_insert, mock_query, mock_update, _ = app_release_repository

    mock_query.return_value = (
        [
            {"id": "teste app 1", "id_range": "SF01#1.0.0"},
            {"id": "teste app 2", "id_range": "SF01#1.1.0"},
        ],
        None,
    )

    mock_update.return_value = None

    mock_insert.return_value = {"id": "teste app 3", "id_range": "SF01#2.0.0"}

    repo.pilot_approve_app("teste app 3", "SF01", "2.0.0")


def test_pilot_reprove_app(app_release_repository):
    repo, mock_insert, mock_query, mock_batch_update = app_release_repository

    mock_query.return_value = (
        [
            {"id": "teste app 1", "id_range": "SF01#1.0.0"},
            {"id": "teste app 2", "id_range": "SF01#1.1.0"},
        ],
        None,
    )

    mock_batch_update.return_value = None

    mock_insert.return_value = {"id": "teste app 3", "id_range": "SF01#2.0.0"}

    repo.pilot_reprove_app("teste app 3", "SF01", {"release_id": 1}, "2.0.0")


def test_rollout_app(app_release_repository):
    repo, mock_insert, mock_query, mock_batch_update = app_release_repository

    mock_query.return_value = (
        [
            {"id": "teste app 1", "id_range": "SF01#1.0.0"},
            {"id": "teste app 2", "id_range": "SF01#1.1.0"},
        ],
        None,
    )

    mock_batch_update.return_value = None

    mock_insert.return_value = {"id": "teste app 3", "id_range": "SF01#2.0.0"}

    repo.rollout_app("teste app 3", "SF01", {"release_id": 1}, "2.0.0")

from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import AsyncClient

from icarus.services.neo4j_service import CypherLoader

INVESTIGATION_CYPHER_FILES = [
    "investigation_create",
    "investigation_get",
    "investigation_list",
    "investigation_update",
    "investigation_delete",
    "investigation_add_entity",
    "investigation_share",
    "investigation_by_token",
    "annotation_create",
    "annotation_list",
    "tag_create",
    "tag_list",
    "tag_add_to_entity",
]


def test_all_investigation_cypher_files_exist() -> None:
    for name in INVESTIGATION_CYPHER_FILES:
        try:
            CypherLoader.load(name)
        except FileNotFoundError:
            pytest.fail(f"Missing .cypher file: {name}.cypher")
        finally:
            CypherLoader.clear_cache()


def _mock_record(data: dict[str, object]) -> MagicMock:
    """Create a MagicMock that behaves like a neo4j.Record."""
    record = MagicMock()
    record.__getitem__ = lambda self, key: data[key]
    record.__contains__ = lambda self, key: key in data
    record.keys.return_value = list(data.keys())
    record.__iter__ = lambda self: iter(data.keys())
    # Make isinstance check pass for neo4j.Record
    return record


def _fake_result(records: list[MagicMock]) -> AsyncMock:
    """Create a mock Result that yields records."""
    result = AsyncMock()

    async def _iter(self: object) -> object:  # noqa: ANN001
        for r in records:
            yield r

    result.__aiter__ = _iter
    result.single = AsyncMock(return_value=records[0] if records else None)
    return result


@pytest.mark.anyio
async def test_create_investigation(client: AsyncClient) -> None:
    record_data = {
        "id": "test-uuid",
        "title": "Test Investigation",
        "description": "",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
        "share_token": None,
        "entity_ids": [],
    }
    mock_record = _mock_record(record_data)

    from icarus.main import app

    driver = app.state.neo4j_driver
    mock_session = AsyncMock()
    mock_session.run = AsyncMock(return_value=_fake_result([mock_record]))
    driver.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)

    response = await client.post(
        "/api/v1/investigations/",
        json={"title": "Test Investigation"},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["title"] == "Test Investigation"
    assert "id" in data


@pytest.mark.anyio
async def test_list_investigations(client: AsyncClient) -> None:
    record_data = {
        "total": 1,
        "id": "test-uuid",
        "title": "Test",
        "description": "",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
        "share_token": None,
        "entity_ids": [],
    }
    mock_record = _mock_record(record_data)

    from icarus.main import app

    driver = app.state.neo4j_driver
    mock_session = AsyncMock()

    result = AsyncMock()

    async def _iter(self: object) -> object:  # noqa: ANN001
        yield mock_record

    result.__aiter__ = _iter
    result.single = AsyncMock(return_value=mock_record)
    mock_session.run = AsyncMock(return_value=result)
    driver.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)

    response = await client.get("/api/v1/investigations/")
    assert response.status_code == 200
    data = response.json()
    assert "investigations" in data
    assert "total" in data


@pytest.mark.anyio
async def test_get_nonexistent_investigation(client: AsyncClient) -> None:
    from icarus.main import app

    driver = app.state.neo4j_driver
    mock_session = AsyncMock()

    result = AsyncMock()

    async def _iter(self: object) -> object:  # noqa: ANN001
        return
        yield  # noqa: UP028

    result.__aiter__ = _iter
    result.single = AsyncMock(return_value=None)
    mock_session.run = AsyncMock(return_value=result)
    driver.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)

    response = await client.get("/api/v1/investigations/nonexistent-id")
    assert response.status_code == 404


@pytest.mark.anyio
async def test_delete_investigation(client: AsyncClient) -> None:
    delete_record = _mock_record({"deleted": 1})

    from icarus.main import app

    driver = app.state.neo4j_driver
    mock_session = AsyncMock()
    mock_session.run = AsyncMock(return_value=_fake_result([delete_record]))
    driver.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)

    response = await client.delete("/api/v1/investigations/test-uuid")
    assert response.status_code == 204


@pytest.mark.anyio
async def test_create_annotation(client: AsyncClient) -> None:
    record_data = {
        "id": "ann-uuid",
        "entity_id": "entity-1",
        "investigation_id": "inv-uuid",
        "text": "Note about entity",
        "created_at": "2026-01-01T00:00:00Z",
    }
    mock_record = _mock_record(record_data)

    from icarus.main import app

    driver = app.state.neo4j_driver
    mock_session = AsyncMock()
    mock_session.run = AsyncMock(return_value=_fake_result([mock_record]))
    driver.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)

    response = await client.post(
        "/api/v1/investigations/inv-uuid/annotations",
        json={"entity_id": "entity-1", "text": "Note about entity"},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["text"] == "Note about entity"


@pytest.mark.anyio
async def test_create_tag(client: AsyncClient) -> None:
    record_data = {
        "id": "tag-uuid",
        "investigation_id": "inv-uuid",
        "name": "important",
        "color": "#E07A2F",
    }
    mock_record = _mock_record(record_data)

    from icarus.main import app

    driver = app.state.neo4j_driver
    mock_session = AsyncMock()
    mock_session.run = AsyncMock(return_value=_fake_result([mock_record]))
    driver.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)

    response = await client.post(
        "/api/v1/investigations/inv-uuid/tags",
        json={"name": "important"},
    )
    assert response.status_code == 201
    data = response.json()
    assert data["name"] == "important"
    assert data["color"] == "#E07A2F"


@pytest.mark.anyio
async def test_share_investigation(client: AsyncClient) -> None:
    record_data = {
        "id": "inv-uuid",
        "share_token": "share-token-uuid",
    }
    mock_record = _mock_record(record_data)

    from icarus.main import app

    driver = app.state.neo4j_driver
    mock_session = AsyncMock()
    mock_session.run = AsyncMock(return_value=_fake_result([mock_record]))
    driver.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)

    response = await client.post("/api/v1/investigations/inv-uuid/share")
    assert response.status_code == 200
    data = response.json()
    assert "share_token" in data


@pytest.mark.anyio
async def test_export_investigation(client: AsyncClient) -> None:
    inv_record = _mock_record({
        "id": "inv-uuid",
        "title": "Test",
        "description": "",
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
        "share_token": None,
        "entity_ids": [],
    })

    from icarus.main import app

    call_count = 0

    driver = app.state.neo4j_driver
    mock_session = AsyncMock()

    async def _run_side_effect(*args: object, **kwargs: object) -> AsyncMock:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            # investigation_get
            return _fake_result([inv_record])
        # annotation_list / tag_list return empty
        result = AsyncMock()

        async def _empty_iter(self: object) -> object:  # noqa: ANN001
            return
            yield  # noqa: UP028

        result.__aiter__ = _empty_iter
        result.single = AsyncMock(return_value=None)
        return result

    mock_session.run = AsyncMock(side_effect=_run_side_effect)
    driver.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)

    response = await client.get("/api/v1/investigations/inv-uuid/export")
    assert response.status_code == 200
    data = response.json()
    assert "investigation" in data
    assert "annotations" in data
    assert "tags" in data

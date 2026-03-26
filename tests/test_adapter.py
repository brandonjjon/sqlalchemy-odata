"""Tests for the OData adapter."""

from datetime import datetime, timezone

import pytest
import responses

from shillelagh_odata.adapter import (
    ODataAdapter,
    _build_session,
    _parse_datetime,
    _sanitize_url,
    get_entity_set_names,
    parse_metadata,
    resolve_entity_set,
)

SAMPLE_METADATA = """\
<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Example.Models" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Product">
        <Key><PropertyRef Name="ProductId" /></Key>
        <Property Name="ProductId" Type="Edm.Guid" Nullable="false" />
        <Property Name="Name" Type="Edm.String" />
        <Property Name="Price" Type="Edm.Decimal" />
        <Property Name="InStock" Type="Edm.Boolean" Nullable="false" />
        <Property Name="CreatedAt" Type="Edm.DateTimeOffset" />
      </EntityType>
      <EntityType Name="Order">
        <Key><PropertyRef Name="OrderId" /></Key>
        <Property Name="OrderId" Type="Edm.Int32" Nullable="false" />
        <Property Name="Total" Type="Edm.Double" />
      </EntityType>
      <EntityContainer Name="Default">
        <EntitySet Name="products" EntityType="Example.Models.Product" />
        <EntitySet Name="orders" EntityType="Example.Models.Order" />
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""


# ---------------------------------------------------------------------------
# parse_metadata / resolve_entity_set / get_entity_set_names
# ---------------------------------------------------------------------------


def test_parse_metadata():
    result = parse_metadata(SAMPLE_METADATA)
    assert "Example.Models.Product" in result
    assert "Example.Models.Order" in result
    product = result["Example.Models.Product"]
    assert product["ProductId"] == "Edm.Guid"
    assert product["Name"] == "Edm.String"
    assert product["Price"] == "Edm.Decimal"
    assert product["InStock"] == "Edm.Boolean"
    assert product["CreatedAt"] == "Edm.DateTimeOffset"


def test_parse_metadata_collection_type():
    xml = """\
<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Test" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="Item">
        <Property Name="Tags" Type="Collection(Edm.String)" />
      </EntityType>
      <EntityContainer Name="Default">
        <EntitySet Name="items" EntityType="Test.Item" />
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""
    result = parse_metadata(xml)
    assert result["Test.Item"]["Tags"] == "Edm.String"


def test_get_entity_set_names():
    names = get_entity_set_names(SAMPLE_METADATA)
    assert names == ["orders", "products"]


def test_get_entity_set_names_empty():
    xml = """\
<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="Empty" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityContainer Name="Default" />
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>
"""
    assert get_entity_set_names(xml) == []


def test_resolve_entity_set():
    result = resolve_entity_set(SAMPLE_METADATA, "products")
    assert result is not None
    fqn, props = result
    assert fqn == "Example.Models.Product"
    assert "Name" in props
    assert props["Price"] == "Edm.Decimal"


def test_resolve_entity_set_not_found():
    result = resolve_entity_set(SAMPLE_METADATA, "nonexistent")
    assert result is None


# ---------------------------------------------------------------------------
# _parse_datetime
# ---------------------------------------------------------------------------


def test_parse_datetime_iso_utc():
    result = _parse_datetime("2024-01-15T10:30:00Z")
    assert result == datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)


def test_parse_datetime_iso_offset():
    result = _parse_datetime("2024-01-15T10:30:00+05:30")
    assert result is not None
    assert result.hour == 10
    assert result.minute == 30


def test_parse_datetime_with_microseconds():
    result = _parse_datetime("2024-01-15T10:30:00.123456Z")
    assert result is not None
    assert result.microsecond == 123456


def test_parse_datetime_with_fractional_and_offset():
    result = _parse_datetime("2024-01-15T10:30:00.123+02:00")
    assert result is not None
    assert result.year == 2024


def test_parse_datetime_no_timezone():
    result = _parse_datetime("2024-01-15T10:30:00")
    assert result is not None
    assert result.hour == 10


def test_parse_datetime_invalid_returns_none():
    result = _parse_datetime("not-a-date")
    assert result is None


def test_parse_datetime_empty_returns_none():
    result = _parse_datetime("")
    assert result is None


# ---------------------------------------------------------------------------
# _sanitize_url
# ---------------------------------------------------------------------------


def test_sanitize_url_with_credentials():
    assert (
        _sanitize_url("https://user:pass@host.com/path") == "https://***@host.com/path"
    )


def test_sanitize_url_without_credentials():
    assert _sanitize_url("https://host.com/path") == "https://host.com/path"


# ---------------------------------------------------------------------------
# _build_session
# ---------------------------------------------------------------------------


def test_build_session_no_auth():
    session = _build_session()
    assert session.auth is None
    session.close()


def test_build_session_with_auth():
    session = _build_session(username="user", password="pass")
    assert session.auth == ("user", "pass")
    session.close()


def test_build_session_username_only():
    session = _build_session(username="apikey", password=None)
    assert session.auth == ("apikey", "")
    session.close()


def test_build_session_extra_headers():
    session = _build_session(extra_headers={"X-Custom": "value"})
    assert session.headers["X-Custom"] == "value"
    assert session.headers["Accept"] == "application/json"
    session.close()


# ---------------------------------------------------------------------------
# ODataAdapter static methods
# ---------------------------------------------------------------------------


def test_adapter_supports():
    assert ODataAdapter.supports("anything") is True


def test_adapter_parse_uri():
    assert ODataAdapter.parse_uri("incidents") == ("incidents",)


def test_adapter_safe_is_true():
    assert ODataAdapter.safe is True


# ---------------------------------------------------------------------------
# ODataAdapter lifecycle (with mocked HTTP)
# ---------------------------------------------------------------------------

SERVICE_URL = "https://api.example.com/odata"


@responses.activate
def test_adapter_discover_schema():
    responses.add(
        responses.GET,
        f"{SERVICE_URL}/$metadata",
        body=SAMPLE_METADATA,
        content_type="application/xml",
    )

    adapter = ODataAdapter("products", service_url=SERVICE_URL)
    columns = adapter.get_columns()
    assert "ProductId" in columns
    assert "Name" in columns
    assert "Price" in columns
    assert "InStock" in columns
    assert "CreatedAt" in columns
    adapter.close()


@responses.activate
def test_adapter_discover_schema_failure():
    responses.add(
        responses.GET,
        f"{SERVICE_URL}/$metadata",
        status=500,
    )

    try:
        ODataAdapter("products", service_url=SERVICE_URL)
        assert False, "Should have raised RuntimeError"
    except RuntimeError as exc:
        assert "$metadata" in str(exc)


@responses.activate
def test_adapter_discover_schema_entity_not_found():
    responses.add(
        responses.GET,
        f"{SERVICE_URL}/$metadata",
        body=SAMPLE_METADATA,
        content_type="application/xml",
    )

    try:
        ODataAdapter("nonexistent", service_url=SERVICE_URL)
        assert False, "Should have raised RuntimeError"
    except RuntimeError as exc:
        assert "nonexistent" in str(exc)


@responses.activate
def test_adapter_get_data_single_page():
    responses.add(
        responses.GET,
        f"{SERVICE_URL}/$metadata",
        body=SAMPLE_METADATA,
        content_type="application/xml",
    )
    responses.add(
        responses.GET,
        f"{SERVICE_URL}/products",
        json={
            "value": [
                {
                    "ProductId": "abc-123",
                    "Name": "Widget",
                    "Price": 9.99,
                    "InStock": True,
                    "CreatedAt": "2024-01-15T10:30:00Z",
                },
            ]
        },
    )

    adapter = ODataAdapter("products", service_url=SERVICE_URL)
    rows = list(adapter.get_data({}, []))
    assert len(rows) == 1
    assert rows[0]["Name"] == "Widget"
    assert rows[0]["Price"] == 9.99
    assert rows[0]["InStock"] is True
    assert rows[0]["CreatedAt"].year == 2024
    adapter.close()


@responses.activate
def test_adapter_get_data_empty():
    responses.add(
        responses.GET,
        f"{SERVICE_URL}/$metadata",
        body=SAMPLE_METADATA,
        content_type="application/xml",
    )
    responses.add(
        responses.GET,
        f"{SERVICE_URL}/products",
        json={"value": []},
    )

    adapter = ODataAdapter("products", service_url=SERVICE_URL)
    rows = list(adapter.get_data({}, []))
    assert len(rows) == 0
    adapter.close()


@responses.activate
def test_adapter_get_data_odata_error():
    responses.add(
        responses.GET,
        f"{SERVICE_URL}/$metadata",
        body=SAMPLE_METADATA,
        content_type="application/xml",
    )
    responses.add(
        responses.GET,
        f"{SERVICE_URL}/products",
        json={"error": {"code": "403", "message": "Forbidden"}},
    )

    adapter = ODataAdapter("products", service_url=SERVICE_URL)
    rows = list(adapter.get_data({}, []))
    assert len(rows) == 0
    adapter.close()


@responses.activate
def test_adapter_get_data_nextlink_pagination():
    responses.add(
        responses.GET,
        f"{SERVICE_URL}/$metadata",
        body=SAMPLE_METADATA,
        content_type="application/xml",
    )
    responses.add(
        responses.GET,
        f"{SERVICE_URL}/products",
        json={
            "value": [
                {
                    "ProductId": "1",
                    "Name": "A",
                    "Price": 1.0,
                    "InStock": True,
                    "CreatedAt": None,
                },
            ],
            "@odata.nextLink": f"{SERVICE_URL}/products?$skip=1",
        },
    )
    responses.add(
        responses.GET,
        f"{SERVICE_URL}/products?$skip=1",
        json={
            "value": [
                {
                    "ProductId": "2",
                    "Name": "B",
                    "Price": 2.0,
                    "InStock": False,
                    "CreatedAt": None,
                },
            ],
        },
    )

    adapter = ODataAdapter("products", service_url=SERVICE_URL)
    rows = list(adapter.get_data({}, []))
    assert len(rows) == 2
    assert rows[0]["Name"] == "A"
    assert rows[1]["Name"] == "B"
    adapter.close()


@responses.activate
def test_adapter_get_data_http_error():
    responses.add(
        responses.GET,
        f"{SERVICE_URL}/$metadata",
        body=SAMPLE_METADATA,
        content_type="application/xml",
    )
    responses.add(
        responses.GET,
        f"{SERVICE_URL}/products",
        status=500,
        json={"error": "internal server error"},
    )

    adapter = ODataAdapter("products", service_url=SERVICE_URL)
    rows = list(adapter.get_data({}, []))
    assert len(rows) == 0
    adapter.close()


@responses.activate
def test_adapter_get_data_null_values():
    responses.add(
        responses.GET,
        f"{SERVICE_URL}/$metadata",
        body=SAMPLE_METADATA,
        content_type="application/xml",
    )
    responses.add(
        responses.GET,
        f"{SERVICE_URL}/products",
        json={
            "value": [
                {
                    "ProductId": "abc",
                    "Name": None,
                    "Price": None,
                    "InStock": True,
                    "CreatedAt": None,
                },
            ]
        },
    )

    adapter = ODataAdapter("products", service_url=SERVICE_URL)
    rows = list(adapter.get_data({}, []))
    assert len(rows) == 1
    assert rows[0]["Name"] is None
    assert rows[0]["Price"] is None
    assert rows[0]["CreatedAt"] is None
    adapter.close()


@responses.activate
def test_adapter_get_data_complex_value_serialized():
    responses.add(
        responses.GET,
        f"{SERVICE_URL}/$metadata",
        body=SAMPLE_METADATA,
        content_type="application/xml",
    )
    responses.add(
        responses.GET,
        f"{SERVICE_URL}/products",
        json={
            "value": [
                {
                    "ProductId": "abc",
                    "Name": {"nested": "object"},
                    "Price": 1.0,
                    "InStock": True,
                    "CreatedAt": None,
                },
            ]
        },
    )

    adapter = ODataAdapter("products", service_url=SERVICE_URL)
    rows = list(adapter.get_data({}, []))
    assert len(rows) == 1
    assert rows[0]["Name"] == '{"nested": "object"}'
    adapter.close()


# ---------------------------------------------------------------------------
# Dialect tests
# ---------------------------------------------------------------------------


def test_dialect_service_url():
    from sqlalchemy.engine.url import make_url

    from shillelagh_odata.dialect import APSWODataDialect

    url = make_url("odata://user:pass@api.example.com/odata/v1")
    result = APSWODataDialect._service_url(url)
    assert result == "https://api.example.com/odata/v1"


def test_dialect_service_url_with_port():
    from sqlalchemy.engine.url import make_url

    from shillelagh_odata.dialect import APSWODataDialect

    url = make_url("odata://user:pass@api.example.com:8443/odata")
    result = APSWODataDialect._service_url(url)
    assert result == "https://api.example.com:8443/odata"


def test_dialect_service_url_localhost_uses_http():
    from sqlalchemy.engine.url import make_url

    from shillelagh_odata.dialect import APSWODataDialect

    url = make_url("odata://localhost:8080/odata")
    result = APSWODataDialect._service_url(url)
    assert result == "http://localhost:8080/odata"


def test_dialect_service_url_127_uses_http():
    from sqlalchemy.engine.url import make_url

    from shillelagh_odata.dialect import APSWODataDialect

    url = make_url("odata://127.0.0.1:8080/odata")
    result = APSWODataDialect._service_url(url)
    assert result == "http://127.0.0.1:8080/odata"


def test_dialect_service_url_no_path():
    from sqlalchemy.engine.url import make_url

    from shillelagh_odata.dialect import APSWODataDialect

    url = make_url("odata://api.example.com")
    result = APSWODataDialect._service_url(url)
    assert result == "https://api.example.com"


@responses.activate
def test_adapter_get_data_topskip_pagination():
    """Test $top/$skip fallback when @odata.nextLink is not present."""
    from shillelagh_odata.adapter import DEFAULT_PAGE_SIZE

    responses.add(
        responses.GET,
        f"{SERVICE_URL}/$metadata",
        body=SAMPLE_METADATA,
        content_type="application/xml",
    )
    # First page: return exactly page_size items to trigger next page fetch
    page1 = [
        {
            "ProductId": str(i),
            "Name": f"P{i}",
            "Price": 1.0,
            "InStock": True,
            "CreatedAt": None,
        }
        for i in range(DEFAULT_PAGE_SIZE)
    ]
    # Second page: return fewer than page_size to signal end
    page2 = [
        {
            "ProductId": "last",
            "Name": "Last",
            "Price": 2.0,
            "InStock": False,
            "CreatedAt": None,
        },
    ]
    responses.add(responses.GET, f"{SERVICE_URL}/products", json={"value": page1})
    responses.add(responses.GET, f"{SERVICE_URL}/products", json={"value": page2})

    adapter = ODataAdapter("products", service_url=SERVICE_URL)
    rows = list(adapter.get_data({}, []))
    assert len(rows) == DEFAULT_PAGE_SIZE + 1
    assert rows[-1]["Name"] == "Last"
    adapter.close()


@responses.activate
def test_adapter_get_data_invalid_json():
    """Test that invalid JSON response is handled gracefully."""
    responses.add(
        responses.GET,
        f"{SERVICE_URL}/$metadata",
        body=SAMPLE_METADATA,
        content_type="application/xml",
    )
    responses.add(
        responses.GET,
        f"{SERVICE_URL}/products",
        body="not valid json",
        content_type="application/json",
    )

    adapter = ODataAdapter("products", service_url=SERVICE_URL)
    rows = list(adapter.get_data({}, []))
    assert len(rows) == 0
    adapter.close()


@responses.activate
def test_dialect_get_table_names():
    from unittest.mock import MagicMock

    from sqlalchemy.engine.url import make_url

    from shillelagh_odata.dialect import APSWODataDialect

    responses.add(
        responses.GET,
        "https://api.example.com/odata/$metadata",
        body=SAMPLE_METADATA,
        content_type="application/xml",
    )

    dialect = APSWODataDialect.__new__(APSWODataDialect)
    connection = MagicMock()
    connection.engine.url = make_url("odata://user:pass@api.example.com/odata")

    names = dialect.get_table_names(connection)
    assert names == ["orders", "products"]


@responses.activate
def test_dialect_get_table_names_failure():
    from unittest.mock import MagicMock

    from sqlalchemy.engine.url import make_url

    from shillelagh_odata.dialect import APSWODataDialect

    responses.add(
        responses.GET,
        "https://api.example.com/odata/$metadata",
        status=500,
    )

    dialect = APSWODataDialect.__new__(APSWODataDialect)
    connection = MagicMock()
    connection.engine.url = make_url("odata://user:pass@api.example.com/odata")

    names = dialect.get_table_names(connection)
    assert names == []


def test_dialect_create_connect_args():
    from sqlalchemy.engine.url import make_url

    from shillelagh_odata.dialect import APSWODataDialect

    dialect = APSWODataDialect()
    url = make_url("odata://myuser:mypass@api.example.com/odata/v1")
    _args, kwargs = dialect.create_connect_args(url)

    assert kwargs["path"] == ":memory:"
    assert "adapter_kwargs" in kwargs
    adapter_kwargs = kwargs["adapter_kwargs"]
    assert "odataapi" in adapter_kwargs
    assert (
        adapter_kwargs["odataapi"]["service_url"] == "https://api.example.com/odata/v1"
    )
    assert adapter_kwargs["odataapi"]["username"] == "myuser"
    assert adapter_kwargs["odataapi"]["password"] == "mypass"


@responses.activate
def test_dialect_get_columns():
    from unittest.mock import MagicMock

    from sqlalchemy.engine.url import make_url

    from shillelagh_odata.dialect import APSWODataDialect

    responses.add(
        responses.GET,
        "https://api.example.com/odata/$metadata",
        body=SAMPLE_METADATA,
        content_type="application/xml",
    )

    dialect = APSWODataDialect.__new__(APSWODataDialect)
    connection = MagicMock()
    connection.engine.url = make_url("odata://user:pass@api.example.com/odata")

    columns = dialect.get_columns(connection, "products")
    col_names = [c["name"] for c in columns]
    assert "ProductId" in col_names
    assert "Name" in col_names
    assert "Price" in col_names
    assert "InStock" in col_names
    assert "CreatedAt" in col_names
    assert len(columns) == 5
    assert all(c["nullable"] is True for c in columns)
    assert [c["ordinal_position"] for c in columns] == [0, 1, 2, 3, 4]


@responses.activate
def test_dialect_get_columns_not_found():
    from unittest.mock import MagicMock

    from sqlalchemy.engine.url import make_url

    from shillelagh_odata.dialect import APSWODataDialect

    responses.add(
        responses.GET,
        "https://api.example.com/odata/$metadata",
        body=SAMPLE_METADATA,
        content_type="application/xml",
    )

    dialect = APSWODataDialect.__new__(APSWODataDialect)
    connection = MagicMock()
    connection.engine.url = make_url("odata://user:pass@api.example.com/odata")

    columns = dialect.get_columns(connection, "nonexistent")
    assert columns == []


@responses.activate
def test_dialect_has_table():
    from unittest.mock import MagicMock

    from sqlalchemy.engine.url import make_url

    from shillelagh_odata.dialect import APSWODataDialect

    responses.add(
        responses.GET,
        "https://api.example.com/odata/$metadata",
        body=SAMPLE_METADATA,
        content_type="application/xml",
    )
    responses.add(
        responses.GET,
        "https://api.example.com/odata/$metadata",
        body=SAMPLE_METADATA,
        content_type="application/xml",
    )

    dialect = APSWODataDialect.__new__(APSWODataDialect)
    connection = MagicMock()
    connection.engine.url = make_url("odata://user:pass@api.example.com/odata")

    assert dialect.has_table(connection, "products") is True
    assert dialect.has_table(connection, "nonexistent") is False


def test_dialect_get_schema_names():
    from unittest.mock import MagicMock

    from shillelagh_odata.dialect import APSWODataDialect

    dialect = APSWODataDialect.__new__(APSWODataDialect)
    connection = MagicMock()

    assert dialect.get_schema_names(connection) == ["main"]


def test_dialect_get_pk_constraint():
    from unittest.mock import MagicMock

    from shillelagh_odata.dialect import APSWODataDialect

    dialect = APSWODataDialect.__new__(APSWODataDialect)
    connection = MagicMock()

    result = dialect.get_pk_constraint(connection, "Products")
    assert result == {"constrained_columns": [], "name": None}


def test_dialect_get_foreign_keys():
    from unittest.mock import MagicMock

    from shillelagh_odata.dialect import APSWODataDialect

    dialect = APSWODataDialect.__new__(APSWODataDialect)
    connection = MagicMock()

    assert dialect.get_foreign_keys(connection, "Products") == []


def test_dialect_get_indexes():
    from unittest.mock import MagicMock

    from shillelagh_odata.dialect import APSWODataDialect

    dialect = APSWODataDialect.__new__(APSWODataDialect)
    connection = MagicMock()

    assert dialect.get_indexes(connection, "Products") == []


def test_engine_spec_attributes():
    from shillelagh_odata.engine_spec import ODataEngineSpec

    assert ODataEngineSpec.engine == "odata"
    assert ODataEngineSpec.engine_name == "OData"
    assert ODataEngineSpec.allows_joins is True
    assert ODataEngineSpec.allows_subqueries is True


# ---------------------------------------------------------------------------
# Integration tests (require network access, skipped by default)
# ---------------------------------------------------------------------------

NORTHWIND_URL = "odata://services.odata.org/V4/Northwind/Northwind.svc"


@pytest.mark.integration
def test_northwind_table_discovery():
    from sqlalchemy import create_engine, inspect

    engine = create_engine(NORTHWIND_URL)
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    assert len(tables) > 0
    assert "Products" in tables
    assert "Orders" in tables
    assert "Customers" in tables


@pytest.mark.integration
def test_northwind_get_columns():
    from sqlalchemy import create_engine, inspect

    engine = create_engine(NORTHWIND_URL)
    inspector = inspect(engine)
    columns = inspector.get_columns("Products")
    col_names = [c["name"] for c in columns]
    assert "ProductID" in col_names
    assert "ProductName" in col_names
    assert "UnitPrice" in col_names


@pytest.mark.integration
def test_northwind_query():
    from sqlalchemy import create_engine, text

    engine = create_engine(NORTHWIND_URL)
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT ProductName, UnitPrice FROM Products LIMIT 5")
        )
        rows = result.fetchall()
        assert len(rows) == 5
        assert all(row[0] is not None for row in rows)

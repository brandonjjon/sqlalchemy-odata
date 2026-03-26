"""Shillelagh adapter for OData v4 services.

Exposes OData entity sets as virtual SQL tables. Schema is discovered
from the service's $metadata endpoint (CSDL/EDMX).

Data is fetched via $top/$skip pagination and queried locally by SQLite.

The adapter is used via the ``odata://`` SQLAlchemy dialect, which passes
connection-level configuration (service URL, credentials) through
``adapter_kwargs``.  The *table name* in SQL is the entity set name
(e.g. ``incidents``, ``injuries``).
"""

from __future__ import annotations

import json as json_mod
import logging
import re
import xml.etree.ElementTree as ET
from collections.abc import Iterator
from datetime import datetime
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from shillelagh.adapters.base import Adapter
from shillelagh.fields import (
    Boolean,
    DateTime,
    Field,
    Filter,
    Float,
    Integer,
    Order,
    String,
)
from shillelagh.typing import RequestedOrder, Row
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# OData EDMX namespaces
EDMX_NS = {
    "edmx": "http://docs.oasis-open.org/odata/ns/edmx",
    "edm": "http://docs.oasis-open.org/odata/ns/edm",
}

# Map OData EDM types to Shillelagh field classes
EDM_TYPE_MAP: dict[str, type[Field]] = {
    "Edm.Boolean": Boolean,
    "Edm.Byte": Integer,
    "Edm.Int16": Integer,
    "Edm.Int32": Integer,
    "Edm.Int64": Integer,
    "Edm.SByte": Integer,
    "Edm.Single": Float,
    "Edm.Double": Float,
    "Edm.Decimal": Float,
    "Edm.String": String,
    "Edm.Guid": String,
    "Edm.Binary": String,
    "Edm.Date": String,
    "Edm.TimeOfDay": String,
    "Edm.DateTimeOffset": DateTime,
    "Edm.Duration": String,
    "Edm.Stream": String,
}

# Default page size for OData pagination
DEFAULT_PAGE_SIZE = 500

# Pattern to sanitize credentials from URLs in log messages
_CRED_PATTERN = re.compile(r"://[^@]+@")


def _sanitize_url(url: str) -> str:
    """Remove credentials from a URL for safe logging."""
    return _CRED_PATTERN.sub("://***@", url)


def _parse_datetime(value: str) -> datetime | None:
    """Parse an OData datetime string into a Python datetime.

    Handles ISO 8601 formats with and without timezone info.
    Returns None if the value cannot be parsed.
    """
    # Remove trailing Z and treat as UTC
    s = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        # Fallback: strip fractional seconds and try again
        if "." in s:
            base = s[: s.index(".")]
            # Re-attach timezone offset if present
            remainder = s[s.index(".") + 1 :]
            for sep in ("+", "-"):
                idx = remainder.find(sep)
                if idx >= 0:
                    base += remainder[idx:]
                    break
            s = base
        try:
            return datetime.fromisoformat(s)
        except ValueError:
            logger.warning("Could not parse datetime: %s", value)
            return None


def _build_session(
    username: str | None = None,
    password: str | None = None,
    extra_headers: dict[str, str] | None = None,
) -> requests.Session:
    """Build a requests session with retry logic and optional auth."""
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.headers["Accept"] = "application/json"

    if username is not None:
        session.auth = (username, password or "")

    if extra_headers:
        session.headers.update(extra_headers)

    return session


def _parse_entity_types(root: ET.Element) -> dict[str, dict[str, str]]:
    """Extract entity types from a parsed EDMX root element."""
    entity_types: dict[str, dict[str, str]] = {}
    for schema in root.findall(".//edm:Schema", EDMX_NS):
        namespace = schema.get("Namespace", "")
        for entity_type in schema.findall("edm:EntityType", EDMX_NS):
            name = entity_type.get("Name", "")
            fqn = f"{namespace}.{name}"
            props: dict[str, str] = {}
            for prop in entity_type.findall("edm:Property", EDMX_NS):
                prop_name = prop.get("Name", "")
                prop_type = prop.get("Type", "Edm.String")
                if prop_type.startswith("Collection("):
                    prop_type = "Edm.String"
                props[prop_name] = prop_type
            entity_types[fqn] = props
    return entity_types


def parse_metadata(xml_text: str) -> dict[str, dict[str, str]]:
    """Parse OData $metadata EDMX XML into entity-type -> {property: edm_type}.

    Returns a mapping from fully-qualified entity type name to a dict of
    property names and their EDM types.
    """
    root = ET.fromstring(xml_text)
    return _parse_entity_types(root)


def resolve_entity_set(
    xml_text: str,
    entity_set_name: str,
) -> tuple[str, dict[str, str]] | None:
    """Find the entity type for a given entity set name.

    Returns (entity_type_fqn, {property: edm_type}) or None.
    """
    root = ET.fromstring(xml_text)
    entity_types = _parse_entity_types(root)

    for container in root.findall(".//edm:EntityContainer", EDMX_NS):
        for entity_set in container.findall("edm:EntitySet", EDMX_NS):
            if entity_set.get("Name") == entity_set_name:
                type_fqn = entity_set.get("EntityType", "")
                if type_fqn in entity_types:
                    return type_fqn, entity_types[type_fqn]

    return None


def get_entity_set_names(xml_text: str) -> list[str]:
    """Extract all entity set names from $metadata XML."""
    root = ET.fromstring(xml_text)
    names: list[str] = []
    for container in root.findall(".//edm:EntityContainer", EDMX_NS):
        for entity_set in container.findall("edm:EntitySet", EDMX_NS):
            name = entity_set.get("Name", "")
            if name:
                names.append(name)
    return sorted(names)


class ODataAdapter(Adapter):
    """Adapter that maps an OData entity set to a virtual SQL table.

    Connection-level configuration (``service_url``, ``username``,
    ``password``) is injected by the dialect via ``adapter_kwargs``.
    The table name passed to ``supports()`` / ``parse_uri()`` /
    ``__init__()`` is the OData entity set name (e.g. ``incidents``).
    """

    safe = True
    supports_limit = False
    supports_offset = False
    supports_requested_columns = False

    # Connection-level config injected by the dialect
    service_url: str = ""
    username: str | None = None
    password: str | None = None

    @staticmethod
    def supports(uri: str, fast: bool = True, **kwargs: Any) -> bool | None:
        """Return True unconditionally — routing is handled by the dialect."""
        return True

    @staticmethod
    def parse_uri(uri: str) -> tuple[str]:
        """The URI is the entity set name (table name)."""
        return (uri,)

    def __init__(
        self,
        entity_set_name: str,
        service_url: str = "",
        username: str | None = None,
        password: str | None = None,
        extra_headers: dict | None = None,
    ):
        super().__init__()

        self._entity_set_name = entity_set_name

        # Connection config from adapter_kwargs (injected by dialect)
        self._service_url = (service_url or self.service_url).rstrip("/")
        self._username = username if username is not None else self.username
        self._password = password if password is not None else self.password

        # Build the entity set endpoint URL
        self._entity_set_url = f"{self._service_url}/{entity_set_name}"

        # HTTP session
        self._session = _build_session(
            username=self._username,
            password=self._password,
            extra_headers=extra_headers,
        )

        # Discover schema from $metadata
        self._columns: dict[str, Field] = {}
        self._edm_properties: dict[str, str] = {}
        self._discover_schema()

    def _discover_schema(self) -> None:
        """Fetch $metadata and build column definitions."""
        meta_url = f"{self._service_url}/$metadata"
        safe_url = _sanitize_url(meta_url)
        try:
            resp = self._session.get(
                meta_url, timeout=30, headers={"Accept": "application/xml"}
            )
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise RuntimeError(
                f"Failed to fetch OData $metadata from {safe_url}: {exc}"
            ) from exc

        result = resolve_entity_set(resp.text, self._entity_set_name)
        if not result:
            raise RuntimeError(
                f"Entity set '{self._entity_set_name}' not found "
                f"in $metadata at {safe_url}"
            )

        _type_fqn, properties = result
        self._edm_properties = properties

        for prop_name, edm_type in properties.items():
            field_class = EDM_TYPE_MAP.get(edm_type, String)
            self._columns[prop_name] = field_class(
                filters=[],
                order=Order.NONE,
                exact=True,
            )

    def get_columns(self) -> dict[str, Field]:
        """Return the columns discovered from $metadata."""
        return self._columns

    def get_data(
        self,
        bounds: dict[str, Filter],
        order: list[tuple[str, RequestedOrder]],
        **kwargs: Any,
    ) -> Iterator[Row]:
        """Fetch all rows from the OData entity set via pagination.

        Supports both $top/$skip and @odata.nextLink cursor-based pagination.
        """
        page_size = DEFAULT_PAGE_SIZE
        skip = 0
        rowid = 0
        next_url: str | None = None

        while True:
            try:
                if next_url:
                    resp = self._session.get(next_url, timeout=60)
                else:
                    params: dict[str, str] = {
                        "$top": str(page_size),
                        "$skip": str(skip),
                    }
                    resp = self._session.get(
                        self._entity_set_url,
                        params=params,
                        timeout=60,
                    )
                resp.raise_for_status()
                data = resp.json()
            except requests.RequestException as exc:
                logger.error("OData fetch failed at skip=%d: %s", skip, exc)
                break
            except ValueError as exc:
                logger.error("OData response not valid JSON at skip=%d: %s", skip, exc)
                break

            # Check for OData error responses
            if "error" in data and "value" not in data:
                error = data["error"]
                msg = error.get("message", str(error))
                logger.error("OData service returned error: %s", msg)
                break

            items = data.get("value", [])
            if not items:
                break

            for item in items:
                row: Row = {"rowid": rowid}
                for col_name, field in self._columns.items():
                    value = item.get(col_name)
                    if value is None:
                        row[col_name] = None
                    elif isinstance(value, (list, dict)):
                        row[col_name] = json_mod.dumps(value)
                    elif isinstance(field, DateTime) and isinstance(value, str):
                        row[col_name] = _parse_datetime(value)
                    else:
                        row[col_name] = value
                rowid += 1
                yield row

            # Use @odata.nextLink if available (cursor-based pagination)
            next_url = data.get("@odata.nextLink")
            if next_url:
                continue

            # Fall back to $top/$skip pagination
            if len(items) < page_size:
                break

            skip += len(items)

    def close(self) -> None:
        """Close the HTTP session."""
        self._session.close()

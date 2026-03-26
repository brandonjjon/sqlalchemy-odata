"""SQLAlchemy dialect for OData via Shillelagh.

Connection string format::

    odata://username:password@host:port/service/path

Example::

    odata://myuser:mypassword@api.example.com/odata/v1

Tables are OData entity set names discovered from ``$metadata``.
"""

from __future__ import annotations

import logging
from typing import Any

from shillelagh.backends.apsw.dialects.base import APSWDialect
from sqlalchemy import types as sa_types
from sqlalchemy.engine.url import URL

from .adapter import (
    _build_session,
    _sanitize_url,
    get_entity_set_names,
    resolve_entity_set,
)

# Map OData EDM types to SQLAlchemy column types
EDM_TO_SA: dict[str, sa_types.TypeEngine] = {
    "Edm.Boolean": sa_types.Boolean(),
    "Edm.Byte": sa_types.SmallInteger(),
    "Edm.Int16": sa_types.SmallInteger(),
    "Edm.Int32": sa_types.Integer(),
    "Edm.Int64": sa_types.BigInteger(),
    "Edm.SByte": sa_types.SmallInteger(),
    "Edm.Single": sa_types.Float(),
    "Edm.Double": sa_types.Float(),
    "Edm.Decimal": sa_types.Numeric(),
    "Edm.String": sa_types.Text(),
    "Edm.Guid": sa_types.Text(),
    "Edm.Binary": sa_types.LargeBinary(),
    "Edm.Date": sa_types.Text(),
    "Edm.TimeOfDay": sa_types.Text(),
    "Edm.DateTimeOffset": sa_types.DateTime(),
    "Edm.Duration": sa_types.Text(),
    "Edm.Stream": sa_types.Text(),
}

logger = logging.getLogger(__name__)

# Must match the entry point name in pyproject.toml [shillelagh.adapter].
# Shillelagh's connect() remaps from entry point names to class names internally.
ADAPTER_NAME = "odataapi"


class APSWODataDialect(APSWDialect):
    """SQLAlchemy dialect that exposes OData entity sets as SQL tables."""

    name = "odata"
    supports_statement_cache = False

    def __init__(self, **kwargs: Any):
        super().__init__(safe=True, adapters=[ADAPTER_NAME], **kwargs)

    @staticmethod
    def _service_url(url: URL) -> str:
        """Build the service root URL from a SQLAlchemy URL.

        Uses HTTP for localhost/127.0.0.1 (local development),
        HTTPS for everything else.
        """
        host = url.host or "localhost"
        port = f":{url.port}" if url.port else ""
        path = f"/{url.database}" if url.database else ""
        scheme = "http" if host in ("localhost", "127.0.0.1") else "https"
        return f"{scheme}://{host}{port}{path}"

    def _fetch_metadata(self, url: URL) -> str | None:
        """Fetch raw $metadata XML from the OData service."""
        service_url = self._service_url(url)
        meta_url = f"{service_url}/$metadata"

        session = _build_session(
            username=url.username,
            password=url.password,
            extra_headers={"Accept": "application/xml"},
        )
        try:
            resp = session.get(meta_url, timeout=30)
            resp.raise_for_status()
            return resp.text
        except Exception:
            logger.exception(
                "Failed to fetch OData $metadata from %s",
                _sanitize_url(meta_url),
            )
            return None
        finally:
            session.close()

    def get_schema_names(
        self,
        connection: Any,
        **kwargs: Any,
    ) -> list[str]:
        """Return available schema names."""
        return ["main"]

    def get_table_names(
        self,
        connection: Any,
        schema: str | None = None,
        **kwargs: Any,
    ) -> list[str]:
        """Discover entity set names from the OData $metadata endpoint."""
        xml = self._fetch_metadata(connection.engine.url)
        if xml is None:
            return []
        return get_entity_set_names(xml)

    def get_columns(
        self,
        connection: Any,
        table_name: str,
        schema: str | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Return column information for a table."""
        xml = self._fetch_metadata(connection.engine.url)
        if xml is None:
            return []

        result = resolve_entity_set(xml, table_name)
        if result is None:
            return []

        _type_fqn, properties = result
        columns = []
        for i, (prop_name, edm_type) in enumerate(properties.items()):
            if edm_type.startswith("Collection("):
                edm_type = "Edm.String"
            sa_type = EDM_TO_SA.get(edm_type, sa_types.Text())
            columns.append(
                {
                    "name": prop_name,
                    "type": sa_type,
                    "nullable": True,
                    "default": None,
                    "ordinal_position": i,
                }
            )
        return columns

    def get_pk_constraint(
        self,
        connection: Any,
        table_name: str,
        schema: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Return an empty primary key constraint.

        OData entity sets don't expose primary key info through $metadata
        in a way that maps to SQL constraints.
        """
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(
        self,
        connection: Any,
        table_name: str,
        schema: str | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Return an empty list — OData navigation properties are not mapped."""
        return []

    def get_indexes(
        self,
        connection: Any,
        table_name: str,
        schema: str | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """Return an empty list — OData has no concept of indexes."""
        return []

    def has_table(
        self,
        connection: Any,
        table_name: str,
        schema: str | None = None,
        **kwargs: Any,
    ) -> bool:
        """Check if a table (entity set) exists."""
        return table_name in self.get_table_names(connection, schema)

    def create_connect_args(
        self,
        url: URL,
    ) -> tuple[tuple[()], dict[str, Any]]:
        """Configure Shillelagh to route table names to the OData adapter."""
        args, kwargs = super().create_connect_args(url)
        service_url = self._service_url(url)

        adapter_kwargs = {
            ADAPTER_NAME: {
                "service_url": service_url,
                "username": url.username,
                "password": url.password,
            },
        }

        return args, {**kwargs, "path": ":memory:", "adapter_kwargs": adapter_kwargs}

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
from sqlalchemy.engine.url import URL

from .adapter import _build_session, _sanitize_url, get_entity_set_names

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

    def get_table_names(
        self,
        connection: Any,
        schema: str | None = None,
        **kwargs: Any,
    ) -> list[str]:
        """Discover entity set names from the OData $metadata endpoint."""
        url = connection.engine.url
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
            return get_entity_set_names(resp.text)
        except Exception:
            logger.exception(
                "Failed to discover OData entity sets from %s",
                _sanitize_url(meta_url),
            )
            return []
        finally:
            session.close()

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

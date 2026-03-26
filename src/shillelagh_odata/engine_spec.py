"""Superset DB engine spec for OData connections.

Registers the ``odata://`` scheme with Superset so it appears in the
"Add Database" dialog and gets proper time-grain expressions (inherited
from SQLite via Shillelagh).
"""

from __future__ import annotations

try:
    from superset.db_engine_specs.sqlite import SqliteEngineSpec
except ImportError:
    from shillelagh.backends.apsw.dialects.base import APSWDialect as SqliteEngineSpec


class ODataEngineSpec(SqliteEngineSpec):
    """Engine spec for OData via Shillelagh."""

    engine = "odata"
    engine_name = "OData"
    default_driver = "apsw"
    sqlalchemy_uri_placeholder = "odata://user:password@host/service-path"

    allows_joins = True
    allows_subqueries = True

# sqlalchemy-odata

[![PyPI version](https://img.shields.io/pypi/v/sqlalchemy-odata.svg)](https://pypi.org/project/sqlalchemy-odata/)
[![Python versions](https://img.shields.io/pypi/pyversions/sqlalchemy-odata.svg)](https://pypi.org/project/sqlalchemy-odata/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Open-source OData v4 connector for SQLAlchemy and [Apache Superset](https://superset.apache.org/).

A [Shillelagh](https://github.com/betodealmeida/shillelagh) adapter that lets you query any OData v4 API with SQL.

## What it does

- Connects to any OData v4 service and reads `$metadata` to **auto-discover** all entity sets and their schemas
- Exposes each entity set as a SQL table (e.g. `Products`, `Orders`, `Customers`)
- Fetches data via `$top`/`$skip` and `@odata.nextLink` pagination
- SQLite (via [Shillelagh](https://github.com/betodealmeida/shillelagh)/APSW) handles all SQL operations locally — `SELECT`, `WHERE`, `GROUP BY`, `JOIN`, subqueries, etc.
- Registers an `odata://` SQLAlchemy dialect for easy connection strings
- Includes a Superset engine spec so it appears in the "Add Database" dialog

## Installation

```bash
pip install sqlalchemy-odata
```

For Apache Superset, add to your `requirements-local.txt` or Docker image:

```
sqlalchemy-odata
```

## Quick start

Try it with the public [Northwind OData service](https://services.odata.org/) — no auth required:

```python
from sqlalchemy import create_engine, text

engine = create_engine("odata://services.odata.org/V4/Northwind/Northwind.svc")

with engine.connect() as conn:
    result = conn.execute(text("SELECT ProductName, UnitPrice FROM Products LIMIT 5"))
    for row in result:
        print(row)
```

## Usage

### Connection string

```
odata://username:password@hostname/service-path
```

The username and password are passed as HTTP Basic Auth credentials. The service path is the OData service root (everything before the entity set names).

HTTPS is used by default. For local development servers (`localhost` / `127.0.0.1`), HTTP is used automatically.

> **Note:** Credentials are embedded in the connection string. If you're using Superset, be aware that connection strings are stored in Superset's metadata database. Consider using Superset's [secrets management](https://superset.apache.org/docs/configuration/configuring-superset/#secret-management) for production deployments.

### In Python

```python
from sqlalchemy import create_engine, text

engine = create_engine(
    "odata://myuser:mypassword@api.example.com/odata/v1"
)

with engine.connect() as conn:
    # Auto-discovers tables from $metadata
    result = conn.execute(text("SELECT * FROM Products LIMIT 10"))
    for row in result:
        print(row)

    # Full SQL support — GROUP BY, JOIN, subqueries, etc.
    result = conn.execute(text("""
        SELECT Category, COUNT(*) as cnt, AVG(Price) as avg_price
        FROM Products
        WHERE InStock = 1
        GROUP BY Category
        ORDER BY cnt DESC
    """))
```

### With HammerTech

```python
engine = create_engine(
    "odata://myuser:api_key@us-reporting-01.hammertechonline.com/v0.1"
)

with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM incidents LIMIT 10"))
```

### In Apache Superset

1. Go to **Settings > Database Connections > + Database**
2. Select **OData** (or use "Other" and enter the URI manually)
3. Enter the connection string: `odata://user:pass@host/path`
4. Click **Connect** — all entity sets appear as tables in SQL Lab

### Table discovery

Tables are automatically discovered from the OData `$metadata` endpoint. You can also inspect them programmatically:

```python
from sqlalchemy import create_engine, inspect

engine = create_engine("odata://user:pass@host/path")
inspector = inspect(engine)
print(inspector.get_table_names())
# ['Customers', 'Orders', 'Products', ...]
```

## How it works

```
┌──────────────────────────────────────────────────────────┐
│  Your SQL query                                          │
│  SELECT * FROM Products WHERE Price > 100                │
└────────────────────┬─────────────────────────────────────┘
                     │
         ┌───────────▼───────────┐
         │  SQLite (via APSW)    │  Handles SQL parsing,
         │  + Shillelagh         │  filtering, joins, etc.
         └───────────┬───────────┘
                     │
         ┌───────────▼───────────┐
         │  sqlalchemy-odata     │  Fetches data from the
         │  ODataAdapter         │  OData API via HTTP
         └───────────┬───────────┘
                     │
         ┌───────────▼───────────┐
         │  OData v4 Service     │  $metadata for schema,
         │  (any provider)       │  $top/$skip for data
         └───────────────────────┘
```

1. On first query, the adapter fetches the `$metadata` EDMX document to discover entity types, properties, and their EDM types
2. EDM types are mapped to SQLite types (`Edm.String` -> `TEXT`, `Edm.Int32` -> `INTEGER`, `Edm.DateTimeOffset` -> `TIMESTAMP`, etc.)
3. Data is fetched via paginated `GET` requests with `$top`/`$skip` parameters (or `@odata.nextLink` if the server provides it)
4. SQLite handles all query operations (filtering, sorting, grouping, joins) locally
5. Results are returned through the standard SQLAlchemy/DB-API interface

## Supported OData features

| Feature | Status |
|---------|--------|
| `$metadata` schema discovery | Supported |
| `$top` / `$skip` pagination | Supported |
| `@odata.nextLink` pagination | Supported |
| `@odata.count` | Not yet |
| Basic Auth | Supported |
| Bearer Token Auth | Not yet |
| OAuth2 | Not yet |
| `$filter` pushdown | Not yet (filtered locally by SQLite) |
| `$select` pushdown | Not yet (all columns fetched) |
| `$orderby` pushdown | Not yet (sorted locally by SQLite) |
| `$expand` (relationships) | Not yet |
| Write operations (POST/PATCH/DELETE) | Not supported (read-only) |

> **Note:** Even without server-side pushdown, all SQL operations work because SQLite handles them locally. Pushdown is a performance optimization for large datasets.

## Limitations

- **Performance on large datasets:** Without `$filter` pushdown, the adapter fetches all rows from an entity set and filters locally. For entity sets with hundreds of thousands of rows, this can be slow and memory-intensive. Pushdown support is planned for a future release.
- **Auth:** Only HTTP Basic Auth is currently supported. Bearer tokens and OAuth2 are planned.
- **Read-only:** Write operations (INSERT, UPDATE, DELETE) are not supported.

## Architecture

This package provides three components built on the [Shillelagh](https://github.com/betodealmeida/shillelagh) framework:

| Component | Purpose |
|-----------|---------|
| `shillelagh_odata.adapter` | [Shillelagh adapter](https://shillelagh.readthedocs.io/en/latest/development.html) — fetches data from OData, parses `$metadata` |
| `shillelagh_odata.dialect` | [SQLAlchemy dialect](https://shillelagh.readthedocs.io/en/latest/development.html#creating-a-custom-sqlalchemy-dialect) (`odata://`) — handles connection strings, table discovery |
| `shillelagh_odata.engine_spec` | Superset `BaseEngineSpec` subclass — registers OData in the Superset UI |

These register via entry points:

```toml
[project.entry-points."shillelagh.adapter"]
odataapi = "shillelagh_odata.adapter:ODataAdapter"

[project.entry-points."sqlalchemy.dialects"]
odata = "shillelagh_odata.dialect:APSWODataDialect"

[project.entry-points."superset.db_engine_specs"]
odata = "shillelagh_odata.engine_spec:ODataEngineSpec"
```

## Troubleshooting

**No tables found / empty table list**
- Verify your OData service URL is correct and the `$metadata` endpoint is accessible
- Check credentials — a 401/403 response will result in an empty table list
- Try accessing `https://your-host/your-path/$metadata` in a browser to verify the service

**Empty query results**
- The entity set may exist in `$metadata` but contain no data
- Check that the entity set name is spelled exactly as it appears in `$metadata` (case-sensitive)

**Connection timeouts**
- The default timeout is 30 seconds for metadata and 60 seconds for data requests
- Large entity sets with many pages may take time to fully load

**Can't connect to local development server**
- `localhost` and `127.0.0.1` automatically use HTTP instead of HTTPS
- For other local hostnames, ensure your server supports HTTPS or use localhost

## Development

```bash
git clone https://github.com/brandonjjon/sqlalchemy-odata.git
cd sqlalchemy-odata
pip install -e ".[dev]"
pytest
```

## Related projects

- [Shillelagh](https://github.com/betodealmeida/shillelagh) — the framework this adapter is built on
- [Apache Superset](https://github.com/apache/superset) — the BI platform this integrates with
- [graphql-db-api](https://github.com/cancan101/graphql-db-api) — similar adapter for GraphQL APIs (also built on Shillelagh)

## License

MIT

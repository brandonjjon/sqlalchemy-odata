# Changelog

## [0.2.3](https://github.com/brandonjjon/sqlalchemy-odata/compare/v0.2.2...v0.2.3) (2026-03-26)


### Bug Fixes

* override get_pk_constraint, get_foreign_keys, get_indexes ([6c61876](https://github.com/brandonjjon/sqlalchemy-odata/commit/6c618769a18de6baf1b91f8dca0fc7752cab438b))

## [0.2.2](https://github.com/brandonjjon/sqlalchemy-odata/compare/v0.2.1...v0.2.2) (2026-03-26)


### Bug Fixes

* add ordinal_position to get_columns to fix Superset sorting ([79bf42a](https://github.com/brandonjjon/sqlalchemy-odata/commit/79bf42a940b3547ed4b93c8b83c87377b91b4e80))

## [0.2.1](https://github.com/brandonjjon/sqlalchemy-odata/compare/v0.2.0...v0.2.1) (2026-03-26)


### Bug Fixes

* use application/xml Accept header for $metadata requests ([7b5cd74](https://github.com/brandonjjon/sqlalchemy-odata/commit/7b5cd745655614cdbdad365d4028e0caa0a8d7e6))

## [0.2.0](https://github.com/brandonjjon/sqlalchemy-odata/compare/v0.1.0...v0.2.0) (2026-03-26)


### Features

* add schema introspection to dialect ([7389204](https://github.com/brandonjjon/sqlalchemy-odata/commit/7389204a66537489cfb77334ff30b0df32715d93))


### Bug Fixes

* set adapter safe=True to work with Shillelagh's adapter loading ([e7377af](https://github.com/brandonjjon/sqlalchemy-odata/commit/e7377afa1f5c13cfcb4731f670079f41ce06eacf))

## 0.1.0 — Initial release

- Shillelagh adapter for OData v4 services
- SQLAlchemy `odata://` dialect with connection string support
- Auto-discovery of entity sets and schemas from `$metadata`
- Data fetching via `$top`/`$skip` and `@odata.nextLink` pagination
- HTTP Basic Auth support
- Superset engine spec for "Add Database" integration
- EDM type mapping to SQLite types

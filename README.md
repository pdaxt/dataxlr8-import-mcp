# dataxlr8-import-mcp

Handles CSV and JSON data imports into DataXLR8 schemas. Provides data validation, field mapping, error tracking, and rollback capabilities for bulk data operations.

## Tools

| Tool | Description |
|------|-------------|
| import_csv | Parse CSV text and insert rows into specified schema.table. Returns job ID and row counts. |
| import_json | Parse JSON array and insert rows into specified schema.table. Returns job ID and row counts. |
| export_table | Export rows from a table as JSON with optional WHERE filter and pagination (limit/offset). |
| import_status | Check status of an import job — rows processed, errors, current state. |
| list_imports | Show import history with optional status filter and pagination (limit/offset). |
| validate_data | Check data against target table schema without importing. Reports column mismatches and type errors. |
| map_fields | Define or update field mapping from source columns to target columns for an import job. |
| rollback_import | Delete all rows from a specific import batch. Uses the batch_id to identify rows. |

## Setup

```bash
DATABASE_URL=postgres://dataxlr8:dataxlr8@localhost:5432/dataxlr8 cargo run
```

## Schema

Creates `imports` schema in PostgreSQL with tables for tracking import jobs and error logs.

## Part of

[DataXLR8](https://github.com/pdaxt) - AI-powered recruitment platform

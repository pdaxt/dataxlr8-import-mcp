use dataxlr8_mcp_core::mcp::{error_result, get_i64, get_str, json_result, make_schema};
use dataxlr8_mcp_core::Database;
use rmcp::model::*;
use rmcp::service::{RequestContext, RoleServer};
use rmcp::ServerHandler;
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

// ============================================================================
// Constants
// ============================================================================

/// Maximum allowed limit for list/export queries.
const MAX_LIMIT: i64 = 10_000;
/// Default limit when none is specified.
const DEFAULT_LIST_LIMIT: i64 = 50;
/// Default limit for export_table when none is specified.
const DEFAULT_EXPORT_LIMIT: i64 = 1000;
/// Default offset when none is specified.
const DEFAULT_OFFSET: i64 = 0;
/// Valid status values for import jobs.
const VALID_STATUSES: &[&str] = &["pending", "running", "completed", "failed", "rolled_back"];

// ============================================================================
// Data types
// ============================================================================

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct ImportJob {
    pub id: String,
    pub source_type: String,
    pub target_schema: String,
    pub target_table: String,
    pub row_count: i32,
    pub error_count: i32,
    pub field_mapping: serde_json::Value,
    pub status: String,
    pub batch_id: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct ImportError {
    pub id: String,
    pub job_id: String,
    pub row_num: i32,
    pub error: String,
    pub data: serde_json::Value,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize)]
pub struct ImportResult {
    pub job_id: String,
    pub batch_id: String,
    pub rows_inserted: i32,
    pub error_count: i32,
    pub status: String,
    pub errors: Vec<ImportError>,
}

#[derive(Debug, Serialize)]
pub struct ValidationResult {
    pub valid: bool,
    pub total_rows: usize,
    pub valid_rows: usize,
    pub error_count: usize,
    pub errors: Vec<ValidationError>,
}

#[derive(Debug, Serialize)]
pub struct ValidationError {
    pub row: usize,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct ExportResult {
    pub table: String,
    pub row_count: usize,
    pub data: Vec<serde_json::Value>,
    pub limit: i64,
    pub offset: i64,
}

#[derive(Debug, Serialize)]
pub struct FieldMapping {
    pub job_id: String,
    pub mapping: serde_json::Value,
    pub status: String,
}

#[derive(Debug, Serialize)]
pub struct ListImportsResult {
    pub jobs: Vec<ImportJob>,
    pub count: usize,
    pub limit: i64,
    pub offset: i64,
}

// ============================================================================
// Input helpers
// ============================================================================

/// Extract a trimmed, non-empty string from args. Returns None if missing or blank after trim.
fn get_trimmed_str(args: &serde_json::Value, key: &str) -> Option<String> {
    get_str(args, key).map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
}

/// Clamp a limit value to [1, MAX_LIMIT].
fn clamp_limit(raw: i64, default: i64) -> i64 {
    if raw <= 0 { default } else { raw.min(MAX_LIMIT) }
}

/// Clamp an offset value to [0, ...).
fn clamp_offset(raw: i64) -> i64 {
    raw.max(0)
}

// ============================================================================
// Tool definitions
// ============================================================================

fn build_tools() -> Vec<Tool> {
    vec![
        Tool {
            name: "import_csv".into(),
            title: None,
            description: Some(
                "Parse CSV text and insert rows into specified schema.table. Returns job ID and row counts."
                    .into(),
            ),
            input_schema: make_schema(
                serde_json::json!({
                    "csv_data": { "type": "string", "description": "Raw CSV text with header row" },
                    "target_schema": { "type": "string", "description": "Target PostgreSQL schema" },
                    "target_table": { "type": "string", "description": "Target table name" },
                    "field_mapping": { "type": "object", "description": "Optional mapping from CSV columns to table columns, e.g. {\"Name\": \"full_name\"}" }
                }),
                vec!["csv_data", "target_schema", "target_table"],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "import_json".into(),
            title: None,
            description: Some(
                "Parse JSON array and insert rows into specified schema.table. Returns job ID and row counts."
                    .into(),
            ),
            input_schema: make_schema(
                serde_json::json!({
                    "json_data": { "type": "string", "description": "JSON array of objects to import" },
                    "target_schema": { "type": "string", "description": "Target PostgreSQL schema" },
                    "target_table": { "type": "string", "description": "Target table name" },
                    "field_mapping": { "type": "object", "description": "Optional mapping from JSON keys to table columns" }
                }),
                vec!["json_data", "target_schema", "target_table"],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "export_table".into(),
            title: None,
            description: Some(
                "Export rows from a table as JSON with optional WHERE filter and pagination (limit/offset)".into(),
            ),
            input_schema: make_schema(
                serde_json::json!({
                    "schema": { "type": "string", "description": "PostgreSQL schema name" },
                    "table": { "type": "string", "description": "Table name" },
                    "where_clause": { "type": "string", "description": "Optional WHERE clause (without WHERE keyword), e.g. \"status = 'active'\"" },
                    "limit": { "type": "integer", "description": "Max rows to export (default 1000, max 10000)" },
                    "offset": { "type": "integer", "description": "Number of rows to skip (default 0)" }
                }),
                vec!["schema", "table"],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "import_status".into(),
            title: None,
            description: Some(
                "Check status of an import job — rows processed, errors, current state".into(),
            ),
            input_schema: make_schema(
                serde_json::json!({
                    "job_id": { "type": "string", "description": "Import job ID (UUID)" }
                }),
                vec!["job_id"],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "list_imports".into(),
            title: None,
            description: Some(
                "Show import history with optional status filter and pagination (limit/offset)".into(),
            ),
            input_schema: make_schema(
                serde_json::json!({
                    "status": { "type": "string", "enum": ["pending", "running", "completed", "failed", "rolled_back"], "description": "Filter by status" },
                    "limit": { "type": "integer", "description": "Max results (default 50, max 10000)" },
                    "offset": { "type": "integer", "description": "Number of rows to skip (default 0)" }
                }),
                vec![],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "validate_data".into(),
            title: None,
            description: Some(
                "Check data against target table schema without importing. Reports column mismatches and type errors."
                    .into(),
            ),
            input_schema: make_schema(
                serde_json::json!({
                    "data": { "type": "string", "description": "CSV or JSON data to validate" },
                    "format": { "type": "string", "enum": ["csv", "json"], "description": "Data format" },
                    "target_schema": { "type": "string", "description": "Target PostgreSQL schema" },
                    "target_table": { "type": "string", "description": "Target table name" },
                    "field_mapping": { "type": "object", "description": "Optional field mapping" }
                }),
                vec!["data", "format", "target_schema", "target_table"],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "map_fields".into(),
            title: None,
            description: Some(
                "Define or update field mapping from source columns to target columns for an import job"
                    .into(),
            ),
            input_schema: make_schema(
                serde_json::json!({
                    "job_id": { "type": "string", "description": "Import job ID (UUID) to update mapping for" },
                    "mapping": { "type": "object", "description": "Mapping from source column names to target column names, e.g. {\"Name\": \"full_name\", \"Email Address\": \"email\"}" }
                }),
                vec!["job_id", "mapping"],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
        Tool {
            name: "rollback_import".into(),
            title: None,
            description: Some(
                "Delete all rows from a specific import batch. Uses the batch_id to identify rows."
                    .into(),
            ),
            input_schema: make_schema(
                serde_json::json!({
                    "job_id": { "type": "string", "description": "Import job ID (UUID) to rollback" }
                }),
                vec!["job_id"],
            ),
            output_schema: None,
            annotations: None,
            execution: None,
            icons: None,
            meta: None,
        },
    ]
}

// ============================================================================
// MCP Server
// ============================================================================

#[derive(Clone)]
pub struct ImportMcpServer {
    db: Database,
}

impl ImportMcpServer {
    pub fn new(db: Database) -> Self {
        Self { db }
    }

    /// Sanitize an identifier (schema or table name) to prevent SQL injection.
    /// Only allows alphanumeric characters and underscores.
    fn sanitize_identifier(name: &str) -> Option<String> {
        let trimmed = name.trim();
        if trimmed.is_empty() {
            return None;
        }
        if trimmed.chars().all(|c| c.is_alphanumeric() || c == '_') {
            Some(trimmed.to_string())
        } else {
            None
        }
    }

    /// Validate that a string looks like a UUID (basic format check).
    fn validate_uuid(value: &str) -> bool {
        let trimmed = value.trim();
        // UUID v4 format: 8-4-4-4-12 hex chars
        if trimmed.len() != 36 {
            return false;
        }
        trimmed.chars().enumerate().all(|(i, c)| {
            if i == 8 || i == 13 || i == 18 || i == 23 {
                c == '-'
            } else {
                c.is_ascii_hexdigit()
            }
        })
    }

    /// Get target table columns from information_schema.
    async fn get_table_columns(&self, schema: &str, table: &str) -> Result<Vec<String>, String> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_schema = $1 AND table_name = $2 \
             ORDER BY ordinal_position",
        )
        .bind(schema)
        .bind(table)
        .fetch_all(self.db.pool())
        .await
        .map_err(|e| {
            error!(schema = schema, table = table, error = %e, "Failed to query table columns");
            format!("Failed to query table columns: {e}")
        })?;

        if rows.is_empty() {
            return Err(format!("Table {schema}.{table} not found or has no columns"));
        }

        Ok(rows.into_iter().map(|(name,)| name).collect())
    }

    /// Apply field mapping to a key: if mapping contains the key, return mapped value.
    fn apply_mapping(key: &str, mapping: &serde_json::Value) -> String {
        mapping
            .get(key)
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| key.to_string())
    }

    /// Parse CSV data into rows of (column_name -> value) maps.
    fn parse_csv(
        csv_data: &str,
        mapping: &serde_json::Value,
    ) -> Result<Vec<serde_json::Map<String, serde_json::Value>>, String> {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .from_reader(csv_data.as_bytes());

        let headers: Vec<String> = reader
            .headers()
            .map_err(|e| format!("Failed to read CSV headers: {e}"))?
            .iter()
            .map(|h| Self::apply_mapping(h.trim(), mapping))
            .collect();

        if headers.is_empty() {
            return Err("CSV has no header columns".to_string());
        }

        let mut rows = Vec::new();
        for result in reader.records() {
            let record = result.map_err(|e| format!("CSV parse error: {e}"))?;
            let mut row = serde_json::Map::new();
            for (i, value) in record.iter().enumerate() {
                if let Some(col) = headers.get(i) {
                    row.insert(col.clone(), serde_json::Value::String(value.trim().to_string()));
                }
            }
            rows.push(row);
        }

        Ok(rows)
    }

    /// Parse JSON array data into rows.
    fn parse_json(
        json_data: &str,
        mapping: &serde_json::Value,
    ) -> Result<Vec<serde_json::Map<String, serde_json::Value>>, String> {
        let arr: Vec<serde_json::Value> =
            serde_json::from_str(json_data).map_err(|e| format!("Invalid JSON: {e}"))?;

        if arr.is_empty() {
            return Err("JSON array is empty — nothing to import".to_string());
        }

        let mut rows = Vec::new();
        for (idx, item) in arr.iter().enumerate() {
            let obj = item
                .as_object()
                .ok_or_else(|| format!("JSON array item at index {idx} is not an object"))?;

            let mut row = serde_json::Map::new();
            for (key, value) in obj {
                let mapped_key = Self::apply_mapping(key.trim(), mapping);
                // Trim string values
                let trimmed_value = match value {
                    serde_json::Value::String(s) => serde_json::Value::String(s.trim().to_string()),
                    other => other.clone(),
                };
                row.insert(mapped_key, trimmed_value);
            }
            rows.push(row);
        }

        Ok(rows)
    }

    /// Insert parsed rows into the target table.
    async fn insert_rows(
        &self,
        schema: &str,
        table: &str,
        rows: &[serde_json::Map<String, serde_json::Value>],
        batch_id: &str,
        job_id: &str,
    ) -> (i32, i32, Vec<ImportError>) {
        let table_columns = match self.get_table_columns(schema, table).await {
            Ok(cols) => cols,
            Err(e) => {
                error!(schema = schema, table = table, job_id = job_id, error = %e, "Cannot resolve table columns for insert");
                let err = ImportError {
                    id: uuid::Uuid::new_v4().to_string(),
                    job_id: job_id.to_string(),
                    row_num: 0,
                    error: e,
                    data: serde_json::Value::Null,
                    created_at: chrono::Utc::now(),
                };
                return (0, 1, vec![err]);
            }
        };

        let mut inserted = 0i32;
        let mut error_count = 0i32;
        let mut errors = Vec::new();

        for (idx, row) in rows.iter().enumerate() {
            // Filter to only columns that exist in the target table
            let mut cols: Vec<&str> = Vec::new();
            let mut vals: Vec<String> = Vec::new();
            let mut placeholders: Vec<String> = Vec::new();
            let mut param_idx = 1u32;

            // Add batch_id if the table has that column
            if table_columns.contains(&"_import_batch_id".to_string()) {
                cols.push("_import_batch_id");
                vals.push(batch_id.to_string());
                placeholders.push(format!("${param_idx}"));
                param_idx += 1;
            }

            for (key, value) in row {
                if table_columns.contains(key) {
                    cols.push(key);
                    // Convert JSON value to string for binding
                    let str_val = match value {
                        serde_json::Value::String(s) => s.clone(),
                        serde_json::Value::Null => continue, // skip nulls
                        other => other.to_string(),
                    };
                    vals.push(str_val);
                    placeholders.push(format!("${param_idx}"));
                    param_idx += 1;
                }
            }

            if cols.is_empty() {
                error_count += 1;
                let err_id = uuid::Uuid::new_v4().to_string();
                let err = ImportError {
                    id: err_id.clone(),
                    job_id: job_id.to_string(),
                    row_num: (idx + 1) as i32,
                    error: "No matching columns found for this row".to_string(),
                    data: serde_json::Value::Object(row.clone()),
                    created_at: chrono::Utc::now(),
                };
                if let Err(e) = sqlx::query(
                    "INSERT INTO imports.errors (id, job_id, row_num, error, data) VALUES ($1, $2, $3, $4, $5)",
                )
                .bind(&err_id)
                .bind(job_id)
                .bind((idx + 1) as i32)
                .bind(&err.error)
                .bind(&err.data)
                .execute(self.db.pool())
                .await
                {
                    error!(job_id = job_id, row = idx + 1, error = %e, "Failed to record import error to DB");
                }
                errors.push(err);
                continue;
            }

            let sql = format!(
                "INSERT INTO {schema}.{table} ({}) VALUES ({})",
                cols.join(", "),
                placeholders.join(", ")
            );

            let mut query = sqlx::query(&sql);
            for v in &vals {
                query = query.bind(v);
            }

            match query.execute(self.db.pool()).await {
                Ok(_) => inserted += 1,
                Err(e) => {
                    error_count += 1;
                    let err_id = uuid::Uuid::new_v4().to_string();
                    error!(job_id = job_id, row = idx + 1, error = %e, "Row insert failed");
                    let err = ImportError {
                        id: err_id.clone(),
                        job_id: job_id.to_string(),
                        row_num: (idx + 1) as i32,
                        error: format!("{e}"),
                        data: serde_json::Value::Object(row.clone()),
                        created_at: chrono::Utc::now(),
                    };
                    if let Err(db_err) = sqlx::query(
                        "INSERT INTO imports.errors (id, job_id, row_num, error, data) VALUES ($1, $2, $3, $4, $5)",
                    )
                    .bind(&err_id)
                    .bind(job_id)
                    .bind((idx + 1) as i32)
                    .bind(&err.error)
                    .bind(&err.data)
                    .execute(self.db.pool())
                    .await
                    {
                        error!(job_id = job_id, row = idx + 1, error = %db_err, "Failed to record import error to DB");
                    }
                    errors.push(err);
                }
            }
        }

        (inserted, error_count, errors)
    }

    // ---- Tool handlers ----

    async fn handle_import_csv(&self, args: &serde_json::Value) -> CallToolResult {
        let csv_data = match get_trimmed_str(args, "csv_data") {
            Some(d) => d,
            None => return error_result("Missing or empty required parameter: csv_data"),
        };
        let target_schema = match get_trimmed_str(args, "target_schema") {
            Some(s) => match Self::sanitize_identifier(&s) {
                Some(s) => s,
                None => return error_result("Invalid target_schema: only alphanumeric and underscores allowed"),
            },
            None => return error_result("Missing or empty required parameter: target_schema"),
        };
        let target_table = match get_trimmed_str(args, "target_table") {
            Some(t) => match Self::sanitize_identifier(&t) {
                Some(t) => t,
                None => return error_result("Invalid target_table: only alphanumeric and underscores allowed"),
            },
            None => return error_result("Missing or empty required parameter: target_table"),
        };
        let field_mapping = args
            .get("field_mapping")
            .cloned()
            .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));

        let rows = match Self::parse_csv(&csv_data, &field_mapping) {
            Ok(r) if r.is_empty() => return error_result("CSV data contains headers but no data rows"),
            Ok(r) => r,
            Err(e) => return error_result(&format!("CSV parse error: {e}")),
        };

        let job_id = uuid::Uuid::new_v4().to_string();
        let batch_id = uuid::Uuid::new_v4().to_string();

        // Create job record
        if let Err(e) = sqlx::query(
            "INSERT INTO imports.jobs (id, source_type, target_schema, target_table, field_mapping, status, batch_id) \
             VALUES ($1, 'csv', $2, $3, $4, 'running', $5)",
        )
        .bind(&job_id)
        .bind(&target_schema)
        .bind(&target_table)
        .bind(&field_mapping)
        .bind(&batch_id)
        .execute(self.db.pool())
        .await
        {
            error!(job_id = %job_id, error = %e, "Failed to create import job record");
            return error_result(&format!("Failed to create import job: {e}"));
        }

        let (inserted, err_count, errors) = self
            .insert_rows(&target_schema, &target_table, &rows, &batch_id, &job_id)
            .await;

        let status = if err_count > 0 && inserted == 0 {
            "failed"
        } else {
            "completed"
        };

        // Update job record
        if let Err(e) = sqlx::query(
            "UPDATE imports.jobs SET row_count = $1, error_count = $2, status = $3 WHERE id = $4",
        )
        .bind(inserted)
        .bind(err_count)
        .bind(status)
        .bind(&job_id)
        .execute(self.db.pool())
        .await
        {
            error!(job_id = %job_id, error = %e, "Failed to update import job status");
        }

        info!(job_id = %job_id, rows = inserted, errors = err_count, "CSV import complete");

        json_result(&ImportResult {
            job_id,
            batch_id,
            rows_inserted: inserted,
            error_count: err_count,
            status: status.to_string(),
            errors,
        })
    }

    async fn handle_import_json(&self, args: &serde_json::Value) -> CallToolResult {
        let json_data = match get_trimmed_str(args, "json_data") {
            Some(d) => d,
            None => return error_result("Missing or empty required parameter: json_data"),
        };
        let target_schema = match get_trimmed_str(args, "target_schema") {
            Some(s) => match Self::sanitize_identifier(&s) {
                Some(s) => s,
                None => return error_result("Invalid target_schema: only alphanumeric and underscores allowed"),
            },
            None => return error_result("Missing or empty required parameter: target_schema"),
        };
        let target_table = match get_trimmed_str(args, "target_table") {
            Some(t) => match Self::sanitize_identifier(&t) {
                Some(t) => t,
                None => return error_result("Invalid target_table: only alphanumeric and underscores allowed"),
            },
            None => return error_result("Missing or empty required parameter: target_table"),
        };
        let field_mapping = args
            .get("field_mapping")
            .cloned()
            .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));

        let rows = match Self::parse_json(&json_data, &field_mapping) {
            Ok(r) => r,
            Err(e) => return error_result(&format!("JSON parse error: {e}")),
        };

        let job_id = uuid::Uuid::new_v4().to_string();
        let batch_id = uuid::Uuid::new_v4().to_string();

        if let Err(e) = sqlx::query(
            "INSERT INTO imports.jobs (id, source_type, target_schema, target_table, field_mapping, status, batch_id) \
             VALUES ($1, 'json', $2, $3, $4, 'running', $5)",
        )
        .bind(&job_id)
        .bind(&target_schema)
        .bind(&target_table)
        .bind(&field_mapping)
        .bind(&batch_id)
        .execute(self.db.pool())
        .await
        {
            error!(job_id = %job_id, error = %e, "Failed to create import job record");
            return error_result(&format!("Failed to create import job: {e}"));
        }

        let (inserted, err_count, errors) = self
            .insert_rows(&target_schema, &target_table, &rows, &batch_id, &job_id)
            .await;

        let status = if err_count > 0 && inserted == 0 {
            "failed"
        } else {
            "completed"
        };

        if let Err(e) = sqlx::query(
            "UPDATE imports.jobs SET row_count = $1, error_count = $2, status = $3 WHERE id = $4",
        )
        .bind(inserted)
        .bind(err_count)
        .bind(status)
        .bind(&job_id)
        .execute(self.db.pool())
        .await
        {
            error!(job_id = %job_id, error = %e, "Failed to update import job status");
        }

        info!(job_id = %job_id, rows = inserted, errors = err_count, "JSON import complete");

        json_result(&ImportResult {
            job_id,
            batch_id,
            rows_inserted: inserted,
            error_count: err_count,
            status: status.to_string(),
            errors,
        })
    }

    async fn handle_export_table(&self, args: &serde_json::Value) -> CallToolResult {
        let schema = match get_trimmed_str(args, "schema") {
            Some(s) => match Self::sanitize_identifier(&s) {
                Some(s) => s,
                None => return error_result("Invalid schema name: only alphanumeric and underscores allowed"),
            },
            None => return error_result("Missing or empty required parameter: schema"),
        };
        let table = match get_trimmed_str(args, "table") {
            Some(t) => match Self::sanitize_identifier(&t) {
                Some(t) => t,
                None => return error_result("Invalid table name: only alphanumeric and underscores allowed"),
            },
            None => return error_result("Missing or empty required parameter: table"),
        };
        let where_clause = get_trimmed_str(args, "where_clause");
        let limit = clamp_limit(
            get_i64(args, "limit").unwrap_or(DEFAULT_EXPORT_LIMIT),
            DEFAULT_EXPORT_LIMIT,
        );
        let offset = clamp_offset(get_i64(args, "offset").unwrap_or(DEFAULT_OFFSET));

        let sql = if let Some(ref wc) = where_clause {
            format!(
                "SELECT row_to_json(t) FROM {schema}.{table} t WHERE {wc} LIMIT {limit} OFFSET {offset}"
            )
        } else {
            format!(
                "SELECT row_to_json(t) FROM {schema}.{table} t LIMIT {limit} OFFSET {offset}"
            )
        };

        let rows: Vec<(serde_json::Value,)> = match sqlx::query_as(&sql)
            .fetch_all(self.db.pool())
            .await
        {
            Ok(r) => r,
            Err(e) => {
                error!(schema = %schema, table = %table, error = %e, "Export query failed");
                return error_result(&format!("Export failed: {e}"));
            }
        };

        let data: Vec<serde_json::Value> = rows.into_iter().map(|(v,)| v).collect();
        let count = data.len();

        info!(schema = %schema, table = %table, rows = count, "Table export complete");

        json_result(&ExportResult {
            table: format!("{schema}.{table}"),
            row_count: count,
            data,
            limit,
            offset,
        })
    }

    async fn handle_import_status(&self, job_id: &str) -> CallToolResult {
        let job_id = job_id.trim();
        if job_id.is_empty() {
            return error_result("Missing or empty required parameter: job_id");
        }
        if !Self::validate_uuid(job_id) {
            return error_result(&format!("Invalid job_id format: expected UUID, got '{job_id}'"));
        }

        let job: Option<ImportJob> = match sqlx::query_as(
            "SELECT * FROM imports.jobs WHERE id = $1",
        )
        .bind(job_id)
        .fetch_optional(self.db.pool())
        .await
        {
            Ok(j) => j,
            Err(e) => {
                error!(job_id = %job_id, error = %e, "Failed to query import job");
                return error_result(&format!("Database error: {e}"));
            }
        };

        let job = match job {
            Some(j) => j,
            None => return error_result(&format!("Import job '{job_id}' not found")),
        };

        let errors: Vec<ImportError> = match sqlx::query_as(
            "SELECT * FROM imports.errors WHERE job_id = $1 ORDER BY row_num",
        )
        .bind(job_id)
        .fetch_all(self.db.pool())
        .await
        {
            Ok(e) => e,
            Err(e) => {
                warn!(job_id = %job_id, error = %e, "Failed to fetch import errors, returning job without errors");
                Vec::new()
            }
        };

        json_result(&serde_json::json!({
            "job": job,
            "errors": errors
        }))
    }

    async fn handle_list_imports(&self, args: &serde_json::Value) -> CallToolResult {
        let status = get_trimmed_str(args, "status");
        let limit = clamp_limit(
            get_i64(args, "limit").unwrap_or(DEFAULT_LIST_LIMIT),
            DEFAULT_LIST_LIMIT,
        );
        let offset = clamp_offset(get_i64(args, "offset").unwrap_or(DEFAULT_OFFSET));

        // Validate status value if provided
        if let Some(ref s) = status {
            if !VALID_STATUSES.contains(&s.as_str()) {
                return error_result(&format!(
                    "Invalid status '{}'. Must be one of: {}",
                    s,
                    VALID_STATUSES.join(", ")
                ));
            }
        }

        let jobs: Vec<ImportJob> = if let Some(ref s) = status {
            match sqlx::query_as(
                "SELECT * FROM imports.jobs WHERE status = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3",
            )
            .bind(s)
            .bind(limit)
            .bind(offset)
            .fetch_all(self.db.pool())
            .await
            {
                Ok(j) => j,
                Err(e) => {
                    error!(status = %s, error = %e, "Failed to list import jobs");
                    return error_result(&format!("Database error: {e}"));
                }
            }
        } else {
            match sqlx::query_as(
                "SELECT * FROM imports.jobs ORDER BY created_at DESC LIMIT $1 OFFSET $2",
            )
            .bind(limit)
            .bind(offset)
            .fetch_all(self.db.pool())
            .await
            {
                Ok(j) => j,
                Err(e) => {
                    error!(error = %e, "Failed to list import jobs");
                    return error_result(&format!("Database error: {e}"));
                }
            }
        };

        let count = jobs.len();

        json_result(&ListImportsResult {
            jobs,
            count,
            limit,
            offset,
        })
    }

    async fn handle_validate_data(&self, args: &serde_json::Value) -> CallToolResult {
        let data = match get_trimmed_str(args, "data") {
            Some(d) => d,
            None => return error_result("Missing or empty required parameter: data"),
        };
        let format = match get_trimmed_str(args, "format") {
            Some(f) => f,
            None => return error_result("Missing or empty required parameter: format"),
        };
        if format != "csv" && format != "json" {
            return error_result("Invalid format: must be 'csv' or 'json'");
        }
        let target_schema = match get_trimmed_str(args, "target_schema") {
            Some(s) => match Self::sanitize_identifier(&s) {
                Some(s) => s,
                None => return error_result("Invalid target_schema: only alphanumeric and underscores allowed"),
            },
            None => return error_result("Missing or empty required parameter: target_schema"),
        };
        let target_table = match get_trimmed_str(args, "target_table") {
            Some(t) => match Self::sanitize_identifier(&t) {
                Some(t) => t,
                None => return error_result("Invalid target_table: only alphanumeric and underscores allowed"),
            },
            None => return error_result("Missing or empty required parameter: target_table"),
        };
        let field_mapping = args
            .get("field_mapping")
            .cloned()
            .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));

        // Get target table columns
        let table_columns = match self.get_table_columns(&target_schema, &target_table).await {
            Ok(cols) => cols,
            Err(e) => return error_result(&e),
        };

        // Parse data
        let rows = match format.as_str() {
            "csv" => match Self::parse_csv(&data, &field_mapping) {
                Ok(r) => r,
                Err(e) => return error_result(&format!("CSV parse error: {e}")),
            },
            "json" => match Self::parse_json(&data, &field_mapping) {
                Ok(r) => r,
                Err(e) => return error_result(&format!("JSON parse error: {e}")),
            },
            _ => return error_result("format must be 'csv' or 'json'"),
        };

        let total_rows = rows.len();
        let mut valid_rows = 0usize;
        let mut validation_errors = Vec::new();

        for (idx, row) in rows.iter().enumerate() {
            let mut row_valid = true;
            for key in row.keys() {
                if !table_columns.contains(key) {
                    row_valid = false;
                    validation_errors.push(ValidationError {
                        row: idx + 1,
                        message: format!("Column '{key}' does not exist in {target_schema}.{target_table}"),
                    });
                }
            }
            if row_valid {
                valid_rows += 1;
            }
        }

        let error_count = validation_errors.len();
        json_result(&ValidationResult {
            valid: error_count == 0,
            total_rows,
            valid_rows,
            error_count,
            errors: validation_errors,
        })
    }

    async fn handle_map_fields(&self, args: &serde_json::Value) -> CallToolResult {
        let job_id = match get_trimmed_str(args, "job_id") {
            Some(id) => id,
            None => return error_result("Missing or empty required parameter: job_id"),
        };
        if !Self::validate_uuid(&job_id) {
            return error_result(&format!("Invalid job_id format: expected UUID, got '{job_id}'"));
        }
        let mapping = match args.get("mapping") {
            Some(m) if m.is_object() => m.clone(),
            Some(_) => return error_result("Parameter 'mapping' must be a JSON object"),
            None => return error_result("Missing required parameter: mapping"),
        };
        if mapping.as_object().map_or(true, |m| m.is_empty()) {
            return error_result("Parameter 'mapping' must be a non-empty JSON object");
        }

        // Verify job exists
        let job: Option<ImportJob> = match sqlx::query_as(
            "SELECT * FROM imports.jobs WHERE id = $1",
        )
        .bind(&job_id)
        .fetch_optional(self.db.pool())
        .await
        {
            Ok(j) => j,
            Err(e) => {
                error!(job_id = %job_id, error = %e, "Failed to query import job for field mapping");
                return error_result(&format!("Database error: {e}"));
            }
        };

        match job {
            None => error_result(&format!("Import job '{job_id}' not found")),
            Some(_) => {
                match sqlx::query(
                    "UPDATE imports.jobs SET field_mapping = $1 WHERE id = $2",
                )
                .bind(&mapping)
                .bind(&job_id)
                .execute(self.db.pool())
                .await
                {
                    Ok(_) => {
                        info!(job_id = %job_id, "Updated field mapping");
                        json_result(&FieldMapping {
                            job_id,
                            mapping,
                            status: "updated".to_string(),
                        })
                    }
                    Err(e) => {
                        error!(job_id = %job_id, error = %e, "Failed to update field mapping");
                        error_result(&format!("Failed to update mapping: {e}"))
                    }
                }
            }
        }
    }

    async fn handle_rollback_import(&self, job_id: &str) -> CallToolResult {
        let job_id = job_id.trim();
        if job_id.is_empty() {
            return error_result("Missing or empty required parameter: job_id");
        }
        if !Self::validate_uuid(job_id) {
            return error_result(&format!("Invalid job_id format: expected UUID, got '{job_id}'"));
        }

        // Get job details
        let job: Option<ImportJob> = match sqlx::query_as(
            "SELECT * FROM imports.jobs WHERE id = $1",
        )
        .bind(job_id)
        .fetch_optional(self.db.pool())
        .await
        {
            Ok(j) => j,
            Err(e) => {
                error!(job_id = %job_id, error = %e, "Failed to query import job for rollback");
                return error_result(&format!("Database error: {e}"));
            }
        };

        let job = match job {
            Some(j) => j,
            None => return error_result(&format!("Import job '{job_id}' not found")),
        };

        if job.status == "rolled_back" {
            return error_result("This import has already been rolled back");
        }

        let schema = match Self::sanitize_identifier(&job.target_schema) {
            Some(s) => s,
            None => return error_result("Invalid schema in job record"),
        };
        let table = match Self::sanitize_identifier(&job.target_table) {
            Some(t) => t,
            None => return error_result("Invalid table in job record"),
        };

        // Check if table has _import_batch_id column
        let table_columns = match self.get_table_columns(&schema, &table).await {
            Ok(cols) => cols,
            Err(e) => return error_result(&e),
        };

        let deleted = if table_columns.contains(&"_import_batch_id".to_string()) {
            let sql = format!(
                "DELETE FROM {schema}.{table} WHERE _import_batch_id = $1"
            );
            match sqlx::query(&sql)
                .bind(&job.batch_id)
                .execute(self.db.pool())
                .await
            {
                Ok(r) => r.rows_affected() as i64,
                Err(e) => {
                    error!(job_id = %job_id, batch_id = %job.batch_id, error = %e, "Rollback DELETE failed");
                    return error_result(&format!("Rollback failed: {e}"));
                }
            }
        } else {
            return error_result(
                &format!(
                    "Table {schema}.{table} does not have _import_batch_id column. \
                     Add this column to enable rollback support."
                ),
            );
        };

        // Update job status
        if let Err(e) = sqlx::query(
            "UPDATE imports.jobs SET status = 'rolled_back' WHERE id = $1",
        )
        .bind(job_id)
        .execute(self.db.pool())
        .await
        {
            error!(job_id = %job_id, error = %e, "Failed to update job status after rollback");
        }

        info!(job_id = %job_id, deleted = deleted, "Import rolled back");

        json_result(&serde_json::json!({
            "job_id": job_id,
            "batch_id": job.batch_id,
            "rows_deleted": deleted,
            "status": "rolled_back"
        }))
    }
}

// ============================================================================
// ServerHandler trait implementation
// ============================================================================

impl ServerHandler for ImportMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::V_2024_11_05,
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation::from_build_env(),
            instructions: Some(
                "DataXLR8 Import MCP — bulk data import/export across the dataxlr8 ecosystem. \
                 Supports CSV and JSON import, table export, validation, field mapping, and rollback."
                    .into(),
            ),
        }
    }

    fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<ListToolsResult, rmcp::ErrorData>> + Send + '_
    {
        async {
            Ok(ListToolsResult {
                tools: build_tools(),
                next_cursor: None,
                meta: None,
            })
        }
    }

    fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> impl std::future::Future<Output = Result<CallToolResult, rmcp::ErrorData>> + Send + '_
    {
        async move {
            let args =
                serde_json::to_value(&request.arguments).unwrap_or(serde_json::Value::Null);
            let name_str: &str = request.name.as_ref();

            let result = match name_str {
                "import_csv" => self.handle_import_csv(&args).await,
                "import_json" => self.handle_import_json(&args).await,
                "export_table" => self.handle_export_table(&args).await,
                "import_status" => match get_trimmed_str(&args, "job_id") {
                    Some(id) => self.handle_import_status(&id).await,
                    None => error_result("Missing or empty required parameter: job_id"),
                },
                "list_imports" => self.handle_list_imports(&args).await,
                "validate_data" => self.handle_validate_data(&args).await,
                "map_fields" => self.handle_map_fields(&args).await,
                "rollback_import" => match get_trimmed_str(&args, "job_id") {
                    Some(id) => self.handle_rollback_import(&id).await,
                    None => error_result("Missing or empty required parameter: job_id"),
                },
                _ => error_result(&format!("Unknown tool: {}", request.name)),
            };

            Ok(result)
        }
    }
}

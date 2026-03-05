use anyhow::Result;
use sqlx::PgPool;

pub async fn setup_schema(pool: &PgPool) -> Result<()> {
    sqlx::raw_sql(
        r#"
        CREATE SCHEMA IF NOT EXISTS imports;

        CREATE TABLE IF NOT EXISTS imports.jobs (
            id              TEXT PRIMARY KEY,
            source_type     TEXT NOT NULL DEFAULT 'csv'
                            CHECK (source_type IN ('csv', 'json')),
            target_schema   TEXT NOT NULL,
            target_table    TEXT NOT NULL,
            row_count       INT NOT NULL DEFAULT 0,
            error_count     INT NOT NULL DEFAULT 0,
            field_mapping   JSONB NOT NULL DEFAULT '{}',
            status          TEXT NOT NULL DEFAULT 'pending'
                            CHECK (status IN ('pending', 'running', 'completed', 'failed', 'rolled_back')),
            batch_id        TEXT NOT NULL,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE TABLE IF NOT EXISTS imports.errors (
            id              TEXT PRIMARY KEY,
            job_id          TEXT NOT NULL REFERENCES imports.jobs(id) ON DELETE CASCADE,
            row_num         INT NOT NULL,
            error           TEXT NOT NULL,
            data            JSONB NOT NULL DEFAULT '{}',
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        );

        CREATE INDEX IF NOT EXISTS idx_jobs_status ON imports.jobs(status);
        CREATE INDEX IF NOT EXISTS idx_jobs_batch_id ON imports.jobs(batch_id);
        CREATE INDEX IF NOT EXISTS idx_jobs_target ON imports.jobs(target_schema, target_table);
        CREATE INDEX IF NOT EXISTS idx_errors_job_id ON imports.errors(job_id);
        "#,
    )
    .execute(pool)
    .await?;

    Ok(())
}

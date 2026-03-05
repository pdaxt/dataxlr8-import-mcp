use anyhow::Result;
use rmcp::transport::io::stdio;
use rmcp::ServiceExt;
use tracing::info;

mod db;
mod tools;

use tools::ImportMcpServer;

#[tokio::main]
async fn main() -> Result<()> {
    let config = dataxlr8_mcp_core::Config::from_env("dataxlr8-import-mcp")
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    dataxlr8_mcp_core::logging::init(&config.log_level);

    info!(
        server = config.server_name,
        "Starting DataXLR8 Import MCP server"
    );

    let database = dataxlr8_mcp_core::Database::connect(&config.database_url)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    db::setup_schema(database.pool()).await?;

    let server = ImportMcpServer::new(database.clone());

    let transport = stdio();
    let service = server.serve(transport).await?;

    info!("Import MCP server connected via stdio");
    service.waiting().await?;

    database.close().await;
    info!("Import MCP server shut down");

    Ok(())
}

# MCP Atlas

An MCP discovery server that helps AI agents find the right MCP tools. Backed by a DGraph database of 2,100+ categorized, quality-scored MCP servers with extracted tool schemas.

## Tools

| Tool | Description |
|------|-------------|
| `search_servers` | Find servers by keyword, category, or language |
| `get_server_details` | Full details for a specific server (tools, packages, listings) |
| `browse_categories` | List all 15 categories with server counts |
| `recommend_servers` | Contextual recommendations for a described task |

## Quick start

```bash
# 1. Start DGraph
docker compose up -d

# 2. Load data
python3 data/restore.py

# 3. Run the server
uv run server.py
```

## Add to Claude Code

```json
{
  "mcpServers": {
    "mcp-atlas": {
      "command": "uv",
      "args": ["--directory", "/path/to/mcp-atlas", "run", "server.py"]
    }
  }
}
```

## Data pipeline

Crawlers populate the DGraph catalog from multiple registries:

| Crawler | Source | Auth |
|---------|--------|------|
| `official_registry.py` | registry.modelcontextprotocol.io | None |
| `smithery_crawler.py` | registry.smithery.ai | API key |
| `glama_crawler.py` | glama.ai | None |
| `github_enricher.py` | GitHub REST API | Token (optional) |
| `npm_enricher.py` | npmjs.org | None |
| `pypi_enricher.py` | pypi.org + pypistats.org | None |
| `quality_scorer.py` | DGraph (computed) | — |
| `category_classifier.py` | AI classifier (via CLI) | — |
| `tool_extractor.py` | MCP servers (stdio) | — |

Run order: registry crawlers → enrichers → quality scorer → classifier → tool extractor.

## Configuration

Copy `.env.template` to `.env` and fill in values:

```bash
DGRAPH_URL=http://localhost:18080
DGRAPH_GROOT_PASSWORD=          # blank if ACL disabled (local Docker)
SMITHERY_API_KEY=               # from smithery.ai/account/api-keys
GITHUB_TOKEN=                   # optional, for higher rate limits
```

## Schema

DGraph GraphQL schema defines: `Server`, `Tool`, `Category`, `Stack`, `Listing`. See `schema.graphql`.

## Quality score

```
qualityScore = 0.25 * norm(stars) + 0.25 * norm(downloads)
             + 0.20 * recency(lastCommit) + 0.15 * hasDescription
             + 0.15 * toolCompleteness
```

## Current data

- 2,136 servers from 3 registries
- 15 categories (100% classified)
- 109 servers with extracted tool schemas (1,554 tools)
- GitHub metadata for 1,414 servers
- npm downloads for 798 packages
- PyPI downloads for 368 packages

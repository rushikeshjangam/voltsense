#!/usr/bin/env bash
# Run the Phase 2 Step 2a knowledge-base ingest (chunk + embed + upsert to Qdrant).
set -euo pipefail
cd "$(dirname "$0")/../.."
exec rag/.venv/bin/python -m rag.ingest.ingest

#!/usr/bin/env bash
# Run the Step 2b query constructor + retriever (plain Python, not Flink).
set -euo pipefail
cd "$(dirname "$0")/../.."
exec rag/.venv/bin/python -m rag.retrieve.retriever

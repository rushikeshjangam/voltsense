#!/usr/bin/env bash
# Run the Step 2c LLM diagnosis generator. Requires GROQ_API_KEY in the environment.
set -euo pipefail
cd "$(dirname "$0")/../.."
: "${GROQ_API_KEY:?GROQ_API_KEY is not set. export GROQ_API_KEY=\"your-key\" first.}"
exec rag/.venv/bin/python -m rag.serve.diagnose

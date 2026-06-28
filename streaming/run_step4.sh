#!/usr/bin/env bash
# Run the Step 4 anomaly detector (plain Python, not Flink — no JAVA_HOME needed).
set -euo pipefail
cd "$(dirname "$0")/.."
exec producer/.venv/bin/python -m streaming.step4_anomaly_detector

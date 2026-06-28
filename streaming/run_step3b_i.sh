#!/usr/bin/env bash
# Run the Step 3b-i feature job on the local PyFlink mini-cluster.
set -euo pipefail
cd "$(dirname "$0")/.."

export JAVA_HOME="${JAVA_HOME:-$HOME/.local/jdks/jdk-11.0.31+11}"
export PATH="$JAVA_HOME/bin:$PATH"

echo "JAVA_HOME=$JAVA_HOME"
exec streaming/.venv/bin/python -m streaming.step3b_i_features

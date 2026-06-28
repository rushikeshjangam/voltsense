#!/usr/bin/env bash
# Run the Step 3a plumbing job on the local PyFlink mini-cluster.
# Uses the portable JDK 11 and the isolated streaming/.venv.
set -euo pipefail
cd "$(dirname "$0")/.."

export JAVA_HOME="${JAVA_HOME:-$HOME/.local/jdks/jdk-11.0.31+11}"
export PATH="$JAVA_HOME/bin:$PATH"

echo "JAVA_HOME=$JAVA_HOME"
java -version
exec streaming/.venv/bin/python -m streaming.step3a_consume

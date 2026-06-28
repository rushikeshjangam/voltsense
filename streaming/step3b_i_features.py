"""VoltSense Step 3b-i — TIME-DOMAIN feature extraction + features-topic sink.

Pipeline:  raw-telemetry (avro-confluent)  ->  Python UDF (numpy)  ->  features (avro-confluent)

NO FFT / frequency-domain features here — that is Step 3b-ii. The features schema is
designed so those can be added later as NULLABLE columns (BACKWARD-compatible).

Run with run_step3b_i.sh (sets JAVA_HOME + connector jars).
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
from pyflink.common import Row
from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment
from pyflink.table.udf import udf

JARS_DIR = Path(__file__).resolve().parent / "jars"
JARS = [
    JARS_DIR / "flink-sql-connector-kafka-3.3.0-1.20.jar",
    JARS_DIR / "flink-sql-avro-confluent-registry-1.20.1.jar",
]

BOOTSTRAP = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
SRC_TOPIC = "raw-telemetry"
SINK_TOPIC = "features"


# --- the time-domain feature UDF -------------------------------------------------
# Input: ARRAY<FLOAT> (one 1024-sample window) -> Python list[float].
# Output: ROW of the four scalar features. Invoked once per row in a Python worker.
@udf(
    input_types=[DataTypes.ARRAY(DataTypes.FLOAT())],
    result_type=DataTypes.ROW([
        DataTypes.FIELD("rms", DataTypes.DOUBLE()),
        DataTypes.FIELD("peak", DataTypes.DOUBLE()),
        DataTypes.FIELD("crest_factor", DataTypes.DOUBLE()),
        DataTypes.FIELD("kurtosis", DataTypes.DOUBLE()),
    ]),
)
def time_features(samples):
    x = np.asarray(samples, dtype=np.float64)
    rms = float(np.sqrt(np.mean(x * x)))
    peak = float(np.max(np.abs(x)))
    crest = float(peak / rms) if rms > 0.0 else 0.0
    mu = float(x.mean())
    var = float(np.mean((x - mu) ** 2))
    # excess kurtosis: 0 for Gaussian, large+ for impulsive/spiky signals
    kurt = float(np.mean((x - mu) ** 4) / (var ** 2) - 3.0) if var > 0.0 else 0.0
    return Row(rms, peak, crest, kurt)


def main() -> None:
    settings = EnvironmentSettings.in_batch_mode()
    t_env = TableEnvironment.create(settings)
    t_env.get_config().set("pipeline.jars", ";".join(f"file://{p}" for p in JARS))

    # --- SOURCE: raw-telemetry, decoded via registry (schema id 2) ---
    t_env.execute_sql(
        f"""
        CREATE TABLE raw_telemetry (
            asset_id       STRING NOT NULL,
            timestamp_ms   BIGINT NOT NULL,
            channel        STRING NOT NULL,
            sample_rate_hz INT NOT NULL,
            window_index   BIGINT NOT NULL,
            samples        ARRAY<FLOAT NOT NULL> NOT NULL,
            fault          ROW<type STRING, fault_diameter_in DOUBLE, load_hp INT> NOT NULL
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{SRC_TOPIC}',
            'properties.bootstrap.servers' = '{BOOTSTRAP}',
            'properties.group.id' = 'flink-step3b-i',
            'scan.startup.mode' = 'earliest-offset',
            'scan.bounded.mode' = 'latest-offset',
            'value.format' = 'avro-confluent',
            'value.avro-confluent.url' = '{SCHEMA_REGISTRY_URL}',
            'value.fields-include' = 'EXCEPT_KEY'
        )
        """
    )

    # --- SINK: features topic. The avro-confluent sink DERIVES an Avro schema from
    # this DDL and REGISTERS it under 'features-value' on first write (auto-register).
    # The Kafka key is asset_id (partition-by-asset, like the producer). Column order
    # here defines the INSERT target order. ---
    t_env.execute_sql(
        f"""
        CREATE TABLE features (
            asset_id       STRING NOT NULL,
            timestamp_ms   BIGINT NOT NULL,
            window_index   BIGINT NOT NULL,
            channel        STRING NOT NULL,
            sample_rate_hz INT NOT NULL,
            fault          ROW<type STRING, fault_diameter_in DOUBLE, load_hp INT> NOT NULL,
            rms            DOUBLE NOT NULL,
            peak           DOUBLE NOT NULL,
            crest_factor   DOUBLE NOT NULL,
            kurtosis       DOUBLE NOT NULL
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{SINK_TOPIC}',
            'properties.bootstrap.servers' = '{BOOTSTRAP}',
            'key.format' = 'raw',
            'key.fields' = 'asset_id',
            'value.format' = 'avro-confluent',
            'value.avro-confluent.url' = '{SCHEMA_REGISTRY_URL}',
            'value.fields-include' = 'EXCEPT_KEY'
        )
        """
    )

    t_env.create_temporary_function("time_features", time_features)

    # Apply the UDF, then project identity/metadata + the four features into the sink.
    # 'fault' is passed through untouched (labels are metadata, never an input).
    result = t_env.execute_sql(
        """
        INSERT INTO features
        SELECT
            asset_id, timestamp_ms, window_index, channel, sample_rate_hz, fault,
            f.rms, f.peak, f.crest_factor, f.kurtosis
        FROM (
            SELECT asset_id, timestamp_ms, window_index, channel, sample_rate_hz, fault,
                   time_features(samples) AS f
            FROM raw_telemetry
        )
        """
    )
    # In batch mode, wait for the bounded job to finish.
    result.wait()
    print("INSERT complete — features written to topic 'features'.")


if __name__ == "__main__":
    main()

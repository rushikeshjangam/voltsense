"""VoltSense Step 3a — PROVE Flink can read + decode the raw-telemetry stream.

PLUMBING ONLY. No feature extraction (no FFT, no RMS) — that's Step 3b. This job:
  1. spins up a local PyFlink mini-cluster (in-process, started by env.execute),
  2. reads raw-telemetry from Kafka (earliest -> latest, i.e. a BOUNDED drain),
  3. decodes the Confluent-Avro value via the 'avro-confluent' format, which reads the
     writer schema (id 1) from the local Schema Registry — schema is NOT hardcoded,
  4. prints a few decoded records so we can eyeball motor-01 and motor-02.

Run it with run_step3a.sh (which sets JAVA_HOME + the connector jars).
"""

from __future__ import annotations

from pathlib import Path

from pyflink.table import EnvironmentSettings, TableEnvironment

# The two version-matched connector/format jars (Flink 1.20.1).
JARS_DIR = Path(__file__).resolve().parent / "jars"
JARS = [
    JARS_DIR / "flink-sql-connector-kafka-3.3.0-1.20.jar",
    JARS_DIR / "flink-sql-avro-confluent-registry-1.20.1.jar",
]

BOOTSTRAP = "localhost:9092"          # Kafka EXTERNAL listener (mini-cluster runs on host)
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "raw-telemetry"


def main() -> None:
    # Batch mode: we want a bounded read that drains existing messages and stops,
    # so the print job terminates instead of running forever on an unbounded source.
    settings = EnvironmentSettings.in_batch_mode()
    t_env = TableEnvironment.create(settings)

    # Make the connector + format jars available to the planner/runtime.
    jar_urls = ";".join(f"file://{p}" for p in JARS)
    t_env.get_config().set("pipeline.jars", jar_urls)

    # DDL declares the READER's view of the record (a projection of RawTelemetry).
    # The avro-confluent format strips the 5-byte Confluent header, reads schema id 1
    # from each message, and fetches the WRITER schema from the registry to decode.
    # The Avro enum 'channel' surfaces as STRING; the samples array as ARRAY<FLOAT>;
    # the nested 'fault' record as ROW<...>. We only declare what we need to print.
    t_env.execute_sql(
        f"""
        CREATE TABLE raw_telemetry (
            -- NOT NULL so Flink's reader schema uses concrete Avro types (not
            -- union[null,T]); the writer schema (id 1) has these fields non-null.
            -- 'channel' is an Avro enum in the writer schema; declaring it as a plain
            -- (non-null) STRING asks Avro for enum->string promotion on read.
            asset_id       STRING NOT NULL,
            timestamp_ms   BIGINT NOT NULL,
            channel        STRING NOT NULL,
            sample_rate_hz INT NOT NULL,
            window_index   BIGINT NOT NULL,
            samples        ARRAY<FLOAT NOT NULL> NOT NULL,
            fault          ROW<type STRING NOT NULL, fault_diameter_in DOUBLE, load_hp INT NOT NULL> NOT NULL
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{TOPIC}',
            'properties.bootstrap.servers' = '{BOOTSTRAP}',
            'properties.group.id' = 'flink-step3a',
            'scan.startup.mode' = 'earliest-offset',
            'scan.bounded.mode' = 'latest-offset',
            'value.format' = 'avro-confluent',
            'value.avro-confluent.url' = '{SCHEMA_REGISTRY_URL}',
            'value.fields-include' = 'EXCEPT_KEY'
        )
        """
    )

    # Print 5 decoded records: asset_id, window_index, channel, sample_rate_hz, and the
    # first 3 samples. ORDER BY makes the output deterministic so the sanity check
    # (motor-01 window 0 -> -0.083, -0.19573, 0.23342) is reproducible.
    result = t_env.execute_sql(
        """
        SELECT
            asset_id,
            window_index,
            channel,
            sample_rate_hz,
            samples[1] AS s0,
            samples[2] AS s1,
            samples[3] AS s2
        FROM raw_telemetry
        ORDER BY window_index, asset_id   -- interleave so BOTH assets are visible
        LIMIT 6
        """
    )
    result.print()


if __name__ == "__main__":
    main()

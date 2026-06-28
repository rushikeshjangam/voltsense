"""VoltSense Step 3b-ii — FFT / frequency-domain bearing-fault features.

Pipeline:  raw-telemetry (avro-confluent)  ->  Python UDFs (numpy)  ->  features (avro-confluent)

Adds, on top of the Step 3b-i time-domain features (rms/peak/crest_factor/kurtosis):
  bpfi_band_energy, bpfo_band_energy, bsf_band_energy, ftf_band_energy, shaft_1x_energy

Bearing fault frequencies are DERIVED per-row from fault.load_hp (shaft RPM lookup),
not hardcoded — different load -> different shaft speed -> different fault frequencies.

Run with run_step3b_ii.sh (sets JAVA_HOME + connector jars).
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

# CWRU drive-end bearing (6205-2RS JEM SKF) fault-frequency multipliers of shaft_hz.
# Each ratio comes from the bearing's geometry (ball count, ball/pitch diameter,
# contact angle) — fixed for this bearing model, independent of load/speed.
BPFI_RATIO = 5.4152  # ball pass frequency, inner race
BPFO_RATIO = 3.5848  # ball pass frequency, outer race
BSF_RATIO = 2.3574   # ball spin frequency
FTF_RATIO = 0.3983   # fundamental train (cage) frequency

# Band half-width in Hz. At 12 kHz / 1024 samples, FFT bin spacing = 12000/1024 ≈
# 11.7 Hz, so a +-10 Hz band is roughly "one bin wide" either side of the target —
# tight enough to stay fault-specific, wide enough to tolerate small RPM/slip error
# in the nominal shaft speed (CWRU's published RPM is itself a rounded nominal value).
BAND_HALF_WIDTH_HZ = 10.0


# --- time-domain UDF (unchanged from Step 3b-i) -----------------------------------
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
    kurt = float(np.mean((x - mu) ** 4) / (var ** 2) - 3.0) if var > 0.0 else 0.0
    return Row(rms, peak, crest, kurt)


# --- frequency-domain UDF -----------------------------------------------------------
# Inputs: the window's samples + sample rate, plus the 5 target frequencies (already
# derived in SQL from fault.load_hp, since the SQL layer has the lookup table and this
# UDF should stay a pure "samples + targets -> energies" function, not own the RPM map.
@udf(
    input_types=[
        DataTypes.ARRAY(DataTypes.FLOAT()),
        DataTypes.INT(),
        DataTypes.DOUBLE(),
        DataTypes.DOUBLE(),
        DataTypes.DOUBLE(),
        DataTypes.DOUBLE(),
        DataTypes.DOUBLE(),
    ],
    result_type=DataTypes.ROW([
        DataTypes.FIELD("bpfi_band_energy", DataTypes.DOUBLE()),
        DataTypes.FIELD("bpfo_band_energy", DataTypes.DOUBLE()),
        DataTypes.FIELD("bsf_band_energy", DataTypes.DOUBLE()),
        DataTypes.FIELD("ftf_band_energy", DataTypes.DOUBLE()),
        DataTypes.FIELD("shaft_1x_energy", DataTypes.DOUBLE()),
    ]),
)
def fft_features(samples, sample_rate_hz, bpfi_hz, bpfo_hz, bsf_hz, ftf_hz, shaft_hz):
    x = np.asarray(samples, dtype=np.float64)
    n = len(x)

    # rfft: the input is REAL-valued vibration, so the full complex FFT is conjugate-
    # symmetric (negative frequencies carry no extra information). rfft computes only
    # the non-negative half (n//2 + 1 bins), which is exactly the physically meaningful
    # one-sided spectrum and avoids double-counting energy across +/-f.
    spectrum = np.fft.rfft(x)
    mag_sq = np.abs(spectrum) ** 2

    # Bin index -> Hz: each bin k corresponds to k * (sample_rate / n) Hz.
    # rfftfreq builds exactly that mapping for the rfft output length.
    freqs = np.fft.rfftfreq(n, d=1.0 / sample_rate_hz)

    def band_energy(target_hz: float) -> float:
        mask = np.abs(freqs - target_hz) <= BAND_HALF_WIDTH_HZ
        return float(mag_sq[mask].sum())

    return Row(
        band_energy(bpfi_hz),
        band_energy(bpfo_hz),
        band_energy(bsf_hz),
        band_energy(ftf_hz),
        band_energy(shaft_hz),
    )


def main() -> None:
    settings = EnvironmentSettings.in_batch_mode()
    t_env = TableEnvironment.create(settings)
    t_env.get_config().set("pipeline.jars", ";".join(f"file://{p}" for p in JARS))

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
            'properties.group.id' = 'flink-step3b-ii',
            'scan.startup.mode' = 'earliest-offset',
            'scan.bounded.mode' = 'latest-offset',
            'value.format' = 'avro-confluent',
            'value.avro-confluent.url' = '{SCHEMA_REGISTRY_URL}',
            'value.fields-include' = 'EXCEPT_KEY'
        )
        """
    )

    # Sink DDL now carries the 5 new nullable columns; the avro-confluent sink derives
    # an Avro schema from this DDL and registers it as a NEW VERSION under
    # 'features-value' (BACKWARD-compatible: the new fields are nullable unions with
    # default null, so an old 3b-i record read under this schema just yields null there).
    t_env.execute_sql(
        f"""
        CREATE TABLE features (
            asset_id          STRING NOT NULL,
            timestamp_ms      BIGINT NOT NULL,
            window_index      BIGINT NOT NULL,
            channel           STRING NOT NULL,
            sample_rate_hz    INT NOT NULL,
            fault             ROW<type STRING, fault_diameter_in DOUBLE, load_hp INT> NOT NULL,
            rms               DOUBLE NOT NULL,
            peak              DOUBLE NOT NULL,
            crest_factor      DOUBLE NOT NULL,
            kurtosis          DOUBLE NOT NULL,
            bpfi_band_energy  DOUBLE,
            bpfo_band_energy  DOUBLE,
            bsf_band_energy   DOUBLE,
            ftf_band_energy   DOUBLE,
            shaft_1x_energy   DOUBLE
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
    t_env.create_temporary_function("fft_features", fft_features)

    # CWRU drive-end shaft RPM by motor load (published nominal values).
    # shaft_hz = rpm / 60; fault frequencies = ratio * shaft_hz. Derived per-row from
    # fault.load_hp rather than hardcoded, so this generalises across all 4 load levels.
    result = t_env.execute_sql(
        f"""
        INSERT INTO features
        SELECT
            asset_id, timestamp_ms, window_index, channel, sample_rate_hz, fault,
            t.rms, t.peak, t.crest_factor, t.kurtosis,
            f.bpfi_band_energy, f.bpfo_band_energy, f.bsf_band_energy,
            f.ftf_band_energy, f.shaft_1x_energy
        FROM (
            SELECT
                asset_id, timestamp_ms, window_index, channel, sample_rate_hz, fault, samples,
                time_features(samples) AS t,
                fft_features(
                    samples, sample_rate_hz,
                    {BPFI_RATIO} * shaft_hz, {BPFO_RATIO} * shaft_hz,
                    {BSF_RATIO} * shaft_hz, {FTF_RATIO} * shaft_hz, shaft_hz
                ) AS f
            FROM (
                SELECT
                    asset_id, timestamp_ms, window_index, channel, sample_rate_hz, fault, samples,
                    (CASE fault.load_hp
                        WHEN 0 THEN 1797.0
                        WHEN 1 THEN 1772.0
                        WHEN 2 THEN 1750.0
                        WHEN 3 THEN 1730.0
                     END) / 60.0 AS shaft_hz
                FROM raw_telemetry
            )
        )
        """
    )
    result.wait()
    print("INSERT complete — enriched features (time + FFT) written to topic 'features'.")


if __name__ == "__main__":
    main()

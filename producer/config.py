"""VoltSense replay producer — configuration.

All values come from the environment so the same code runs on the host (EXTERNAL
listener -> localhost:9092) and inside docker-compose (INTERNAL -> kafka:29092).
Phase 1: extended with the .mat source, channel selection, windowing, and pacing.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

# The window length is an invariant of the whole pipeline (FFT-friendly power of two,
# ~85 ms at 12 kHz). The Avro schema cannot enforce array length, so the producer must.
WINDOW_SIZE = 1024


@dataclass(frozen=True)
class ProducerConfig:
    # --- Kafka / Schema Registry ---
    # Default targets the EXTERNAL listener for host-side runs.
    # Inside compose, set KAFKA_BOOTSTRAP_SERVERS=kafka:29092.
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    schema_registry_url: str = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    raw_topic: str = os.getenv("RAW_TOPIC", "raw-telemetry")

    # --- Source data ---
    # Path to ONE CWRU .mat file to replay (download separately; see notebooks/).
    mat_path: str = os.getenv("MAT_PATH", "data/cwru/ir007_0.mat")
    # Which accelerometer channel to publish: DE, FE, or BA.
    channel: str = os.getenv("CHANNEL", "DE")
    sample_rate_hz: int = int(os.getenv("SAMPLE_RATE_HZ", "12000"))

    # --- Asset identity (Kafka partition key) ---
    asset_id: str = os.getenv("ASSET_ID", "motor-01")

    # --- Ground-truth labels (CWRU metadata carried on each message) ---
    # These describe the file you point MAT_PATH at; the pipeline must not use them
    # to detect — they exist for validation/EDA only.
    fault_type: str = os.getenv("FAULT_TYPE", "INNER_RACE")  # NORMAL/INNER_RACE/OUTER_RACE/BALL
    fault_diameter_in: float | None = (
        float(os.environ["FAULT_DIAMETER_IN"]) if os.getenv("FAULT_DIAMETER_IN") else None
    )
    load_hp: int = int(os.getenv("LOAD_HP", "0"))

    # --- Windowing ---
    window_size: int = int(os.getenv("WINDOW_SIZE", str(WINDOW_SIZE)))

    # --- Replay pacing ---
    # "realtime": sleep so messages emit at the rate the signal was recorded (realistic).
    # "fast":     emit as fast as possible (handy for quick end-to-end tests).
    pacing: str = os.getenv("PACING", "realtime")

    # Optional cap on how many windows to send (0 = no limit / whole file).
    max_windows: int = int(os.getenv("MAX_WINDOWS", "0"))

    @classmethod
    def from_env(cls) -> "ProducerConfig":
        return cls()

    def window_seconds(self) -> float:
        """Wall-clock duration of one window of signal (used for realtime pacing)."""
        return self.window_size / self.sample_rate_hz

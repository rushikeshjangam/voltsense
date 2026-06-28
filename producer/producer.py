"""VoltSense replay producer — read ONE CWRU .mat file and publish 1024-sample windows
to the raw-telemetry topic as Avro, partitioned by asset_id.

Phase 1, Step 2. Pacing is configurable (realtime | fast). The replay loop runs only
when this module is executed; importing it is side-effect free.

Run (host, realtime):  python -m producer.producer
Run (fast smoke test): PACING=fast MAX_WINDOWS=5 python -m producer.producer
"""

from __future__ import annotations

import logging
import signal
import sys
import time
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from .config import ProducerConfig
from .mat_reader import iter_windows, load_channel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("voltsense.producer")

# Path to the registered schema (we reuse it verbatim so the registry returns id 1
# instead of creating a new version).
SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "raw_telemetry.avsc"

_running = True


def _handle_signal(signum, frame) -> None:  # noqa: ANN001
    global _running
    log.info("received signal %s, finishing current window then stopping", signum)
    _running = False


def _delivery_report(err, msg) -> None:  # noqa: ANN001
    if err is not None:
        log.error("delivery FAILED for key=%s: %s", msg.key(), err)
    else:
        log.debug(
            "delivered key=%s -> partition=%s offset=%s",
            msg.key(), msg.partition(), msg.offset(),
        )


# Allowed values for the string fields (the Avro schema no longer enforces these as
# enums — see schemas/raw_telemetry.avsc — so the producer is the gatekeeper).
VALID_CHANNELS = ("DE", "FE", "BA")
VALID_FAULT_TYPES = ("NORMAL", "INNER_RACE", "OUTER_RACE", "BALL")


def build_value(window, window_index: int, timestamp_ms: int, cfg: ProducerConfig) -> dict:
    """Map one numpy window + metadata into a dict matching the RawTelemetry schema."""
    if cfg.channel not in VALID_CHANNELS:
        raise ValueError(f"channel {cfg.channel!r} not in {VALID_CHANNELS}")
    if cfg.fault_type not in VALID_FAULT_TYPES:
        raise ValueError(f"fault_type {cfg.fault_type!r} not in {VALID_FAULT_TYPES}")
    return {
        "asset_id": cfg.asset_id,
        "timestamp_ms": timestamp_ms,
        "channel": cfg.channel,
        "sample_rate_hz": cfg.sample_rate_hz,
        "window_index": window_index,
        "samples": [float(x) for x in window],  # Avro 'float'; explicit cast for clarity
        "fault": {
            "type": cfg.fault_type,
            "fault_diameter_in": cfg.fault_diameter_in,
            "load_hp": cfg.load_hp,
        },
    }


def main() -> int:
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    cfg = ProducerConfig.from_env()
    log.info(
        "config: bootstrap=%s topic=%s file=%s channel=%s rate=%dHz pacing=%s asset=%s",
        cfg.bootstrap_servers, cfg.raw_topic, cfg.mat_path, cfg.channel,
        cfg.sample_rate_hz, cfg.pacing, cfg.asset_id,
    )

    # --- Schema Registry + Avro serializer (reuses registered schema id 1) ---
    schema_str = SCHEMA_PATH.read_text()
    sr_client = SchemaRegistryClient({"url": cfg.schema_registry_url})
    avro_serializer = AvroSerializer(sr_client, schema_str)  # subject -> raw-telemetry-value
    key_serializer = StringSerializer("utf_8")

    producer = Producer({"bootstrap.servers": cfg.bootstrap_servers})
    ctx = SerializationContext(cfg.raw_topic, MessageField.VALUE)

    # --- Load + window the chosen channel ---
    signal_data = load_channel(cfg.mat_path, cfg.channel)
    window_ms = cfg.window_seconds() * 1000.0
    log.info(
        "loaded %d samples -> %d full windows of %d (dropping %d-sample tail)",
        signal_data.size,
        signal_data.size // cfg.window_size,
        cfg.window_size,
        signal_data.size % cfg.window_size,
    )

    base_ts_ms = int(time.time() * 1000)
    sent = 0
    for idx, window in enumerate(iter_windows(signal_data, cfg.window_size)):
        if not _running:
            break
        if cfg.max_windows and sent >= cfg.max_windows:
            break

        # Our invariant — the schema can't enforce array length.
        assert window.size == cfg.window_size, f"window {idx} has {window.size} samples"

        # data-time timestamp (not wall-clock) so the timeline is continuous in any pacing
        timestamp_ms = base_ts_ms + int(idx * window_ms)
        value = build_value(window, idx, timestamp_ms, cfg)

        producer.produce(
            topic=cfg.raw_topic,
            key=key_serializer(cfg.asset_id, ctx),
            value=avro_serializer(value, ctx),
            on_delivery=_delivery_report,
        )
        producer.poll(0)  # serve delivery callbacks without blocking
        sent += 1

        if cfg.pacing == "realtime":
            time.sleep(cfg.window_seconds())  # ~85 ms at 12 kHz / 1024

    remaining = producer.flush(10)
    log.info("done: produced %d windows (%d undelivered at flush)", sent, remaining)
    return 0


if __name__ == "__main__":
    sys.exit(main())

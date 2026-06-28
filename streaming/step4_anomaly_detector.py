"""VoltSense Step 4 — anomaly detector consuming the 'features' topic.

Plain Python Kafka consumer/producer (NOT a Flink job) — anomaly scoring on a
handful of numeric features per window doesn't need Flink's distributed engine;
this matches the spec's framing of Step 4 as "console/log sink", deliberately simple.

Pipeline:  features (avro-confluent)  ->  two independent detectors  ->  anomalies (avro-confluent)

Two methods, per the project spec's "thresholds -> Isolation Forest" progression:
  1. THRESHOLD   — z-score vs a NORMAL-only baseline; simple, explainable.
  2. ISOLATION FOREST — sklearn, fit on the same NORMAL-only baseline.

Both methods are fit ONLY on motor-02 (NORMAL) records, then used to score every
record (both motors). 'fault' ground truth is read AFTER scoring, purely to build
the confusion matrices — it is never an input to either detector.

Run with run_step4.sh.
"""

from __future__ import annotations

import collections
from pathlib import Path

import numpy as np
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer
from sklearn.ensemble import IsolationForest

BOOTSTRAP = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
FEATURES_TOPIC = "features"
ANOMALIES_TOPIC = "anomalies"
ANOMALIES_SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "anomalies.avsc"

FEATURE_NAMES = ("rms", "crest_factor", "kurtosis")

# Z-score above this on ANY of the 3 features flags a window. 3 sigma is the classic
# "rare under a normal distribution" cutoff (~0.3% false-positive rate per feature
# if the baseline were perfectly Gaussian) — simple and explainable, no tuning knob.
THRESHOLD_Z = 3.0

# IsolationForest contamination: the fraction of the FIT data IF should treat as
# outliers when picking its internal decision boundary. We fit only on motor-02
# (NORMAL) windows, which we believe are clean — but real sensor data always has a
# small amount of noise/transient outliers even when the asset is healthy. 0.05 says
# "assume up to 5% of the normal baseline itself is noisy", which keeps the boundary
# reasonably tight around the bulk of normal behaviour (sensitive to real anomalies)
# without being so tight that ordinary measurement noise constantly trips it.
IF_CONTAMINATION = 0.05
IF_RANDOM_STATE = 0


def consume_all_features() -> list[dict]:
    sr = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    deser = AvroDeserializer(sr)
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "step4-anomaly-detector",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([FEATURES_TOPIC])

    rows = []
    empty_polls = 0
    while empty_polls < 8:
        msg = consumer.poll(1.0)
        if msg is None:
            empty_polls += 1
            continue
        if msg.error():
            continue
        v = deser(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
        v["asset_id"] = msg.key().decode()
        rows.append(v)
    consumer.close()
    return rows


def score_threshold(X: np.ndarray, mean: np.ndarray, std: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    z = np.abs((X - mean) / std)
    score = z.max(axis=1)  # worst-offending feature per window
    is_anomaly = score > THRESHOLD_Z
    return score, is_anomaly


def confusion_counts(actual_anomaly: np.ndarray, predicted_anomaly: np.ndarray) -> dict:
    tp = int(np.sum(actual_anomaly & predicted_anomaly))
    fn = int(np.sum(actual_anomaly & ~predicted_anomaly))
    fp = int(np.sum(~actual_anomaly & predicted_anomaly))
    tn = int(np.sum(~actual_anomaly & ~predicted_anomaly))
    return {"tp": tp, "fn": fn, "fp": fp, "tn": tn}


def print_confusion(name: str, c: dict) -> None:
    total = c["tp"] + c["fn"] + c["fp"] + c["tn"]
    precision = c["tp"] / (c["tp"] + c["fp"]) if (c["tp"] + c["fp"]) else float("nan")
    recall = c["tp"] / (c["tp"] + c["fn"]) if (c["tp"] + c["fn"]) else float("nan")
    print(f"\n--- {name} ---")
    print(f"                 actual=FAULT   actual=NORMAL")
    print(f"  pred=ANOMALY   {c['tp']:>13d}   {c['fp']:>14d}")
    print(f"  pred=NORMAL    {c['fn']:>13d}   {c['tn']:>14d}")
    print(f"  n={total}  precision={precision:.3f}  recall={recall:.3f}")


def main() -> None:
    rows = consume_all_features()
    print(f"consumed {len(rows)} feature records from '{FEATURES_TOPIC}'")

    X = np.array([[r[f] for f in FEATURE_NAMES] for r in rows], dtype=np.float64)
    actual_fault = np.array([r["fault"]["type"] != "NORMAL" for r in rows])
    is_normal_row = ~actual_fault  # the baseline mask: fit ONLY on these

    baseline_X = X[is_normal_row]
    print(f"baseline (NORMAL-only) fit set: {baseline_X.shape[0]} windows, features={FEATURE_NAMES}")

    # --- Method 1: threshold (z-score vs normal baseline) ---
    mean = baseline_X.mean(axis=0)
    std = baseline_X.std(axis=0)
    thr_score, thr_is_anomaly = score_threshold(X, mean, std)

    # --- Method 2: Isolation Forest, fit on the same normal-only baseline ---
    iso = IsolationForest(contamination=IF_CONTAMINATION, random_state=IF_RANDOM_STATE)
    iso.fit(baseline_X)
    if_pred = iso.predict(X)  # -1 = anomaly, 1 = normal
    if_is_anomaly = if_pred == -1
    if_score = -iso.decision_function(X)  # flip sign so HIGHER = more anomalous, consistent with thr_score

    # --- Confusion matrices, side by side ---
    thr_cm = confusion_counts(actual_fault, thr_is_anomaly)
    if_cm = confusion_counts(actual_fault, if_is_anomaly)
    print(f"\nIsolationForest contamination={IF_CONTAMINATION} "
          f"(assumed outlier fraction WITHIN the normal-only fit set; "
          f"see IF_CONTAMINATION comment for rationale)")
    print_confusion("THRESHOLD (z-score > 3.0)", thr_cm)
    print_confusion("ISOLATION FOREST", if_cm)

    # --- Per-asset flagged counts (sanity check from the original plan) ---
    print("\n--- per-asset flagged counts ---")
    by_asset = collections.defaultdict(lambda: {"n": 0, "thr_flagged": 0, "if_flagged": 0, "fault": None})
    for i, r in enumerate(rows):
        a = by_asset[r["asset_id"]]
        a["n"] += 1
        a["thr_flagged"] += int(thr_is_anomaly[i])
        a["if_flagged"] += int(if_is_anomaly[i])
        a["fault"] = r["fault"]["type"]
    print(f"{'asset':9} {'fault':12} {'n':>4} {'thr_flagged':>12} {'if_flagged':>12}")
    for asset, a in sorted(by_asset.items()):
        print(f"{asset:9} {a['fault']:12} {a['n']:4d} {a['thr_flagged']:12d} {a['if_flagged']:12d}")

    # --- Produce AnomalyEvent records for BOTH methods to the 'anomalies' topic ---
    schema_str = ANOMALIES_SCHEMA_PATH.read_text()
    sr = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(sr, schema_str)
    key_serializer = StringSerializer("utf_8")
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    ctx = SerializationContext(ANOMALIES_TOPIC, MessageField.VALUE)

    def emit(r, method, score, is_anomaly):
        value = {
            "asset_id": r["asset_id"],
            "window_index": r["window_index"],
            "timestamp_ms": r["timestamp_ms"],
            "channel": r["channel"],
            "method": method,
            "score": float(score),
            "is_anomaly": bool(is_anomaly),
            "rms": r["rms"],
            "crest_factor": r["crest_factor"],
            "kurtosis": r["kurtosis"],
            "fault": r["fault"],
        }
        producer.produce(
            topic=ANOMALIES_TOPIC,
            key=key_serializer(r["asset_id"], ctx),
            value=avro_serializer(value, ctx),
        )

    for i, r in enumerate(rows):
        emit(r, "threshold", thr_score[i], thr_is_anomaly[i])
        emit(r, "isolation_forest", if_score[i], if_is_anomaly[i])
        producer.poll(0)
    producer.flush()
    print(f"\nproduced {2 * len(rows)} anomaly events (2 methods x {len(rows)} windows) to '{ANOMALIES_TOPIC}'.")


if __name__ == "__main__":
    main()

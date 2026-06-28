"""VoltSense Phase 2 Step 2b -- query constructor + retriever.

Pipeline: anomalies (avro-confluent) -> query construction -> Qdrant top-k -> RetrievalResult

Consumes the 'anomalies' topic, keeps only is_anomaly=true events from the THRESHOLD
method (cleaner precision than Isolation Forest -- see Step 4), builds a rich
natural-language query from the anomaly's actual feature values + fault metadata,
filters Qdrant by doc_type, and prints the top-k retrieved chunks.

NOTE ON HONESTY: 'fault.type' is CWRU GROUND TRUTH, carried through purely as
metadata (see schemas/anomalies.avsc). A real deployment wouldn't have it -- it
would need a fault-classification step we haven't built. Here it stands in for
that future step. Likewise the "elevated vs baseline" phrasing below is NOT
re-fit live -- it reuses the NORMAL-baseline mean/std already computed once in
streaming/step4_anomaly_detector.py, hardcoded here for query phrasing only.

Run with run_retriever.sh.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from qdrant_client import QdrantClient
from qdrant_client.http import models as qmodels
from sentence_transformers import SentenceTransformer

BOOTSTRAP = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
ANOMALIES_TOPIC = "anomalies"
QDRANT_URL = "http://localhost:6333"
COLLECTION_NAME = "fault_kb"

# CRITICAL: must be the exact model used at ingest time (rag/ingest/ingest.py) --
# query vectors from a different model live in a different embedding space, and
# cosine scores against the index would be meaningless.
EMBED_MODEL_NAME = "all-MiniLM-L6-v2"

TOP_K = 3
N_EVENTS_TO_SHOW = 3

# NORMAL-baseline mean from streaming/step4_anomaly_detector.py (motor-02, n=238),
# reused here ONLY to phrase "elevated vs healthy baseline" in the query text --
# not re-fit live, not used for any detection decision (that already happened
# upstream in Step 4; this script only describes the result).
BASELINE_MEAN = {"rms": 0.0737, "crest_factor": 3.019, "kurtosis": -0.252}

# Hard doc_type filter per fault type -- keyed off the CWRU ground-truth fault.type
# (see honesty note above). Generic on purpose: BROKEN_ROTOR_BAR isn't present in our
# CWRU-only dataset, but the mapping is here so this routes correctly if an MCSA-type
# asset/fault is ever added, without code changes.
FAULT_TYPE_TO_DOC_TYPES = {
    "INNER_RACE": ["bearing_physics", "maintenance_practice"],
    "OUTER_RACE": ["bearing_physics", "maintenance_practice"],
    "BALL": ["bearing_physics", "maintenance_practice"],
    "BROKEN_ROTOR_BAR": ["mcsa", "maintenance_practice"],
}
DEFAULT_DOC_TYPES = ["bearing_physics", "maintenance_practice", "mcsa"]


@dataclass
class RetrievedChunk:
    text: str
    source: str
    url: str
    section: str
    score: float


@dataclass
class RetrievalResult:
    asset_id: str
    window_index: int
    method: str
    fault_type: str
    feature_values: dict = field(default_factory=dict)
    query_text: str = ""
    doc_type_filter: list[str] = field(default_factory=list)
    chunks: list[RetrievedChunk] = field(default_factory=list)


def describe_feature(name: str, value: float) -> str:
    baseline = BASELINE_MEAN[name]
    delta = value - baseline
    direction = "elevated" if delta > 0 else "depressed"
    meaning = {
        "rms": "overall vibration energy",
        "crest_factor": "impulsiveness -- sharp transient impacts on a low background",
        "kurtosis": "spikiness vs a Gaussian -- impulsive/spiky vibration" if value > baseline
                    else "near-Gaussian, unremarkable vibration shape",
    }[name]
    return f"{name} = {value:.3g} ({direction} vs healthy baseline of {baseline:.3g} -- {meaning})"


def build_query(event: dict) -> tuple[str, list[str]]:
    fault = event["fault"]
    fault_type = fault["type"]
    fault_label = fault_type.replace("_", " ").lower()

    diameter = fault.get("fault_diameter_in")
    diameter_text = f"defect diameter {diameter} in" if diameter is not None else "no seeded defect"

    feature_lines = ", ".join(
        describe_feature(name, event[name]) for name in ("kurtosis", "rms", "crest_factor")
    )

    query_text = (
        f"Bearing {fault_label} fault, {diameter_text}, motor load {fault['load_hp']} HP. "
        f"Vibration features for this window: {feature_lines}. "
        f"What is the likely physical cause and recommended maintenance action?"
    )

    doc_types = FAULT_TYPE_TO_DOC_TYPES.get(fault_type, DEFAULT_DOC_TYPES)
    return query_text, doc_types


def consume_anomaly_events(n: int) -> list[dict]:
    sr = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    deser = AvroDeserializer(sr)
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "step2b-retriever",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([ANOMALIES_TOPIC])

    matched = []
    empty_polls = 0
    while empty_polls < 8 and len(matched) < n:
        msg = consumer.poll(1.0)
        if msg is None:
            empty_polls += 1
            continue
        if msg.error():
            continue
        v = deser(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
        v["asset_id"] = msg.key().decode()
        if v["method"] == "threshold" and v["is_anomaly"]:
            matched.append(v)
    consumer.close()
    return matched


def main() -> None:
    print(f"loading embedding model '{EMBED_MODEL_NAME}' (must match rag/ingest/ingest.py)...")
    model = SentenceTransformer(EMBED_MODEL_NAME)

    qdrant = QdrantClient(url=QDRANT_URL)

    events = consume_anomaly_events(N_EVENTS_TO_SHOW)
    print(f"\nfound {len(events)} threshold-method anomaly events (is_anomaly=true)\n")

    for event in events:
        query_text, doc_types = build_query(event)
        query_vec = model.encode(query_text, normalize_embeddings=True).tolist()

        hits = qdrant.query_points(
            collection_name=COLLECTION_NAME,
            query=query_vec,
            query_filter=qmodels.Filter(
                must=[qmodels.FieldCondition(key="doc_type", match=qmodels.MatchAny(any=doc_types))]
            ),
            limit=TOP_K,
        ).points

        result = RetrievalResult(
            asset_id=event["asset_id"],
            window_index=event["window_index"],
            method=event["method"],
            fault_type=event["fault"]["type"],
            feature_values={k: event[k] for k in ("rms", "crest_factor", "kurtosis")},
            query_text=query_text,
            doc_type_filter=doc_types,
            chunks=[
                RetrievedChunk(
                    text=h.payload["text"],
                    source=h.payload["source"],
                    url=h.payload["url"],
                    section=h.payload["section"],
                    score=h.score,
                )
                for h in hits
            ],
        )

        print("=" * 100)
        print(f"asset_id={result.asset_id}  window_index={result.window_index}  "
              f"fault_type={result.fault_type}  method={result.method}")
        print(f"feature_values={result.feature_values}")
        print(f"doc_type_filter={result.doc_type_filter}")
        print(f"\nquery_text:\n  {result.query_text}\n")
        print(f"top-{TOP_K} retrieved chunks:")
        for rank, chunk in enumerate(result.chunks, 1):
            print(f"  [{rank}] score={chunk.score:.4f}  source={chunk.source}  section={chunk.section!r}")
            print(f"      {chunk.text[:280]}...")
        print()


if __name__ == "__main__":
    main()

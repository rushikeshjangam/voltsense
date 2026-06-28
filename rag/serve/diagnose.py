"""VoltSense Phase 2 Step 2c -- LLM diagnosis generation.

Pipeline: RetrievalResult (Step 2b) -> numbered-context prompt -> LLM -> DiagnosisEvent -> diagnostics topic

Reuses Step 2b's retriever unchanged (same anomaly events, same query construction,
same Qdrant top-3) so the diagnosis can be directly compared against the retrieval
that grounds it.

LLM PROVIDER: Groq's OpenAI-compatible endpoint (free tier, no GPU needed locally).
Uses the `openai` SDK pointed at Groq's base_url -- this is deliberate: the prompt
logic below is provider-agnostic. Phase 3 swap: change base_url to the vLLM endpoint
and `model` to the locally-served model name -- prompt logic unchanged, nothing else
in this file needs to change.

Run with run_diagnose.sh. Requires GROQ_API_KEY in the environment.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer
from openai import OpenAI

from rag.retrieve.retriever import N_EVENTS_TO_SHOW, RetrievalResult, build_query, consume_anomaly_events
from qdrant_client import QdrantClient
from qdrant_client.http import models as qmodels
from sentence_transformers import SentenceTransformer
from rag.retrieve.retriever import COLLECTION_NAME, EMBED_MODEL_NAME, QDRANT_URL, TOP_K, RetrievedChunk

SCHEMA_REGISTRY_URL = "http://localhost:8081"
BOOTSTRAP = "localhost:9092"
DIAGNOSTICS_TOPIC = "diagnostics"
DIAGNOSTICS_SCHEMA_PATH = Path(__file__).resolve().parent.parent.parent / "schemas" / "diagnostics.avsc"

# --- LLM provider config -----------------------------------------------------------
# Phase 3 swap: change BASE_URL to the vLLM endpoint (e.g. http://localhost:8000/v1)
# and MODEL_NAME to the locally-served model name. Everything below this -- the
# prompt construction, citation parsing, Kafka sink -- is unchanged either way.
USE_LOCAL_VLLM = False
BASE_URL = "https://api.groq.com/openai/v1"
MODEL_NAME = "llama-3.3-70b-versatile"
MODEL_USED_LABEL = f"groq:{MODEL_NAME}"

SYSTEM_PROMPT = """You are a maintenance diagnostics assistant for industrial rotating \
machinery (motors, bearings). You are given an anomaly detected in vibration telemetry, \
plus retrieved excerpts from real technical references (bearing failure-mode literature, \
predictive-maintenance guides). Your job:

1. Explain the likely physical fault mechanism behind the anomaly.
2. State which of the reported vibration features are diagnostic, and why.
3. Recommend a concrete maintenance action.
4. Cite the numbered context sources that support each claim, e.g. [1], [2].
5. If the retrieved context does not support a claim, say "insufficient information" \
rather than inventing a fact, a frequency, or a standard.

Keep the diagnosis to 150-200 words. Write like a maintenance engineer filing a \
condition report, not a chatbot -- direct, technical, no filler like "I'd be happy \
to help" or "In conclusion."."""


def build_user_prompt(result: RetrievalResult) -> str:
    feature_lines = "\n".join(f"- {k} = {v:.3g}" for k, v in result.feature_values.items())
    context_blocks = []
    for i, chunk in enumerate(result.chunks, 1):
        context_blocks.append(
            f"[{i}] (score={chunk.score:.3f}) {chunk.source} -- {chunk.section}\n"
            f"    URL: {chunk.url}\n"
            f'    "{chunk.text}"'
        )
    context_text = "\n\n".join(context_blocks)

    return f"""ANOMALY DETECTED
Asset: {result.asset_id}, window {result.window_index}
Fault label (ground truth, for context only): {result.fault_type.lower()}
Vibration features for this window:
{feature_lines}

RETRIEVED CONTEXT
{context_text}

Using only the features above and the numbered context, write the diagnosis."""


def generate(client: OpenAI, system: str, user: str) -> str:
    response = client.chat.completions.create(
        model=MODEL_NAME,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0.2,
        max_tokens=400,
    )
    return response.choices[0].message.content.strip()


def parse_cited_sources(diagnosis_text: str, result: RetrievalResult) -> list[str]:
    """Extract which [n] markers actually appear in the text, map to chunk URLs,
    in first-appearance order, deduplicated. A retrieved-but-uncited chunk is
    intentionally excluded -- this is what makes cited_sources mean "cited", not
    just "was offered to the model".
    """
    cited_indices = sorted({int(n) for n in re.findall(r"\[(\d+)\]", diagnosis_text)})
    urls = []
    for idx in cited_indices:
        if 1 <= idx <= len(result.chunks):
            url = result.chunks[idx - 1].url
            if url not in urls:
                urls.append(url)
    return urls


def main() -> None:
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        raise SystemExit("GROQ_API_KEY is not set in the environment. Set it and re-run.")

    llm_client = OpenAI(base_url=BASE_URL, api_key=api_key)

    print(f"loading embedding model '{EMBED_MODEL_NAME}' (must match rag/ingest/ingest.py)...")
    embed_model = SentenceTransformer(EMBED_MODEL_NAME)
    qdrant = QdrantClient(url=QDRANT_URL)

    events = consume_anomaly_events(N_EVENTS_TO_SHOW)
    print(f"\nfound {len(events)} threshold-method anomaly events (is_anomaly=true)\n")

    schema_str = DIAGNOSTICS_SCHEMA_PATH.read_text()
    sr = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(sr, schema_str)
    key_serializer = StringSerializer("utf_8")
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    ctx = SerializationContext(DIAGNOSTICS_TOPIC, MessageField.VALUE)

    for event in events:
        query_text, doc_types = build_query(event)
        query_vec = embed_model.encode(query_text, normalize_embeddings=True).tolist()
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
                    text=h.payload["text"], source=h.payload["source"],
                    url=h.payload["url"], section=h.payload["section"], score=h.score,
                )
                for h in hits
            ],
        )

        user_prompt = build_user_prompt(result)
        diagnosis_text = generate(llm_client, SYSTEM_PROMPT, user_prompt)
        cited_sources = parse_cited_sources(diagnosis_text, result)

        print("=" * 100)
        print(f"asset_id={result.asset_id}  window_index={result.window_index}  fault_type={result.fault_type}")
        print(f"\n{diagnosis_text}\n")
        print(f"cited_sources: {cited_sources}")
        print()

        value = {
            "asset_id": result.asset_id,
            "window_index": result.window_index,
            "fault_type": result.fault_type,
            "diagnosis_text": diagnosis_text,
            "cited_sources": cited_sources,
            "model_used": MODEL_USED_LABEL,
            "retrieval_scores": [c.score for c in result.chunks],
        }
        producer.produce(
            topic=DIAGNOSTICS_TOPIC,
            key=key_serializer(result.asset_id, ctx),
            value=avro_serializer(value, ctx),
        )
        producer.poll(0)

    producer.flush()
    print(f"produced {len(events)} diagnosis events to '{DIAGNOSTICS_TOPIC}'.")


if __name__ == "__main__":
    main()

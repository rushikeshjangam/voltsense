"""VoltSense Phase 2 Step 2a -- knowledge base ingest: chunk + embed + upsert to Qdrant.

Reads the citation-headed .txt files in rag/ingest/sources/, splits each into
[SECTION: ...]-delimited sections, chunks each section to roughly fit under the
all-MiniLM-L6-v2 256-token limit (target ~200 tokens, ~35-token / ~15% overlap),
embeds with sentence-transformers, and upserts into the Qdrant collection 'fault_kb'.

Run with run_ingest.sh.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field
from pathlib import Path

from qdrant_client import QdrantClient
from qdrant_client.http import models as qmodels
from sentence_transformers import SentenceTransformer

SOURCES_DIR = Path(__file__).resolve().parent / "sources"
QDRANT_URL = "http://localhost:6333"
COLLECTION_NAME = "fault_kb"
EMBED_MODEL_NAME = "all-MiniLM-L6-v2"
VECTOR_SIZE = 384

TARGET_TOKENS = 200   # well under MiniLM's 256-token max sequence length
OVERLAP_TOKENS = 35   # ~15% overlap, enough to protect a formula/definition at a boundary

# Maps each source file to the doc_type used for optional retrieval filtering later.
DOC_TYPES = {
    "cwru_bearing_data_center.txt": "bearing_physics",
    "skf_bearing_damage_and_failure_analysis.txt": "bearing_physics",
    "mcsa_broken_rotor_bar_cnn.txt": "mcsa",
    "doe_om_best_practices_guide.txt": "maintenance_practice",
    "wikipedia_rolling_element_bearing.txt": "bearing_physics",
}

# Namespace for deterministic point IDs, so re-running ingest.py upserts (overwrites)
# existing chunks in place instead of duplicating them, and adding a new source file
# only adds new points -- the collection is never wiped on a rerun.
POINT_ID_NAMESPACE = uuid.UUID("a55e2f1e-9f3c-4b1a-8e2f-2f6b1f3c9a01")


def stable_point_id(source: str, chunk_index: int) -> str:
    return str(uuid.uuid5(POINT_ID_NAMESPACE, f"{source}::{chunk_index}"))

HEADER_KEYS = ("SOURCE", "URL", "PUBLISHER", "LICENSE", "RETRIEVED", "NOTE")


@dataclass
class SourceDoc:
    filename: str
    title: str
    url: str
    license: str
    retrieved: str
    doc_type: str
    sections: list[tuple[str, str]] = field(default_factory=list)  # (heading, text)


def parse_source_file(path: Path) -> SourceDoc:
    raw = path.read_text(encoding="utf-8")
    header_text, _, body = raw.partition("\n[SECTION:")
    body = "[SECTION:" + body if body else ""

    # Parse "KEY: value" header lines, folding un-keyed continuation lines into the
    # previous key's value (e.g. NOTE spans multiple lines, URL has 2 lines for CWRU).
    fields: dict[str, str] = {}
    last_key = None
    for line in header_text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        matched_key = next((k for k in HEADER_KEYS if stripped.startswith(k + ":")), None)
        if matched_key:
            fields[matched_key] = stripped[len(matched_key) + 1:].strip()
            last_key = matched_key
        elif last_key:
            fields[last_key] += " " + stripped

    sections: list[tuple[str, str]] = []
    for chunk in re.split(r"\[SECTION:\s*", body):
        chunk = chunk.strip()
        if not chunk:
            continue
        heading, _, text = chunk.partition("]")
        text = text.strip()
        if text:
            sections.append((heading.strip(), text))

    return SourceDoc(
        filename=path.name,
        title=fields.get("SOURCE", path.stem),
        url=fields.get("URL", ""),
        license=fields.get("LICENSE", ""),
        retrieved=fields.get("RETRIEVED", ""),
        doc_type=DOC_TYPES.get(path.name, "unknown"),
        sections=sections,
    )


def chunk_section(text: str, target_tokens: int = TARGET_TOKENS, overlap_tokens: int = OVERLAP_TOKENS) -> list[str]:
    """Split on sentence boundaries, then pack sentences into ~target_tokens windows.

    Uses whitespace word count as a token-count proxy -- not MiniLM's real BPE tokenizer,
    but close enough for sizing purposes (the goal is staying comfortably under the
    256-token truncation point, not hitting an exact count).
    """
    sentences = re.split(r"(?<=[.!?])\s+", text.strip())
    sentences = [s for s in sentences if s]

    chunks: list[str] = []
    current: list[str] = []
    current_len = 0
    i = 0
    while i < len(sentences):
        sentence = sentences[i]
        sentence_len = len(sentence.split())
        if current and current_len + sentence_len > target_tokens:
            chunks.append(" ".join(current))
            # back up to build ~overlap_tokens of trailing context into the next chunk
            overlap: list[str] = []
            overlap_len = 0
            for s in reversed(current):
                overlap_len += len(s.split())
                overlap.insert(0, s)
                if overlap_len >= overlap_tokens:
                    break
            current = overlap
            current_len = overlap_len
        current.append(sentence)
        current_len += sentence_len
        i += 1
    if current:
        chunks.append(" ".join(current))
    return chunks


def build_chunk_records(doc: SourceDoc) -> list[dict]:
    records = []
    chunk_index = 0
    for heading, text in doc.sections:
        for chunk_text in chunk_section(text):
            records.append({
                "source": doc.filename,
                "title": doc.title,
                "url": doc.url,
                "license": doc.license,
                "retrieved": doc.retrieved,
                "doc_type": doc.doc_type,
                "section": heading,
                "chunk_index": chunk_index,
                "text": chunk_text,
                "char_count": len(chunk_text),
            })
            chunk_index += 1
    return records


def main() -> None:
    paths = sorted(SOURCES_DIR.glob("*.txt"))
    print(f"found {len(paths)} source files in {SOURCES_DIR}")

    all_records: list[dict] = []
    for path in paths:
        doc = parse_source_file(path)
        records = build_chunk_records(doc)
        all_records.extend(records)
        print(f"  {doc.filename}: {len(doc.sections)} sections -> {len(records)} chunks "
              f"(doc_type={doc.doc_type})")

    print(f"\ntotal chunks: {len(all_records)}")

    print(f"\nloading embedding model '{EMBED_MODEL_NAME}' ...")
    model = SentenceTransformer(EMBED_MODEL_NAME)
    assert model.get_embedding_dimension() == VECTOR_SIZE

    texts = [r["text"] for r in all_records]
    embeddings = model.encode(texts, normalize_embeddings=True, show_progress_bar=True)

    client = QdrantClient(url=QDRANT_URL)
    if not client.collection_exists(COLLECTION_NAME):
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=qmodels.VectorParams(size=VECTOR_SIZE, distance=qmodels.Distance.COSINE),
        )

    # Stable IDs (hash of source + chunk_index) make this upsert idempotent: rerunning
    # with the same sources overwrites the same points; adding a new source file only
    # adds new points. The collection is never dropped/recreated.
    points = [
        qmodels.PointStruct(
            id=stable_point_id(all_records[i]["source"], all_records[i]["chunk_index"]),
            vector=embeddings[i].tolist(),
            payload=all_records[i],
        )
        for i in range(len(all_records))
    ]
    client.upsert(collection_name=COLLECTION_NAME, points=points)

    info = client.get_collection(COLLECTION_NAME)
    print(f"\ncollection '{COLLECTION_NAME}': {info.points_count} points, "
          f"vector_size={info.config.params.vectors.size}, "
          f"distance={info.config.params.vectors.distance}")

    # --- test query ---
    query = "bearing inner race fault vibration symptoms"
    print(f"\n--- test query: {query!r} ---")
    query_vec = model.encode(query, normalize_embeddings=True)
    hits = client.query_points(collection_name=COLLECTION_NAME, query=query_vec.tolist(), limit=3).points
    for rank, hit in enumerate(hits, 1):
        p = hit.payload
        print(f"\n[{rank}] score={hit.score:.4f}  source={p['source']}  section={p['section']!r}")
        print(f"    {p['text'][:280]}...")


if __name__ == "__main__":
    main()

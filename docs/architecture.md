# Architecture

> **TODO (Phase 0 polish):** export this ASCII diagram to `docs/architecture.png`
> (e.g. via [Excalidraw](https://excalidraw.com) or draw.io) and reference the PNG in
> the README hero section. The ASCII below is the source of truth until then.

```
                          ┌─────────────────────────────────────────────┐
                          │   ELECTRICAL DOMAIN LAYER (the EE moat)       │
                          │   3-phase current · vibration · temp · RPM    │
                          └───────────────────┬─────────────────────────┘
                                              │ replay producer (real dataset → live stream)
                                              ▼
   ┌──────────┐   raw-telemetry   ┌──────────────────┐   features    ┌────────────────────┐
   │  Kafka   │──────────────────▶│   Flink stream   │──────────────▶│  anomaly detector  │
   │ + Schema │   (Avro/Protobuf) │  feature extract │   (windowed)  │  (statistical/IF)  │
   │ Registry │◀──────────────────│  FFT · RMS · THD │               └─────────┬──────────┘
   └────┬─────┘    diagnostics    └──────────────────┘                         │ anomalies topic
        │                                                                       ▼
        │                          ┌─────────────────────────────────────────────────────┐
        │                          │              RAG DIAGNOSTIC LAYER                     │
        │                          │  anomaly signature → embed → Qdrant top-k retrieval  │
        │                          │  → vLLM (open model) → diagnosis + action + citations│
        │                          │  FastAPI orchestrator                                │
        │                          └───────────────────────────┬─────────────────────────┘
        │                                                       │
        ▼                                                       ▼
   ┌─────────────────────────┐                      ┌────────────────────────┐
   │  Dashboard (Streamlit/   │                      │   Knowledge base       │
   │  Grafana): assets, anomaly│                     │  motor-fault guides,   │
   │  timeline, diagnoses      │                     │  IEC/IEEE summaries,   │
   └─────────────────────────┘                      │  DGA tables, work orders│
                                                     └────────────────────────┘

   Cross-cutting:  Kubernetes (GKE / kind)  ·  Helm  ·  Terraform IaC  ·  KEDA autoscale
                   ArgoCD GitOps  ·  Datadog / Prometheus+Grafana observability
```

## Phase 0 (current) — what actually runs locally

```
   docker-compose
   ┌──────────────────────────────────────────────────────────┐
   │  kafka (KRaft)        schema-registry        qdrant       │
   │  INTERNAL kafka:29092  http://...:8081        :6333 REST  │
   │  EXTERNAL localhost:9092                       :6334 gRPC │
   └──────────────────────────────────────────────────────────┘
```

Only Kafka, Schema Registry and Qdrant are provisioned in Phase 0. Flink, the
producer, the RAG layer, vLLM and the dashboard arrive in later phases.

# VoltSense — Real-Time Predictive Maintenance & RAG Diagnostics for Electrical Assets

> Streaming condition-monitoring for rotating electrical machines: ingest sensor
> telemetry through Kafka, extract fault signatures in Flink, detect anomalies in
> real time, and use a RAG + LLM layer (Qdrant + vLLM) to turn a raw anomaly into a
> plain-language diagnosis with a recommended action and cited references.

<!-- 1. HERO -->
<!-- TODO: hero GIF of an anomaly -> cited diagnosis goes here (added in Phase 2/4). -->
<!-- ![VoltSense demo](docs/demo.gif) -->

> **Status:** 🚧 Phase 0 (foundations) — local dev stack runs; streaming MVP is next.
> See the full plan in [docs/PROJECT_SPEC.md](docs/PROJECT_SPEC.md).

---

## 2. The problem

Industrial electrical assets — motors, drives, transformers — fail in ways that show
up in their electrical and vibration signatures long before catastrophic breakdown.
Most monitoring stops at "an alert fired." VoltSense closes the gap between an alert
and an action: it streams machine telemetry, flags fault signatures in real time, and
explains *what's wrong and what to do* with cited maintenance references.

## 3. Architecture

See [docs/architecture.md](docs/architecture.md) for the full diagram.

**Data flow in one sentence:** dataset replayed as live telemetry → Kafka → Flink
extracts electrical fault features → anomaly detector flags deviations → anomaly
signature drives a RAG query → LLM returns a cited diagnosis → results stream back to
Kafka and surface on a dashboard.

## 4. What's interesting technically

- **Production streaming** — multi-topic Kafka design, Schema Registry, dual-listener
  config, Flink stateful windowed feature extraction.
- **Self-hosted AI infra** — vLLM model serving + Qdrant vector retrieval, no
  third-party LLM API.
- **Lag-driven scaling** — KEDA autoscales consumers on Kafka consumer lag.
- **IaC + GitOps** — Terraform-provisioned GKE, Helm, ArgoCD; one-command teardown.

## 5. The electrical layer (the domain moat)

Real fault physics, not generic anomaly detection — motor current signature analysis
(MCSA), bearing fault frequencies (BPFO/BPFI/BSF/FTF), broken-rotor-bar sidebands at
`f_s(1 ± 2s)`, and current THD. Full detail in
[docs/fault-signatures.md](docs/fault-signatures.md).

| Fault | Physical signature | Feature |
|---|---|---|
| Bearing defect | Vibration peaks at BPFO/BPFI/BSF/FTF | Envelope spectrum / FFT band energy |
| Broken rotor bar | Current sidebands at `f_s(1 ± 2s)` | FFT of phase current; sideband ratio |
| Stator imbalance | Negative-sequence current, rising THD | Symmetrical components; per-phase THD |
| Misalignment | 1×, 2× RPM harmonics | Order analysis at running speed |

**Dataset:** [CWRU Bearing Data Set](https://engineering.case.edu/bearingdatacenter) —
see [notebooks/01_cwru_explore.ipynb](notebooks/01_cwru_explore.ipynb) for download
instructions and a raw-signal walkthrough.

## 6. Quickstart (local, ₹0)

Requirements: Docker + Docker Compose.

```bash
make up        # start Kafka + Schema Registry + Qdrant
make health    # prove the stack: list Kafka topics + curl Qdrant
make replay    # (Phase 1) replay a CWRU fault file into Kafka
make down      # stop the stack
```

| Service | Host endpoint |
|---|---|
| Kafka (external listener) | `localhost:9092` |
| Schema Registry | `http://localhost:8081` |
| Qdrant (REST / gRPC) | `http://localhost:6333` / `localhost:6334` |

> **WSL + Docker Desktop:** if `docker` isn't found in your distro, enable WSL
> integration in Docker Desktop settings, or run targets with the Windows client:
> `make up DOCKER="docker.exe"`.

## 7. Cloud deploy (Phase 3)

```bash
terraform apply    # bring up the full stack on GKE
make destroy       # ruthless teardown — runs after every cloud session
```

Cost guardrails (non-negotiable): no NAT Gateway (use VPC endpoints / Private Google
Access), preemptible/spot nodes, a single small GPU node only during the demo run, and
a low billing alert before any `apply`. Details in
[docs/PROJECT_SPEC.md](docs/PROJECT_SPEC.md) §8.

## 8. Observability

<!-- TODO: Grafana/Datadog screenshots — pipeline lag, throughput, anomaly rate,
     inference latency, tokens/s, retrieval latency (Phase 3). -->

## 9. Roadmap

- [x] **Phase 0** — repo, license, architecture, compose stack (Kafka + SR + Qdrant)
- [ ] **Phase 1** — replay producer → Flink features → anomaly detection (streaming MVP)
- [ ] **Phase 2** — RAG diagnostic layer (Qdrant + vLLM + FastAPI)
- [ ] **Phase 3** — Helm + Terraform on GKE, KEDA lag scaling, ArgoCD, dashboards
- [ ] **Phase 4** — Streamlit dashboard, demo video, write-up

## License

[MIT](LICENSE) © 2026 Rushi Jangam

# VoltSense — Real-Time Predictive Maintenance & RAG Diagnostics for Electrical Assets

> **One-line pitch:** A streaming condition-monitoring platform that ingests electrical-machine sensor telemetry through Kafka, extracts fault signatures in Flink, detects anomalies in real time, and uses a RAG + LLM layer (Qdrant + vLLM) to generate root-cause diagnostics and maintenance actions — deployed on Kubernetes with Terraform IaC and full observability.

This is your portfolio anchor: it sits exactly on the seam between what you already are (Kafka/streaming/platform engineer) and what you're moving toward (AI/ML infrastructure), with your **Electrical Engineering background as the domain moat** that 95% of platform engineers chasing AI cannot replicate.

---

## 0. How to use this file

- **Drop this into a Claude Project** as the project instructions / knowledge. Every future chat ("help me write the Flink feature job", "review my Qdrant schema", "draft the Phase 2 LinkedIn post") then inherits full context without re-explaining.
- It is **spec-level**, not tutorial-level — it assumes you know Kafka, K8s, Terraform, Python. It tells you *what* to build, *why it matters for hiring*, and *in what order*, not how to write a `for` loop.
- **Default assumptions baked in** (flip any of these and the plan adjusts):
  1. **Cloud:** GCP-primary, but **local-first** (kind/k3s + docker-compose) for ~90% of the build so dev cost ≈ ₹0. Cloud only for the "hero" demo run, then `terraform destroy`.
  2. **Fault domain:** rotating electrical machines (induction motors / bearings) — best public datasets and the strongest EE-credibility angle.
  3. **Deliverable:** a public GitHub repo + demo video + LinkedIn series. Not a product.

---

## 1. Why this project (honest positioning)

**The differentiation problem this solves.** The market is flooded with "I built a RAG chatbot over my PDFs" portfolios. They signal nothing because anyone can wire LangChain to an OpenAI key. This project signals three scarce things at once:

1. **Production streaming depth** — Kafka multi-topic design, schema registry, stream processing, consumer-lag-aware scaling. This is *your existing edge* and it's what gets you the Dubai data/platform role now.
2. **AI-infrastructure capability** — self-hosted model serving (vLLM), vector DB ops on K8s (Qdrant), embedding pipelines, retrieval design. This is *Move #2* — the AI/ML-infra pivot.
3. **Industrial domain credibility** — real electrical fault physics (motor current signature analysis, bearing fault frequencies, harmonics/THD). This is the **Industry 4.0 / condition-based-maintenance** angle that your EE degree makes authentic and almost no competing candidate can fake.

**Hiring/salary connection.** The combination maps directly to the bands in your Dubai plan: streaming/platform roles (AED 20–40K/mo) on the strength of tracks 1, and the AI-Platform-Engineer band (AED 35–45K/mo) once track 2 is demonstrable. Industrial/energy employers (ADNOC Digital, utilities, G42's industrial AI work, GE/Siemens/Hitachi Energy digital units) value the domain layer specifically — it's a smaller, higher-margin candidate pool than generic MLOps.

**The honest caveat.** This is a multi-month evening build, not a weekend. The **Phase 1 MVP alone is already portfolio-worthy** — ship that first, post about it, then extend. Don't let the full vision block you from shipping the streaming core.

---

## 2. The narrative (what the README and LinkedIn say)

> Industrial electrical assets — motors, drives, transformers — fail in ways that show up in their electrical signatures long before catastrophic breakdown. VoltSense streams live machine telemetry, detects these signatures in real time, and turns a raw anomaly into a *plain-language diagnosis with a recommended action and cited maintenance references* — bridging the gap between "an alert fired" and "here's what's wrong and what to do."

This framing is deliberately **outcome-first** (maintenance decisions), not tech-first. Recruiters and engineering managers remember the story; the stack is the proof underneath it.

---

## 3. System architecture

```
                          ┌─────────────────────────────────────────────┐
                          │   ELECTRICAL DOMAIN LAYER (your EE moat)      │
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

**Data flow in one sentence:** dataset replayed as live telemetry → Kafka → Flink extracts electrical fault features → anomaly detector flags deviations → anomaly signature drives a RAG query → LLM returns a cited diagnosis → results stream back to Kafka and surface on a dashboard.

---

## 4. The electrical domain layer (this is what makes it *yours*)

This is the section that separates you from every generic-RAG portfolio. Ground it in real fault physics.

### Datasets (use real, public, citable data)
- **CWRU Bearing Data Set** (Case Western) — the canonical motor-bearing vibration dataset; labelled inner-race / outer-race / ball faults at known loads. Best starting point.
- **MetroPT-3** (UCI) — metro train compressor with analog + digital signals incl. motor current; good for a second asset type.
- **Broken Rotor Bar** datasets (e.g., IEEE DataPort induction-motor sets) — 3-phase current signals for **MCSA**.
- **Paderborn Bearing Dataset** — synchronized motor current *and* vibration; ideal because it lets you fuse both signal types.

Pick **one** to start (CWRU or Paderborn). A replay producer reads it and publishes timestamped, multi-channel records to Kafka *as if* a live PLC/edge gateway were emitting them.

### Faults to detect (and the signatures that reveal them)
| Fault | Physical signature | Feature to extract |
|---|---|---|
| Bearing defect (inner/outer race, ball) | Vibration peaks at bearing fault frequencies (BPFO, BPFI, BSF, FTF) | Envelope spectrum / FFT band energy at those freqs |
| Broken rotor bar | Current sidebands at `f_s(1 ± 2s)` around line frequency | FFT of phase current; sideband magnitude vs fundamental |
| Stator winding / phase imbalance | Negative-sequence current, rising current THD | Symmetrical components; THD on each phase |
| Misalignment / eccentricity | 1×, 2× RPM harmonics in vibration & current | Order analysis at running speed |
| Thermal stress | Temperature trend vs load | Rolling temp/load ratio, rate-of-rise |

### Feature engineering (the Flink job)
Windowed (e.g., 1 s tumbling) per asset: RMS, peak, crest factor, kurtosis, FFT spectral bands, current THD, sideband ratios, bearing-frequency band energy, temperature rate-of-rise. **These features ARE the EE credibility** — they show you understand the machine, not just the pipeline.

> Naming the fault frequencies and the `f_s(1±2s)` sideband relationship in your README/posts is what makes an EE reviewer trust you instantly. Don't bury it.

---

## 5. Tech stack & why each choice signals hireability

| Layer | Tool | Why it's here (hiring signal) |
|---|---|---|
| Ingestion | **Apache Kafka + Schema Registry** | Your core strength; Avro/Protobuf + registry shows production discipline, not toy usage |
| Stream processing | **Apache Flink** (PyFlink ok) | Closes your one noted skill gap; real-time stateful processing is in most UAE streaming JDs |
| Anomaly detection | Statistical thresholds → **Isolation Forest** | Keep deliberately simple — the *infra* is the story, not a novel model |
| Embeddings | **sentence-transformers** (`all-MiniLM-L6-v2`) | Lightweight, CPU-friendly, well-known; embedding-pipeline skill for Move #2 |
| Vector DB | **Qdrant** on K8s (Helm) | Explicit Move #2 target skill; vector-DB-ops is a scarce, paid skill |
| Model serving | **vLLM** (open model; CPU-mode small model for cost) | The flagship AI-infra skill; self-hosting > calling an API for signalling |
| Orchestration API | **FastAPI** | Standard, fast to build, clean demo surface |
| Platform | **Kubernetes** (kind local / GKE cloud) + **Helm** | Your CKA work made visible in a real deployment |
| Autoscaling | **KEDA** (scale consumers on Kafka lag) | Lag-driven scaling is a genuinely senior, on-brand touch |
| GitOps | **ArgoCD** | Shows you deploy like a platform team, not by `kubectl apply` |
| IaC | **Terraform** | Your existing strength; reproducible infra is table stakes |
| Observability | **Datadog** (or Prometheus + Grafana free) | Your existing edge; instrument pipeline lag, inference latency, anomaly rate |

**Model choice for vLLM:** start with a small instruct model for CPU-mode local dev (e.g., a 1–3B-class open model). For the cloud "hero" run, a 7–8B-class instruct model on a single GPU node, then destroy. The point is demonstrating *self-hosted serving + retrieval-grounded generation*, not model size.

---

## 6. Phased build plan

Each phase ends in something **shippable and postable**. Time estimates assume ~8–10 hrs/week.

### Phase 0 — Foundations (week 1)
- Repo, license, architecture diagram (export the ASCII above to a real diagram), dataset chosen and explored in a notebook.
- `docker-compose` up: Kafka + Schema Registry + Qdrant locally.
- **Milestone:** repo public with README skeleton + architecture diagram; compose stack runs.

### Phase 1 — Streaming MVP (weeks 2–4) ← *already portfolio-worthy; ship this first*
- Replay producer → `raw-telemetry` (Avro, partitioned by `asset_id`).
- Flink feature job → `features` (RMS, FFT bands, current THD, sideband ratios).
- Anomaly detector → `anomalies` topic; console/log sink.
- **Milestone:** end-to-end stream that turns a known CWRU fault file into a flagged anomaly. Post #1.

### Phase 2 — RAG diagnostic layer (weeks 5–8)
- Build the electrical knowledge base (curate fault guides, IEC/IEEE summaries, DGA tables, synthetic work orders). Chunk + embed → Qdrant.
- Anomaly → query construction from the signature → top-k retrieval → vLLM diagnosis with citations → `diagnostics` topic.
- FastAPI endpoint: `POST /diagnose` (anomaly in, cited diagnosis out).
- **Milestone:** a flagged broken-rotor-bar anomaly returns a cited, plausible diagnosis + action. Post #2 (the "wow" demo).

### Phase 3 — Platform & IaC (weeks 9–12)
- Helm charts for every component; deploy to **kind** locally, then **GKE** via Terraform.
- KEDA scaling consumers on Kafka lag; ArgoCD GitOps; cost guardrails (Section 8).
- Datadog/Grafana dashboards: pipeline lag, throughput, anomaly rate, inference latency, token throughput.
- **Milestone:** one-command `terraform apply` brings up the full stack on GKE; one-command destroy tears it down. Post #3.

### Phase 4 — Polish & portfolio (weeks 13–14)
- Streamlit/Grafana dashboard: asset list, live anomaly timeline, diagnosis cards.
- 2–3 min demo video; README finalised; "lessons learned" write-up.
- **Milestone:** repo is demo-ready and linkable on your CV/LinkedIn. Post #4 (recap + GitHub link).

---

## 7. Repository structure

```
voltsense/
├── README.md                  # the narrative + architecture + quickstart (Section 9)
├── docs/
│   ├── architecture.png
│   ├── fault-signatures.md     # the EE physics: fault freqs, MCSA, THD (your moat)
│   └── adr/                    # short architecture decision records (senior signal)
├── infra/
│   ├── terraform/             # GKE, networking (VPC endpoints, NO NAT gw), Qdrant, node pools
│   └── helm/                  # charts: kafka, flink, qdrant, vllm, api, keda, argocd apps
├── producer/                  # dataset replay → Kafka (Avro)
├── streaming/                 # Flink jobs: feature extraction + anomaly detection
├── schemas/                   # Avro/Protobuf schemas (registry-managed)
├── rag/
│   ├── ingest/                # knowledge-base chunk + embed → Qdrant
│   ├── retrieve/              # query construction from anomaly signature
│   └── serve/                 # FastAPI + vLLM client
├── dashboard/                 # Streamlit/Grafana
├── observability/             # Datadog monitors / Grafana dashboards as code
├── notebooks/                 # dataset EDA, fault-signature validation
├── docker-compose.yml         # local dev stack
└── Makefile                   # up / down / replay / deploy / destroy
```

**Senior signals to include:** ADRs (why Flink over Kafka Streams, why Qdrant over pgvector), a `Makefile` with a clean `destroy` target, schemas under version control, dashboards-as-code.

---

## 8. Cost discipline & guardrails (non-negotiable)

Your standing principles, applied:
- **Local-first.** Phases 0–2 run entirely on `docker-compose` / `kind` → ₹0. Only Phase 3's hero run touches the cloud.
- **NAT Gateway is the #1 trap** — do **not** provision one. Use **VPC endpoints / Private Google Access** for registry/storage egress.
- **Preemptible/spot** node pools; single small GPU node only during the demo run.
- **`terraform destroy` after every cloud session.** The `Makefile destroy` target must be ruthless.
- **Billing alert** set low (e.g., the equivalent of ~$25) before any `apply`.
- Target cloud spend: within your usual ₹1,200–2,100/month envelope, and most months ₹0 because you stay local.

---

## 9. README outline (the repo's front door)

1. **Hero line + GIF** of an anomaly → diagnosis (the demo, top of the page).
2. **The problem** (electrical-asset failure / condition-based maintenance) — 3 sentences.
3. **Architecture diagram** + the one-sentence data flow.
4. **What's interesting technically** — streaming design, self-hosted serving, vector retrieval, lag-driven scaling.
5. **The electrical layer** — fault signatures table, link to `docs/fault-signatures.md` (your moat, front-and-centre).
6. **Quickstart** — `make up && make replay` (local, ₹0).
7. **Cloud deploy** — `terraform apply` / `make destroy`, with the cost guardrails noted.
8. **Observability** — screenshots of the dashboards.
9. **Roadmap / what I'd do next** — honesty signals maturity.

---

## 10. Observability — what to actually measure

Instrument these (it's your existing edge, so make it visible): Kafka consumer lag per topic, end-to-end pipeline latency (telemetry → diagnosis), throughput (records/s), anomaly rate per asset, **inference latency and tokens/s** from vLLM, retrieval latency from Qdrant, and resource utilisation (CPU/GPU). A single Grafana/Datadog board that ties *streaming* metrics to *inference* metrics is itself a differentiator — few portfolios show both worlds in one pane.

---

## 11. LinkedIn content plan (build-in-public = the actual portfolio)

The repo is the proof; the posts are the distribution. One post per phase, each tying back to your Kafka/Cruise-Control authority so recruiters connect the dots.

- **Post 1 (Phase 1):** "Turning a real motor-bearing dataset into a live Kafka stream that flags faults in real time." Architecture diagram + the fault-frequency angle. *Hook: streaming + EE.*
- **Post 2 (Phase 2):** The demo — an anomaly becomes a cited diagnosis via self-hosted vLLM + Qdrant. Short video. *Hook: this is AI infra, self-hosted, grounded.*
- **Post 3 (Phase 3):** "Deploying the whole thing on GKE with Terraform + KEDA lag-based autoscaling — and tearing it down for ₹0." *Hook: platform discipline + cost sense.*
- **Post 4 (Phase 4):** Recap, lessons, GitHub link, "what I'd build next." *Hook: invite conversation, surface to recruiters.*
- **Cross-cutting:** a `docs/fault-signatures.md` deep-dive post (MCSA sidebands, bearing frequencies) — this is the one only *you* can write, and it's catnip for industrial/energy hiring managers.

Write each post in your own voice; I can draft them per phase when you get there.

---

## 12. How this ladders into Move #2 (and Dubai)

- **Now (Dubai entry):** Phases 1 + 3 are pure platform/streaming evidence — they reinforce the profile that lands the AED 20–40K data/platform role on existing strength.
- **Move #2 (AI-infra pivot, from inside Dubai):** Phase 2's vLLM + Qdrant + embedding work is exactly the AI-Platform-Engineer (AED 35–45K) skill set, now backed by a *running system* rather than a course certificate.
- **Domain optionality:** the electrical/Industry-4.0 layer opens energy/industrial employers (ADNOC Digital, utilities, OT/IIoT vendors) where your EE degree is a moat, not a footnote.

---

## 13. Stretch goals (only after Phase 4 ships)
- Add a second asset type (transformer DGA, or power-quality/THD monitoring) to show generalisation.
- Online/streaming feature store; drift detection on the anomaly model.
- Agentic step: let the LLM propose a work-order and route it (Kafka → ticketing) — careful, scope creep.
- Swap the small model for a fine-tuned/quantised variant and benchmark latency vs cost (great post).

---

## 14. Risks & scope honesty
- **Scope creep is the main risk.** The full stack is large. Ship Phase 1, then Phase 2 — each is independently postable. Don't wait for "done" to publish.
- **PyFlink learning curve** is your real new-skill cost; budget for it in Phase 1.
- **Don't over-engineer the ML.** A simple, explainable anomaly detector is *better* here — the narrative is infra + domain, not a Kaggle model.
- **Keep the knowledge base honest** — cite real standards/guides; don't fabricate authoritative-sounding maintenance text the LLM then "cites."

---

## 15. Definition of done
A public GitHub repo where a reviewer can: read a clear narrative + architecture, see the electrical fault-signature physics documented, run the streaming MVP locally with two commands, watch a 2–3 min video of an anomaly becoming a cited diagnosis, see Terraform bring the full stack up on GKE and a `destroy` take it down, and view dashboards tying streaming lag to inference latency — all linked from four LinkedIn posts that connect the work back to your Kafka/platform authority.

---

*Starter checklist: (1) confirm the three default assumptions in §0, (2) pick the dataset (CWRU or Paderborn), (3) create the repo + this file as `docs/PROJECT_SPEC.md`, (4) drop this file into a Claude Project, (5) start Phase 0. Tell me when you're ready and I'll generate the Phase 0 scaffolding — compose file, repo skeleton, and the replay producer — as the first build chat.*
# producer/

Dataset **replay producer**: reads the CWRU bearing dataset and publishes timestamped,
multi-channel records to Kafka *as if* a live PLC/edge gateway were emitting them.

> **Phase 0 status:** skeleton only. `producer.py` constructs config and a Kafka
> connection and has a documented TODO main loop. No replay logic yet — that's Phase 1.

## Files

| File | Purpose |
|---|---|
| `config.py` | Env-driven config (bootstrap servers, topic, dataset path, replay speed) |
| `producer.py` | Entry point + skeleton; the replay loop is a Phase 1 TODO |
| `requirements.txt` | `confluent-kafka`, `scipy`, `numpy` (install in Phase 1) |

## Run

```bash
# host (uses EXTERNAL listener localhost:9092 by default)
python -m producer.producer

# inside docker-compose network (INTERNAL listener)
KAFKA_BOOTSTRAP_SERVERS=kafka:29092 python -m producer.producer
```

See [../notebooks/01_cwru_explore.ipynb](../notebooks/01_cwru_explore.ipynb) for how to
download CWRU data and what the raw signal looks like.

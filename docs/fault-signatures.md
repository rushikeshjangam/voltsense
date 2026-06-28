# Fault Signatures — the electrical domain layer

> This is the EE moat. The Flink feature job (Phase 1) computes exactly these
> quantities; the README links here front-and-centre. Keep it honest — every
> relationship below is standard rotating-machine diagnostics, not invented.

## Why electrical/vibration signatures predict failure

Rotating-machine faults perturb the magnetic field, the mechanical vibration, or both,
in *characteristic frequency bands* tied to geometry and running speed. Detecting energy
in those specific bands — rather than a generic "value too high" threshold — is what
separates a domain-aware monitor from a naive one.

## Fault → signature → feature

| Fault | Physical signature | Feature to extract |
|---|---|---|
| Bearing defect (inner/outer race, ball) | Vibration peaks at bearing fault frequencies (BPFO, BPFI, BSF, FTF) | Envelope (Hilbert) spectrum / FFT band energy at those freqs |
| Broken rotor bar | Stator current sidebands at `f_s(1 ± 2s)` around the line frequency | FFT of phase current; sideband magnitude vs fundamental |
| Stator winding / phase imbalance | Negative-sequence current, rising current THD | Symmetrical components; THD per phase |
| Misalignment / eccentricity | 1×, 2× RPM harmonics in vibration & current | Order analysis at running speed |
| Thermal stress | Temperature trend vs load | Rolling temp/load ratio, rate-of-rise |

## Bearing fault frequencies

For a bearing with `n` rolling elements, ball diameter `d`, pitch diameter `D`,
contact angle `φ`, and shaft rotation frequency `f_r` (Hz):

```
BPFO (outer race)  = (n/2) · f_r · (1 − (d/D)·cos φ)
BPFI (inner race)  = (n/2) · f_r · (1 + (d/D)·cos φ)
BSF  (ball)        = (D/2d) · f_r · (1 − ((d/D)·cos φ)²)
FTF  (cage)        = (1/2) · f_r · (1 − (d/D)·cos φ)
```

The CWRU bearings (SKF 6205-2RS / 6203) have published geometry, so these are
computable per file — useful for validating that energy actually appears at the
expected band for a labelled fault.

## Motor current signature analysis (MCSA) — broken rotor bar

With supply frequency `f_s` and per-unit slip `s`, broken/cracked rotor bars produce
current sidebands at:

```
f_sideband = f_s · (1 ± 2s)
```

The magnitude of the lower sideband relative to the fundamental is the classic severity
indicator (often quoted in dB down from the fundamental).

## THD and symmetrical components

- **Current THD** rises with stator-winding degradation and supply distortion.
- **Negative-sequence current** (from symmetrical-component decomposition of the three
  phases) indicates voltage/winding imbalance.

## References to cite (Phase 2 knowledge base)

> TODO: replace with exact citations as the KB is built. Keep these real — do not let
> the LLM "cite" fabricated maintenance text (see PROJECT_SPEC §14).

- IEEE / IEC machine-condition-monitoring standards (e.g. ISO 20958, ISO 10816 vibration severity).
- CWRU Bearing Data Center documentation (bearing geometry & fault descriptions).
- Standard MCSA literature on rotor-bar sideband detection.

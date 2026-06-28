"""Read a CWRU .mat file and yield fixed-size, non-overlapping sample windows.

CWRU files are MATLAB v5 (.mat). scipy.io.loadmat returns a dict whose data keys look
like 'X097_DE_time' / 'X097_FE_time' / 'X097_BA_time' — the 'X<id>' prefix is the file's
record number and changes per file, so we match on the '_<CHANNEL>_time' SUFFIX instead
of hardcoding the id.
"""

from __future__ import annotations

from typing import Iterator

import numpy as np
from scipy.io import loadmat

VALID_CHANNELS = ("DE", "FE", "BA")


def load_channel(mat_path: str, channel: str) -> np.ndarray:
    """Return the 1-D float signal for the requested channel.

    Raises KeyError if that channel is not present in the file (not every CWRU file
    has FE/BA).
    """
    if channel not in VALID_CHANNELS:
        raise ValueError(f"channel must be one of {VALID_CHANNELS}, got {channel!r}")

    mat = loadmat(mat_path)
    suffix = f"_{channel}_time"
    key = next((k for k in mat if k.endswith(suffix)), None)
    if key is None:
        available = [k for k in mat if not k.startswith("__")]
        raise KeyError(
            f"channel {channel!r} (key '*{suffix}') not found in {mat_path}. "
            f"Available variables: {available}"
        )
    # CWRU arrays are shape (N, 1); flatten to (N,) and ensure float.
    return mat[key].ravel().astype(np.float64)


def iter_windows(signal: np.ndarray, window_size: int) -> Iterator[np.ndarray]:
    """Yield consecutive non-overlapping windows of exactly `window_size` samples.

    The final partial window (len < window_size) is DROPPED: a short window would have
    different FFT resolution and zero-padding it would distort the spectrum. Losing a
    sub-window tail of a multi-second recording is analytically negligible.
    """
    n_full = signal.size // window_size
    for i in range(n_full):
        start = i * window_size
        yield signal[start : start + window_size]


def describe(mat_path: str, channel: str, window_size: int) -> dict:
    """Small helper for EDA / sanity checks before replaying (no Kafka involved)."""
    sig = load_channel(mat_path, channel)
    n_full = sig.size // window_size
    return {
        "samples": int(sig.size),
        "window_size": window_size,
        "full_windows": int(n_full),
        "dropped_tail_samples": int(sig.size - n_full * window_size),
    }

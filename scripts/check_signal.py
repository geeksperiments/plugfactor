#!/usr/bin/env python3
"""
PlugFactor Headless Audio Signal Verifier (check_signal.py)

Executed inside the Daytona Cloud sandbox after:
  1. cmaj validate <patch.cmajpatch>
  2. cmaj render <patch.cmajpatch> --output test.wav --duration 2.0

Asserts 4 mandatory acoustic invariants using only the Python 3 standard library:
  - No NaN samples
  - No Inf samples
  - Non-silence (RMS > -60.0 dBFS)
  - Peak gain <= 0.0 dBFS (peak_linear <= 1.0)
"""

import json
import math
import struct
import sys
import wave
from typing import List, Tuple


SILENCE_THRESHOLD_DB = -60.0
MAX_PEAK_DB = 0.0
MAX_PEAK_LINEAR = 1.0


def decode_wav_samples(path: str) -> Tuple[int, int, int, List[float]]:
    """Reads a 16-bit, 24-bit, or 32-bit PCM/float WAV file into normalized floats."""
    with wave.open(path, "rb") as wf:
        channels = wf.getnchannels()
        sampwidth = wf.getsampwidth()
        framerate = wf.getframerate()
        nframes = wf.getnframes()
        raw = wf.readframes(nframes)

    total_samples = nframes * channels
    samples: List[float] = []

    if sampwidth == 2:
        ints = struct.unpack(f"<{total_samples}h", raw)
        samples = [s / 32768.0 for s in ints]
    elif sampwidth == 3:
        for i in range(0, len(raw), 3):
            b0, b1, b2 = raw[i], raw[i + 1], raw[i + 2]
            val = b0 | (b1 << 8) | (b2 << 16)
            if val & 0x800000:
                val -= 0x1000000
            samples.append(val / 8388608.0)
    elif sampwidth == 4:
        floats = struct.unpack(f"<{total_samples}f", raw)
        samples = list(floats)
    else:
        raise ValueError(f"Unsupported WAV sample width: {sampwidth} bytes")

    return framerate, channels, nframes, samples


def analyze_signal(path: str) -> dict:
    sample_rate, channels, num_frames, samples = decode_wav_samples(path)
    duration_sec = num_frames / float(sample_rate) if sample_rate > 0 else 0.0

    has_nan = False
    has_inf = False
    peak_linear = 0.0
    sum_sq = 0.0
    valid_count = 0

    for s in samples:
        if math.isnan(s):
            has_nan = True
            continue
        if math.isinf(s):
            has_inf = True
            continue
        abs_s = abs(s)
        if abs_s > peak_linear:
            peak_linear = abs_s
        sum_sq += s * s
        valid_count += 1

    if peak_linear > 0.0:
        peak_db = 20.0 * math.log10(peak_linear)
    else:
        peak_db = -120.0

    if valid_count > 0 and sum_sq > 0.0:
        rms_linear = math.sqrt(sum_sq / valid_count)
        rms_db = 20.0 * math.log10(rms_linear)
    else:
        rms_db = -120.0

    is_silent = rms_db <= SILENCE_THRESHOLD_DB

    violations: List[str] = []
    if num_frames == 0 or valid_count == 0:
        violations.append("EMPTY_RENDER: WAV file contains zero valid audio frames.")
    if has_nan:
        violations.append("NAN_DETECTED: Audio stream contains NaN sample values.")
    if has_inf:
        violations.append("INF_DETECTED: Audio stream contains +/-Inf sample values.")
    if is_silent:
        violations.append(
            f"SILENT_OUTPUT: RMS level {rms_db:.2f} dBFS is below silence threshold ({SILENCE_THRESHOLD_DB:.1f} dBFS)."
        )
    if peak_db > MAX_PEAK_DB or peak_linear > MAX_PEAK_LINEAR:
        violations.append(
            f"SIGNAL_CLIPPING: Peak gain {peak_db:.2f} dB (linear {peak_linear:.5f}) exceeds {MAX_PEAK_DB:.1f} dB ceiling."
        )

    passed = len(violations) == 0

    return {
        "file": path,
        "sample_rate": sample_rate,
        "channels": channels,
        "num_frames": num_frames,
        "duration_sec": round(duration_sec, 4),
        "has_nan": has_nan,
        "has_inf": has_inf,
        "is_silent": is_silent,
        "peak_linear": round(peak_linear, 6),
        "peak_db": round(peak_db, 2),
        "rms_db": round(rms_db, 2),
        "passed": passed,
        "violations": violations,
    }


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: check_signal.py <path-to-wav>", file=sys.stderr)
        return 2

    wav_path = sys.argv[1]
    try:
        report = analyze_signal(wav_path)
    except Exception as exc:
        error_report = {
            "file": wav_path,
            "passed": False,
            "violations": [f"WAV_READ_ERROR: {exc}"],
        }
        print(json.dumps(error_report, indent=2))
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(json.dumps(report, indent=2))
    if not report["passed"]:
        for v in report["violations"]:
            print(f"ASSERTION FAILED: {v}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())

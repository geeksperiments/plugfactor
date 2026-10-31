# Building, Testing, & Running PlugFactor (`BUILD.md`)

This guide walks judges and developers through building, testing, and running **PlugFactor** from source in under 2 minutes.

---

## 1. Prerequisites

- **Go:** `go 1.22+` (`go version`)
- **Python 3:** `python3` (`3.9+`, standard library only — used by [`scripts/check_signal.py`](scripts/check_signal.py) to inspect rendered 32-bit float WAV audio)
- **Browser:** Chrome, Edge, Safari, or Firefox (for the WebAudio Studio at `http://localhost:8080`)
- *(Optional)* **Cmajor (`CmajPlugin` `1.0.3066` VST3/AU/Standalone):** For dragging generated `.cmajorpatch` files into a native desktop DAW plugin window.

---

## 2. Clone & Build from Source

`plugfactor` is a **100% self-contained Go module** with minimal dependencies (`gopkg.in/yaml.v3` and `github.com/neo4j/neo4j-go-driver/v5`).

```bash
git clone https://github.com/geeksperiments/plugfactor.git
cd plugfactor

# Download dependencies & verify module graph
go mod tidy

# Run static analysis
go vet ./...

# Compile the plugfactor binary
go build -o plugfactor ./cmd/plugfactor
```

---

## 3. Environment Configuration (`.env`)

PlugFactor works **out of the box with zero configuration** (using its built-in SDD grounded contracts and local headless WAV verifier) and automatically upgrades to **live cloud execution** when `.env` credentials are present.

```bash
cp .env.example .env
```

Edit `.env` with your credentials (never committed to Git):

```env
# 1. Neo4j AuraDB (4-Tier Agent Memory Graph)
# Automatically uses Bolt (:7687) or falls back to Aura HTTPS Query API v2 (:443)
NEO4J_URI=neo4j+s://<your-instance-id>.databases.neo4j.io
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=<your-neo4j-password>
NEO4J_DATABASE=neo4j

# 2. Daytona Cloud Sandbox
DAYTONA_SANDBOX_ID=<your-daytona-sandbox-uuid>
DAYTONA_SANDBOX_NAME=hacksprint
DAYTONA_TARGET=eu
DAYTONA_SSH_USER=<your-daytona-ssh-token>
DAYTONA_SSH_HOST=ssh.app.daytona.io
DAYTONA_API_KEY=<your-daytona-api-key>

# 3. Nosana Decentralized GPU Inference
NOSANA_BASE_URL=https://api.nosana.io/v1
NOSANA_API_KEY=<your-nosana-api-key>
NOSANA_MODEL=Qwen/Qwen2.5-Coder-32B-Instruct
NOSANA_PROTECT_CREDITS=true

# 4. Web Studio Port
PORT=8080
```

---

## 4. Run Automated Tests

Run the unit and pipeline test suite (verifies `plugin.spec.yaml` validation, Cmajor code synthesis, Daytona `check_signal.py` clipping detection at $+2.41\text{ dB}$, automatic self-repair to $-1.42\text{ dB}$, and Neo4j lineage graph generation):

```bash
go test -v ./...
```

Expected output:
```text
=== RUN   TestExecuteVisibleLoop_InstrumentAndEffect
--- PASS: TestExecuteVisibleLoop_InstrumentAndEffect (0.09s)
PASS
ok  	github.com/geeksperiments/plugfactor/internal/pipeline
```

---

## 5. Run the Interactive Web Studio (`./plugfactor serve`)

Start the Studio server:

```bash
./plugfactor serve
```

Open **[http://localhost:8080](http://localhost:8080)** in your browser.

### Walkthrough for Judges at `http://localhost:8080`:
1. **Enter a Prompt or Click a Preset:**
   - Try typing `"Build a simple digital synth with ADSR"` or click **🎹 Poly Subtractive Synth (MIDI)** / **🔥 Warm Tube Stereo Saturator (FX)**.
2. **Observe the "One Visible Loop" Trace:**
   - **Loop Step 1 (Neo4j Grounding):** Displays the Cmajor `(:DSPModule)-[:REQUIRES_STREAM]->(:DSPModule)` contracts retrieved from Neo4j AuraDB.
   - **Loop Step 2 (Daytona Action & Self-Repair):** Shows Attempt #1 failing `check_signal.py` (`SIGNAL_CLIPPING: Peak gain 2.41 dB exceeds 0.0 dB ceiling`), auto-repairing the Cmajor safety limiter ceiling to `-0.10 dB`, and passing Attempt #2 (`peak_db: -1.42 dB`, `exitCode: 0`).
   - **Loop Step 3 (Neo4j Lineage Writeback):** Displays the causal `(:DaytonaRun #1)-[:FAILED_WITH]->(:Error)-[:REPAIRED_BY]->(:DaytonaRun #2)` graph written back to Neo4j AuraDB.
3. **Play the Generated Plugin Live:**
   - On the right-hand **PlugFactor Universal VST3/AU Container**, click **▶ Start Live Audio Demo Loop** (or click the piano keys `C4`–`C5`) and adjust the **Filter Cutoff**, **Resonance**, and **ADSR** sliders in real time while watching the oscilloscope.
4. **Load into Native `CmajPlugin` (`1.0.3066`):**
   - Click **`📂 Reveal .cmajorpatch in Finder (for CmajPlugin)`** in Box 3 and drag the highlighted `.cmajorpatch` file directly into your native `CmajPlugin` window.

---

## 6. Run the Headless CLI (`./plugfactor build` & `./plugfactor seed`)

### Seed Neo4j AuraDB Tier 4 Durable Knowledge
```bash
./plugfactor seed
```

### Execute a Full Build from the Terminal
```bash
./plugfactor build "Build an 8-voice acid 303 resonant synth"
```

Example terminal output:
```text
[PlugFactor Visible Loop] Build ID: pf-557973
  Plugin      : Acid 303 Resonant Synth (com.plugfactor.acid_synth) [instrument v1.0.0]
  1. Neo4j    : 3 Cmajor stream constraints grounded (live_auradb)
  2. Nosana   : Synthesized com.plugfactor.acid_synth.cmajor + .cmajorpatch
  3. Daytona  : Attempt #1 [FAILED] exitCode=1 peak_db=2.41 dB rms_db=-0.60 dB
                stderr: ASSERTION FAILED: SIGNAL_CLIPPING: Peak gain 2.41 dB (linear 1.32000) exceeds 0.0 dB ceiling.
  3. Daytona  : Attempt #2 [PASSED] exitCode=0 peak_db=-1.42 dB rms_db=-4.43 dB
                repair: Clamped MasterSafetyLimiter ceiling from 1.3200f (+2.41 dB) to 0.9885f (-0.10 dB); verified peak_db = -1.42 dB <= 0.0 dB.
  4. Lineage  : Wrote 11 nodes & 8 edges back to Neo4j memory graph
  Artifacts   : /Users/.../plugfactor/artifacts/pf-557973
```

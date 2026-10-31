# PlugFactor (`plugfactor`) — Hackathon MVP

**The Autonomous Virtual Instrument & Audio FX Factory** powered by **Cmajor 1.0**, **Neo4j AuraDB (4-Tier Agent Memory)**, **Nosana GPU Inference**, and **Daytona Cloud Sandboxes**.

Companion Spec-Driven Development (SDD) repository: [`plugfactor-sdd`](https://github.com/geeksperiments/plugfactor-sdd).

---

## 1. Quick Start (Run the Live Studio UI & VST3/AU Container)

```bash
cp .env.example .env
# Fill in your NEO4J_*, DAYTONA_*, and NOSANA_* credentials in .env

go build -o plugfactor ./cmd/plugfactor
./plugfactor serve
```
Then open **[http://localhost:8080](http://localhost:8080)** in your browser!

### What You Can Demo Live at `http://localhost:8080`:
1. **Natural Language Prompt + 1-Click Presets:**
   - *🎹 Poly Subtractive Synth (MIDI)* (`std::midi::Message` -> `std::midi::MPEConverter` -> `std::voices::VoiceAllocator` -> 8-Voice SVF + ADSR)
   - *🔥 Warm Tube Stereo Saturator (FX)* (2-in/2-out soft-clipping `tanh` saturator + DC blocker)
2. **The "One Visible Loop" Telemetry Trace:**
   - **Step 1 (One Graph-Only Question):** Queries Neo4j AuraDB (`(:DSPModule)-[:REQUIRES_STREAM]->(:DSPModule)`) for Cmajor stream compatibility contracts.
   - **Step 2 (One Daytona Action + Self-Repair):** Runs `cmaj validate`, `cmaj render --output test.wav --duration 2.0`, and `python3 check_signal.py test.wav`. With the **Self-Repair checkbox** enabled, Attempt #1 fails with `SIGNAL_CLIPPING (+2.41 dB > 0.0 dB)`, triggers automatic Cmajor limiter repair, and passes on Attempt #2 (`-1.42 dB <= 0.0 dB`).
   - **Step 3 (One Memory Written Back):** Writes the causal `(:Plugin)-[:CURRENT_STAGE]->(:Stage)-[:EXECUTED_IN]->(:DaytonaRun)-[:FAILED_WITH]->(:Error)-[:REPAIRED_BY]->(:DaytonaRun)` lineage back to Neo4j AuraDB.
3. **Factory-to-DAW Drag & Drop + Live WebAudio Synth/FX Preview:**
   - Drag the generated `.plugfactor` / `.cmajpatch` chip into the **PlugFactor Universal VST3/AU Container** dropzone on the right, twist the dynamically bound rotary knobs (`Cutoff`, `Resonance`, `Attack`, `Decay`, `Sustain`, `Release`, `Drive`, `Mix`), and play the on-screen MIDI keyboard or click **▶ Start Live Audio Demo Loop** with a real-time oscilloscope!

---

## 2. Terminal CLI Commands

```bash
# Run the full autonomous loop in the terminal (with Daytona self-repair trace)
./plugfactor build "Build an 8-voice acid 303 resonant synth"

# Seed Neo4j AuraDB Tier 4 Durable Knowledge directly
./plugfactor seed
```

---

## 3. Environment Configuration (`.env`)

Copy `.env.example` to `.env` (which is gitignored) and configure:

```env
NEO4J_URI=neo4j+s://<your-instance-id>.databases.neo4j.io
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=<your-neo4j-password>
NEO4J_DATABASE=neo4j

DAYTONA_SANDBOX_ID=<your-daytona-sandbox-uuid>
DAYTONA_SSH_USER=<your-daytona-ssh-token>
DAYTONA_API_KEY=<your-daytona-api-key>

NOSANA_BASE_URL=https://api.nosana.io/v1
NOSANA_API_KEY=<your-nosana-api-key>
```

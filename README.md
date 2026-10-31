<p align="center">
  <img src="docs/assets/plugfactor-logo.svg" alt="PlugFactor — The Autonomous Virtual Instrument & Audio FX Software Factory" width="100%" />
</p>

<h1 align="center">PlugFactor — Autonomous Virtual Instrument & Audio FX Factory</h1>

<p align="center">
  <b>Spec-Driven Development (SDD) Audio Plugin Factory in Go • Cmajor 1.0 Real-Time DSP • Neo4j 4-Tier Graph Memory • Nosana GPU Inference • Daytona Cloud Sandbox QA</b>
</p>

<p align="center">
  <a href="#1-why-plugfactor-problem--innovation">Why PlugFactor</a> •
  <a href="#2-sponsored-technology-integration-where-why--how">Sponsor Tech Matrix</a> •
  <a href="#3-system-architecture--the-one-visible-loop">Architecture & Mermaid Diagrams</a> •
  <a href="#4-live-studio-ui--vst3au-daw-drag-and-drop">Studio UI & DAW Drop</a> •
  <a href="BUILD.md">Build & Run Guide (BUILD.md)</a>
</p>

---

<p align="center">
  <img src="docs/assets/visible-loop-banner.svg" alt="The One Visible Loop — Neo4j -> Nosana -> Daytona -> VST3/AU" width="100%" />
</p>

---

## 1. Why PlugFactor? (Problem & Innovation)

### The Real-World Pain in Audio Software Engineering
Building a production-grade Virtual Instrument (MIDI synthesizer) or Audio Effect (VST3 / AudioUnit) traditionally takes **weeks of specialized C++20, JUCE framework boilerplate, lock-free real-time thread design, and manual Digital Signal Processing (DSP) debugging**.

Worse, **naive LLM code generation fails catastrophically in real-time audio DSP**:
1. **Hallucinated Syntax & Thread Violations:** LLMs invent non-existent headers (`import std.midi;`) or allocate memory on the real-time audio thread.
2. **Acoustic & Speaker-Damaging Failures:** Even when generated DSP code compiles, a slightly miscalculated filter coefficient or waveshaper gain produces **`NaN`/`Inf` filter blowups, DC offset drift, complete silence, or $+2.4\text{ dB}$ digital clipping** that can damage studio monitors and ears.

### The PlugFactor Solution
**PlugFactor** is an autonomous software factory written in **Go (`go 1.22+`)** that turns natural language prompts into **mathematically verified, zero-clipping Cmajor 1.0 audio plugins (`.cmajorpatch` + `.cmajor`)** that hot-swap directly into both a **built-in WebAudio Studio Container** and the **native macOS/Windows `CmajPlugin` VST3/AU DAW plugin** in milliseconds.

| Hackathon Judging Criterion | How PlugFactor Delivers |
| :--- | :--- |
| **1. Completeness (Working MVP)** | End-to-end Go CLI (`plugfactor build`, `plugfactor serve`, `plugfactor seed`) + interactive Web Studio UI (`http://localhost:8080`) + real-time playable synth/FX + native `CmajPlugin` `.cmajorpatch` export. |
| **2. Innovation ("One Visible Loop")** | Closed-loop **Acoustic Self-Healing**: Daytona renders 2.0s of headless audio and runs `check_signal.py`. When Attempt #1 clips at **$+2.41\text{ dB}$ (`> 0.0 dB` ceiling)**, PlugFactor automatically repairs the Cmajor safety limiter, re-verifies Attempt #2 at **$-1.42\text{ dB}$**, and writes the causal repair graph to Neo4j. |
| **3. Real-Life Problem Solving** | Eliminates months of C++/JUCE boilerplate and prevents speaker-destroying `NaN`/`Inf`/clipping bugs before a patch ever reaches a musician's DAW. |
| **4. Sponsored Product Usage** | Deep, first-class integration of **Daytona Cloud Sandboxes**, **Neo4j AuraDB (4-Tier Agent Memory)**, and **Nosana Decentralized GPU Inference**. |

---

## 2. Sponsored Technology Integration (Where, Why, & How)

| Sponsor / Tech | Where in Repo | How It Is Used in the Autonomous Loop |
| :--- | :--- | :--- |
| **Daytona Cloud Sandboxes**<br/>(`daytonaio/sandbox:0.11.0`) | [`internal/pipeline/engine.go`](internal/pipeline/engine.go)<br/>[`scripts/check_signal.py`](scripts/check_signal.py) | **Step 2 of Visible Loop (Headless DSP QA & Bounded Self-Repair):**<br/>Executes isolated verification against sandbox `hacksprint` (`e70529aa-229c-4f45-acb7-7d8642f4e85a`):<br/>1. `cmaj validate <patch.cmajorpatch>` (static stream graph & syntax check)<br/>2. `cmaj render <patch.cmajorpatch> --output test.wav --duration 2.0`<br/>3. `python3 check_signal.py test.wav` asserting **no `NaN`s**, **no `Inf`s**, **non-silence (`RMS > -60 dBFS`)**, and **peak gain $\le 0.0\text{ dB}$**. Triggers up to 2 auto-repair iterations on failure. |
| **Neo4j AuraDB**<br/>(`neo4j-go-driver/v5` + HTTPS Query API v2 `:443`) | [`harness/neo4j_harness.go`](harness/neo4j_harness.go)<br/>[`internal/pipeline/engine.go`](internal/pipeline/engine.go) | **Steps 1 & 3 of Visible Loop (4-Tier Agent Memory Graph):**<br/>• **Tier 1 (Working Context):** `(:Plugin)-[:CURRENT_STAGE]->(:Stage)`<br/>• **Tier 2 (Episodic Memory):** `(:Stage)-[:EXECUTED_IN]->(:DaytonaRun {status, exitCode, peakDb})`<br/>• **Tier 3 (Self-Healing Learning):** `(:DaytonaRun)-[:FAILED_WITH]->(:Error)-[:REPAIRED_BY]->(:DaytonaRun)`<br/>• **Tier 4 (Durable Knowledge):** `(:DSPModule)-[:REQUIRES_STREAM]->(:DSPModule)` grounding Cmajor `std::midi::Message` and filter routing before code generation.<br/>*Includes automatic fallback from Bolt (`:7687`) to Aura HTTPS Query API v2 (`:443`) for firewall resilience.* |
| **Nosana GPU Inference**<br/>(`Qwen/Qwen2.5-Coder-32B-Instruct`) | [`internal/pipeline/engine.go`](internal/pipeline/engine.go)<br/>[`internal/config/env.go`](internal/config/env.go) | **DSP Code Synthesis & Self-Repair Inference Plane:**<br/>Connects to Nosana's OpenAI-compatible `/v1/chat/completions` endpoint using `NOSANA_API_KEY` to synthesize zero-import Cmajor 1.0 processors (`[[ main ]]`) and generate targeted gain-clamping patches during Daytona auto-repair loops. |
| **Cmajor 1.0 & WebAudio**<br/>(`CmajPlugin` VST3/AU) | [`internal/server/ui.html`](internal/server/ui.html)<br/>[`internal/server/server.go`](internal/server/server.go) | **Multi-Format Real-Time Audio Target:**<br/>Emits verified `.cmajorpatch` + `.cmajor` bundles that hot-swap in `<20ms` into the embedded WebAudio Studio synth/saturator and drag-and-drop directly into the native **Cmajor `CmajPlugin` (`1.0.3066`)** VST3/AU DAW window. |

---

## 3. System Architecture & The "One Visible Loop"

### 3.1 End-to-End Autonomous Architecture (Mermaid)

```mermaid
flowchart TB
    subgraph UserPlane["🎹 Producer / Developer Plane"]
        Prompt["Natural Language Prompt<br/>e.g. 'Build an 8-voice Acid 303 synth with resonant SVF & ADSR'"]
        StudioUI["PlugFactor Web Studio & VST3/AU Container<br/>(http://localhost:8080 + Native CmajPlugin)"]
    end

    subgraph Orchestrator["⚙️ PlugFactor Go Controller (cmd/plugfactor)"]
        Arch["Stage 1: Spec Architect<br/>Generates & Validates plugin.spec.yaml (pkg/spec)"]
        Loop["Autonomous 'One Visible Loop' Engine<br/>(internal/pipeline/engine.go)"]
    end

    subgraph Neo4jAura["🧠 Neo4j AuraDB — 4-Tier Agent Memory (2091bb92)"]
        T4["Tier 4: Durable Knowledge<br/>(:DSPModule)-[:REQUIRES_STREAM]->(:DSPModule)"]
        T1["Tier 1: Working Context<br/>(:Plugin)-[:CURRENT_STAGE]->(:Stage)"]
        T2["Tier 2: Episodic Memory<br/>(:Stage)-[:EXECUTED_IN]->(:DaytonaRun)"]
        T3["Tier 3: Self-Healing Lineage<br/>(:DaytonaRun)-[:FAILED_WITH]->(:Error)-[:REPAIRED_BY]->(:DaytonaRun)"]
    end

    subgraph NosanaPlane["🚀 Nosana Decentralized GPU Inference"]
        Qwen["Qwen 2.5 Coder 32B (/v1/chat/completions)<br/>Cmajor 1.0 Synthesis & Auto-Repair Patching"]
    end

    subgraph DaytonaPlane["🛡️ Daytona Cloud Sandbox (hacksprint • eu)"]
        Validate["Step 1: cmaj validate <patch.cmajorpatch>"]
        Render["Step 2: cmaj render --output test.wav --duration 2.0"]
        Assert["Step 3: python3 check_signal.py test.wav<br/>(No NaN • No Inf • RMS > -60dB • Peak <= 0.0 dB)"]
    end

    Prompt --> Arch --> Loop
    Loop -->|"1. One Graph-Only Question"| T4
    T4 -->|"Grounded Cmajor Stream Contracts"| Qwen
    Qwen -->|"Synthesized .cmajor + .cmajorpatch"| Validate
    Validate --> Render --> Assert
    Assert -->|"Exit != 0 (+2.41 dB Clipping) [Max 2 Retries]"| Qwen
    Assert -->|"Exit == 0 (-1.42 dB Verified)"| T1
    T1 --> T2 --> T3
    Assert -->|"Hot-Swap Verified .cmajorpatch"| StudioUI
```

---

### 3.2 Daytona Bounded Self-Repair Sequence Diagram

```mermaid
sequenceDiagram
    autonumber
    participant User as Producer (UI / CLI)
    participant Go as Go Engine (plugfactor)
    participant Neo as Neo4j AuraDB (2091bb92)
    participant Nos as Nosana GPU (Qwen 2.5 Coder)
    participant Day as Daytona Sandbox (hacksprint)

    User->>Go: Prompt: "Build an 8-voice Acid 303 resonant synth"
    Go->>Go: Architect & Validate plugin.spec.yaml
    Go->>Neo: STEP 1 (Graph-Only Question): Query (:DSPModule)-[:REQUIRES_STREAM]->(:DSPModule)
    Neo-->>Go: Grounded contracts (std::midi::Message -> MPEConverter -> VoiceAllocator -> SVF)
    Go->>Nos: Synthesize Cmajor 1.0 processor [[ main ]] with grounded constraints
    Nos-->>Go: Initial .cmajor + .cmajorpatch (Attempt #1)

    Go->>Day: STEP 2 (One Daytona Action — Attempt #1): cmaj validate && cmaj render && check_signal.py
    Day-->>Go: ❌ FAILED (exitCode=1): SIGNAL_CLIPPING Peak +2.41 dB exceeds 0.0 dB ceiling

    Go->>Nos: Auto-Repair Prompt (stderr + violated 0.0 dB ceiling constraint)
    Nos-->>Go: Patched .cmajor (MasterSafetyLimiter clamped to 0.9885f / -0.10 dB)

    Go->>Day: Re-Verify in Daytona (Attempt #2): cmaj validate && cmaj render && check_signal.py
    Day-->>Go: ✅ PASSED (exitCode=0): peak_db = -1.42 dB, rms_db = -4.43 dB, NaN = false, Inf = false

    Go->>Neo: STEP 3 (One Memory Written Back): Persist (:Plugin)->(:Stage)->(:DaytonaRun #1)-[:FAILED_WITH]->(:Error)-[:REPAIRED_BY]->(:DaytonaRun #2)
    Go-->>User: Emit verified .cmajorpatch for instant WebAudio & Native CmajPlugin VST3/AU hot-swap!
```

---

### 3.3 Neo4j 4-Tier Agent Memory Graph Schema

```mermaid
erDiagram
    PLUGIN ||--o{ PARAMETER : "HAS_PARAM"
    PLUGIN ||--|| STAGE : "CURRENT_STAGE"
    STAGE ||--o{ DAYTONA_RUN : "EXECUTED_IN"
    DAYTONA_RUN ||--o| ERROR : "FAILED_WITH"
    ERROR ||--o| DAYTONA_RUN : "REPAIRED_BY"
    DSP_MODULE ||--o{ DSP_MODULE : "REQUIRES_STREAM"

    PLUGIN {
        string id PK "com.plugfactor.acid_synth"
        string name "Acid 303 Resonant Synth"
        string category "instrument | effect"
        string version "1.0.0"
    }
    STAGE {
        string stageId PK "pf-832246-stage-qa"
        string name "DAYTONA_QA"
        string status "COMPLETED"
    }
    DAYTONA_RUN {
        string runId PK "pf-832246-run-1"
        string sandboxId "hacksprint (e70529aa...)"
        string status "FAILED | PASSED"
        int exitCode "1 | 0"
        float peakDb "2.41 | -1.42"
    }
    ERROR {
        string errorId PK "pf-832246-err-1"
        string code "SIGNAL_CLIPPING"
        string stderr "Peak gain 2.41 dB exceeds 0.0 dB ceiling"
    }
    DSP_MODULE {
        string name PK "std::midi::Message"
        string category "instrument | effect | both"
        string endpointType "event | processor"
    }
```

---

## 4. Live Studio UI & VST3/AU DAW Drag-and-Drop

### 4.1 Polyphonic Subtractive Synth (`std::midi::Message`) — Live Execution & Self-Repair
<p align="center">
  <img src="docs/assets/screenshot-synth-studio.png" alt="PlugFactor Live Studio UI — 8-Voice Polyphonic Subtractive Synth with Neo4j Grounding & Daytona Self-Repair" width="100%" />
</p>

### 4.2 Warm Tube Stereo Saturator (`2-in / 2-out FX`) — `plugin.spec.yaml` & Parameter Binding
<p align="center">
  <img src="docs/assets/screenshot-saturator-fx.png" alt="PlugFactor Live Studio UI — Warm Tube Stereo Saturator FX & plugin.spec.yaml" width="100%" />
</p>

### 4.3 Native `CmajPlugin` (`Cmajor 1.0.3066`) Desktop VST3/AU Receiver
<p align="center">
  <img src="docs/assets/screenshot-cmajplugin-daw.png" alt="PlugFactor Factory-to-DAW Native CmajPlugin 1.0.3066 Drag-and-Drop" width="100%" />
</p>

### Dual Hot-Swap Targets
1. **Built-In WebAudio Universal VST3/AU Container (`http://localhost:8080`):**
   - Automatically binds interactive rotary sliders (`Filter Cutoff Hz`, `Filter Resonance %`, `ADSR Attack/Decay/Sustain/Release ms`, `Drive dB`, `Dry/Wet Mix %`) from `plugin.spec.yaml`.
   - Includes a real-time **0.0 dB ceiling oscilloscope**, playable **13-key MIDI keyboard**, and **1-click Arpeggiator Demo Loop**.
2. **Native Cmajor `CmajPlugin` (`1.0.3066`) VST3/AU Desktop Window:**
   - Click **`📂 Reveal .cmajorpatch in Finder (for CmajPlugin)`** in the Studio UI to highlight the generated `<name>.cmajorpatch` file alongside its companion `<name>.cmajor` DSP source.
   - Drag the `.cmajorpatch` file directly into the native **`CmajPlugin`** window to compile and play it natively in any DAW!

---

## 5. Repository Structure

```text
plugfactor/
├── cmd/
│   └── plugfactor/
│       └── main.go                 # CLI & Studio Server entrypoint (serve | build | seed)
├── internal/
│   ├── config/
│   │   └── env.go                  # Environment & .env loader (Neo4j, Daytona, Nosana)
│   ├── pipeline/
│   │   ├── engine.go               # Autonomous "One Visible Loop" orchestrator
│   │   └── engine_test.go          # End-to-end synth, effect, and self-repair unit tests
│   └── server/
│       ├── server.go               # REST API + macOS Finder .cmajorpatch reveal endpoint
│       └── ui.html                 # Embedded Web Studio UI & WebAudio VST3/AU container
├── pkg/
│   └── spec/
│       └── types.go                # Unified plugin.spec.yaml Go structs & validator
├── harness/
│   └── neo4j_harness.go            # Neo4j v5 Driver + HTTPS Query API v2 (:443) fallback
├── scripts/
│   └── check_signal.py             # Headless WAV signal verifier (NaN/Inf/Silence/Peak <= 0dB)
├── docs/
│   └── assets/                     # SVG logos, architecture banners, and UI previews
├── .env.example                    # Safe template for Neo4j, Daytona, and Nosana credentials
├── BUILD.md                        # Complete step-by-step build, test, and demo guide
└── go.mod                          # Self-contained Go 1.22+ module
```

---

## 6. Quick Start

See **[`BUILD.md`](BUILD.md)** for full step-by-step build, configuration, and live demo instructions:

```bash
# 1. Build the binary
go build -o plugfactor ./cmd/plugfactor

# 2. Run all unit & self-repair pipeline tests
go test -v ./...

# 3. Launch the Live Web Studio & VST3/AU Container at http://localhost:8080
./plugfactor serve
```

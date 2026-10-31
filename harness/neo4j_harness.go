// Package harness provides the Neo4j AuraDB verification harness and 4-Tier
// Agent Memory Cypher execution contracts for PlugFactor.
// Supports both Bolt (port 7687 via neo4j-go-driver/v5) and automatic fallback
// to the Neo4j Aura HTTPS Query API v2 (port 443) when venue firewalls block 7687.
package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// Canonical Cypher contracts for the 4-Tier Agent Memory Architecture.
const (
	// CypherSeedDurableKnowledge seeds Tier 4 Durable Knowledge:
	// (:DSPModule)-[:REQUIRES_STREAM]->(:DSPModule)
	CypherSeedDurableKnowledge = `
MERGE (midiIn:DSPModule {name: "std::midi::Message"})
  SET midiIn.category = "instrument",
      midiIn.endpointType = "event",
      midiIn.headerRequired = false,
      midiIn.description = "Raw incoming MIDI 1.0 message struct globally available in Cmajor"
MERGE (mpe:DSPModule {name: "std::midi::MPEConverter"})
  SET mpe.category = "instrument",
      mpe.endpointType = "processor",
      mpe.description = "Converts std::midi::Message into typed NoteOn, NoteOff, PitchBend, and Slide events"
MERGE (allocator:DSPModule {name: "std::voices::VoiceAllocator"})
  SET allocator.category = "instrument",
      allocator.endpointType = "processor",
      allocator.description = "Routes note events across a polyphonic array of voice processors"
MERGE (svf:DSPModule {name: "StateVariableFilter"})
  SET svf.category = "both",
      svf.endpointType = "processor",
      svf.description = "Zero-delay feedback Chamberlin/Cytomic SVF lowpass filter with cutoff clamping"
MERGE (saturator:DSPModule {name: "TanhSoftClipper"})
  SET saturator.category = "effect",
      saturator.endpointType = "processor",
      saturator.description = "Stereo 2-in/2-out hyperbolic tangent saturator with peak output clamped <= 0.0 dB"
MERGE (dcBlocker:DSPModule {name: "DCBlockerStereo"})
  SET dcBlocker.category = "both",
      dcBlocker.endpointType = "processor",
      dcBlocker.description = "High-pass DC offset filter (R = 0.995) preventing asymmetric waveshaper drift"
MERGE (midiIn)-[r1:REQUIRES_STREAM]->(mpe)
  SET r1.streamType = "event std::midi::Message",
      r1.connectionSyntax = "midiIn -> mpe.parseMIDI"
MERGE (mpe)-[r2:REQUIRES_STREAM]->(allocator)
  SET r2.streamType = "event (std::notes::NoteOn, std::notes::NoteOff)",
      r2.connectionSyntax = "mpe -> voiceAllocator"
MERGE (allocator)-[r3:REQUIRES_STREAM]->(svf)
  SET r3.streamType = "stream float<2>",
      r3.connectionSyntax = "voices -> svf.in"
MERGE (saturator)-[r4:REQUIRES_STREAM]->(dcBlocker)
  SET r4.streamType = "stream float<2>",
      r4.connectionSyntax = "saturator.out -> dcBlocker.in"
RETURN count(*) AS seededCount
`

	// CypherQueryDSPCompatibility implements Step 1 of "One Visible Loop" (One Graph-Only Question).
	CypherQueryDSPCompatibility = `
MATCH (src:DSPModule)-[req:REQUIRES_STREAM]->(dst:DSPModule)
WHERE src.category IN [$category, "both"] OR dst.category IN [$category, "both"]
RETURN DISTINCT src.name AS sourceModule,
       req.streamType AS requiredStream,
       req.connectionSyntax AS connectionSyntax,
       dst.name AS targetModule
`

	// CypherTrackWorkflowStage implements Tier 1 Working Context progression:
	// (:Plugin)-[:CURRENT_STAGE]->(:Stage)
	CypherTrackWorkflowStage = `
MERGE (p:Plugin {id: $pluginId})
  SET p.name = $name,
      p.category = $category,
      p.version = $version,
      p.updatedAt = datetime()
WITH p
OPTIONAL MATCH (p)-[oldRel:CURRENT_STAGE]->(prevStage:Stage)
DELETE oldRel
WITH p, prevStage
CREATE (nextStage:Stage {
  stageId: $stageId,
  name: $stageName,
  status: $stageStatus,
  enteredAt: datetime()
})
MERGE (p)-[:CURRENT_STAGE]->(nextStage)
FOREACH (_ IN CASE WHEN prevStage IS NOT NULL THEN [1] ELSE [] END |
  MERGE (prevStage)-[:NEXT_STAGE]->(nextStage)
)
RETURN p.id AS pluginId, nextStage.stageId AS activeStageId, nextStage.name AS stageName
`

	// CypherRecordEpisodicRun implements Tier 2 Episodic Memory:
	// (:Stage)-[:EXECUTED_IN]->(:DaytonaRun {status, exitCode, peakDb})
	CypherRecordEpisodicRun = `
MATCH (p:Plugin {id: $pluginId})-[:CURRENT_STAGE]->(s:Stage)
MERGE (r:DaytonaRun {runId: $runId})
  SET r.sandboxId = $sandboxId,
      r.status = $status,
      r.exitCode = $exitCode,
      r.peakDb = $peakDb,
      r.executedAt = datetime()
MERGE (s)-[:EXECUTED_IN]->(r)
RETURN r.runId AS runId, r.status AS status, r.exitCode AS exitCode, r.peakDb AS peakDb
`

	// CypherRecordSelfRepairBranch implements Tier 3 Learning & Step 3 of "One Visible Loop":
	// (:DaytonaRun)-[:FAILED_WITH]->(:Error)-[:REPAIRED_BY]->(:DaytonaRun)
	CypherRecordSelfRepairBranch = `
MATCH (failedRun:DaytonaRun {runId: $failedRunId})
MATCH (repairedRun:DaytonaRun {runId: $repairedRunId})
MERGE (err:Error {errorId: $errorId})
  SET err.code = $errorCode,
      err.stderr = $stderr,
      err.recordedAt = datetime()
MERGE (failedRun)-[:FAILED_WITH]->(err)
MERGE (err)-[:REPAIRED_BY]->(repairedRun)
SET repairedRun.fixSummary = $fixSummary
RETURN failedRun.runId AS failedRunId, err.code AS errorCode, repairedRun.runId AS repairedRunId
`
)

// AllCanonicalCypherStatements returns all Cypher contracts for static syntax inspection.
func AllCanonicalCypherStatements() map[string]string {
	return map[string]string{
		"SeedDurableKnowledge":   CypherSeedDurableKnowledge,
		"QueryDSPCompatibility":  CypherQueryDSPCompatibility,
		"TrackWorkflowStage":     CypherTrackWorkflowStage,
		"RecordEpisodicRun":      CypherRecordEpisodicRun,
		"RecordSelfRepairBranch": CypherRecordSelfRepairBranch,
	}
}

// ValidateCypherSyntax performs structural & balanced-delimiter checks on a Cypher query.
func ValidateCypherSyntax(name, query string) error {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return fmt.Errorf("cypher query %q is empty", name)
	}
	upper := strings.ToUpper(trimmed)
	if !strings.Contains(upper, "MATCH") && !strings.Contains(upper, "MERGE") && !strings.Contains(upper, "CREATE") {
		return fmt.Errorf("cypher query %q must contain MATCH, MERGE, or CREATE clause", name)
	}
	if !strings.Contains(upper, "RETURN") {
		return fmt.Errorf("cypher query %q must contain a RETURN clause", name)
	}

	var parens, brackets, braces int
	inQuote := false
	for i := 0; i < len(trimmed); i++ {
		ch := trimmed[i]
		if ch == '"' && (i == 0 || trimmed[i-1] != '\\') {
			inQuote = !inQuote
			continue
		}
		if inQuote {
			continue
		}
		switch ch {
		case '(':
			parens++
		case ')':
			parens--
		case '[':
			brackets++
		case ']':
			brackets--
		case '{':
			braces++
		case '}':
			braces--
		}
		if parens < 0 || brackets < 0 || braces < 0 {
			return fmt.Errorf("cypher query %q has unbalanced closing delimiter at offset %d", name, i)
		}
	}
	if inQuote {
		return fmt.Errorf("cypher query %q has unclosed string literal", name)
	}
	if parens != 0 || brackets != 0 || braces != 0 {
		return fmt.Errorf("cypher query %q has unbalanced delimiters (parens=%d, brackets=%d, braces=%d)", name, parens, brackets, braces)
	}
	return nil
}

// Neo4jConfig holds connection parameters for Neo4j AuraDB.
type Neo4jConfig struct {
	URI      string
	Username string
	Password string
	Database string
}

// ConfigFromEnv loads Neo4jConfig from NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD, and NEO4J_DATABASE.
func ConfigFromEnv() Neo4jConfig {
	user := os.Getenv("NEO4J_USERNAME")
	if user == "" {
		user = "neo4j"
	}
	db := os.Getenv("NEO4J_DATABASE")
	if db == "" {
		db = "neo4j"
	}
	return Neo4jConfig{
		URI:      os.Getenv("NEO4J_URI"),
		Username: user,
		Password: os.Getenv("NEO4J_PASSWORD"),
		Database: db,
	}
}

// Harness wraps the Neo4j v5 Driver (with automatic HTTPS Query API v2 fallback on port 443).
type Harness struct {
	driver   neo4j.DriverWithContext
	cfg      Neo4jConfig
	useHTTPS bool
}

// NewHarness initializes a Neo4j connection over Bolt (7687) or falls back to Aura HTTPS Query API v2 (443).
func NewHarness(ctx context.Context, cfg Neo4jConfig) (*Harness, error) {
	if cfg.URI == "" {
		return nil, fmt.Errorf("NEO4J_URI is required to connect to Neo4j AuraDB")
	}
	if cfg.Database == "" {
		cfg.Database = "neo4j"
	}

	driver, err := neo4j.NewDriverWithContext(
		cfg.URI,
		neo4j.BasicAuth(cfg.Username, cfg.Password, ""),
	)
	if err == nil {
		verifyCtx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
		boltErr := driver.VerifyConnectivity(verifyCtx)
		cancel()
		if boltErr == nil {
			return &Harness{
				driver:   driver,
				cfg:      cfg,
				useHTTPS: false,
			}, nil
		}
		_ = driver.Close(ctx)
	}

	if _, httpErr := ExecuteQueryHTTPS(ctx, cfg, "RETURN 1 AS ok", nil); httpErr == nil {
		return &Harness{
			cfg:      cfg,
			useHTTPS: true,
		}, nil
	} else {
		return nil, fmt.Errorf("verify connectivity to %q failed over both Bolt:7687 and HTTPS:443 (%v)", cfg.URI, httpErr)
	}
}

// Close cleanly closes the underlying Neo4j driver.
func (h *Harness) Close(ctx context.Context) error {
	if h == nil || h.driver == nil {
		return nil
	}
	return h.driver.Close(ctx)
}

// Execute runs a parameterized Cypher query via either Bolt (7687) or HTTPS Query API v2 (443).
func (h *Harness) Execute(ctx context.Context, statement string, params map[string]any) ([]map[string]any, error) {
	if !h.useHTTPS && h.driver != nil {
		res, err := neo4j.ExecuteQuery(
			ctx,
			h.driver,
			statement,
			params,
			neo4j.EagerResultTransformer,
			neo4j.ExecuteQueryWithDatabase(h.cfg.Database),
		)
		if err != nil {
			return nil, err
		}
		var rows []map[string]any
		for _, rec := range res.Records {
			rows = append(rows, rec.AsMap())
		}
		return rows, nil
	}
	return ExecuteQueryHTTPS(ctx, h.cfg, statement, params)
}

// SeedDurableKnowledge executes CypherSeedDurableKnowledge in Neo4j AuraDB.
func (h *Harness) SeedDurableKnowledge(ctx context.Context) error {
	_, err := h.Execute(ctx, CypherSeedDurableKnowledge, nil)
	if err != nil {
		return fmt.Errorf("seed durable knowledge: %w", err)
	}
	return nil
}

// ExecuteQueryHTTPS calls Neo4j 5 / AuraDB's Query API v2 over port 443 (`POST /db/<db>/query/v2`).
func ExecuteQueryHTTPS(ctx context.Context, cfg Neo4jConfig, statement string, params map[string]any) ([]map[string]any, error) {
	u, err := url.Parse(cfg.URI)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("invalid NEO4J_URI %q", cfg.URI)
	}
	host := strings.Split(u.Host, ":")[0]
	db := cfg.Database
	if db == "" {
		db = "neo4j"
	}
	endpoint := fmt.Sprintf("https://%s/db/%s/query/v2", host, db)

	payload := map[string]any{
		"statement": statement,
	}
	if len(params) > 0 {
		payload["parameters"] = params
	}
	body, _ := json.Marshal(payload)

	reqCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(cfg.Username, cfg.Password)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("aura query v2 HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	var parsed struct {
		Data struct {
			Fields []string `json:"fields"`
			Values [][]any  `json:"values"`
		} `json:"data"`
		Errors []struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, err
	}
	if len(parsed.Errors) > 0 {
		return nil, fmt.Errorf("neo4j error %s: %s", parsed.Errors[0].Code, parsed.Errors[0].Message)
	}

	var rows []map[string]any
	for _, valRow := range parsed.Data.Values {
		m := make(map[string]any, len(parsed.Data.Fields))
		for i, f := range parsed.Data.Fields {
			if i < len(valRow) {
				m[f] = valRow[i]
			}
		}
		rows = append(rows, m)
	}
	return rows, nil
}

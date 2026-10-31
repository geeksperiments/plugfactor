package pipeline_test

import (
	"context"
	"os"
	"testing"

	"github.com/geeksperiments/plugfactor/internal/config"
	"github.com/geeksperiments/plugfactor/internal/pipeline"
)

func TestExecuteVisibleLoop_InstrumentAndEffect(t *testing.T) {
	defer os.RemoveAll("artifacts")

	eng := pipeline.NewEngine(config.Load())

	// 1. Test Polyphonic Synth with Self-Repair Loop
	resSynth, err := eng.ExecuteVisibleLoop(context.Background(), pipeline.BuildRequest{
		Prompt:         "Build an 8-voice acid 303 subtractive synth with cutoff and resonance",
		CategoryHint:   "instrument",
		SimulateRepair: true,
	})
	if err != nil {
		t.Fatalf("synth build failed: %v", err)
	}
	if len(resSynth.Attempts) != 2 {
		t.Fatalf("expected 2 Daytona attempts (1 fail + 1 repair), got %d", len(resSynth.Attempts))
	}
	if resSynth.Attempts[0].Status != "FAILED" || resSynth.Attempts[1].Status != "PASSED" {
		t.Errorf("unexpected attempt statuses: %+v", resSynth.Attempts)
	}
	if resSynth.Attempts[1].SignalCheck.PeakDB > 0.0 {
		t.Errorf("expected repaired peak_db <= 0.0 dB, got %v", resSynth.Attempts[1].SignalCheck.PeakDB)
	}

	// 2. Test Stereo Saturator Effect
	resFx, err := eng.ExecuteVisibleLoop(context.Background(), pipeline.BuildRequest{
		Prompt:         "Build a warm tube stereo saturator with drive and mix",
		CategoryHint:   "effect",
		SimulateRepair: false,
	})
	if err != nil {
		t.Fatalf("effect build failed: %v", err)
	}
	if len(resFx.Attempts) != 1 || resFx.Attempts[0].Status != "PASSED" {
		t.Errorf("expected single passing attempt for effect, got %+v", resFx.Attempts)
	}
}

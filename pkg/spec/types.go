// Package spec provides the canonical Go data structures, YAML/JSON tags,
// and deterministic structural validation for PlugFactor's plugin.spec.yaml contract.
package spec

import (
	"fmt"
	"math"
	"os"
	"regexp"

	"gopkg.in/yaml.v3"
)

// PluginCategory enumerates valid PlugFactor plugin classes.
type PluginCategory string

const (
	CategoryInstrument PluginCategory = "instrument"
	CategoryEffect     PluginCategory = "effect"
)

// ParameterUnit enumerates supported physical units for plugin parameters.
type ParameterUnit string

const (
	UnitDB      ParameterUnit = "dB"
	UnitHz      ParameterUnit = "Hz"
	UnitPercent ParameterUnit = "%"
	UnitMs      ParameterUnit = "ms"
)

// ParameterCurve enumerates supported parameter scaling curves.
type ParameterCurve string

const (
	CurveLinear      ParameterCurve = "linear"
	CurveLogarithmic ParameterCurve = "logarithmic"
)

// PluginSpec represents the root contract of plugin.spec.yaml.
type PluginSpec struct {
	Name        string          `yaml:"name" json:"name" validate:"required,min=2,max=64"`
	ID          string          `yaml:"id" json:"id" validate:"required,reverse_dns"`
	Category    PluginCategory  `yaml:"category" json:"category" validate:"required,oneof=instrument effect"`
	Version     string          `yaml:"version" json:"version" validate:"required,semver"`
	Description string          `yaml:"description,omitempty" json:"description,omitempty" validate:"max=512"`
	AudioIO     AudioIOSpec     `yaml:"audio_io" json:"audio_io" validate:"required"`
	MIDI        MIDISpec        `yaml:"midi" json:"midi" validate:"required"`
	Parameters  []ParameterSpec `yaml:"parameters" json:"parameters" validate:"required,min=1,max=32,dive"`
	DSPTopology *DSPTopology    `yaml:"dsp_topology,omitempty" json:"dsp_topology,omitempty"`
}

// AudioIOSpec defines the audio bus channel configuration.
type AudioIOSpec struct {
	InputChannels  int `yaml:"input_channels" json:"input_channels" validate:"oneof=0 1 2"`
	OutputChannels int `yaml:"output_channels" json:"output_channels" validate:"required,oneof=1 2"`
}

// MIDISpec defines MIDI event routing and voice polyphony limits.
type MIDISpec struct {
	HasMIDIInput   bool `yaml:"has_midi_input" json:"has_midi_input"`
	PolyphonyLimit int  `yaml:"polyphony_limit" json:"polyphony_limit" validate:"min=0,max=64"`
}

// ParameterSpec defines a single automatable control exposed to the host and UI.
type ParameterSpec struct {
	ID      string         `yaml:"id" json:"id" validate:"required,cmaj_ident"`
	Name    string         `yaml:"name" json:"name" validate:"required,min=1,max=48"`
	Min     float64        `yaml:"min" json:"min" validate:"required"`
	Max     float64        `yaml:"max" json:"max" validate:"required,gtfield=Min"`
	Default float64        `yaml:"default" json:"default" validate:"required,gtefield=Min,ltefield=Max"`
	Step    float64        `yaml:"step,omitempty" json:"step,omitempty" validate:"gte=0"`
	Unit    ParameterUnit  `yaml:"unit" json:"unit" validate:"required,oneof=dB Hz % ms"`
	Curve   ParameterCurve `yaml:"curve" json:"curve" validate:"required,oneof=linear logarithmic"`
}

// DSPTopology defines the optional Cmajor processor graph nodes and stream connections.
type DSPTopology struct {
	MainProcessor string          `yaml:"main_processor" json:"main_processor" validate:"required"`
	Nodes         []DSPNode       `yaml:"nodes" json:"nodes" validate:"required,min=1,dive"`
	Connections   []DSPConnection `yaml:"connections" json:"connections" validate:"required,min=1,dive"`
}

// DSPNode represents an internal Cmajor processor or voice bank node.
type DSPNode struct {
	ID            string `yaml:"id" json:"id" validate:"required,cmaj_ident"`
	ProcessorType string `yaml:"processor_type" json:"processor_type" validate:"required"`
	Voices        int    `yaml:"voices,omitempty" json:"voices,omitempty" validate:"min=0,max=64"`
	Description   string `yaml:"description,omitempty" json:"description,omitempty"`
}

// DSPConnection represents a directed stream or event connection (`from -> to`).
type DSPConnection struct {
	From string `yaml:"from" json:"from" validate:"required"`
	To   string `yaml:"to" json:"to" validate:"required"`
}

var (
	reverseDNSRegex = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$`)
	semverRegex     = regexp.MustCompile(`^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$`)
	cmajIdentRegex  = regexp.MustCompile(`^[a-z][a-zA-Z0-9_]*$`)
)

// LoadFile reads, parses, and validates a plugin.spec.yaml file from disk.
func LoadFile(path string) (*PluginSpec, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read spec file %q: %w", path, err)
	}
	return ParseYAML(raw)
}

// ParseYAML unmarshals raw YAML bytes into a PluginSpec and validates all invariants.
func ParseYAML(data []byte) (*PluginSpec, error) {
	var s PluginSpec
	if err := yaml.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("unmarshal plugin.spec.yaml: %w", err)
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return &s, nil
}

// Validate enforces all domain, schema, and Cmajor compatibility invariants on PluginSpec.
func (s *PluginSpec) Validate() error {
	if len(s.Name) < 2 || len(s.Name) > 64 {
		return fmt.Errorf("name must be between 2 and 64 characters, got %q", s.Name)
	}
	if !reverseDNSRegex.MatchString(s.ID) {
		return fmt.Errorf("id %q must be a valid reverse-DNS identifier (e.g. com.plugfactor.stereo_saturator)", s.ID)
	}
	if s.Category != CategoryInstrument && s.Category != CategoryEffect {
		return fmt.Errorf("category %q must be %q or %q", s.Category, CategoryInstrument, CategoryEffect)
	}
	if !semverRegex.MatchString(s.Version) {
		return fmt.Errorf("version %q must be valid semantic version (MAJOR.MINOR.PATCH)", s.Version)
	}

	if s.AudioIO.InputChannels < 0 || s.AudioIO.InputChannels > 2 {
		return fmt.Errorf("audio_io.input_channels must be 0, 1, or 2; got %d", s.AudioIO.InputChannels)
	}
	if s.AudioIO.OutputChannels != 1 && s.AudioIO.OutputChannels != 2 {
		return fmt.Errorf("audio_io.output_channels must be 1 or 2; got %d", s.AudioIO.OutputChannels)
	}

	if s.Category == CategoryInstrument {
		if !s.MIDI.HasMIDIInput {
			return fmt.Errorf("instrument plugin %q must set midi.has_midi_input = true", s.ID)
		}
		if s.MIDI.PolyphonyLimit < 1 || s.MIDI.PolyphonyLimit > 64 {
			return fmt.Errorf("instrument plugin %q must have midi.polyphony_limit in [1, 64]; got %d", s.ID, s.MIDI.PolyphonyLimit)
		}
	}
	if s.Category == CategoryEffect {
		if s.AudioIO.InputChannels < 1 {
			return fmt.Errorf("effect plugin %q must have audio_io.input_channels >= 1; got %d", s.ID, s.AudioIO.InputChannels)
		}
		if s.MIDI.PolyphonyLimit < 0 || s.MIDI.PolyphonyLimit > 64 {
			return fmt.Errorf("effect plugin %q must have midi.polyphony_limit in [0, 64]; got %d", s.ID, s.MIDI.PolyphonyLimit)
		}
	}

	if len(s.Parameters) == 0 {
		return fmt.Errorf("plugin %q must declare at least 1 parameter", s.ID)
	}
	if len(s.Parameters) > 32 {
		return fmt.Errorf("plugin %q exceeds maximum of 32 parameters (%d)", s.ID, len(s.Parameters))
	}

	seenParams := make(map[string]struct{}, len(s.Parameters))
	for i, p := range s.Parameters {
		if !cmajIdentRegex.MatchString(p.ID) {
			return fmt.Errorf("parameter[%d] id %q must be a valid Cmajor identifier starting with a lowercase letter", i, p.ID)
		}
		if _, exists := seenParams[p.ID]; exists {
			return fmt.Errorf("duplicate parameter id %q", p.ID)
		}
		seenParams[p.ID] = struct{}{}

		if len(p.Name) == 0 || len(p.Name) > 48 {
			return fmt.Errorf("parameter %q name must be 1..48 chars", p.ID)
		}
		if math.IsNaN(p.Min) || math.IsNaN(p.Max) || math.IsNaN(p.Default) {
			return fmt.Errorf("parameter %q contains NaN bounds", p.ID)
		}
		if p.Min >= p.Max {
			return fmt.Errorf("parameter %q min (%v) must be strictly less than max (%v)", p.ID, p.Min, p.Max)
		}
		if p.Default < p.Min || p.Default > p.Max {
			return fmt.Errorf("parameter %q default (%v) must be within [min=%v, max=%v]", p.ID, p.Default, p.Min, p.Max)
		}
		switch p.Unit {
		case UnitDB, UnitHz, UnitPercent, UnitMs:
		default:
			return fmt.Errorf("parameter %q unit %q must be one of: dB, Hz, %%, ms", p.ID, p.Unit)
		}
		switch p.Curve {
		case CurveLinear, CurveLogarithmic:
		default:
			return fmt.Errorf("parameter %q curve %q must be one of: linear, logarithmic", p.ID, p.Curve)
		}
		if p.Curve == CurveLogarithmic && p.Min <= 0 {
			return fmt.Errorf("parameter %q with logarithmic curve must have min > 0 (got %v)", p.ID, p.Min)
		}
	}

	if s.DSPTopology != nil {
		if s.DSPTopology.MainProcessor == "" {
			return fmt.Errorf("dsp_topology.main_processor is required when dsp_topology is present")
		}
		if len(s.DSPTopology.Nodes) == 0 {
			return fmt.Errorf("dsp_topology.nodes must contain at least 1 node")
		}
		if len(s.DSPTopology.Connections) == 0 {
			return fmt.Errorf("dsp_topology.connections must contain at least 1 connection")
		}
	}

	return nil
}

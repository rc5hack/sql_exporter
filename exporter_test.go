package sql_exporter

import (
	"context"
	"errors"
	"testing"

	"github.com/burningalchemist/sql_exporter/config"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
)

type mockExporter struct {
	config *config.Config
	target []Target
}

func (e *mockExporter) Config() *config.Config {
	return e.config
}

func (e *mockExporter) UpdateTarget(target []Target) {
	e.target = target
}

func (e *mockExporter) Gather() ([]*dto.MetricFamily, error) {
	return nil, nil
}

func (e *mockExporter) WithContext(ctx context.Context) Exporter {
	return nil
}

func TestReloadCollectors(t *testing.T) {
	testCases := []struct {
		name                string
		configFile          string
		exporterConfig      *config.Config
		expectedErr         error
		expectedTargetCount int
	}{
		{
			name:       "no config file",
			configFile: "",
			exporterConfig: &config.Config{
				Globals:        &config.GlobalConfig{},
				CollectorFiles: []string{},
				Target:         &config.TargetConfig{EnablePing: &config.EnablePing},
				Jobs:           []*config.JobConfig{},
				Collectors:     []*config.CollectorConfig{},
			},
			expectedErr:         errors.New("open : no such file or directory"),
			expectedTargetCount: 0,
		},
		{
			name:       "error recreating target",
			configFile: "testdata/config_target_wrong.yml",
			exporterConfig: &config.Config{
				Globals:        &config.GlobalConfig{},
				CollectorFiles: []string{},
				Target:         &config.TargetConfig{Name: "test", DSN: "test", EnablePing: &config.EnablePing},
				Collectors:     []*config.CollectorConfig{},
			},
			expectedErr:         errors.New("no values defined for metric \"test\""),
			expectedTargetCount: 0,
		},
		{
			name:       "error recreating jobs",
			configFile: "testdata/config_jobs_wrong.yml",
			exporterConfig: &config.Config{
				Globals:        &config.GlobalConfig{},
				CollectorFiles: []string{},
				Jobs: []*config.JobConfig{
					{
						Name:       "test",
						EnablePing: &config.EnablePing,
						StaticConfigs: []*config.StaticConfig{
							{
								Targets: map[string]config.Secret{},
							},
						},
					},
				},
				Collectors: []*config.CollectorConfig{},
			},
			expectedErr:         errors.New("no values defined for metric \"test\""),
			expectedTargetCount: 0,
		},
		{
			name:       "target - success",
			configFile: "testdata/config_target_correct.yml",
			exporterConfig: &config.Config{
				Globals:        &config.GlobalConfig{},
				CollectorFiles: []string{},
				Target:         &config.TargetConfig{Name: "test", DSN: "test", EnablePing: &config.EnablePing},
				Collectors:     []*config.CollectorConfig{},
				XXX:            map[string]any{},
			},
			expectedErr:         nil,
			expectedTargetCount: 1,
		},
		{
			name:       "jobs - success",
			configFile: "testdata/config_jobs_correct.yml",
			exporterConfig: &config.Config{
				Globals:        &config.GlobalConfig{},
				CollectorFiles: []string{},
				Jobs: []*config.JobConfig{
					{
						Name: "test",
						StaticConfigs: []*config.StaticConfig{
							{
								Targets: map[string]config.Secret{},
							},
						},
					},
				},
				Collectors: []*config.CollectorConfig{},
			},
			expectedErr:         nil,
			expectedTargetCount: 3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config.ConfigFile = tc.configFile
			exporter := &mockExporter{config: tc.exporterConfig}
			err := ReloadCollectors(exporter)
			assert.Equal(t, tc.expectedErr, err)
			assert.Equal(t, tc.expectedTargetCount, len(exporter.target))
		})
	}
}

func TestParseContextLog(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected map[string]string
	}{
		{
			name:     "empty input",
			input:    "",
			expected: map[string]string{},
		},
		{
			name:     "single key-value pair",
			input:    "key=value",
			expected: map[string]string{"key": "value"},
		},
		{
			name:     "multiple key-value pairs",
			input:    "key1=value1,key2=value2,key3=value3",
			expected: map[string]string{"key1": "value1", "key2": "value2", "key3": "value3"},
		},
		{
			name:     "missing value",
			input:    "key1=value1,key2=",
			expected: map[string]string{"key1": "value1", "key2": ""},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := parseContextLog(tc.input)
			assert.Equal(t, tc.expected, actual)
		})
	}
}

func TestExporter_WithContext(t *testing.T) {
	e := &exporter{}
	ctx := context.Background()

	// Test that WithContext returns a new Exporter with the provided context.
	e2 := e.WithContext(ctx)
	assert.NotEqual(t, e, e2)
	assert.Equal(t, ctx, e2.(*exporter).ctx)
}

package sql_exporter

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/burningalchemist/sql_exporter/config"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/protobuf/proto"
)

var (
	SvcRegistry     = prometheus.NewRegistry()
	svcMetricLabels = []string{"job", "target", "collector", "query"}
)

// Exporter is a prometheus.Gatherer that gathers SQL metrics from targets and merges them with the default registry.
type Exporter interface {
	prometheus.Gatherer

	// WithContext returns a (single use) copy of the Exporter, which will use the provided context for Gather() calls.
	WithContext(context.Context) Exporter
	// Config returns the Exporter's underlying Config object.
	Config() *config.Config
	UpdateTarget([]Target)
}

type exporter struct {
	config  *config.Config
	targets []Target

	ctx          context.Context
	scrapeErrors *prometheus.CounterVec
}

// NewExporter returns a new Exporter with the provided config.
func NewExporter(configFile string) (Exporter, error) {
	c, err := config.Load(configFile)
	if err != nil {
		return nil, err
	}

	if val, ok := os.LookupEnv(config.EnvDsnOverride); ok {
		config.DsnOverride = val
	}
	// Override the DSN if requested (and in single target mode).
	if config.DsnOverride != "" {
		if len(c.Jobs) > 0 {
			return nil, fmt.Errorf("the config.data-source-name flag (value %q) only applies in single target mode", config.DsnOverride)
		}
		c.Target.DSN = config.Secret(config.DsnOverride)
	}

	var targets []Target
	if c.Target != nil {
		if c.Target.EnablePing == nil {
			c.Target.EnablePing = &config.EnablePing
		}
		target, err := NewTarget("", c.Target.Name, string(c.Target.DSN), c.Target.Collectors(), nil, c.Globals, c.Target.EnablePing)
		if err != nil {
			return nil, err
		}
		targets = []Target{target}
	} else {
		if len(c.Jobs) > (config.MaxInt32 / 3) {
			return nil, errors.New("'jobs' list is too large")
		}
		targets = make([]Target, 0, len(c.Jobs)*3)
		for _, jc := range c.Jobs {
			job, err := NewJob(jc, c.Globals)
			if err != nil {
				return nil, err
			}
			targets = append(targets, job.Targets()...)
		}
	}

	scrapeErrors := newScrapeErrors()
	SvcRegistry.MustRegister(scrapeErrors)

	return &exporter{
		config:       c,
		targets:      targets,
		ctx:          context.Background(),
		scrapeErrors: scrapeErrors,
	}, nil
}

func (e *exporter) WithContext(ctx context.Context) Exporter {
	return &exporter{
		config:       e.config,
		targets:      e.targets,
		ctx:          ctx,
		scrapeErrors: e.scrapeErrors,
	}
}

// Gather implements prometheus.Gatherer.
func (e *exporter) Gather() ([]*dto.MetricFamily, error) {
	var (
		metricChan = make(chan Metric, capMetricChan)
		errs       prometheus.MultiError
	)

	var wg sync.WaitGroup
	wg.Add(len(e.targets))
	for _, t := range e.targets {
		go func(target Target) {
			defer wg.Done()
			target.Collect(e.ctx, metricChan)
		}(t)
	}

	// Wait for all collectors to complete, then close the channel.
	go func() {
		wg.Wait()
		close(metricChan)
	}()

	// Drain metricChan in case of premature return.
	defer func() {
		for range metricChan {
		}
	}()

	// Gather.
	dtoMetricFamilies := make(map[string]*dto.MetricFamily, 10)
	for metric := range metricChan {
		dtoMetric := &dto.Metric{}
		if err := metric.Write(dtoMetric); err != nil {
			errs = append(errs, err)
			if err.Context() != "" {
				ctxLabels := parseContextLog(err.Context())
				values := make([]string, len(svcMetricLabels))
				for i, label := range svcMetricLabels {
					values[i] = ctxLabels[label]
				}
				e.scrapeErrors.WithLabelValues(values...).Inc()
			}
			continue
		}
		metricDesc := metric.Desc()
		dtoMetricFamily, ok := dtoMetricFamilies[metricDesc.Name()]
		if !ok {
			dtoMetricFamily = &dto.MetricFamily{}
			dtoMetricFamily.Name = proto.String(metricDesc.Name())
			dtoMetricFamily.Help = proto.String(metricDesc.Help())
			switch {
			case dtoMetric.Gauge != nil:
				dtoMetricFamily.Type = dto.MetricType_GAUGE.Enum()
			case dtoMetric.Counter != nil:
				dtoMetricFamily.Type = dto.MetricType_COUNTER.Enum()
			default:
				errs = append(errs, fmt.Errorf("don't know how to handle metric %v", dtoMetric))
				continue
			}
			dtoMetricFamilies[metricDesc.Name()] = dtoMetricFamily
		}
		dtoMetricFamily.Metric = append(dtoMetricFamily.Metric, dtoMetric)
	}

	// No need to sort metric families, prometheus.Gatherers will do that for us when merging.
	result := make([]*dto.MetricFamily, 0, len(dtoMetricFamilies))
	for _, mf := range dtoMetricFamilies {
		result = append(result, mf)
	}
	return result, errs
}

// Config implements Exporter.
func (e *exporter) Config() *config.Config {
	return e.config
}

func (e *exporter) UpdateTarget(target []Target) {
	e.targets = target
}

// newScrapeErrors registers the metrics for the exporter itself.
func newScrapeErrors() *prometheus.CounterVec {
	return prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "scrape_errors",
		Help: "Total number of scrape errors per job, target, collector and query.",
	}, svcMetricLabels)
}

// split comma separated list of key=value pairs and return a map of key value pairs
func parseContextLog(keyvals string) map[string]string {
	m := make(map[string]string)
	if keyvals != "" {
		for _, item := range strings.Split(keyvals, ",") {
			parts := strings.SplitN(item, "=", 2)
			m[parts[0]] = parts[1]
		}
	}
	return m
}

/* // ReloadCollectors returns a function that reloads collectors for the given SQL exporter.
// The returned function takes an HTTP response writer and request as arguments.
// It reads the configuration file, updates the collectors for the exporter's target or jobs,
// and returns an HTTP status code indicating success or failure.
func ReloadCollectors(e Exporter) error {
	klog.Warning("Reloading collectors has started...")
	klog.Warning("Connections will not be changed upon the restart of the exporter")
	exporterNewConfig, err := config.Load(config.ConfigFile)
	if err != nil {
		klog.Errorf("Error reading config file - %v", err)
		return errors.New(err.Error())
	}
	currentConfig := e.Config()
	klog.Infof("Total collector size change: %v -> %v", len(currentConfig.Collectors),
		len(exporterNewConfig.Collectors))

	if len(currentConfig.Collectors) > 0 {
		currentConfig.Collectors = currentConfig.Collectors[:0]
	}
	currentConfig.Collectors = exporterNewConfig.Collectors

	// Reload Collectors for a single target if there is one
	if currentConfig.Target != nil {
		klog.Warning("Reloading target collectors...")
		// FIXME: Should be t.Collectors() instead of config.Collectors
		target, err := NewTarget("", currentConfig.Target.Name, string(currentConfig.Target.DSN),
			exporterNewConfig.Target.Collectors(), nil, currentConfig.Globals, currentConfig.Target.EnablePing)
		if err != nil {
			klog.Errorf("Error recreating a target - %v", err)
			return errors.New(err.Error())
		}
		e.UpdateTarget([]Target{target})
		klog.Warning("Collectors have been successfully reloaded for target")
		return nil
	}

	// Reload Collectors for Jobs if there are any
	if len(currentConfig.Jobs) > 0 {
		klog.Warning("Recreating jobs...")

		// We want to preserve `static_configs`` from the previous config revision to avoid any connection changes
		for _, currentJob := range currentConfig.Jobs {
			for _, newJob := range exporterNewConfig.Jobs {
				if newJob.Name == currentJob.Name {
					// Create a new slice of StaticConfig structs that contains the StaticConfigs from the current job
					staticConfigs := make([]*config.StaticConfig, len(currentJob.StaticConfigs))
					copy(staticConfigs, currentJob.StaticConfigs)
					newJob.StaticConfigs = staticConfigs
				}
			}
		}
		currentConfig.Jobs = exporterNewConfig.Jobs

		var updateErr error
		targets := make([]Target, 0, len(currentConfig.Jobs))

		for _, jobConfigItem := range currentConfig.Jobs {
			job, err := NewJob(jobConfigItem, currentConfig.Globals)
			if err != nil {
				updateErr = err
				break
			}
			targets = append(targets, job.Targets()...)
			klog.Infof("Recreated Job: %s", jobConfigItem.Name)
		}

		if updateErr != nil {
			klog.Errorf("Error recreating jobs - %v", err)
			return errors.New(err.Error())
		}

		e.UpdateTarget(targets)
		klog.Warning("Query collectors have been successfully reloaded for jobs")
		return nil
	}
	klog.Warning("No target or jobs have been found - nothing to reload")
	return errors.New("no target or jobs found - nothing to reload")
} */

func ReloadCollectors(e Exporter) error {
	exporterNewConfig, err := config.Load(config.ConfigFile)
	if err != nil {
		return errors.New(err.Error())
	}

	currentConfig := e.Config()
	currentConfig.Collectors = exporterNewConfig.Collectors

	if currentConfig.Target != nil {
		target, err := NewTarget("", currentConfig.Target.Name, string(currentConfig.Target.DSN),
			exporterNewConfig.Target.Collectors(), nil, currentConfig.Globals, currentConfig.Target.EnablePing)
		if err != nil {
			return errors.New(err.Error())
		}
		e.UpdateTarget([]Target{target})
		return nil
	}

	if len(currentConfig.Jobs) > 0 {
		currentConfig.Jobs = exporterNewConfig.Jobs

		var updateErr error
		targets := make([]Target, 0, len(currentConfig.Jobs))

		for _, jobConfigItem := range currentConfig.Jobs {
			job, err := NewJob(jobConfigItem, currentConfig.Globals)
			if err != nil {
				updateErr = err
				break
			}
			targets = append(targets, job.Targets()...)
		}

		if updateErr != nil {
			return errors.New(err.Error())
		}

		e.UpdateTarget(targets)
		return nil
	}

	return errors.New("no target or jobs found")
}

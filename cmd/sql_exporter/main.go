package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/burningalchemist/sql_exporter"
	cfg "github.com/burningalchemist/sql_exporter/config"
	_ "github.com/kardianos/minwinsvc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/common/version"
	"github.com/prometheus/exporter-toolkit/web"
	"k8s.io/klog/v2"
)

const (
	appName string = "sql_exporter"

	httpReadHeaderTimeout time.Duration = time.Duration(time.Second * 60)
	debugMaxLevel         klog.Level    = 3
)

var (
	showVersion   = flag.Bool("version", false, "Print version information")
	listenAddress = flag.String("web.listen-address", ":9399", "Address to listen on for web interface and telemetry")
	metricsPath   = flag.String("web.metrics-path", "/metrics", "Path under which to expose metrics")
	enableReload  = flag.Bool("web.enable-reload", false, "Enable reload collector data handler")
	webConfigFile = flag.String("web.config.file", "", "TLS/BasicAuth configuration file path")
	logFormat     = flag.String("log.format", "text", "Set log output format to JSON")
	logLevel      = flag.String("log.level", "info", "Set log level")
)

func init() {
	prometheus.MustRegister(version.NewCollector("sql_exporter"))
	flag.StringVar(&cfg.ConfigFile, "config.file", "sql_exporter.yml", "SQL Exporter configuration file path")
	flag.BoolVar(&cfg.EnablePing, "config.enable-ping", true, "Enable ping for targets")
	flag.StringVar(&cfg.DsnOverride, "config.data-source-name", "", "Data source name to override the value in the configuration file with")
	flag.StringVar(&cfg.TargetLabel, "config.target-label", "target", "Target label name")
}

func main() {
	if os.Getenv(cfg.EnvDebug) != "" {
		runtime.SetBlockProfileRate(1)
		runtime.SetMutexProfileFraction(1)
	}
	flag.Parse()

	promlogConfig := &promlog.Config{
		Level:  &promlog.AllowedLevel{},
		Format: &promlog.AllowedFormat{},
	}
	_ = promlogConfig.Level.Set(*logLevel)
	_ = promlogConfig.Format.Set(*logFormat)

	// Overriding the default klog with our go-kit klog implementation.
	// Thus we need to pass it our go-kit logger object.
	logger := promlog.New(promlogConfig)
	klog.SetLogger(logger)
	klog.ClampLevel(debugMaxLevel)

	// Override --alsologtostderr default value.
	if alsoLogToStderr := flag.Lookup("alsologtostderr"); alsoLogToStderr != nil {
		alsoLogToStderr.DefValue = "true"
		_ = alsoLogToStderr.Value.Set("true")
	}
	// Override the config.file default with the SQLEXPORTER_CONFIG environment variable if set.
	if val, ok := os.LookupEnv(cfg.EnvConfigFile); ok {
		cfg.ConfigFile = val
	}

	if *showVersion {
		fmt.Println(version.Print(appName))
		os.Exit(0)
	}

	klog.Warningf("Starting SQL exporter %s %s", version.Info(), version.BuildContext())

	exporter, err := sql_exporter.NewExporter(cfg.ConfigFile)
	if err != nil {
		klog.Fatalf("Error creating exporter: %s", err)
	}

	// Setup and start webserver.
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { http.Error(w, "OK", http.StatusOK) })
	http.HandleFunc("/", HomeHandlerFunc(*metricsPath))
	http.HandleFunc("/config", ConfigHandlerFunc(*metricsPath, exporter))
	http.Handle(*metricsPath, promhttp.InstrumentMetricHandler(prometheus.DefaultRegisterer, ExporterHandlerFor(exporter)))
	// Expose exporter metrics separately, for debugging purposes.
	http.Handle("/sql_exporter_metrics", promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{}))

	// Expose refresh handler to reload query collections
	if *enableReload {
		http.HandleFunc("/reload", func(w http.ResponseWriter, r *http.Request) {
			err := sql_exporter.ReloadCollectors(exporter)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			} else {
				http.Error(w, "OK", http.StatusOK)
			}
		})
	}

	server := &http.Server{Addr: *listenAddress, ReadHeaderTimeout: httpReadHeaderTimeout}
	if err := web.ListenAndServe(server, &web.FlagConfig{WebListenAddresses: &([]string{*listenAddress}),
		WebConfigFile: webConfigFile, WebSystemdSocket: OfBool(false)}, logger); err != nil {
		klog.Fatal(err)
	}
}

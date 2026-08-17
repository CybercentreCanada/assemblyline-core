package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"

	"github.com/CybercentreCanada/assemblyline-kubernetes/internal"
	"github.com/redis/go-redis/v9"
	"k8s.io/component-base/logs"
	"k8s.io/klog/v2"
	basecmd "sigs.k8s.io/custom-metrics-apiserver/pkg/cmd"
	"sigs.k8s.io/custom-metrics-apiserver/pkg/provider"
)

type AlAdapter struct {
	basecmd.AdapterBase

	// Redis connection string
	VolatileRedis   string
	PersistentRedis string

	// CA cert path for redis connections
	CaCert string
}

func (a *AlAdapter) makeProviderOrDie(volatile *redis.Client, persistent *redis.Client) provider.ExternalMetricsProvider {

	monitor, err := internal.StartQueueMonitor(volatile, persistent)
	if err != nil {
		klog.Fatalf("unable to initialize queue state: %v", err)
	}

	return internal.NewAlProvider(monitor)
}

func main() {
	logs.InitLogs()
	defer logs.FlushLogs()

	cmd := &AlAdapter{}
	cmd.Name = "test-adapter"

	cmd.Flags().StringVar(&cmd.VolatileRedis, "volatile", "rediss://localhost:6379", "Redis connection string for volatile redis instance")
	cmd.Flags().StringVar(&cmd.PersistentRedis, "persistent", "rediss://localhost:6379", "Redis connection string for persistent redis instance")
	cmd.Flags().StringVar(&cmd.CaCert, "ca-cert", "", "Path to CA cert used to connect to redis servers")
	logs.AddFlags(cmd.Flags())

	if err := cmd.Flags().Parse(os.Args); err != nil {
		klog.Fatalf("unable to parse flags: %v", err)
	}

	vol_redis_opts, err := redis.ParseURL(cmd.VolatileRedis)
	if err != nil {
		panic(err)
	}
	per_redis_opts, err := redis.ParseURL(cmd.PersistentRedis)
	if err != nil {
		panic(err)
	}

	if cmd.CaCert != "" {
		cert, err := os.ReadFile(cmd.CaCert)
		if err != nil {
			panic(err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(cert)
		vol_redis_opts.TLSConfig = &tls.Config{
			RootCAs: caCertPool,
		}
		per_redis_opts.TLSConfig = &tls.Config{
			RootCAs: caCertPool,
		}
	}

	klog.Info("Arguments parsed.")
	klog.Info("Connecting to volatile redis...")
	volatile := redis.NewClient(vol_redis_opts)
	defer volatile.Close()

	klog.Info("Connecting to persistent redis...")
	persistent := redis.NewClient(per_redis_opts)
	defer persistent.Close()

	klog.Info("Make metrics provider.")
	provider := cmd.makeProviderOrDie(volatile, persistent)

	klog.Info("Register metrics.")
	// cmd.WithCustomMetrics(provider)
	cmd.WithExternalMetrics(provider)
	// if err := custom_metrics.RegisterMetrics(legacyregistry.Register); err != nil {
	// 	klog.Fatalf("unable to register metrics: %v", err)
	// }

	klog.Info("Serve metrics.")
	if err := cmd.Run(context.Background()); err != nil {
		klog.Fatalf("unable to run custom metrics adapter: %v", err)
	}
}

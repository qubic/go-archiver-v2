package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ardanlabs/conf/v3"
	grpcProm "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/qubic/go-archiver-v2/api"
	"github.com/qubic/go-archiver-v2/db"
	metrics "github.com/qubic/go-archiver-v2/metrics"
	"github.com/qubic/go-archiver-v2/network"
	"github.com/qubic/go-archiver-v2/processor"
	"github.com/qubic/go-archiver-v2/protobuf"
	"github.com/qubic/go-archiver-v2/validator"
	qubic "github.com/qubic/go-node-connector"
	"github.com/qubic/go-node-connector/types"
)

const prefix = "QUBIC_ARCHIVER_V2"

func main() {
	if err := run(); err != nil {
		log.Fatalf("main: exited with error: %s", err.Error())
	}
}

func run() error {
	log.SetOutput(os.Stdout)
	var cfg struct {
		Server struct {
			HttpHost          string `conf:"default:0.0.0.0:8000"`
			GrpcHost          string `conf:"default:0.0.0.0:8001"`
			ProfilingHost     string `conf:"default:0.0.0.0:8002"`
			NodeSyncThreshold uint32 `conf:"default:10"`
		}
		Pool struct {
			NodeFetcherUrl     string        `conf:"default:http://127.0.0.1:8080/status"`
			NodeFetcherTimeout time.Duration `conf:"default:2s"`
			InitialCap         int           `conf:"default:5"`
			MaxIdle            int           `conf:"default:20"`
			MaxCap             int           `conf:"default:30"`
			IdleTimeout        time.Duration `conf:"default:15s"`
			NodePort           string        `conf:"default:21841"`
		}
		Qubic struct {
			ProcessTickTimeout  time.Duration `conf:"default:5s"`
			EnableTxStatusAddon bool          `conf:"default:true"`
			ArbitratorIdentity  string        `conf:"default:AFZPUAIYVPNUYGJRQVLUKOPPVLHAZQTGLYAAUUNBXFTVTAMSBKQBLEIEPCVJ"`
			OverrideTick        bool          `conf:"default:false"`
			StartTick           uint32        `conf:"default:0"`
			StartEpoch          uint16        `conf:"default:0"`
			ProcessingEnabled   bool          `conf:"default:true"`
		}
		Store struct {
			StorageFolder   string `conf:"default:archive-data"`
			OpenEpochsCount int    `conf:"default:10"`
		}
		Metrics struct {
			Namespace string `conf:"default:qubic-archiver-v2"`
		}
	}

	help, err := conf.Parse(prefix, &cfg)
	if err != nil {
		if errors.Is(err, conf.ErrHelpWanted) {
			fmt.Println(help)
			return nil
		}
		return fmt.Errorf("parsing config: %w", err)
	}

	out, err := conf.String(&cfg)
	if err != nil {
		return fmt.Errorf("generating config for output: %w", err)
	}
	log.Printf("main: Config :\n%v\n", out)

	prometheusRegistry := prometheus.NewRegistry()
	m := metrics.NewProcessingMetrics(prometheusRegistry, cfg.Metrics.Namespace)

	// handle shutdowns
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	// create database
	dbPool, err := db.NewDatabasePool(cfg.Store.StorageFolder, cfg.Store.OpenEpochsCount)
	if err != nil {
		return fmt.Errorf("creating db pool: %w", err)
	}
	if cfg.Qubic.OverrideTick && cfg.Qubic.StartEpoch > 0 && cfg.Qubic.StartTick > 0 {
		database, err := dbPool.GetOrCreateDbForEpoch(cfg.Qubic.StartEpoch)
		if err != nil {
			return fmt.Errorf("creating db for setting start tick: %w", err)
		}
		err = database.SetLastProcessedTick(context.Background(), &protobuf.ProcessedTick{
			TickNumber: cfg.Qubic.StartTick - 1,
			Epoch:      uint32(cfg.Qubic.StartEpoch),
		})
		if err != nil {
			return fmt.Errorf("setting start tick: %w", err)
		}
	}
	defer dbPool.Close()

	clientPool, err := network.NewNodeConnectorPool(qubic.PoolConfig{
		InitialCap:         cfg.Pool.InitialCap,
		MaxCap:             cfg.Pool.MaxCap,
		MaxIdle:            cfg.Pool.MaxIdle,
		IdleTimeout:        cfg.Pool.IdleTimeout,
		NodeFetcherUrl:     cfg.Pool.NodeFetcherUrl,
		NodeFetcherTimeout: cfg.Pool.NodeFetcherTimeout,
		NodePort:           cfg.Pool.NodePort,
	})
	if err != nil {
		return fmt.Errorf("creating node pool: %w", err)
	}

	// start processor
	arbitratorID := types.Identity(cfg.Qubic.ArbitratorIdentity)
	arbitratorPubKey, err := arbitratorID.ToPubKey(false)
	if err != nil {
		return fmt.Errorf("calculating arbitrator public key from [%s]: %w", cfg.Qubic.ArbitratorIdentity, err)
	}
	tickValidator := validator.NewValidator(arbitratorPubKey, cfg.Qubic.EnableTxStatusAddon)
	proc := processor.NewProcessor(clientPool, dbPool, tickValidator, processor.Config{
		ProcessTickTimeout: cfg.Qubic.ProcessTickTimeout,
	}, m)

	procErrors := make(chan error, 1)
	if cfg.Qubic.ProcessingEnabled {
		go func() { procErrors <- proc.Start() }()
	}

	// start API endpoints
	rpcServer := api.NewArchiveServer(dbPool, proc.GetTickStatus(), cfg.Server.GrpcHost, cfg.Server.HttpHost, cfg.Server.NodeSyncThreshold)
	serverError := make(chan error, 1)

	// start metrics
	srvMetrics := grpcProm.NewServerMetrics(
		grpcProm.WithServerCounterOptions(grpcProm.WithConstLabels(prometheus.Labels{"namespace": cfg.Metrics.Namespace})),
	)

	prometheusRegistry.MustRegister(srvMetrics)
	prometheusRegistry.MustRegister(collectors.NewGoCollector())
	err = rpcServer.Start(serverError, srvMetrics.UnaryServerInterceptor())
	if err != nil {
		return fmt.Errorf("starting server: %w", err)
	}
	metricsError := make(chan error, 1)
	go func() {
		log.Printf("main: Starting metrics server listening at [%s].", cfg.Server.ProfilingHost)
		http.Handle("/metrics", promhttp.HandlerFor(prometheusRegistry, promhttp.HandlerOpts{EnableOpenMetrics: true}))
		metricsError <- http.ListenAndServe(cfg.Server.ProfilingHost, nil)
	}()

	for {
		select {
		case <-shutdown:
			log.Println("main: Received shutdown signal, shutting down...")
			return nil
		case err := <-metricsError:
			return fmt.Errorf("[ERROR] starting metrics endpoint: %w", err)
		case err := <-serverError:
			return fmt.Errorf("[ERROR] starting server endpoint(s): %w", err)
		case err := <-procErrors:
			return fmt.Errorf("[ERROR] processing: %w", err)
		case err := <-metricsError:
			log.Printf("[ERROR] serving metrics: %s/n", err.Error())
		}
	}

}

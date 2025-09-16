package main

import (
	"errors"
	"fmt"
	"github.com/ardanlabs/conf/v3"
	grpcProm "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/qubic/go-archiver/api"
	"github.com/qubic/go-archiver/db"
	"github.com/qubic/go-archiver/network"
	"github.com/qubic/go-archiver/processor"
	"github.com/qubic/go-archiver/validator"
	qubic "github.com/qubic/go-node-connector"
	"github.com/qubic/go-node-connector/types"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const prefix = "QUBIC_ARCHIVER"

func main() {
	if err := run(); err != nil {
		log.Fatalf("main: exited with error: %s", err.Error())
	}
}

func run() error {
	var cfg struct {
		Server struct {
			ReadTimeout       time.Duration `conf:"default:5s"`
			WriteTimeout      time.Duration `conf:"default:5s"`
			ShutdownTimeout   time.Duration `conf:"default:5s"`
			HttpHost          string        `conf:"default:0.0.0.0:8000"`
			GrpcHost          string        `conf:"default:0.0.0.0:8001"`
			ProfilingHost     string        `conf:"default:0.0.0.0:8002"`
			NodeSyncThreshold int           `conf:"default:3"`
			ChainTickFetchUrl string        `conf:"default:http://127.0.0.1:8080/max-tick"`
		}
		Pool struct {
			NodeFetcherUrl     string        `conf:"default:http://127.0.0.1:8080/status"`
			NodeFetcherTimeout time.Duration `conf:"default:2s"`
			InitialCap         int           `conf:"default:5"`
			MaxIdle            int           `conf:"default:20"`
			MaxCap             int           `conf:"default:30"`
			IdleTimeout        time.Duration `conf:"default:15s"`
		}
		Qubic struct {
			NodePort            string        `conf:"default:21841"`
			ProcessTickTimeout  time.Duration `conf:"default:5s"`
			EnableTxStatusAddon bool          `conf:"default:false"`
			ArbitratorIdentity  string        `conf:"default:AFZPUAIYVPNUYGJRQVLUKOPPVLHAZQTGLYAAUUNBXFTVTAMSBKQBLEIEPCVJ"`
		}
		Store struct {
			ResetEmptyTickKeys bool   `conf:"default:false"`
			StorageFolder      string `conf:"default:archive-data"`
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

	dbPool, err := db.NewDatabasePool(cfg.Store.StorageFolder)
	if err != nil {
		return fmt.Errorf("creating db pool: %w", err)
	}
	defer dbPool.Close()

	// TODO check if we need the following
	//if cfg.Store.ResetEmptyTickKeys {
	//	fmt.Printf("Resetting empty ticks for all epochs...\n")
	//	err = tick.ResetEmptyTicksForAllEpochs(ps)
	//	if err != nil {

	//		return fmt.Errorf("resetting empty ticks keys: %w", err)
	//	}
	//}
	//
	//err = tick.CalculateEmptyTicksForAllEpochs(ps)
	//if err != nil {
	//	return fmt.Errorf("calculating empty ticks for all epochs: %w", err)
	//}

	// FIXME
	clientPool, err := network.NewNodeConnectorPool(qubic.PoolConfig{
		InitialCap:         cfg.Pool.InitialCap,
		MaxCap:             cfg.Pool.MaxCap,
		MaxIdle:            cfg.Pool.MaxIdle,
		IdleTimeout:        cfg.Pool.IdleTimeout,
		NodeFetcherUrl:     cfg.Pool.NodeFetcherUrl,
		NodeFetcherTimeout: cfg.Pool.NodeFetcherTimeout,
		NodePort:           cfg.Qubic.NodePort,
	})
	//clientPool := &qubic.Pool{}
	if err != nil {
		return fmt.Errorf("creating node pool: %w", err)
	}

	srvMetrics := grpcProm.NewServerMetrics(
		grpcProm.WithServerCounterOptions(grpcProm.WithConstLabels(prometheus.Labels{"namespace": "archiver"})),
	)
	reg := prometheus.NewRegistry()
	reg.MustRegister(srvMetrics)
	reg.MustRegister(collectors.NewGoCollector())

	rpcServer := api.NewArchiveServer(cfg.Server.GrpcHost, cfg.Server.HttpHost)
	serverError := make(chan error, 1)

	err = rpcServer.Start(serverError, srvMetrics.UnaryServerInterceptor())
	if err != nil {
		return fmt.Errorf("starting server: %w", err)
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	arbitratorID := types.Identity(cfg.Qubic.ArbitratorIdentity)
	arbitratorPubKey, err := arbitratorID.ToPubKey(false)
	if err != nil {
		return fmt.Errorf("calculating arbitrator public key from [%s]: %w", cfg.Qubic.ArbitratorIdentity, err)
	}

	tickValidator := validator.NewValidator(arbitratorPubKey, cfg.Qubic.EnableTxStatusAddon)
	proc := processor.NewProcessor(clientPool, dbPool, tickValidator, cfg.Qubic.ProcessTickTimeout)
	procErrors := make(chan error, 1)

	// Start the service listening for requests.
	go func() {
		procErrors <- proc.Start()
	}()

	metricsError := make(chan error, 1)

	go func() {
		log.Printf("main: Starting metrics server listening at [%s].", cfg.Server.ProfilingHost)
		http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{EnableOpenMetrics: true}))
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

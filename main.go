package main

import (
	"errors"
	"fmt"
	"github.com/ardanlabs/conf/v3"
	"github.com/cockroachdb/pebble"
	grpcProm "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/qubic/go-archiver/api"
	"github.com/qubic/go-archiver/processor"
	"github.com/qubic/go-archiver/store"
	qubic "github.com/qubic/go-node-connector"
	"github.com/qubic/go-node-connector/types"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
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
			NodePort                      string        `conf:"default:21841"`
			StorageFolder                 string        `conf:"default:archive-data"`
			ProcessTickTimeout            time.Duration `conf:"default:5s"`
			DisableTransactionStatusAddon bool          `conf:"default:true"`
			ArbitratorIdentity            string        `conf:"default:AFZPUAIYVPNUYGJRQVLUKOPPVLHAZQTGLYAAUUNBXFTVTAMSBKQBLEIEPCVJ"`
		}
		Store struct {
			ResetEmptyTickKeys bool `conf:"default:false"`
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

	l1Options := pebble.LevelOptions{
		BlockRestartInterval: 16,
		BlockSize:            4096,
		BlockSizeThreshold:   90,
		Compression:          pebble.NoCompression,
		FilterPolicy:         nil,
		FilterType:           pebble.TableFilter,
		IndexBlockSize:       4096,
		TargetFileSize:       268435456, // 256 MB
	}
	l2Options := pebble.LevelOptions{
		BlockRestartInterval: 16,
		BlockSize:            4096,
		BlockSizeThreshold:   90,
		Compression:          pebble.ZstdCompression,
		FilterPolicy:         nil,
		FilterType:           pebble.TableFilter,
		IndexBlockSize:       4096,
		TargetFileSize:       l1Options.TargetFileSize * 10, // 2.5 GB
	}
	l3Options := pebble.LevelOptions{
		BlockRestartInterval: 16,
		BlockSize:            4096,
		BlockSizeThreshold:   90,
		Compression:          pebble.ZstdCompression,
		FilterPolicy:         nil,
		FilterType:           pebble.TableFilter,
		IndexBlockSize:       4096,
		TargetFileSize:       l2Options.TargetFileSize * 10, // 25 GB
	}
	l4Options := pebble.LevelOptions{
		BlockRestartInterval: 16,
		BlockSize:            4096,
		BlockSizeThreshold:   90,
		Compression:          pebble.ZstdCompression,
		FilterPolicy:         nil,
		FilterType:           pebble.TableFilter,
		IndexBlockSize:       4096,
		TargetFileSize:       l3Options.TargetFileSize * 10, // 250 GB
	}

	pebbleOptions := pebble.Options{
		Levels:                   []pebble.LevelOptions{l1Options, l2Options, l3Options, l4Options},
		MaxConcurrentCompactions: func() int { return runtime.NumCPU() },
		MemTableSize:             268435456, // 256 MB
		EventListener:            store.NewPebbleEventListener(),
	}

	db, err := pebble.Open(cfg.Qubic.StorageFolder, &pebbleOptions)
	if err != nil {
		return fmt.Errorf("opening db: %w", err)
	}
	defer func(db *pebble.DB) {
		err := db.Close()
		if err != nil {
			log.Printf("closing db: %v", err)
		}
	}(db)

	ps := store.NewPebbleStore(db, nil)

	//if cfg.Store.ResetEmptyTickKeys {
	//	fmt.Printf("Resetting empty ticks for all epochs...\n")
	//	err = tick.ResetEmptyTicksForAllEpochs(ps)
	//	if err != nil {
	//		return errors.Wrap(err, "resetting empty ticks keys")
	//	}
	//}
	//
	//err = tick.CalculateEmptyTicksForAllEpochs(ps)
	//if err != nil {
	//	return errors.Wrap(err, "calculating empty ticks for all epochs")
	//}

	p, err := qubic.NewPoolConnection(qubic.PoolConfig{
		InitialCap:         cfg.Pool.InitialCap,
		MaxCap:             cfg.Pool.MaxCap,
		MaxIdle:            cfg.Pool.MaxIdle,
		IdleTimeout:        cfg.Pool.IdleTimeout,
		NodeFetcherUrl:     cfg.Pool.NodeFetcherUrl,
		NodeFetcherTimeout: cfg.Pool.NodeFetcherTimeout,
		NodePort:           cfg.Qubic.NodePort,
	})
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

	proc := processor.NewProcessor(p, ps, cfg.Qubic.ProcessTickTimeout, arbitratorPubKey, cfg.Qubic.DisableTransactionStatusAddon)
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

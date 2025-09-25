package api

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/qubic/go-archiver-v2/db"
	"github.com/qubic/go-archiver-v2/processor"
	"github.com/qubic/go-archiver-v2/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
	"log"
	"net"
	"net/http"
)

type ArchiveServiceServer struct {
	protobuf.UnimplementedArchiveServiceServer
	listenAddrGRPC string
	listenAddrHTTP string
	dbPool         *db.DatabasePool
	tickStatus     *processor.TickStatus
	maxDelay       uint32
}

func NewArchiveServer(dbPool *db.DatabasePool, tickStatus *processor.TickStatus, listenAddrGRPC string, listenAddrHTTP string, syncThreshold uint32) *ArchiveServiceServer {
	return &ArchiveServiceServer{
		UnimplementedArchiveServiceServer: protobuf.UnimplementedArchiveServiceServer{},
		listenAddrGRPC:                    listenAddrGRPC,
		listenAddrHTTP:                    listenAddrHTTP,
		dbPool:                            dbPool,
		tickStatus:                        tickStatus,
		maxDelay:                          syncThreshold,
	}
}

func (s *ArchiveServiceServer) GetHealth(context.Context, *emptypb.Empty) (*protobuf.GetHealthResponse, error) {
	upToDate := s.tickStatus.ProcessedTick > 0 &&
		s.tickStatus.LiveTick > 0 &&
		s.tickStatus.ProcessedTick > s.tickStatus.LiveTick-s.maxDelay
	return &protobuf.GetHealthResponse{
		Status:        "UP",
		UpToDate:      upToDate,
		ProcessedTick: s.tickStatus.ProcessedTick,
		LiveTick:      s.tickStatus.LiveTick,
		LiveEpoch:     uint32(s.tickStatus.LiveEpoch),
	}, nil
}

// GetStatus returns the last processed tick and the tick intervals. Similar to the older archiver but some data is missing.
func (s *ArchiveServiceServer) GetStatus(ctx context.Context, _ *emptypb.Empty) (*protobuf.GetStatusResponse, error) {

	epochs := s.dbPool.GetAvailableEpochsAscending() // oldest first. to stay compatible with v1 order.
	if len(epochs) == 0 {
		log.Printf("[WARN] no epoch databases found.")
		return nil, status.Error(codes.NotFound, "no tick intervals found")
	}

	intervalsPerEpoch := make([]*protobuf.ProcessedTickIntervalsPerEpoch, 0, len(epochs))
	for _, epoch := range epochs {
		database, err := s.dbPool.GetDbForEpoch(epoch)
		if err != nil {
			id := uuid.New()
			log.Printf("[ERROR] (%s) getting database for epoch [%d]: %v", id.String(), epoch, err)
			return nil, status.Errorf(codes.Internal, "error accessing data (%v)", id.String())
		}
		intervalsForEpoch, err := database.GetProcessedTickIntervalsPerEpoch(ctx, uint32(epoch))
		if err != nil {
			id := uuid.New()
			log.Printf("[ERROR] (%s) getting processed tick intervals for epoch [%d]: %v", id.String(), epoch, err)
			return nil, status.Errorf(codes.Internal, "error accessing data (%v)", id.String())
		}
		intervalsPerEpoch = append(intervalsPerEpoch, intervalsForEpoch)
	}

	latest := intervalsPerEpoch[len(intervalsPerEpoch)-1]
	return &protobuf.GetStatusResponse{
		LastProcessedTick: &protobuf.ProcessedTick{
			TickNumber: latest.GetIntervals()[len(latest.GetIntervals())-1].GetLastProcessedTick(),
			Epoch:      latest.GetEpoch(),
		},
		ProcessedTickIntervalsPerEpoch: intervalsPerEpoch,
	}, nil
}

// GetTickTransactionsV2 returns an empty list if the tick is available in the database. 404 if the requested tick is not available.
func (s *ArchiveServiceServer) GetTickTransactionsV2(ctx context.Context, in *protobuf.GetTickTransactionsRequestV2) (*protobuf.GetTickTransactionsResponseV2, error) {
	tick := in.GetTickNumber()

	epochs := s.dbPool.GetAvailableEpochsDescending() // first look in current epoch
	for _, epoch := range epochs {

		database, err := s.dbPool.GetDbForEpoch(epoch)
		if err != nil {
			id := uuid.New()
			log.Printf("[ERROR] (%s) getting database for epoch [%d]: %v", id.String(), epoch, err)
			return nil, status.Errorf(codes.Internal, "error getting transactions (%v)", id.String())
		}

		intervalsForEpoch, err := database.GetProcessedTickIntervalsPerEpoch(ctx, uint32(epoch))
		if err != nil {
			id := uuid.New()
			log.Printf("[ERROR] (%s) getting processed tick intervals for epoch [%d]: %v", id.String(), epoch, err)
			return nil, status.Errorf(codes.Internal, "error getting transactions (%v)", id.String())
		}

		for _, interval := range intervalsForEpoch.GetIntervals() {
			if tick > interval.InitialProcessedTick && tick < interval.LastProcessedTick {
				response, err := s.getAllTickTransactionsV2(ctx, database, tick)
				if err != nil { // shouldn't this be a server error because of missing data integrity?
					if errors.Is(err, db.ErrNotFound) { // 404
						return nil, status.Errorf(codes.NotFound, "no transactions found for tick [%d].", tick)
					}
					id := uuid.New()
					log.Printf("[ERROR] (%s) getting transactions for tick [%d]: %v", id.String(), tick, err)
					return nil, status.Errorf(codes.Internal, "error getting transactions (%v)", err.Error())
				}
				return response, nil
			}
		}
	}
	// out of stored tick range
	return nil, status.Errorf(codes.NotFound, "no data available for tick [%d]", tick)
}

// GetTickData returns the tick data or 404 if the requested tick is not available.
func (s *ArchiveServiceServer) GetTickData(ctx context.Context, in *protobuf.GetTickDataRequest) (*protobuf.GetTickDataResponse, error) {
	tick := in.GetTickNumber()

	epochs := s.dbPool.GetAvailableEpochsDescending() // first look in current epoch
	for _, epoch := range epochs {

		database, err := s.dbPool.GetDbForEpoch(epoch)
		if err != nil {
			id := uuid.New()
			log.Printf("[ERROR] (%s) getting database for epoch [%d]: %v", id.String(), epoch, err)
			return nil, status.Errorf(codes.Internal, "error getting tick data (%v)", id.String())
		}

		intervalsForEpoch, err := database.GetProcessedTickIntervalsPerEpoch(ctx, uint32(epoch))
		if err != nil {
			id := uuid.New()
			log.Printf("[ERROR] (%s) getting processed tick intervals for epoch [%d]: %v", id.String(), epoch, err)
			return nil, status.Errorf(codes.Internal, "error getting tick data (%v)", id.String())
		}

		for _, interval := range intervalsForEpoch.GetIntervals() {
			if tick > interval.InitialProcessedTick && tick < interval.LastProcessedTick {
				// response, err := s.getAllTickTransactionsV2(ctx, database, tick)

				tickData, err := database.GetTickData(ctx, tick)
				if err != nil {
					if errors.Is(err, db.ErrNotFound) { // shouldn't this be a server error because of missing data integrity?
						return nil, status.Errorf(codes.NotFound, "no tick data found for tick [%d]", tick)
					}
					id := uuid.New()
					log.Printf("[ERROR] (%s) getting tick data for tick [%d]: %v", id.String(), tick, err)
					return nil, status.Errorf(codes.Internal, "error getting tick data (%v)", id.String())
				}

				return &protobuf.GetTickDataResponse{
					TickData: tickData,
				}, nil
			}
		}
	}
	// out of stored tick range
	return nil, status.Errorf(codes.NotFound, "no data available for tick [%d]", tick)
}

func (s *ArchiveServiceServer) GetComputors(ctx context.Context, in *protobuf.GetComputorsRequest) (*protobuf.GetComputorsResponse, error) {
	epoch := uint16(in.GetEpoch())

	if s.dbPool.HasDbForEpoch(epoch) {
		database, err := s.dbPool.GetDbForEpoch(epoch)
		if err != nil {
			id := uuid.New()
			log.Printf("[ERROR] (%s) getting database for epoch [%d]: %v", id.String(), epoch, err)
			return nil, status.Errorf(codes.Internal, "error getting computors (%v)", id.String())
		}
		computors, err := database.GetComputors(ctx, uint32(epoch))
		if err != nil && !errors.Is(err, db.ErrNotFound) {
			id := uuid.New()
			log.Printf("[ERROR] (%s) getting computors for epoch [%d]: %v", id.String(), epoch, err)
			return nil, status.Errorf(codes.Internal, "error getting computors (%v)", id.String())
		}

		if computors == nil || len(computors.Computors) == 0 || errors.Is(err, db.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "no computors found for epoch [%d]", epoch)
		}

		return &protobuf.GetComputorsResponse{
			Computors: computors.Computors[len(computors.Computors)-1],
		}, nil
	}

	return nil, status.Errorf(codes.NotFound, "no data available for epoch [%d]", epoch)

}

func (s *ArchiveServiceServer) getAllTickTransactionsV2(ctx context.Context, database *db.PebbleStore, tickNumber uint32) (*protobuf.GetTickTransactionsResponseV2, error) {
	txs, err := database.GetTickTransactions(ctx, tickNumber)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			return nil, err // return raw for status code handling
		}
		return nil, fmt.Errorf("getting tick transactions: %w", err)
	}

	var transactions []*protobuf.TransactionData
	for _, transaction := range txs {
		timestamp, moneyFlew, err := getMoreTransactionInformation(ctx, database, transaction.TxId, transaction.TickNumber)
		if err != nil {
			return nil, fmt.Errorf("failed to get transaction info: %w", err)
		}

		transactions = append(transactions, &protobuf.TransactionData{
			Transaction: transaction,
			Timestamp:   timestamp,
			MoneyFlew:   moneyFlew,
		})
	}

	return &protobuf.GetTickTransactionsResponseV2{Transactions: transactions}, nil
}

func getMoreTransactionInformation(ctx context.Context, pebbleStore *db.PebbleStore, transactionId string, tickNumber uint32) (uint64, bool, error) {
	tickData, err := pebbleStore.GetTickData(ctx, tickNumber)
	if err != nil {
		return 0, false, fmt.Errorf("getting tick data for tick [%d]: %w", tickNumber, err)
	}
	txStatus, err := pebbleStore.GetTransactionStatus(ctx, transactionId)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			// default false
			return tickData.Timestamp, false, nil
		}
		return 0, false, fmt.Errorf("getting transaction status for transaction [%s]: %w", transactionId, err)
	}
	return tickData.Timestamp, txStatus.MoneyFlew, nil
}

func (s *ArchiveServiceServer) Start(errChan chan error, interceptors ...grpc.UnaryServerInterceptor) error {

	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(600*1024*1024),
		grpc.MaxSendMsgSize(600*1024*1024),
		grpc.ChainUnaryInterceptor(interceptors...),
	)

	protobuf.RegisterArchiveServiceServer(srv, s)
	reflection.Register(srv)

	lis, err := net.Listen("tcp", s.listenAddrGRPC)
	if err != nil {
		return fmt.Errorf("failed to listen on grpc port: %v", err)
	}

	go func() {
		if err := srv.Serve(lis); err != nil {
			errChan <- fmt.Errorf("serving grpc listener: %v", err)
		}
	}()

	if s.listenAddrHTTP == "" {
		return nil
	}

	mux := runtime.NewServeMux(runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{
		MarshalOptions: protojson.MarshalOptions{EmitDefaultValues: true, EmitUnpopulated: true},
	}))

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(600*1024*1024),
			grpc.MaxCallSendMsgSize(600*1024*1024),
		),
	}

	err = protobuf.RegisterArchiveServiceHandlerFromEndpoint(context.Background(), mux, s.listenAddrGRPC, opts)
	if err != nil {
		return fmt.Errorf("registering grpc gateway handler: %v", err)
	}

	go func() {
		if err := http.ListenAndServe(s.listenAddrHTTP, mux); err != nil {
			errChan <- fmt.Errorf("serving http listener: %v", err)
		}
	}()

	return nil
}

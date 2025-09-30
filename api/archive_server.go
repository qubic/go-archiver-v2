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
	"github.com/qubic/go-archiver-v2/validator/quorum"
	"github.com/qubic/go-node-connector/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
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
		id := uuid.New()
		log.Printf("[WARN] (%s) no epoch databases found.", id)
		return nil, status.Errorf(codes.Internal, "error getting status (%s)", id)
	}

	intervalsPerEpoch := make([]*protobuf.ProcessedTickIntervalsPerEpoch, 0, len(epochs))
	for _, epoch := range epochs {
		database, err := s.dbPool.GetDbForEpoch(epoch)
		if err != nil {
			id := uuid.New()
			log.Printf("[ERROR] (%s) getting database for epoch [%d]: %v", id.String(), epoch, err)
			return nil, status.Errorf(codes.Internal, "error getting status (%s)", id.String())
		}
		intervalsForEpoch, err := database.GetProcessedTickIntervalsPerEpoch(ctx, uint32(epoch))
		if err != nil {
			id := uuid.New()
			log.Printf("[ERROR] (%s) getting processed tick intervals for epoch [%d]: %v", id.String(), epoch, err)
			return nil, status.Errorf(codes.Internal, "error getting status (%s)", id.String())
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
			return nil, status.Errorf(codes.Internal, "error getting transactions (%s)", id.String())
		}

		intervalsForEpoch, err := database.GetProcessedTickIntervalsPerEpoch(ctx, uint32(epoch))
		if err != nil {
			id := uuid.New()
			log.Printf("[ERROR] (%s) getting processed tick intervals for epoch [%d]: %v", id.String(), epoch, err)
			return nil, status.Errorf(codes.Internal, "error getting transactions (%s)", id.String())
		}

		for _, interval := range intervalsForEpoch.GetIntervals() {
			if tick >= interval.InitialProcessedTick && tick <= interval.LastProcessedTick {
				response, err := getAllTickTransactionsV2(ctx, database, tick)
				if err != nil { // shouldn't this be a server error because of missing data integrity?
					if errors.Is(err, db.ErrNotFound) { // 404
						return nil, status.Errorf(codes.NotFound, "no transactions found for tick [%d]", tick)
					}
					id := uuid.New()
					log.Printf("[ERROR] (%s) getting transactions for tick [%d]: %v", id.String(), tick, err)
					return nil, status.Errorf(codes.Internal, "error getting transactions (%s)", id.String())
				}
				return response, nil
			}
		}
	}

	// out of stored tick range
	return nil, noDataAvailableForTick(tick, s.tickStatus.ProcessedTick)
}

var emptyTickData = &protobuf.TickData{}

// GetTickData returns the tick data or 404 if the requested tick is not available.
func (s *ArchiveServiceServer) GetTickData(ctx context.Context, in *protobuf.GetTickDataRequest) (*protobuf.GetTickDataResponse, error) {
	tick := in.GetTickNumber()

	epochs := s.dbPool.GetAvailableEpochsDescending() // first look in current epoch
	for _, epoch := range epochs {

		database, err := s.dbPool.GetDbForEpoch(epoch)
		if err != nil {
			id := uuid.New()
			log.Printf("[ERROR] (%s) getting database for epoch [%d]: %v", id.String(), epoch, err)
			return nil, status.Errorf(codes.Internal, "error getting tick data (%s)", id.String())
		}

		intervalsForEpoch, err := database.GetProcessedTickIntervalsPerEpoch(ctx, uint32(epoch))
		if err != nil {
			id := uuid.New()
			log.Printf("[ERROR] (%s) getting processed tick intervals for epoch [%d]: %v", id.String(), epoch, err)
			return nil, status.Errorf(codes.Internal, "error getting tick data (%s)", id.String())
		}

		for _, interval := range intervalsForEpoch.GetIntervals() {
			if tick >= interval.InitialProcessedTick && tick <= interval.LastProcessedTick {

				tickData, err := database.GetTickData(ctx, tick)
				if err != nil {
					if errors.Is(err, db.ErrNotFound) { // shouldn't this be a server error because of missing data integrity?
						return nil, status.Errorf(codes.NotFound, "no tick data found for tick [%d]", tick)
					}
					id := uuid.New()
					log.Printf("[ERROR] (%s) getting tick data for tick [%d]: %v", id.String(), tick, err)
					return nil, status.Errorf(codes.Internal, "error getting tick data (%s)", id.String())
				}

				// we store ticks that are empty, but
				// we don't want to return tick data information for empty ticks
				// other services rely on that and do not want to check this information
				if proto.Equal(tickData, emptyTickData) {
					tickData = nil
				}

				return &protobuf.GetTickDataResponse{
					TickData: tickData,
				}, nil
			}
		}
	}

	// out of stored tick range
	return nil, noDataAvailableForTick(tick, s.tickStatus.ProcessedTick)
}

func (s *ArchiveServiceServer) GetComputors(ctx context.Context, in *protobuf.GetComputorsRequest) (*protobuf.GetComputorsResponse, error) {
	epoch := uint16(in.GetEpoch())

	if s.dbPool.HasDbForEpoch(epoch) {
		database, err := s.dbPool.GetDbForEpoch(epoch)
		if err != nil {
			id := uuid.New()
			log.Printf("[ERROR] (%s) getting database for epoch [%d]: %v", id.String(), epoch, err)
			return nil, status.Errorf(codes.Internal, "error getting computors (%s)", id.String())
		}
		computors, err := database.GetComputors(ctx, uint32(epoch))
		if err != nil && !errors.Is(err, db.ErrNotFound) {
			id := uuid.New()
			log.Printf("[ERROR] (%s) getting computors for epoch [%d]: %v", id.String(), epoch, err)
			return nil, status.Errorf(codes.Internal, "error getting computors (%s)", id.String())
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

func (s *ArchiveServiceServer) GetTransactionV2(ctx context.Context, req *protobuf.GetTransactionRequestV2) (*protobuf.GetTransactionResponseV2, error) {

	hash := types.Identity(req.TxId)
	_, err := hash.ToPubKey(true)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid transaction hash")
	}

	epochs := s.dbPool.GetAvailableEpochsDescending() // first look in current epoch
	for _, epoch := range epochs {

		database, err := s.dbPool.GetDbForEpoch(epoch)
		if err != nil {
			id := uuid.New()
			log.Printf("[ERROR] (%s) getting database for epoch [%d]: %v", id.String(), epoch, err)
			return nil, status.Errorf(codes.Internal, "error getting transaction (%s)", id.String())
		}

		tx, err := database.GetTransaction(ctx, req.TxId)
		if err != nil && !errors.Is(err, db.ErrNotFound) {
			id := uuid.New()
			log.Printf("[ERROR] (%s) getting tx with hash [%s] in epoch [%d]: %v",
				id.String(), req.GetTxId(), epoch, err)
			return nil, status.Errorf(codes.Internal, "error getting transaction (%s)", id.String())
		}

		if tx != nil { // && err == nil

			timestamp, moneyFlew, err := getMoreTransactionInformation(ctx, database, req.GetTxId(), tx.GetTickNumber())
			if err != nil {
				id := uuid.New()
				log.Printf("[ERROR] (%s) getting info for tx with hash [%s] in tick [%d] and epoch [%d]: %v",
					id.String(), req.GetTxId(), tx.GetTickNumber(), epoch, err)
				return nil, status.Errorf(codes.Internal, "error getting transaction (%s)", id.String())
			}

			return &protobuf.GetTransactionResponseV2{
				Transaction: tx,
				MoneyFlew:   moneyFlew,
				Timestamp:   timestamp,
			}, nil

		}

	}

	return nil, status.Errorf(codes.NotFound, "transaction not found.")
}

func (s *ArchiveServiceServer) GetTickQuorumDataV2(ctx context.Context, req *protobuf.GetQuorumTickDataRequest) (*protobuf.GetQuorumTickDataResponse, error) {
	tick := req.GetTickNumber()

	epochs := s.dbPool.GetAvailableEpochsDescending() // first look in current epoch
	for _, epoch := range epochs {

		database, err := s.dbPool.GetDbForEpoch(epoch)
		if err != nil {
			id := uuid.New()
			log.Printf("[ERROR] (%s) getting database for epoch [%d]: %v", id.String(), epoch, err)
			return nil, status.Errorf(codes.Internal, "error getting quorum data (%s)", id.String())
		}

		intervalsForEpoch, err := database.GetProcessedTickIntervalsPerEpoch(ctx, uint32(epoch))
		if err != nil {
			id := uuid.New()
			log.Printf("[ERROR] (%s) getting processed tick intervals for epoch [%d]: %v", id.String(), epoch, err)
			return nil, status.Errorf(codes.Internal, "error getting quorum data (%s)", id.String())
		}

		for intervalIndex, interval := range intervalsForEpoch.GetIntervals() {

			if tick >= interval.InitialProcessedTick && tick < interval.LastProcessedTick {

				quorumData, err := database.GetQuorumTickData(ctx, tick)
				if err != nil {
					if errors.Is(err, db.ErrNotFound) { // shouldn't this be a server error because of missing data integrity?
						return nil, status.Errorf(codes.NotFound, "no quorum data found for tick [%d]", tick)
					}
					id := uuid.New()
					log.Printf("[ERROR] (%s) getting quorum data for tick [%d]: %v", id.String(), tick, err)
					return nil, status.Errorf(codes.Internal, "error getting quorum data (%s)", id.String())
				}

				nextQuorumData, err := database.GetQuorumTickData(ctx, tick+1)
				if err != nil {
					id := uuid.New()
					log.Printf("[ERROR] (%s) getting quorum data for (next) tick [%d]: %v", id.String(), tick+1, err)
					return nil, status.Errorf(codes.Internal, "error getting quorum data (%s)", id.String())
				}

				computors, err := getComputors(ctx, database, tick, epoch)
				if err != nil {
					id := uuid.New()
					log.Printf("[ERROR] (%s) getting computors: %v", id.String(), err)
					return nil, status.Errorf(codes.Internal, "error getting quorum data (%s)", id.String())
				}

				reconstructedQuorumData, err := quorum.ReconstructQuorumData(quorumData, nextQuorumData, computors)
				if err != nil {
					id := uuid.New()
					log.Printf("[ERROR] (%s) reconstructing quorum data for tick [%d]: %v", id.String(), tick, err)
					return nil, status.Errorf(codes.Internal, "error getting quorum data (%s)", id.String())
				}

				return &protobuf.GetQuorumTickDataResponse{
					QuorumTickData: reconstructedQuorumData,
				}, nil

			} else if tick == interval.LastProcessedTick {

				// try to get data for last tick in interval
				lastQuorumData, err := tryToGetLastTickQuorumData(database, intervalIndex, tick, epoch)
				if err != nil {
					id := uuid.New()
					if tick == s.tickStatus.ProcessedTick {
						log.Printf("[WARN] requested tick is last processed tick. possible race condition.")
					}
					log.Printf("[ERROR] (%s) getting (last) quorum data for tick [%d]: %v", id.String(), tick, err)
					return nil, status.Errorf(codes.Internal, "error getting quorum data (%s)", id.String())
				}
				return &protobuf.GetQuorumTickDataResponse{QuorumTickData: lastQuorumData}, nil

			}
		}
	}

	return nil, noDataAvailableForTick(tick, s.tickStatus.ProcessedTick)
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

func noDataAvailableForTick(tick, processedTick uint32) error {
	return status.Errorf(codes.Internal, "no data available for requested tick [%d], last processed tick [%d]", tick, processedTick)
}

package api

import (
	"context"
	"fmt"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/qubic/go-archiver/db"
	"github.com/qubic/go-archiver/processor"
	"github.com/qubic/go-archiver/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/emptypb"
	"net"
	"net/http"
)

type ArchiveServiceServer struct {
	protobuf.UnimplementedArchiveServiceServer
	listenAddrGRPC string
	listenAddrHTTP string
	dbPool         *db.DatabasePool
	tickStatus     *processor.TickStatus
}

func NewArchiveServer(dbPool *db.DatabasePool, tickStatus *processor.TickStatus, listenAddrGRPC string, listenAddrHTTP string) *ArchiveServiceServer {
	return &ArchiveServiceServer{
		UnimplementedArchiveServiceServer: protobuf.UnimplementedArchiveServiceServer{},
		listenAddrGRPC:                    listenAddrGRPC,
		listenAddrHTTP:                    listenAddrHTTP,
		dbPool:                            dbPool,
		tickStatus:                        tickStatus,
	}
}

func (s *ArchiveServiceServer) GetHealth(context.Context, *emptypb.Empty) (*protobuf.GetHealthResponse, error) {
	upToDate := s.tickStatus.ProcessedTick > 0 &&
		s.tickStatus.LiveTick > 0 &&
		s.tickStatus.ProcessedTick > s.tickStatus.LiveTick-10
	return &protobuf.GetHealthResponse{
		Status:        "UP",
		UpToDate:      upToDate,
		ProcessedTick: s.tickStatus.ProcessedTick,
		LiveTick:      s.tickStatus.LiveTick,
		LiveEpoch:     uint32(s.tickStatus.LiveEpoch),
	}, nil
}

func (s *ArchiveServiceServer) GetStatus(_ context.Context, _ *emptypb.Empty) (*protobuf.GetStatusResponse, error) {

	// FIXME
	return &protobuf.GetStatusResponse{
		LastProcessedTick: &protobuf.ProcessedTick{
			TickNumber: s.tickStatus.ProcessedTick,
			Epoch:      uint32(s.tickStatus.ProcessingEpoch),
		},
		ProcessedTickIntervalsPerEpoch: nil,
	}, nil
}

//
//func (s *ArchiveServiceServer) GetTickTransactionsV2(ctx context.Context, in *protobuf.GetTickTransactionsRequestV2) (*protobuf.GetTickTransactionsResponseV2, error) {
//
//}
//
//func (s *ArchiveServiceServer) GetTickData(ctx context.Context, in *protobuf.GetTickDataRequest) (*protobuf.GetTickDataResponse, error) {
//
//}
//
//func (s *ArchiveServiceServer) GetComputors(ctx context.Context, in *protobuf.GetComputorsRequest) (*protobuf.GetComputorsResponse, error) {
//
//}

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

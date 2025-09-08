package api

import (
	"context"
	"fmt"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
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
}

func NewArchiveServer(listenAddrGRPC string, listenAddrHTTP string) *ArchiveServiceServer {
	return &ArchiveServiceServer{
		UnimplementedArchiveServiceServer: protobuf.UnimplementedArchiveServiceServer{},
		listenAddrGRPC:                    listenAddrGRPC,
		listenAddrHTTP:                    listenAddrHTTP,
	}
}

func (s *ArchiveServiceServer) GetHealth(context.Context, *emptypb.Empty) (*protobuf.GetHealthResponse, error) {
	return &protobuf.GetHealthResponse{Status: "UP"}, nil
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

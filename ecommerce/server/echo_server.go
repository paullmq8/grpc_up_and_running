package main

import (
	"context"
	pb "ecommerce/interface/pbs"
	"fmt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ecServer struct {
	addr string
	pb.UnimplementedEchoServer
}

func (s *ecServer) UnaryEcho(ctx context.Context, req *pb.EchoRequest) (*pb.EchoResponse, error) {
	return &pb.EchoResponse{
		Message: fmt.Sprintf("%s (from %s)", req.Message, s.addr),
	}, nil
}

// ServerStreamingEcho is server side streaming.
func (s *ecServer) ServerStreamingEcho(*pb.EchoRequest, pb.Echo_ServerStreamingEchoServer) error {
	return status.Errorf(codes.Unimplemented, "not implemented")
}

// ClientStreamingEcho is client side streaming.
func (s *ecServer) ClientStreamingEcho(pb.Echo_ClientStreamingEchoServer) error {
	return status.Errorf(codes.Unimplemented, "not implemented")
}

// BidirectionalStreamingEcho is bidi streaming.
func (s *ecServer) BidirectionalStreamingEcho(pb.Echo_BidirectionalStreamingEchoServer) error {
	return status.Errorf(codes.Unimplemented, "not implemented")
}
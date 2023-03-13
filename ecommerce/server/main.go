package main

import (
	"context"
	"log"
	"net"
	"sync"
	"time"

	pb "ecommerce/interface/pbs"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
)

const (
	productPort = ":50051"
	orderPort   = ":50052"
)

func init() {
	log.SetFlags(log.Lshortfile)
}

var (
	wg    sync.WaitGroup
	addrs = []string{":50053", ":50054"}
)

func main() {

	wg.Add(1)
	go productInfoServerStart()
	wg.Add(1)
	go orderManagementServerStart()
	echoServerStart()
	wg.Wait()
}

func productInfoServerStart() {
	defer wg.Done()
	grpcLis1, err := net.Listen("tcp", productPort)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer1 := grpc.NewServer()
	pb.RegisterProductInfoServer(grpcServer1, &productInfoServer{})
	log.Printf("Starting gRPC ProductInfo server on port %s", productPort)

	if err := grpcServer1.Serve(grpcLis1); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func orderUnaryServerInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	// Preprocessing logic
	// Gets info about the current RPC call by examining the args passed in
	log.Println("==============[Server Interceptor] ", info.FullMethod)

	// Invoking the handler to complete the normal execution of a unary RPC.
	m, err := handler(ctx, req)

	// Post processing logic
	log.Printf(" Post Proc Message : %s", m)
	return m, err
}

type wrappedStream struct {
	grpc.ServerStream
}

func (w *wrappedStream) RecvMsg(m interface{}) error {
	log.Printf("====== [Server Stream Interceptor Wrapper] Receive a message (Type: %T) at %s", m, time.Now().Format(time.RFC3339))
	return w.ServerStream.RecvMsg(m)
}

func (w *wrappedStream) SendMsg(m interface{}) error {
	log.Printf("====== [Server Stream Interceptor Wrapper] Send a message (Type: %T) at %v", m, time.Now().Format(time.RFC3339))
	return w.ServerStream.SendMsg(m)
}

func newWrappedStream(s grpc.ServerStream) grpc.ServerStream {
	return &wrappedStream{s}
}

func orderStreamServerInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	// Pre-processing
	log.Println("==============[Server Stream Interceptor] ", info.FullMethod)

	// Invoking the StreamHandler to complete the execution of RPC invocation
	err := handler(srv, newWrappedStream(ss))
	if err != nil {
		log.Printf("RPC failed with error %v", err)
	}
	return err
}

func orderManagementServerStart() {
	defer wg.Done()
	grpcLis2, err := net.Listen("tcp", orderPort)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	// Registering the Interceptor at the server-side.
	grpcServer2 := grpc.NewServer(grpc.UnaryInterceptor(orderUnaryServerInterceptor),
		grpc.StreamInterceptor(orderStreamServerInterceptor))
	s := &orderManagementServer{}
	s.PrepareOrders()
	pb.RegisterOrderManagementServer(grpcServer2, s)
	pb.RegisterGreetingServer(grpcServer2, &helloServer{})
	log.Printf("Starting gRPC OrderManagement server on port %s", orderPort)

	if err := grpcServer2.Serve(grpcLis2); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func startEchoServer(addr string) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterEchoServer(s, &ecServer{addr: addr})
	log.Printf("serving on %s\n", addr)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func echoServerStart() {
	for _, addr := range addrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			startEchoServer(addr)
		}(addr)
	}
}

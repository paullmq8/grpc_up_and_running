package main

import (
	"log"
	"net"
	"sync"

	pb "ecommerce/interface/pbs"

	"google.golang.org/grpc"
)

const (
	productPort = ":50051"
    orderPort = ":50052"
)

var wg sync.WaitGroup

func main() {

    wg.Add(1)
    go productInfoServerStart()
    wg.Add(1)
    go orderManagementServerStart()
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

func orderManagementServerStart() {
    defer wg.Done()
	grpcLis2, err := net.Listen("tcp", orderPort)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
    grpcServer2 := grpc.NewServer()
	s := &orderManagementServer{}
    s.PrepareOrders()
    pb.RegisterOrderManagementServer(grpcServer2, s)
	log.Printf("Starting gRPC OrderManagement server on port %s", orderPort)

    if err := grpcServer2.Serve(grpcLis2); err != nil {
        log.Fatalf("failed to serve: %v", err)
    }
}

package main

import (
	"context"
	"log"

	"github.com/gofrs/uuid"

	pb "ecommerce/interface/pbs"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// server is used to implement ecommerce.product_info.
type productInfoServer struct {
	productMap map[string]*pb.Product
    pb.UnimplementedProductInfoServer
}

// AddProduct implements ecommerce.AddProduct in unary RPC mode
func (s *productInfoServer) AddProduct(ctx context.Context, in *pb.Product) (*pb.ProductID, error) {
	out, err := uuid.NewV4()
	if err != nil {
		return nil, status.Errorf(codes.Internal,
			"Error while generating Product ID", err)
	}
	in.Id = out.String()
	if s.productMap == nil {
		s.productMap = make(map[string]*pb.Product)
	}
	s.productMap[in.Id] = in
    log.Printf("AddProduct is done. ProductId: %v", in.Id)
	return &pb.ProductID{Value: in.Id}, status.New(codes.OK, "").Err()
}

// GetProduct implements ecommerce.GetProduct in unary RPC mode
func (s *productInfoServer) GetProduct(ctx context.Context, in *pb.ProductID) (*pb.Product, error) {
	value, exists := s.productMap[in.Value]
	if exists {
        log.Printf("GetProduct is done. ProductId: %v", in.Value)
		return value, status.New(codes.OK, "").Err()
	} else {
		return nil, status.Errorf(codes.NotFound, "Product does not exist.", in.Value)
	}
}



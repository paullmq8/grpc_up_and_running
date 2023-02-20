package main

import (
    "log"
	"context"
    "strconv"

	"github.com/gofrs/uuid"
	"github.com/golang/protobuf/ptypes/wrappers"

	pb "ecommerce/interface/pbs"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// server is used to implement ecommerce.product_info.
type productInfoServer struct {
	productMap map[string]*pb.Product
    pb.UnimplementedProductInfoServer
}

type orderManagementServer struct {
    orderMap map[string]*pb.Order
    pb.UnimplementedOrderManagementServer
}

// AddProduct implements ecommerce.AddProduct
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

// GetProduct implements ecommerce.GetProduct
func (s *productInfoServer) GetProduct(ctx context.Context, in *pb.ProductID) (*pb.Product, error) {
	value, exists := s.productMap[in.Value]
	if exists {
        log.Printf("GetProduct is done. ProductId: %v", in.Value)
		return value, status.New(codes.OK, "").Err()
	} else {
		return nil, status.Errorf(codes.NotFound, "Product does not exist.", in.Value)
	}
}

func prepareOrders(s *orderManagementServer) {
    if s.orderMap == nil {
        log.Println("orderMap is prepared!")
        s.orderMap = make(map[string]*pb.Order)
    }
    arr := [10]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
    for i := range arr {
        num := strconv.Itoa(i)
        s.orderMap[num] = &pb.Order{
            Id: "Order" + num, 
            Items: []string{"item " + num + " A", "item " + num + " B"}, 
            Description: "desc" + num, 
            Price: float32(i) * 37.9, 
            Destination: "dest " + num,
        }
    }
}

func (s *orderManagementServer) GetOrder(ctx context.Context, orderId *wrappers.StringValue) (*pb.Order, error) {
    prepareOrders(s)
    ord, ok := s.orderMap[orderId.Value]
    if !ok {
        log.Println("GetOrder is done but no order found.")
    } else {
        log.Printf("GetOrder is done. OrderId: %v", orderId.Value)
    }
    return ord, nil
}

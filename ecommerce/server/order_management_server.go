package main

import (
	"context"
	pb "ecommerce/interface/pbs"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/golang/protobuf/ptypes/wrappers"
	epb "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"io"
	"log"
	"strings"
)

const (
	orderBatchSize = 3
)

type orderManagementServer struct {
	orderMap map[string]*pb.Order
	pb.UnimplementedOrderManagementServer
}

// PrepareOrders is a utility method and prepares the orderMap with some sample orders
func (s *orderManagementServer) PrepareOrders() {
	if s.orderMap == nil {
		s.orderMap = make(map[string]*pb.Order)
	}
	s.orderMap["102"] = &pb.Order{Id: "102", Items: []string{"Google Pixel 3A", "Mac Book Pro"}, Destination: "Mountain View, CA", Price: 1800.00}
	s.orderMap["103"] = &pb.Order{Id: "103", Items: []string{"Apple Watch S4"}, Destination: "San Jose, CA", Price: 400.00}
	s.orderMap["104"] = &pb.Order{Id: "104", Items: []string{"Google Home Mini", "Google Nest Hub"}, Destination: "Mountain View, CA", Price: 400.00}
	s.orderMap["105"] = &pb.Order{Id: "105", Items: []string{"Amazon Echo"}, Destination: "San Jose, CA", Price: 30.00}
	s.orderMap["106"] = &pb.Order{Id: "106", Items: []string{"Amazon Echo", "Apple iPhone XS"}, Destination: "Mountain View, CA", Price: 300.00}
	log.Println("orderMap is prepared!")
}

func (s *orderManagementServer) AddOrder(ctx context.Context, in *pb.Order) (*wrappers.StringValue, error) {
	// reading metadata sent from unary client
	printUnaryMetadata(ctx)

	if in.Id == "-1" {
		log.Printf("Order ID is invalid! -> Received Order ID %s", in.Id)
		errorStatus := status.New(codes.InvalidArgument, "Invalid information received")
		ds, err := errorStatus.WithDetails(&epb.BadRequest_FieldViolation{
			Field: "ID",
			Description: fmt.Sprintf(
				"Order ID received is not valid %s : %s",
				in.Id, in.Description,
			),
		})
		if err != nil {
			return nil, errorStatus.Err()
		}
		return nil, ds.Err()
	}
	s.orderMap[in.Id] = in
	log.Println("Order : ", in.Id, " -> Added")
	log.Println("Setting server header and trailer here")

	// set metadata in header and trailer
	setMetadataForRPCCall(ctx)

	//time.Sleep(5 * time.Second)
	return &wrappers.StringValue{Value: "Order Added: " + in.Id}, nil
}

// GetOrder returns the order for the given order id in unary RPC mode
func (s *orderManagementServer) GetOrder(ctx context.Context, orderId *wrappers.StringValue) (*pb.Order, error) {
	// reading metadata sent from unary client
	printUnaryMetadata(ctx)

	ord, ok := s.orderMap[orderId.Value]
	if !ok {
		log.Println("GetOrder is done but no order found.")
	} else {
		log.Printf("GetOrder is done. OrderId: %v", orderId.Value)
	}

	// set metadata in header and trailer
	setMetadataForRPCCall(ctx)

	return ord, nil
}

// SearchOrders returns the orders for the given search query in server streaming RPC mode
func (s *orderManagementServer) SearchOrders(searchQuery *wrappers.StringValue, stream pb.OrderManagement_SearchOrdersServer) error {
	// print metadata
	printStreamMetadata(stream)

	// set header and trailer metadata
	setMetadataForStreamingCall(stream)

	for key, order := range s.orderMap {
		log.Print(key, order)
		for _, itemStr := range order.Items {
			log.Print(itemStr)
			if strings.Contains(itemStr, searchQuery.Value) {
				// Send the matching orders in a stream
				err := stream.Send(order)
				if err != nil {
					return fmt.Errorf("error sending message to stream: %v", err)
				}
				log.Print("Matching Order Found : " + key)
				break
			}
		}
	}
	return nil
}

// UpdateOrders updates the orders in client streaming RPC mode
func (s *orderManagementServer) UpdateOrders(stream pb.OrderManagement_UpdateOrdersServer) error {
	// print metadata
	printStreamMetadata(stream)

	// set header and trailer metadata
	setMetadataForStreamingCall(stream)

	ordersStr := "Updated Order IDs :"
	for {
		order, err := stream.Recv()
		if err == io.EOF {
			// Finished reading the order stream.
			return stream.SendAndClose(
				&wrappers.StringValue{Value: "Orders processed " + strings.TrimSuffix(ordersStr, ", ")})
		}
		// Update order
		s.orderMap[order.Id] = order

		log.Println("Order ID ", order.Id, ": Updated")
		ordersStr += order.Id + ", "
	}
}

func (s *orderManagementServer) ProcessOrders(stream pb.OrderManagement_ProcessOrdersServer) error {
	// print metadata
	printStreamMetadata(stream)

	// set header and trailer metadata
	setMetadataForStreamingCall(stream)

	batchMarker := 1
	combinedShipmentMap := make(map[string]*pb.CombinedShipment)
	for {
		orderId, err := stream.Recv()
		log.Printf("Reading Proc order : %s", orderId)
		if err == io.EOF {
			// Client has sent all the messages
			// Send remaining shipments
			log.Printf("EOF : %s", orderId)
			for _, shipment := range combinedShipmentMap {
				if err := stream.Send(shipment); err != nil {
					return err
				}
			}
			return nil
		}
		if err != nil {
			log.Println(err)
			return err
		}

		orderIdValue := orderId.GetValue()
		existingOrder, ok := s.orderMap[orderIdValue]
		if !ok {
			continue
		}
		destination := existingOrder.Destination
		shipment, found := combinedShipmentMap[destination]

		if found {
			ord := s.orderMap[orderIdValue]
			shipment.OrdersList = append(shipment.OrdersList, ord)
			combinedShipmentMap[destination] = shipment
		} else {
			comShip := &pb.CombinedShipment{
				Id:         "cmb - " + (s.orderMap[orderIdValue].Destination),
				Status:     "Processed!",
				OrdersList: make([]*pb.Order, 3),
			}
			ord := s.orderMap[orderIdValue]
			comShip.OrdersList = append(comShip.OrdersList, ord)
			combinedShipmentMap[destination] = comShip
			log.Print(len(comShip.OrdersList), comShip.GetId())
		}

		if batchMarker == orderBatchSize {
			for _, comb := range combinedShipmentMap {
				log.Printf("Shipping : %v -> %v", comb.Id, len(comb.OrdersList))
				if err := stream.Send(comb); err != nil {
					return err
				}
			}
			batchMarker = 0
			combinedShipmentMap = make(map[string]*pb.CombinedShipment)
		} else {
			batchMarker++
		}
	}
}

type helloServer struct {
	pb.UnimplementedGreetingServer
}

func (h *helloServer) Hello(ctx context.Context, ept *empty.Empty) (*empty.Empty, error) {

	// reading metadata sent from unary client
	printUnaryMetadata(ctx)

	log.Println("Hello from Hello Service!")
	return ept, nil
}

func printUnaryMetadata(ctx context.Context) {
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		log.Println("Printing metadata sent from client in unary RPC call")
		for k, v := range md {
			log.Println(k, v)
		}
	} else {
		log.Println("No metadata sent from client in unary RPC call")
	}
}

func printStreamMetadata(stream grpc.ServerStream) {
	md, ok := metadata.FromIncomingContext(stream.Context())
	if ok {
		log.Println("Printing metadata sent from client in Streaming RPC call")
		for k, v := range md {
			log.Println(k, v)
		}
	} else {
		log.Println("No metadata sent from client in Streaming RPC call")
	}
}

func setMetadataForRPCCall(ctx context.Context) {
	// set metadata in header
	header := metadata.Pairs("rpc-server-header-key", "val")
	_ = grpc.SetHeader(ctx, header)
	// set metadata in trailer
	trailer := metadata.Pairs("rpc-server-trailer-key", "val")
	_ = grpc.SetTrailer(ctx, trailer)
}

func setMetadataForStreamingCall(stream grpc.ServerStream) {
	// create and send header
	header := metadata.Pairs("streaming-server-header-key", "val")
	_ = stream.SetHeader(header)
	// create and set trailer
	trailer := metadata.Pairs("streaming-server-trailer-key", "val")
	stream.SetTrailer(trailer)
}

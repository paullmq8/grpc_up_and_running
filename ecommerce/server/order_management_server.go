package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"

	pb "ecommerce/interface/pbs"

	"github.com/golang/protobuf/ptypes/wrappers"
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

// GetOrder returns the order for the given order id in unary RPC mode
func (s *orderManagementServer) GetOrder(ctx context.Context, orderId *wrappers.StringValue) (*pb.Order, error) {
	ord, ok := s.orderMap[orderId.Value]
	if !ok {
		log.Println("GetOrder is done but no order found.")
	} else {
		log.Printf("GetOrder is done. OrderId: %v", orderId.Value)
	}
	return ord, nil
}

// SearchOrders returns the orders for the given search query in server streaming RPC mode
func (s *orderManagementServer) SearchOrders(searchQuery *wrappers.StringValue, stream pb.OrderManagement_SearchOrdersServer) error {
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

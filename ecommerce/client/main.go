package main

import (
	"context"
	"io"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"

	pb "ecommerce/interface/pbs"

	"github.com/golang/protobuf/ptypes/wrappers"
	"google.golang.org/grpc"
)

const (
	productAddress = "localhost:50051"
	orderAddress   = "localhost:50052"
)

var wg sync.WaitGroup

func main() {
	// call two unary RPC methods
	// wg.Add(1)
	// go callAddProductAndGetProduct()
	// wg.Add(1)
	// go callGetOrder()
	// call server-side streaming RPC method
	// wg.Add(1)
	// go callSearchOrders()
	// wg.Add(1)
	// go callUpdateOrders()
	wg.Add(1)
	go callProcessOrders()
	wg.Wait()
}

// callAddProductAndGetProduct calls the AddProduct and GetProduct gRPC methods of the ProductInfoServer
// in unary RPC mode.
func callAddProductAndGetProduct() {
	defer wg.Done()
	conn, err := grpc.Dial(productAddress, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewProductInfoClient(conn)
	name := "Apple iPhone 11"
	description := `Meet Apple iPhone 11. All-new dual-camera system with
              Ultra Wide and Night mode.`
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, err := c.AddProduct(ctx, &pb.Product{Name: name, Description: description})
	if err != nil {
		log.Fatalf("Could not add product: %v", err)
	}
	log.Printf("AddProduct: Product ID: %s added successfully", r.Value)

	product, err := c.GetProduct(ctx, &pb.ProductID{Value: r.Value})
	if err != nil {
		log.Fatalf("Could not get product: %v", err)
	}
	log.Println("GetProduct: Product: ", product.String())
}

// callGetOrder calls the GetOrder gRPC methods of the OrderManagementServer
// in unary RPC mode.
func callGetOrder() {
	defer wg.Done()
	// Setting up a connection to the server.
	conn, err := grpc.Dial(orderAddress, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewOrderManagementClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	num := strconv.Itoa(rand.Intn(5) + 102) // 102 - 106
	retrievedOrder, err := c.GetOrder(ctx, &wrappers.StringValue{Value: num})

	if err != nil {
		log.Fatalf("can not get order: %v", err)
	}
	if retrievedOrder != nil {
		log.Println("GetOrder: Response -> : ", retrievedOrder)
	} else {
		log.Println("GetOrder: Response -> : No order found for id ", num)
	}
}

// callSearchOrders calls the SearchOrders gRPC methods of the OrderManagementServer
// in server streaming RPC mode.
func callSearchOrders() {
	defer wg.Done()
	// Setting up a connection to the server.
	conn, err := grpc.Dial(orderAddress, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewOrderManagementClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Hour)
	defer cancel()
	searchStream, _ := c.SearchOrders(ctx, &wrappers.StringValue{Value: "Google"})
	for {
		searchOrder, err := searchStream.Recv()
		if err == io.EOF {
			break
		}
		if err == nil {
			log.Print("Search Result : ", searchOrder)
		}
	}
}

// callUpdateOrders calls the UpdateOrders gRPC methods of the OrderManagementServer
// in client streaming RPC mode.
func callUpdateOrders() {
	defer wg.Done()
	// Setting up a connection to the server.
	conn, err := grpc.Dial(orderAddress, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewOrderManagementClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	updateStream, err := c.UpdateOrders(ctx)
	if err != nil {
		log.Fatalf("%v.UpdateOrders(_) = _, %v", c, err)
	}

	// Update Orders : Client streaming scenario
	updOrder1 := &pb.Order{Id: "102", Items: []string{"Google Pixel 3A", "Google Pixel Book"}, Destination: "Mountain View, CA", Price: 1100.00}
	updOrder2 := &pb.Order{Id: "103", Items: []string{"Apple Watch S4", "Mac Book Pro", "iPad Pro"}, Destination: "San Jose, CA", Price: 2800.00}
	updOrder3 := &pb.Order{Id: "104", Items: []string{"Google Home Mini", "Google Nest Hub", "iPad Mini"}, Destination: "Mountain View, CA", Price: 2200.00}

	// // Updating order 1
	if err := updateStream.Send(updOrder1); err != nil {
		log.Fatalf("%v.Send(%v) = %v", updateStream, updOrder1, err)
	}

	// // Updating order 2
	if err := updateStream.Send(updOrder2); err != nil {
		log.Fatalf("%v.Send(%v) = %v", updateStream, updOrder2, err)
	}

	// // Updating order 3
	if err := updateStream.Send(updOrder3); err != nil {
		log.Fatalf("%v.Send(%v) = %v", updateStream, updOrder3, err)
	}

	updateRes, err := updateStream.CloseAndRecv()
	if err != nil {
		log.Fatalf("%v.CloseAndRecv() got error %v, want %v", updateStream, err, nil)
	}

	log.Printf("Update Orders Res : %s", updateRes)
}

// callProcessOrders calls the ProcessOrders gRPC methods of the OrderManagementServer
// in bidirectional streaming RPC mode.
func callProcessOrders() {
	defer wg.Done()
	// Setting up a connection to the server.
	conn, err := grpc.Dial(orderAddress, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewOrderManagementClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	streamProcOrder, err := c.ProcessOrders(ctx)
	if err != nil {
		log.Fatalf("%v.ProcessOrders(_) = _, %v", c, err)
	}

	if err := streamProcOrder.Send(&wrappers.StringValue{Value: "102"}); err != nil {
		log.Fatalf("%v.Send(%v) = %v", c, "102", err)
	}

	if err := streamProcOrder.Send(&wrappers.StringValue{Value: "103"}); err != nil {
		log.Fatalf("%v.Send(%v) = %v", c, "103", err)
	}

	if err := streamProcOrder.Send(&wrappers.StringValue{Value: "104"}); err != nil {
		log.Fatalf("%v.Send(%v) = %v", c, "104", err)
	}

	channel := make(chan struct{})
	go asncClientBidirectionalRPC(streamProcOrder, channel)
	time.Sleep(time.Millisecond * 1000)

	if err := streamProcOrder.Send(&wrappers.StringValue{Value: "101"}); err != nil {
		log.Fatalf("%v.Send(%v) = %v", c, "101", err)
	}
	if err := streamProcOrder.CloseSend(); err != nil {
		log.Fatal(err)
	}
	<-channel
}

func asncClientBidirectionalRPC(streamProcOrder pb.OrderManagement_ProcessOrdersClient, c chan struct{}) {
	for {
		combinedShipment, errProcOrder := streamProcOrder.Recv()
		if errProcOrder == io.EOF {
			break
		}
		if combinedShipment != nil {
			log.Println("Combined shipment : ", combinedShipment.OrdersList)
		}
	}
	close(c)
}

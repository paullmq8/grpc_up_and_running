package main

import (
	"context"
	"fmt"
	"github.com/golang/protobuf/ptypes/empty"
	epb "google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/status"
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

func init() {
	log.SetFlags(log.Lshortfile)
	resolver.Register(&exampleResolverBuilder{})
}

const (
	productAddress     = "localhost:50051"
	orderAddress       = "localhost:50052"
	exampleScheme      = "example"
	exampleServiceName = "lb.example.grpc.io"
)

var (
	wg    sync.WaitGroup
	addrs = []string{"localhost:50053", "localhost:50054"}
)

type exampleResolverBuilder struct{}

func (*exampleResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r := &exampleResolver{
		target: target,
		cc:     cc,
		addrsStore: map[string][]string{
			exampleServiceName: addrs, // "lb.example.grpc.io": "localhost:50053", "localhost:50054"
		},
	}
	r.start()
	return r, nil
}

func (*exampleResolverBuilder) Scheme() string {
	return exampleScheme // "example"
}

type exampleResolver struct {
	target     resolver.Target
	cc         resolver.ClientConn
	addrsStore map[string][]string
}

func (r *exampleResolver) start() {
	addrStrs := r.addrsStore[r.target.Endpoint()]
	addrList := make([]resolver.Address, len(addrStrs))
	for i, s := range addrStrs {
		addrList[i] = resolver.Address{Addr: s}
	}
	_ = r.cc.UpdateState(resolver.State{Addresses: addrList})
}

func (*exampleResolver) ResolveNow(o resolver.ResolveNowOptions) {}
func (*exampleResolver) Close()                                  {}

func main() {
	// call two unary RPC methods
	//wg.Add(1)
	//go callAddProductAndGetProduct()
	//wg.Add(1)
	//go callGetOrder()
	// call server-side streaming RPC method
	//wg.Add(1)
	//go callSearchOrders()
	//wg.Add(1)
	//go callUpdateOrders()
	//wg.Add(1)
	//go callProcessOrders()
	//wg.Add(1)
	//go callAddOrder()
	wg.Add(1)
	go callEchoRPC()
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

func orderUnaryClientInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	// Preprocessor phase
	log.Println("Method : " + method)

	// Invoking the remote method
	err := invoker(ctx, method, req, reply, cc, opts...)

	// Postprocessor phase
	log.Println(reply)

	return err
}

func orderStreamClientInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string,
	streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	log.Println("======= [Client Interceptor] ", method)
	s, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		return nil, err
	}
	return newWrappedStream(s), nil
}

type wrappedStream struct {
	grpc.ClientStream
}

func (w *wrappedStream) RecvMsg(m interface{}) error {
	log.Printf("====== [Client Stream Interceptor] "+
		"Receive a message (Type: %T) at %v", m, time.Now().Format(time.RFC3339))
	return w.ClientStream.RecvMsg(m)
}

func (w *wrappedStream) SendMsg(m interface{}) error {
	log.Printf("====== [Client Stream Interceptor] "+
		"Send a message (Type: %T) at %v", m, time.Now().Format(time.RFC3339))
	return w.ClientStream.SendMsg(m)
}

func newWrappedStream(s grpc.ClientStream) grpc.ClientStream {
	return &wrappedStream{s}
}

// callGetOrder calls the GetOrder gRPC methods of the OrderManagementServer
// in unary RPC mode.
func callGetOrder() {
	defer wg.Done()
	// Setting up a connection to the server.
	conn, err := grpc.Dial(orderAddress, grpc.WithInsecure(), grpc.WithUnaryInterceptor(orderUnaryClientInterceptor))
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
	conn, err := grpc.Dial(orderAddress, grpc.WithInsecure(), grpc.WithUnaryInterceptor(orderUnaryClientInterceptor))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewOrderManagementClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
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
	conn, err := grpc.Dial(orderAddress, grpc.WithInsecure(), grpc.WithStreamInterceptor(orderStreamClientInterceptor))
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
	conn, err := grpc.Dial(orderAddress, grpc.WithInsecure(), grpc.WithStreamInterceptor(orderStreamClientInterceptor))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewOrderManagementClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
	go asyncClientBidirectionalRPC(streamProcOrder, channel)
	time.Sleep(time.Millisecond * 1000)

	// Cancel the RPC
	cancel()
	log.Printf("RPC Status : %s", ctx.Err())

	if err := streamProcOrder.Send(&wrappers.StringValue{Value: "101"}); err != nil {
		log.Fatalf("%v.Send(%v) = %v", c, "101", err)
	}
	if err := streamProcOrder.CloseSend(); err != nil {
		log.Fatal(err)
	}
	//<-channel
	channel <- struct{}{}
}

func asyncClientBidirectionalRPC(streamProcOrder pb.OrderManagement_ProcessOrdersClient, c <-chan struct{}) {
	for {
		combinedShipment, errProcOrder := streamProcOrder.Recv()
		if errProcOrder == io.EOF {
			break
		}
		if combinedShipment != nil {
			log.Println("Combined shipment : ", combinedShipment.OrdersList)
		}
	}
	//close(c)
	<-c
}

func callAddOrder() {
	defer wg.Done()
	conn, err := grpc.Dial(orderAddress, grpc.WithInsecure(), grpc.WithUnaryInterceptor(orderUnaryClientInterceptor))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	client := pb.NewOrderManagementClient(conn)

	md1 := metadata.New(map[string]string{
		"key1": "val1",
		"key2": "val2",
	})
	md2 := metadata.Pairs(
		"key3", "val3",
		"key3", "val3-2",
		"Key4", "val4",
		"timestamp", time.Now().Format(time.StampNano),
	)
	md := metadata.Join(md1, md2)
	mdCtx := metadata.NewOutgoingContext(context.Background(), md)

	clientDeadline := time.Now().Add(10 * time.Second)
	ctx, cancel := context.WithDeadline(mdCtx, clientDeadline)

	defer cancel()

	// Add Order
	order1 := pb.Order{
		Id:          "101",
		Items:       []string{"iPhone XS", "Mac Book Pro"},
		Destination: "San Jose, CA",
		Price:       2300.00,
	}
	var header, trailer metadata.MD

	// The AddOrder method will return an error: grpc: Decompressor is not installed for grpc-encoding "gzip"
	// if server side doesn't import "google.golang.org/grpc/encoding/gzip"
	// since client side is calling RPC in a compression way
	res, addErr := client.AddOrder(ctx, &order1, grpc.Header(&header), grpc.Trailer(&trailer),
		grpc.UseCompressor(gzip.Name))

	// process header and trailer map here received from server side
	printMetadata("header", &header)
	printMetadata("trailer", &trailer)

	if addErr != nil {
		got := status.Code(addErr)
		log.Printf("Error Occured -> callAddOrder : , %v:", got)
	} else {
		log.Print("AddOrder Response ->", res.Value)
	}

	// call Hello service
	helloClient := pb.NewGreetingClient(conn)
	helloClient.Hello(ctx, &empty.Empty{})

	order2 := pb.Order{
		Id:          "-1", // 1
		Items:       []string{"iPhone XS", "Mac Book Pro"},
		Destination: "San Jose, CA",
		Price:       2300.00,
	}
	res, addOrderError := client.AddOrder(ctx, &order2)

	if addOrderError != nil {
		errorCode := status.Code(addOrderError)
		if errorCode == codes.InvalidArgument {
			log.Printf("Invalid Argument Error : %s", errorCode)
			errorStatus := status.Convert(addOrderError)
			for _, d := range errorStatus.Details() {
				switch info := d.(type) {
				case *epb.BadRequest_FieldViolation:
					log.Printf("Request Field Invalid: %s", info)
				default:
					log.Printf("Unexpected error type: %s", info)
				}
			}
		} else {
			log.Printf("Unhandled error : %s ", errorCode)
		}
	} else {
		log.Print("AddOrder Response -> ", res.Value)
	}
}

func printMetadata(name string, md *metadata.MD) {
	if len(*md) > 0 {
		log.Println("Printing metadata", name)
		for k, v := range *md {
			log.Println("metadata:", k, v)
		}
		log.Println("Printing metadata", name, "done")
	}
}

func callUnaryEcho(c pb.EchoClient, message string) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	r, err := c.UnaryEcho(ctx, &pb.EchoRequest{Message: message})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Println(r.Message)
}

func makeRPCs(cc *grpc.ClientConn, n int) {
	hwc := pb.NewEchoClient(cc)
	for i := 0; i < n; i++ {
		callUnaryEcho(hwc, "This is examples/load_balancing")
	}
}

func callEchoRPC() {
	defer wg.Done()
	pickFirstConn, err := grpc.Dial(
		fmt.Sprintf("%s:///%s", exampleScheme, exampleServiceName), // "example:///lb.example.grpc.io"
		// grpc.WithBalancerName("pick_first"), // "pick_first" is the default, so this DialOption is not necessary.
		//grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"`+grpc.PickFirstBalancerName+`":{}}]}`), // this sets the initial balancing policy.
		grpc.WithInsecure(),
	)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer pickFirstConn.Close()

	log.Println("==== Calling helloworld.Greeter/SayHello with pick_first ====")
	makeRPCs(pickFirstConn, 10)

	// Make another ClientConn with round_robin policy.
	roundrobinConn, err := grpc.Dial(
		fmt.Sprintf("%s:///%s", exampleScheme, exampleServiceName),                     // "example:///lb.example.grpc.io"
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"round_robin":{}}]}`), // this sets the initial balancing policy.
		grpc.WithInsecure(),
	)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer roundrobinConn.Close()

	log.Println("==== Calling helloworld.Greeter/SayHello with round_robin ====")
	makeRPCs(roundrobinConn, 10)
}

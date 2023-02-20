package main

import (
	"context"
	"log"
	"sync"
	"time"
    "math/rand"
    "strconv"

	pb "ecommerce/interface/pbs"

	"github.com/golang/protobuf/ptypes/wrappers"
	"google.golang.org/grpc"
)

const (
    productAddress = "localhost:50051"
    orderAddress = "localhost:50052"
)

var wg sync.WaitGroup

func main() {
    wg.Add(1)
    go callProductInfoService()
    wg.Add(1)
    go callOrderManagementService()
    wg.Wait()
}

func callProductInfoService() {
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

func callOrderManagementService() {
    defer wg.Done()
    conn, err := grpc.Dial(orderAddress, grpc.WithInsecure())
    if err != nil {
        log.Fatalf("did not connect: %v", err)
    }
    defer conn.Close()
    c := pb.NewOrderManagementClient(conn)
    ctx, cancel := context.WithTimeout(context.Background(), time.Second)
    defer cancel()

    num := strconv.Itoa(rand.Intn(10))
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

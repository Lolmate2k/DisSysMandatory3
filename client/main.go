package main

import (
	"context"
	"log"
	"time"

	pb "chitchat/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {

	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to server: %v", err)
	}
	defer conn.Close()

	client := pb.NewChitChatClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)

	defer cancel()

	// TODO: connect to server, join, publish, leave
}

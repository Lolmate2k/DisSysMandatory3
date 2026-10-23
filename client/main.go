package main

import (
	pb "chitchat/grpc"
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type chatClient struct {
	client   pb.ChitChatClient
	username string
	clock    int64
}

func newClient(name string, conn *grpc.ClientConn) *chatClient {
	return &chatClient{
		client:   pb.NewChitChatClient(conn),
		username: name,
		clock:    0,
	}
}

func main() {

	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to server: %v", err)
	}
	defer conn.Close()

	// Creates two clients and joins them to the chat server:
	client1 := newClient("user1", conn)
	client2 := newClient("user2", conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)

	joinRequest1 := &pb.JoinRequest{Username: client1.username}
	joinRequest2 := &pb.JoinRequest{Username: client2.username}
	client1.Join(ctx, joinRequest1)
	client2.Join(ctx, joinRequest2)

	defer cancel()

	// TODO: join, publish, leave
}

// TODO: Impolement the logic below:
func (c *chatClient) Join(ctx context.Context, request *pb.JoinRequest) (grpc.ServerStreamingClient[pb.Broadcast], error) {
	// Joining does NOT increment clock.

	return nil, nil
}

func (c *chatClient) Publish(ctx context.Context, request *pb.PublishRequest) (*pb.PublishResponse, error) {
	return nil, nil
}

func (c *chatClient) Leave(ctx context.Context, request *pb.LeaveRequest) (*pb.LeaveResponse, error) {
	return nil, nil
}

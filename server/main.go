package main

import (
	pb "chitchat/grpc"
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"
)

type chatServer struct {
	pb.UnimplementedChitChatServer

	mu      sync.Mutex
	clients map[string]chan *pb.Broadcast
	clock   int64
}

func (s *chatServer) tick(recieved int64) int64 {
	s.clock = max(recieved, s.clock) + 1
	return s.clock
}

func newChatServer() *chatServer {
	return &chatServer{
		clients: make(map[string]chan *pb.Broadcast),
	}
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterChitChatServer(grpcServer, newChatServer())
	log.Println("Server listening on :50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func (s *chatServer) Join(request *pb.JoinRequest, stream grpc.ServerStreamingServer[pb.Broadcast]) error {
	username := request.GetUsername()

	s.mu.Lock()         // We lock the data.
	defer s.mu.Unlock() // We unlock the data after return.

	if s.clients[username] != nil { // Check if the channel already exists.
		return fmt.Errorf("channel with username %s is already taken", username)
	}

	s.clients[username] = make(chan *pb.Broadcast) // Create channel for the user/client.

	return nil
}

func (s *chatServer) Publish(ctx context.Context, request *pb.PublishRequest) (*pb.PublishResponse, error) {
	// TODO: Implement Publish logic

	return &pb.PublishResponse{}, nil
}

func (s *chatServer) Leave(ctx context.Context, request *pb.LeaveRequest) (*pb.LeaveResponse, error) {
	// TODO: Implement Leave logic
	username := request.GetUsername()
	// TODO: We need to update lamport clock here aswell.

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.clients[username] == nil {
		return nil, fmt.Errorf("channel with username %s does not exist", username)
	}

	delete(s.clients, username)

	return &pb.LeaveResponse{LamportTime: s.clock}, nil
}

// TODO: implement Logging

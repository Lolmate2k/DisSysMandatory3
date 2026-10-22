package main

import (
	"log"
	"net"
	"sync"

	pb "chitchat/grpc"

	"google.golang.org/grpc"
)

type chatServer struct {
	pb.UnimplementedChitChatServer

	mu sync.Mutex
	clients map[string]chan *pb.Broadcast
	clock int64
}

func (s *chatServer) tick(recieved int64) int64 {
	s.clock = max(recieved, s.clock)+1
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

// TODO: implement Join, Publish, Leave, Logging
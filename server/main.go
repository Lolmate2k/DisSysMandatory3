package main

import (
	pb "chitchat/grpc"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"unicode/utf8"

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
	logFile, err := os.OpenFile("logs/server.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterChitChatServer(grpcServer, newChatServer())
	fmt.Println("[Server] Server listening on :50051")
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

	s.clients[username] = make(chan *pb.Broadcast, 100) // Create channel for the user/client.
	ch := s.clients[username]
	log.Printf("[Server] event=CLIENT_CONNECT client=%s lamport=%d", username, s.clock)
	s.broadcast(&pb.Broadcast{
		Content:     fmt.Sprintf("Participant %s joined Chit Chat at logical time %d", username, s.clock),
		LamportTime: s.clock,
	})
	log.Printf("[Server] event=JOIN_BROADCAST client=%s lamport=%d", username, s.clock)
	s.mu.Unlock() // release the lock while this client is connected

	connectionLost := false
deliver:
	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				break deliver
			}
			if err := stream.Send(msg); err != nil {
				log.Printf("[Server] event=SEND_FAILED client=%s err=%v", username, err)
				connectionLost = true
				break deliver
			}
			log.Printf("[Server] event=DELIVER client=%s lamport=%d", username, msg.GetLamportTime())

		case <-stream.Context().Done():
			connectionLost = true
			break deliver
		}
	}

	s.mu.Lock()
	if connectionLost && s.clients[username] == ch {
		delete(s.clients, username)
		close(ch)
		log.Printf("[Server] event=CLIENT_DISCONNECT client=%s reason=connection_lost", username)
	}

	return nil
}

func (s *chatServer) Publish(ctx context.Context, request *pb.PublishRequest) (*pb.PublishResponse, error) {
	// TODO: Implement Publish logic
	username := request.GetUsername()
	content := request.GetContent()

	if !utf8.ValidString(content) || utf8.RuneCountInString(content) > 128 {
		return nil, fmt.Errorf("invalid message: must be valid UTF-8 and at most 128 characters")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.clients[username] == nil {
		return nil, fmt.Errorf("user %s has not joined", username)
	}

	s.tick(request.GetLamportTime()) // Lamport receive event
	log.Printf("[Server] event=PUBLISH_RECEIVED client=%s lamport=%d", username, s.clock)

	ts := s.tick(0)
	s.broadcast(&pb.Broadcast{
		Content:     username + ": " + content,
		LamportTime: ts,
	})

	return &pb.PublishResponse{LamportTime: ts}, nil
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

	s.tick(request.GetLamportTime())
	s.broadcast(&pb.Broadcast{
		Content:     fmt.Sprintf("Participant %s left Chit Chat at logical time %d", username, s.clock),
		LamportTime: s.clock,
	})
	close(s.clients[username])
	log.Printf("[Server] event=JOIN_BROADCAST client=%s lamport=%d", username, s.clock)
	log.Printf("[Server] event=CLIENT_LEAVE client=%s lamport=%d", username, s.clock)

	delete(s.clients, username)

	return &pb.LeaveResponse{LamportTime: s.clock}, nil
}

func (s *chatServer) broadcast(msg *pb.Broadcast) {
	log.Printf("[Server] event=BROADCAST lamport=%d content=%q recipients=%d", msg.GetLamportTime(), msg.GetContent(), len(s.clients))
	for username, ch := range s.clients {
		select {
		case ch <- msg:
		default:
			log.Printf("[Server] event=DROP client=%s reason=buffer_full lamport=%d", username, msg.GetLamportTime())
		}
	}
}

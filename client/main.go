package main

import (
	pb "chitchat/grpc"
	"context"
	"fmt"
	"io"
	"log"
	"sync/atomic"
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
	time.Sleep(500 * time.Millisecond)

	client1.Publish(ctx, &pb.PublishRequest{Content: "Hello from user1"})
	client2.Publish(ctx, &pb.PublishRequest{Content: "Hi user1, user2 here"})

	time.Sleep(1 * time.Second)

	client1.Leave(ctx, &pb.LeaveRequest{Username: client1.username})
	client2.Leave(ctx, &pb.LeaveRequest{Username: client2.username})

	time.Sleep(500 * time.Millisecond)
}

// TODO: Impolement the logic below:
func (c *chatClient) Join(ctx context.Context, request *pb.JoinRequest) (grpc.ServerStreamingClient[pb.Broadcast], error) {
	// Joining does NOT increment clock.
	stream, err := c.client.Join(context.WithoutCancel(ctx), request)
	if err != nil {
		log.Printf("[Client %s] event=JOIN_FAILED err=%v", c.username, err)
		return nil, err
	}
	log.Printf("[Client %s] event=JOINED lamport=%d", c.username, atomic.LoadInt64(&c.clock))

	go c.receive(stream)
	return stream, nil
}

func (c *chatClient) Publish(ctx context.Context, request *pb.PublishRequest) (*pb.PublishResponse, error) {
	request.Username = c.username
	request.LamportTime = c.tickSend()
	log.Printf("[Client %s] event=PUBLISH_SENT lamport=%d content=%q", c.username, request.LamportTime, request.Content)

	resp, err := c.client.Publish(ctx, request)
	if err != nil {
		log.Printf("[Client %s] event=PUBLISH_FAILED err=%v", c.username, err)
		return nil, err
	}
	c.tickRecv(resp.GetLamportTime())
	return resp, nil
}

func (c *chatClient) Leave(ctx context.Context, request *pb.LeaveRequest) (*pb.LeaveResponse, error) {
	request.LamportTime = c.tickSend()
	log.Printf("[Client %s] event=LEAVE_SENT lamport=%d", c.username, request.LamportTime)

	resp, err := c.client.Leave(ctx, request)
	if err != nil {
		log.Printf("[Client %s] event=LEAVE_FAILED err=%v", c.username, err)
		return nil, err
	}
	c.tickRecv(resp.GetLamportTime())
	return resp, nil
}

func (c *chatClient) receive(stream grpc.ServerStreamingClient[pb.Broadcast]) {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			log.Printf("[Client %s] event=STREAM_CLOSED", c.username)
			return
		}
		if err != nil {
			log.Printf("[Client %s] event=STREAM_ERROR err=%v", c.username, err)
			return
		}
		local := c.tickRecv(msg.GetLamportTime())
		fmt.Printf("[%s | L=%d] %s\n", c.username, msg.GetLamportTime(), msg.GetContent())
		log.Printf("[Client %s] event=MESSAGE_RECEIVED msg_lamport=%d local_lamport=%d content=%q",
			c.username, msg.GetLamportTime(), local, msg.GetContent())
	}
}

func (c *chatClient) tickSend() int64 {
	return atomic.AddInt64(&c.clock, 1)
}

func (c *chatClient) tickRecv(received int64) int64 {
	for {
		old := atomic.LoadInt64(&c.clock)
		next := max(old, received) + 1
		if atomic.CompareAndSwapInt64(&c.clock, old, next) {
			return next
		}
	}
}

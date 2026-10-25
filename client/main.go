package main

import (
	"bufio"
	pb "chitchat/grpc"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
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

	name := flag.String("name", "", "your username")
	flag.Parse()
	if *name == "" {
		os.Exit(1)
	}

	logFile, err := os.OpenFile("logs/client_"+*name+".log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("failed to open log file: %v", err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)

	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to server: %v", err)
	}
	defer conn.Close()

	// Creates two clients and joins them to the chat server:
	client := newClient(*name, conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)

	if _, err := client.Join(ctx, &pb.JoinRequest{Username: client.username}); err != nil {
		if status.Code(err) == codes.AlreadyExists {
			fmt.Printf("The username %q is already taken.\n", *name)
		} else {
			fmt.Println("Could not join:", err)
		}
		cancel()
		return
	}

	defer cancel()

	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				continue
			}
			if utf8.RuneCountInString(line) > 128 {
				fmt.Println("message too long (max 128 characters)")
				continue
			}
			pubCtx, pubCancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, err := client.Publish(pubCtx, &pb.PublishRequest{Content: line})
			pubCancel()
			if err != nil {
				fmt.Println("failed to send: ", err)
			}
		}
	}()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig

	// Fresh context: the 10-second ctx above has probably expired by now.
	leaveCtx, leaveCancel := context.WithTimeout(context.Background(), 5*time.Second)
	client.Leave(leaveCtx, &pb.LeaveRequest{Username: client.username})
	leaveCancel()

	time.Sleep(500 * time.Millisecond)
}

func (c *chatClient) Join(ctx context.Context, request *pb.JoinRequest) (grpc.ServerStreamingClient[pb.Broadcast], error) {
	// Joining does NOT increment clock.
	stream, err := c.client.Join(context.WithoutCancel(ctx), request)
	if err != nil {
		log.Printf("[Client %s] event=JOIN_FAILED err=%v", c.username, err)
		return nil, err
	}

	first, err := stream.Recv()
	if err != nil {
		log.Printf("[Client %s] event=JOIN_REJECTED err=%v", c.username, err)
		return nil, err
	}
	log.Printf("[Client %s] event=JOINED lamport=%d", c.username, atomic.LoadInt64(&c.clock))

	c.handle(first)
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
func (c *chatClient) handle(msg *pb.Broadcast) {
	local := c.tickRecv(msg.GetLamportTime())
	fmt.Printf("[%s | L=%d] %s\n", c.username, msg.GetLamportTime(), msg.GetContent())
	log.Printf("[Client %s] event=MESSAGE_RECEIVED msg_lamport=%d local_lamport=%d content=%q",
		c.username, msg.GetLamportTime(), local, msg.GetContent())
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

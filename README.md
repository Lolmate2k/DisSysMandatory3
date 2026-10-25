# Chit Chat

Distributed chat service in Go using gRPC.

## Structure

```
client/   client code
grpc/     .proto file (and generated Go code)
server/   server code
logs/     log output from runs
```

## Generate gRPC code

```bash
protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative grpc/chitchat.proto
```

## Run
To use the program, start by starting a server, and then start clients

To start the server:
```bash
go run ./server
```

To start a client:
```bash
go run ./client -name <CLIENT_NAME>
```
Make CLIENT_NAME the wanted name for the client.
Make sure to use ONE terminal for ONE client.
Duplicate names are not possible.

Connected clients send messages by typing them out afteer joining, and pressing enter.


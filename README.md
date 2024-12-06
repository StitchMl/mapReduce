# MapReduce in Go

A simple distributed application exercise for ordering datasets using MapReduce to be implemented in one month during the Distributed Systems and Cloud Computing course.

---

## Instructions

### 1) Create the Go module
```bash
$ go mod init mapReduce
$ go mod tidy
```

### 2) Compile gRPC definition files
```bash
$ protoc --go_out=. --go-grpc_out=. mapreduce/mapreduce.proto
```

### 3) Run the programme
```bash
$ go run main.go
```

---

Run the programme to start the MapReduce system. Master, Mapper and Reducer nodes will be simulated on the same machine, and the results will be saved to file.

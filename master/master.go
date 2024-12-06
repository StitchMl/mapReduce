package master

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
	pb "mapReduce/mapreduce/mapreduce"
	"mapReduce/utils"
	"math"
	"math/rand"
	"net"
	"sync"
	"time"
)

var (
	chunks     [][]int
	partitions [][]int32
	mutex      sync.Mutex // To synchronise access to shared slices
)

// splitIntoChunks divides a list into chunks of equal size.
func splitIntoChunks(list []int, mapperNum int) {
	if mapperNum <= 0 {
		panic("mapperNum must be greater than zero")
	}

	for i := 0; i < len(list); i += mapperNum {
		end := i + mapperNum
		if end > len(list) {
			end = len(list)
		}
		mutex.Lock()
		chunks = append(chunks, list[i:end])
		mutex.Unlock()
	}

	log.Println(utils.ColoredText(utils.GreenBold, "Data successfully divided into chunks."))
}

// removeRandomChunk removes a random chunk from the list and returns it.
func removeRandomChunk() ([]int, error) {
	mutex.Lock()
	defer mutex.Unlock()

	if len(chunks) == 0 {
		return nil, fmt.Errorf("no chunks available to send")
	}

	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src)
	randomIndex := r.Intn(len(chunks))

	selectedChunk := chunks[randomIndex]
	chunks = append(chunks[:randomIndex], chunks[randomIndex+1:]...)
	return selectedChunk, nil
}

// convertToInt32Slice converts a slice of int to a slice of int32.
func convertToInt32Slice(list []int) []int32 {
	result := make([]int32, len(list))
	for i, v := range list {
		result[i] = int32(v)
	}
	return result
}

// masterServer implements the gRPC server for the master.
type masterServer struct {
	pb.UnimplementedMapReduceServer
}

func (s *masterServer) GetChunk(ctx context.Context, name *pb.NodeName) (*pb.Chunk, error) {
	selectedChunk, err := removeRandomChunk()
	if err != nil {
		log.Println(utils.ColoredText(utils.RedBold, err.Error()))
		return nil, status.Error(codes.NotFound, err.Error())
	}

	int32Slice := convertToInt32Slice(selectedChunk)
	result := fmt.Sprintf("%v", int32Slice)
	log.Printf(utils.ColoredText(utils.YellowBright, "Sending the chunk to the mapper "+name.Name+": "+result))

	return &pb.Chunk{Data: int32Slice}, nil
}

func (s *masterServer) SendChunk(ctx context.Context, chunk *pb.Chunk) (*pb.Ack, error) {
	mutex.Lock()
	partitions = append(partitions, chunk.Data)
	mutex.Unlock()

	result := fmt.Sprintf("%v", chunk.Data)
	log.Printf(utils.ColoredText(utils.GreenBold, "Received chunk: "+result))
	return &pb.Ack{Message: "Chunk received"}, nil
}

func (s *masterServer) GetPartition(ctx context.Context, reducer *pb.ReducerRequest) (*pb.Partition, error) {
	// Wait until there are at least 3 partitions
	for {
		mutex.Lock()
		if len(partitions) >= 3 {
			mutex.Unlock()
			break
		}
		mutex.Unlock()

		log.Println(utils.ColoredText(utils.YellowBold, "Waiting for more chunks..."))
		time.Sleep(500 * time.Millisecond)
	}

	log.Printf("Sending data to the reducer %s with range [%d, %d)", reducer.Name, reducer.MinRange, reducer.MaxRange)

	result := filterPartitionByRange(partitions, reducer.MinRange, reducer.MaxRange)
	return &pb.Partition{SortedData: result}, nil
}

// filterPartitionByRange filters the data according to the range provided.
func filterPartitionByRange(partitions [][]int32, min, max int32) []int32 {
	var result []int32
	for _, sublist := range partitions {
		for _, value := range sublist {
			if value >= min && value < max {
				result = append(result, value)
			}
		}
	}
	return result
}

func Master(config utils.Config, argument []int) error {
	// Split the data into chunks
	go splitIntoChunks(argument, len(config.Mapper.Nodes))

	// Creating the listener for the server
	listener, err := net.Listen("tcp", config.Master.Nodes[0].IP+":"+config.Master.Nodes[0].Port)
	if err != nil {
		log.Fatalf("Error when starting the listener: %v", err)
	}

	// Creation of the gRPC server
	grpcServer := grpc.NewServer(grpc.MaxConcurrentStreams(math.MaxUint32))

	// Service Registration
	pb.RegisterMapReduceServer(grpcServer, &masterServer{})

	// Server start-up
	println(utils.ColoredText(utils.CyanBoldBright, "Master listening on "+config.Master.Nodes[0].IP+":"+config.Master.Nodes[0].Port+"...\n"))
	err = grpcServer.Serve(listener)
	if err != nil {
		log.Fatalf("Error during server start-up: %v", err)
		return err
	}

	select {}
}

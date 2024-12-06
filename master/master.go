package master

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"log"
	pb "mapReduce/mapreduce/mapreduce"
	"mapReduce/utils"
	"math/rand"
	"net"
	"time"
)

var chunks [][]int
var partions [][]int32

// splitIntoChunks divides a list into chunks of equal size.
// If the length of the list is not a multiple of mapperNum, the last chunk will have fewer elements.
func splitIntoChunks(list []int, mapperNum int) {
	if mapperNum <= 0 {
		panic("mapperNum must be greater than zero")
	}

	for i := 0; i < len(list); i += mapperNum {
		end := i + mapperNum
		if end > len(list) {
			end = len(list)
		}
		chunks = append(chunks, list[i:end])
	}
}

type masterServer struct {
	pb.UnimplementedMapReduceServer
}

func (s *masterServer) GetChunk(ctx context.Context, name *pb.Name) (*pb.Chunk, error) {
	if len(chunks) == 0 {
		log.Println(utils.ColoredText(utils.RedBold, "No chunks available to send"))
		return nil, nil
	}

	// Simulates partitioning and distribution to mappers
	log.Printf("Sending a chunk to the mapper: %s", name)

	// Seed the random number generator
	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src) // Create a random number generator with the source
	randomIndex := r.Intn(len(chunks))

	// Converts the selected chunk
	selectedChunk := chunks[randomIndex]

	// Remove the chunk from the list
	chunks = append(chunks[:randomIndex], chunks[randomIndex+1:]...)

	// Creating an empty slice of int32 with the same length
	var int32Slice []int32

	// Conversion from []int to []int32
	for _, v := range selectedChunk {
		int32Slice = append(int32Slice, int32(v)) // Converts each int to int32 and adds to the slice
	}

	log.Printf("Sending a random chunk to the mapper: %s, Chunk: %v\n", name, selectedChunk)
	return &pb.Chunk{Data: int32Slice}, nil
}

func (s *masterServer) SendChunk(ctx context.Context, chunk *pb.Chunk) (*pb.Ack, error) {
	// Simulates partitioning and distribution to mappers
	log.Printf("Received chunk: %v\n", chunk.Data)
	partions = append(partions, chunk.Data)
	return &pb.Ack{Message: "Chunk received\n"}, nil
}

func (s *masterServer) GetPartion(ctx context.Context, name *pb.Name, num *pb.MaxNum) (*pb.Ack, error) {
	for len(partions) < 3 {
		isPrinted := false // Control variable for one-time printing

		if !isPrinted {
			log.Println(utils.ColoredText(utils.YellowBold, "No chunks available to send. I'm wait more chunk...\n"))
			isPrinted = true // Set to true to avoid further printing
		}

		// Add a pause to avoid overly aggressive loops (optional)
		time.Sleep(500 * time.Millisecond)
	}
	// Simulates partitioning and distribution to mappers
	log.Printf("Sending to the reducer: %s", name)
	var result []int32

	for _, sublist := range partions {
		for _, value := range sublist {
			// Check if value is <= num
			if value <= num.Message {
				result = append(result, value)
			}
		}
	}
	return &pb.Ack{Message: "Partion received\n"}, nil
}

func Master(config utils.Config, port string, argument []int) error {
	splitIntoChunks(argument, len(config.Mapper.Nodes))
	log.Printf("Has been split into chunks...\n")

	// Creating the listener for the server
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Error when starting the listener: %v", err)
	}

	// Creation of the gRPC server
	grpcServer := grpc.NewServer()

	// Service Registration
	pb.RegisterMapReduceServer(grpcServer, &masterServer{})

	// Add support for reflection (useful for debugging and tools such as grpcurl)
	reflection.Register(grpcServer)

	// Server start-up
	log.Println("Master listening on port " + port + "...\n")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Error during server start-up: %v", err)
	}
	return nil
}

package reducer

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"log"
	pb "mapReduce/mapreduce/reducer"
	"mapReduce/utils"
	"math"
	"net"
	"os"
	"sync"
	"time"
)

// Data contains DataVar specific to each reducer
type Data struct {
	Name       string
	MinN       int
	MaxN       int
	partitions [][]int32
	mu         sync.Mutex
}

var (
	mu      sync.Mutex
	DataVar []*Data
)

// reducerServer implements the gRPC server for the reducer.
type reducerServer struct {
	pb.UnimplementedMapReduceServer
	data *Data
}

func newReducerServer(data *Data) *reducerServer {
	return &reducerServer{data: data}
}

// logError logs an error and returns it
func logError(message string, err error) {
	log.Printf(utils.ColoredText(utils.RedBold, message+": "+err.Error()))
}

// merge combines two ordered slices `a` and `b` into a single ordered slice.
func merge(a []int32, b []int32) []int32 {
	// Final slice that will contain the sorted values.
	var final []int32
	// Indexes to scroll the elements of `a` and `b`.
	i := 0
	j := 0

	// It compares the elements of `a` and `b` and adds them to the final result in order.
	for i < len(a) && j < len(b) {
		if a[i] < b[j] {
			final = append(final, a[i]) // Adds the element of `a` to the result.
			i++
		} else {
			final = append(final, b[j]) // Adds the element of `b` to the result.
			j++
		}
	}

	// Adds the remaining elements of `a` (if any) to the result.
	for ; i < len(a); i++ {
		final = append(final, a[i])
	}

	// Adds the remaining elements of `b` (if any) to the result.
	for ; j < len(b); j++ {
		final = append(final, b[j])
	}

	// The ordered result returns.
	return final
}

// mergeSort implements the Merge Sort algorithm to sort an array.
func mergeSort(items []int32) []int32 {
	// Base case: if the slice has less than 2 elements, it is already ordered.
	if len(items) < 2 {
		return items
	}

	// Divide il slice in due metà.
	first := mergeSort(items[:len(items)/2])  // Applies the Merge Sort recursively on the first half.
	second := mergeSort(items[len(items)/2:]) // Applies the Merge Sort recursively on the second half.

	// Combine the two ordered halves using `merge`.
	return merge(first, second)
}

// writeToUniqueFile writes the sorted DataVar to a new unique file based on the reducer's name
func writeToUniqueFile(reducerName string, data []int32) error {
	// Generate a unique file name using timestamp
	fileName := fmt.Sprintf("reducer_output_%s_%d.txt", reducerName, time.Now().UnixNano())

	// Create the file
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", fileName, err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			logError("Error: ", err)
		}
	}(file)

	// Write the sorted DataVar to the file
	for _, value := range data {
		_, err := file.WriteString(fmt.Sprintf("%d\n", value))
		if err != nil {
			return fmt.Errorf("failed to write to file %s: %w", fileName, err)
		}
	}

	return nil
}

// filterPartitionByRange filters the DataVar according to the range provided.
func filterPartitionByRange(partitions [][]int32, minN int, maxN int) []int32 {
	var result []int32
	for _, sublist := range partitions {
		for _, value := range sublist {
			if value >= int32(minN) && value < int32(maxN) {
				result = append(result, value)
			}
		}
	}
	return result
}

func processReducerNode(partition []int32, name string) error {
	log.Printf(utils.ColoredText(utils.PURPLE, name+": Processing start"))

	list := mergeSort(partition)
	result := fmt.Sprintf("%v", list)
	log.Printf(utils.ColoredText(utils.GreenBright, name+": Ordered DataVar: "+result))

	err := writeToUniqueFile(name, list)
	if err != nil {
		logError("Error writing to file for "+name, err)
		return err
	}

	log.Printf(utils.ColoredText(utils.BLUE, name+": Data successfully written to file"))
	return nil
}

func startReduce(state *Data) {
	log.Printf(utils.ColoredText(utils.WhiteBoldBright, state.Name+": starting while phase..."))
	for {
		log.Printf(utils.ColoredText(utils.BlueBold, state.Name+": trying to start merge..."))

		mu.Lock()
		if len(state.partitions) == 0 {
			mu.Unlock()
			log.Printf(utils.ColoredText(utils.YELLOW, state.Name+fmt.Sprintf(": Have %d partitions to process. Waiting...\n", len(state.partitions))))
			time.Sleep(2 * time.Second)
			continue
		}

		// Filter state for the current range
		result := filterPartitionByRange(state.partitions, state.MinN, state.MaxN)
		if len(result) == 0 {
			mu.Unlock()
			log.Printf(utils.ColoredText(utils.YELLOW, state.Name+": No state in range to process...\n"))
			time.Sleep(2 * time.Second)
			continue
		}

		state.partitions = nil
		mu.Unlock()

		// Process filtered state
		err := processReducerNode(result, state.Name)
		if err != nil {
			logError(state.Name+": Error during merging phase", err)
		}

		log.Printf(utils.ColoredText(utils.GreenBright, state.Name+": finishing merge phase...\n"))
	}
}

func (s *reducerServer) GetRange(_ context.Context, mapper *pb.NodeName) (*pb.ReducerRequest, error) {
	log.Printf("%s: GetRange of %s (%d, %d]", mapper.Name, s.data.Name, s.data.MinN, s.data.MaxN)

	node := pb.ReducerRequest{
		Name:     s.data.Name,
		MinRange: int32(s.data.MinN),
		MaxRange: int32(s.data.MaxN),
	}
	return &node, nil
}

func (s *reducerServer) SendPartition(_ context.Context, chunk *pb.Partition) (*pb.Ack, error) {
	mu.Lock()
	defer mu.Unlock()
	if len(chunk.SortedData) == 0 {
		log.Printf(utils.ColoredText(utils.YELLOW, "Received empty chunk, ignoring..."))
		return &pb.Ack{Message: "Chunk ignored (empty)"}, nil
	}
	s.data.partitions = append(s.data.partitions, chunk.SortedData)

	log.Printf(utils.ColoredText(utils.GreenBold, s.data.Name+" received chunk: "+fmt.Sprintf("%v", chunk.SortedData)))
	return &pb.Ack{Message: "Chunk received"}, nil
}

func Reducer(config utils.Config, num int, minNum int, maxNum int) error {
	reducer := config.Reducer.Nodes[num]
	DataVar = append(DataVar, &Data{Name: reducer.Name, MinN: minNum, MaxN: maxNum})
	n := len(DataVar) - 1
	println(fmt.Sprintf("%s: %d...", reducer.Name, len(DataVar)))
	log.Printf("Reducer %d initializing with DataVar: Name=%s, Min=%d, Max=%d", n+1, DataVar[n].Name, DataVar[n].MinN, DataVar[n].MaxN)

	go startReduce(DataVar[n]) // Ensure that this function does not overwrite or modify `DataVar`.

	listener, err := net.Listen("tcp", reducer.IP+":"+reducer.Port)
	if err != nil {
		log.Fatalf(DataVar[n].Name+": Error when starting the listener: %v", err)
	}

	grpcServer := grpc.NewServer(grpc.MaxConcurrentStreams(math.MaxUint32))
	pb.RegisterMapReduceServer(grpcServer, newReducerServer(DataVar[n]))

	log.Printf("Reducer %s listening on %s:%s...\n", DataVar[n].Name, reducer.IP, reducer.Port)
	return grpcServer.Serve(listener)
}

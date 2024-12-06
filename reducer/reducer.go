package reducer

import (
	"context"
	"fmt"
	"log"
	"mapReduce/utils"
	"math/rand"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "mapReduce/mapreduce/mapreduce"
)

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

func Reducer(reducer utils.Reducer, master utils.Node) error {
	// Connection to the gRPC server
	conn, err := grpc.Dial(master.IP+":"+master.Port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logError("Error while connecting to master", err)
		return err
	}
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			logError("Error while closing connection", err)
		}
	}(conn)

	client := pb.NewMapReduceClient(conn)

	// Context Definition
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Creates an empty maxNum of type int32
	var maxNum []int

	// Add n random numbers to the maxNum
	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src)
	for i := 0; i < len(reducer.Nodes)-1; i++ {
		maxNum = append(maxNum, r.Intn(1000)) // Add a random int32
	}
	maxNum = append(maxNum, 1000)
	var minN int

	for i, node := range reducer.Nodes {
		if i == 0 {
			minN = 0
		} else {
			minN = maxNum[i-1]
		}
		if err := processReducerNode(ctx, client, &pb.ReducerRequest{Name: reducer.Nodes[i].Name, MinRange: int32(minN), MaxRange: int32(maxNum[i])}); err != nil {
			logError("Error during reducer processing "+node.Name, err)
		}
	}

	return nil
}

func processReducerNode(ctx context.Context, client pb.MapReduceClient, request *pb.ReducerRequest) error {
	var t = "Reducer "
	log.Printf(utils.ColoredText(utils.PURPLE, t+request.Name+": Processing start"))

	// Partition request from master
	partition, err := client.GetPartition(ctx, request)
	if err != nil {
		logError("Error during partition recovery for "+request.Name, err)
		return err
	}

	// Sort received data
	var list = mergeSort(partition.SortedData)
	result := fmt.Sprintf("%v", list)
	log.Printf(utils.ColoredText(utils.GreenBright, t+request.Name+": Ordered data: "+result))

	// Write sorted data to a unique file
	err = writeToUniqueFile(request.Name, partition.SortedData)
	if err != nil {
		logError("Error writing to file for "+request.Name, err)
		return err
	}

	log.Printf(utils.ColoredText(utils.BLUE, t+request.Name+": Data successfully written to file"))

	return nil
}

// writeToUniqueFile writes the sorted data to a new unique file based on the reducer's name
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

	// Write the sorted data to the file
	for _, value := range data {
		_, err := file.WriteString(fmt.Sprintf("%d\n", value))
		if err != nil {
			return fmt.Errorf("failed to write to file %s: %w", fileName, err)
		}
	}

	return nil
}

// logError logs an error and returns it
func logError(message string, err error) {
	log.Printf(utils.ColoredText(utils.RedBold, message+": "+err.Error()))
}

package mapper

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pm "mapReduce/mapreduce/master"
	pr "mapReduce/mapreduce/reducer"
	"mapReduce/utils"
)

var (
	cons    []*grpc.ClientConn
	clients []pr.MapReduceClient
)

// logError logs an error and returns nil
func logError(message string, err error) error {
	log.Printf(utils.ColoredText(utils.RedBold, message+": "+err.Error()))
	return err
}

// filterPartitionByRange filters the data according to the range provided.
func filterPartitionByRange(partitions []int32, min, max int32) []int32 {
	var result []int32
	for _, value := range partitions {
		if value >= min && value < max {
			result = append(result, value)
		}
	}
	return result
}

// handleMapperNode handles operations for a single mapper node
func handleMapperNode(ctx context.Context, client pm.MapReduceClient, node utils.Node) {
	log.Printf(utils.ColoredText(utils.GREEN, "Processing node mapper: "+node.Name))

	// Getting the chunk
	chunk, err := client.GetChunk(ctx, &pm.Node{Name: node.Name})
	if err != nil {
		err := logError("Error during chunk retrieval for "+node.Name, err)
		if err != nil {
			return
		}
		return
	}
	result := fmt.Sprintf("%v", chunk.Data)
	log.Printf(utils.ColoredText(utils.GreenBold, "Chunk received for "+node.Name+": "+result))

	// Sorting the data
	sort.Slice(chunk.Data, func(i, j int) bool {
		return chunk.Data[i] < chunk.Data[j]
	})

	// Send ordered chunk
	for i := range clients {
		// Get the specific client
		r := clients[i]

		// Retrieving the current reducer range
		rRange, err := r.GetRange(ctx, &pr.NodeName{Name: node.Name})
		if err != nil {
			err := logError("Error while retrieving range for "+node.Name, err)
			if err != nil {
				return
			}
			continue // Continue with the next reducer
		}

		// Filter data for the current range
		tempC := filterPartitionByRange(chunk.Data, rRange.MinRange, rRange.MaxRange)

		// Send filtered partition
		response, err := r.SendPartition(ctx, &pr.Partition{SortedData: tempC})
		if err != nil {
			err := logError("Error while sending partition for "+node.Name+" to reducer "+strconv.Itoa(i), err)
			if err != nil {
				return
			}
			continue
		}

		// Completed operation log
		log.Printf(utils.ColoredText(utils.MAGENTA, node.Name+": "+response.GetMessage()+" to reducer "+strconv.Itoa(i+1)))
	}

	log.Printf(utils.ColoredText(utils.GreenBold, node.Name+": sent the whole chunk..."))
}

func closeConnection(conn *grpc.ClientConn) {
	err := conn.Close()
	if err != nil {
		err := logError("Error while closing connection", err)
		if err != nil {
			return
		}
	}
}

func startMapper(wg *sync.WaitGroup, ctx context.Context, client pm.MapReduceClient, node utils.Node) {
	wg.Add(1)
	go func(node utils.Node) {
		defer wg.Done()
		handleMapperNode(ctx, client, node)
	}(node)
}

// Mapper manages the interaction between the mapper nodes and the master
func Mapper(config utils.Config) error {
	master := config.Master.Nodes[0]
	mapper := config.Mapper
	reducer := config.Reducer
	cons = make([]*grpc.ClientConn, len(reducer.Nodes))
	clients = make([]pr.MapReduceClient, len(reducer.Nodes))

	// Connection to the gRPC server master
	var err error
	cons[0], err = grpc.NewClient(master.IP+":"+master.Port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return logError("Error while connecting to master", err)
	}
	defer closeConnection(cons[0])

	log.Println(utils.ColoredText(utils.PurpleBoldBright, "Mapper connected to the server!"))
	client := pm.NewMapReduceClient(cons[0])

	// Connection to the gRPC server reducer
	for i, red := range reducer.Nodes {
		cons[i], err = grpc.NewClient(red.IP+":"+red.Port, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return logError("Error while connecting to "+red.Name, err)
		}
		defer closeConnection(cons[i])

		log.Println(utils.ColoredText(utils.PurpleBoldBright, "Mapper connected to "+red.Name))
		clients[i] = pr.NewMapReduceClient(cons[i])
	}

	// Context Definition
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var wg sync.WaitGroup
	mapperCount := len(mapper.Nodes)

	log.Printf(utils.ColoredText(utils.BLUE, "Number of mappers: "+strconv.Itoa(mapperCount)))

	for _, node := range mapper.Nodes {
		startMapper(&wg, ctx, client, node)
	}

	wg.Wait()
	return nil
}

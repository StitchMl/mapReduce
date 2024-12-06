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
	pb "mapReduce/mapreduce/mapreduce"
	"mapReduce/utils"
)

// Mapper manages the interaction between the mapper nodes and the master
func Mapper(mapper utils.Mapper, master utils.Node) error {
	// Connection to the gRPC server
	conn, err := grpc.Dial(master.IP+":"+master.Port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return logError("Error while connecting to master", err)
	}
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {

		}
	}(conn)

	log.Println(utils.ColoredText(utils.PurpleBoldBright, "Mapper connected to the server!"))
	client := pb.NewMapReduceClient(conn)

	// Context Definition
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	mapperCount := len(mapper.Nodes)

	log.Printf(utils.ColoredText(utils.BLUE, "Number of mappers: "+strconv.Itoa(mapperCount)))

	for _, node := range mapper.Nodes {
		wg.Add(1)
		go func(node utils.Node) {
			defer wg.Done()
			handleMapperNode(ctx, client, node)
		}(node)
	}

	wg.Wait()
	return nil
}

// handleMapperNode handles operations for a single mapper node
func handleMapperNode(ctx context.Context, client pb.MapReduceClient, node utils.Node) {
	log.Printf(utils.ColoredText(utils.GREEN, "Processing node mapper: "+node.Name))

	// Getting the chunk
	chunk, err := client.GetChunk(ctx, &pb.NodeName{Name: node.Name})
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
	response, err := client.SendChunk(ctx, &pb.Chunk{Data: chunk.Data})
	if err != nil {
		err := logError("Error while sending chunk for "+node.Name, err)
		if err != nil {
			return
		}
		return
	}
	log.Printf(utils.ColoredText(utils.MAGENTA, node.Name+": "+response.GetMessage()))
}

// logError logs an error and returns nil
func logError(message string, err error) error {
	log.Printf(utils.ColoredText(utils.RedBold, message+": "+err.Error()))
	return err
}

package mapper

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	pb "mapReduce/mapreduce/mapreduce"
	"mapReduce/utils"
	"sort"
	"sync"
	"time"
)

type mapperServer struct {
	pb.UnimplementedMapReduceServer
}

func (s *mapperServer) SendPartition(ctx context.Context, partition *pb.Partition) (*pb.Ack, error) {
	log.Printf("Received sorted data: %v", partition.SortedData)
	return &pb.Ack{Message: "Partition received"}, nil
}

func Mapper(mapper utils.Node, master utils.Node) error {
	// Contact the gRPC server and print out its response
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	chunk, err := c.GetChunk(ctx, &pb.Name{Message: mapper.Name})
	if err != nil {
		log.Fatalf("could not get: %v", err)
	}
	log.Printf("Going to order the chunk...\n")
	sort.Slice(chunk.Data, func(i, j int) bool {
		return chunk.Data[i] < chunk.Data[j] // Compare the elements in ascending order
	})

	r, err := c.SendChunk(ctx, &pb.Chunk{Data: chunk.Data})
	if err != nil {
		log.Fatalf("could not send: %v", err)
	}
	log.Printf("Greeting: %s", r.GetMessage())

	return nil

	// Connection to the gRPC server
	conn, err := grpc.Dial(master.IP+":"+mapper.Port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Server connection error: %v", err)
	}
	defer conn.Close()

	// Client Creation
	client := pb.NewMapReduceClient(conn)

	var wg sync.WaitGroup
	mapperCount := 5 // numero di client concorrenti

	// Invia messaggi contemporaneamente da più client
	for i := 0; i < clientCount; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			// Creazione della richiesta
			req := &Request{Message: fmt.Sprintf("Messaggio dal client %d", clientID)}

			// Chiamata al metodo del server
			res, err := client.SendMessage(context.Background(), req)
			if err != nil {
				log.Printf("Errore nella richiesta dal client %d: %v", clientID, err)
				return
			}
			fmt.Printf("Risposta dal server al client %d: %s\n", clientID, res.GetReply())
		}(i)
	}

	// Aspetta che tutte le goroutine terminino
	wg.Wait()

	// Attendi un po' di tempo prima che il client termini (utile se hai bisogno di vedere l'output)
	time.Sleep(1 * time.Second)
}

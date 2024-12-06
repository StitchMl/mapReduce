package reducer

import (
	"context"
	"google.golang.org/grpc"
	"log"
	pb "mapReduce/mapreduce/mapreduce"
	"mapReduce/utils"
	"net"
)

type reducerServer struct {
	pb.UnimplementedMapReduceServer
}

func (s *reducerServer) SendPartition(ctx context.Context, partition *pb.Partition) (*pb.Ack, error) {
	log.Printf("Merging data: %v", partition.SortedData)
	return &pb.Ack{Message: "Merge complete"}, nil
}

func Reducer(reducer utils.Node, master utils.Node) error {
	listener, err := net.Listen("tcp", ":"+reducer.Port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
		return err
	}

	grpcServer := grpc.NewServer()
	pb.RegisterMapReduceServer(grpcServer, &reducerServer{})

	log.Println("Reducer running on port " + reducer.Port)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
		return err
	}
	return nil
}

package main

import (
	"context"
	"net"

	"google.golang.org/grpc"

	pb "twitter_clone/api"
	"twitter_clone/functionality"
)

type server struct {
	pb.UnimplementedApiServer
}

func (s *server) Signup(ctx context.Context, req *pb.Accounts) (*pb.Empty, error) {
	functionality.SignupBatch(req.Users, req.Emails)
	return &pb.Empty{}, nil
}

func (s *server) Follow(ctx context.Context, req *pb.Follows) (*pb.Empty, error) {
	functionality.FollowBatch(req.Users, req.Targets)
	return &pb.Empty{}, nil
}

func (s *server) GetUsers(ctx context.Context, req *pb.Empty) (*pb.Users, error) {
	return &pb.Users{Users: functionality.GetUsers()}, nil
}

func main() {
	lis, _ := net.Listen("tcp", ":50051")
	s := grpc.NewServer()
	pb.RegisterApiServer(s, &server{})
	s.Serve(lis)
}

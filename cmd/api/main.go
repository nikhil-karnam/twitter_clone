package main

import (
	"context"
	"net"
	"sync"

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

func (s *server) SetThreshold(ctx context.Context, req *pb.Threshold) (*pb.Empty, error) {
	functionality.CelebThreshold = int(req.Value)
	return &pb.Empty{}, nil
}

func (s *server) SequentialTweet(ctx context.Context, req *pb.Tweets) (*pb.Empty, error) {
	for i := range req.Users {
		functionality.Tweet(req.Users[i], req.Tweets[i])
	}
	return &pb.Empty{}, nil
}

func (s *server) BurstTweet(ctx context.Context, req *pb.Tweets) (*pb.Empty, error) {
	var wg sync.WaitGroup
	for i := range req.Users {
		wg.Go(func() { functionality.Tweet(req.Users[i], req.Tweets[i]) })
	}
	wg.Wait()
	return &pb.Empty{}, nil
}

//getters
func (s *server) GetUsers(ctx context.Context, req *pb.Empty) (*pb.Users, error) {
	return &pb.Users{Users: functionality.GetUsers()}, nil
}

func (s *server) GetCelebs(ctx context.Context, req *pb.Empty) (*pb.Users, error) {
	return &pb.Users{Users: functionality.GetCelebs()}, nil
}

func (s *server) GetRandos(ctx context.Context, req *pb.Empty) (*pb.Users, error) {
	return &pb.Users{Users: functionality.GetRandos()}, nil
}

func main() {
	lis, _ := net.Listen("tcp", ":50051")
	s := grpc.NewServer()
	pb.RegisterApiServer(s, &server{})
	s.Serve(lis)
}

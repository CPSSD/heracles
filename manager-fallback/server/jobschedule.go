package server

import (
	"context"

	pb "github.com/cpssd/heracles/proto/mapreduce"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// Schedule implementation
func (s *Server) Schedule(ctx context.Context, req *pb.ScheduleRequest) (*pb.ScheduleResponse, error) {
	res := &pb.ScheduleResponse{}

	jobID, err := s.sch.Schedule(req.GetJob())
	if err != nil {
		return res, grpc.Errorf(codes.Internal, err.Error())
	}

	res.JobId = jobID
	return res, nil
}

// Cancel implementation
func (s *Server) Cancel(ctx context.Context, req *pb.CancelRequest) (*pb.EmptyMessage, error) {
	if err := s.sch.Cancel(req.GetJobId()); err != nil {
		return &pb.EmptyMessage{}, grpc.Errorf(codes.Internal, err.Error())
	}
	return &pb.EmptyMessage{}, nil
}

// Describe implementation
func (s *Server) Describe(ctx context.Context, req *pb.DescribeRequest) (*pb.Description, error) {
	return nil, nil
}

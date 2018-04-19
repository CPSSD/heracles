package server

import (
	"fmt"
	"net"

	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/cpssd/heracles/manager-fallback/scheduler"
	"github.com/cpssd/heracles/manager-fallback/settings"

	pb "github.com/cpssd/heracles/proto/mapreduce"
)

// Server type
type Server struct {
	*grpc.Server
	sch *scheduler.Scheduler
}

// New Server
func New(sch *scheduler.Scheduler) *Server {
	s := &Server{
		Server: grpc.NewServer(),
		sch:    sch,
	}

	pb.RegisterJobScheduleServiceServer(s.Server, s)

	return s
}

// Run the server
func (s *Server) Run() error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", settings.Int("server.port")))
	if err != nil {
		return errors.Wrap(err, "can't start server")
	}

	return s.Serve(lis)
}

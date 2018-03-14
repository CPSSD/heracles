package app

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/olekukonko/tablewriter"
	"github.com/pkg/errors"
	"google.golang.org/grpc"

	dpb "github.com/cpssd/heracles/proto/datatypes"
	pb "github.com/cpssd/heracles/proto/mapreduce"
)

type conn struct {
	*grpc.ClientConn
	pb.JobScheduleServiceClient
}

func connect(addr string) (*conn, error) {
	c := &conn{}
	var err error
	c.ClientConn, err = grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	c.JobScheduleServiceClient = pb.NewJobScheduleServiceClient(c.ClientConn)
	return c, nil
}

func (c *conn) schedule(job *dpb.Job) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	res, err := c.Schedule(ctx, &pb.ScheduleRequest{Job: job})
	if err != nil {
		return errors.Wrap(err, "unable to schedule job")
	}

	fmt.Println("Successfully scheduled. You can see the job status by running:")
	fmt.Println("\thrctl describe job", res.GetJobId())
	return nil
}

func (c *conn) cancel(jobID string) error {
	ctx, cancelCtx := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancelCtx()

	if _, err := c.Cancel(ctx, &pb.CancelRequest{JobId: jobID}); err != nil {
		return errors.Wrap(err, "unable to cancel job")
	}

	fmt.Printf("Job %s successfully cancelled\n", jobID)

	return nil
}

func (c *conn) describe(req *pb.DescribeRequest) (*pb.Description, error) {
	ctx, cancelCtx := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelCtx()
	return c.Describe(ctx, req)
}

func (c *conn) describeCluster() error {
	req := &pb.DescribeRequest{
		Resource: pb.ResourceType_CLUSTER,
	}

	res, err := c.describe(req)
	if err != nil {
		return errors.Wrap(err, "unable to get cluster information")
	}
	return proto.MarshalText(os.Stdout, res.GetCluster())
}

func (c *conn) describeQueue() error {
	req := &pb.DescribeRequest{
		Resource: pb.ResourceType_QUEUE,
	}

	res, err := c.describe(req)
	if err != nil {
		return errors.Wrap(err, "unable to get queue")
	}

	table := tablewriter.NewWriter(os.Stdout)
	for _, job := range res.GetJobs() {
		age := time.Now().Sub(time.Unix(int64(job.GetTimeScheduled()), 0))
		table.Append([]string{job.GetId(), fmt.Sprint(age, "ago")})
	}
	table.Render()
	return nil
}

func (c *conn) describeJobs(jobID string) error {
	req := &pb.DescribeRequest{
		Resource: pb.ResourceType_JOB,
		JobId:    jobID,
	}

	res, err := c.describe(req)
	if err != nil {
		return errors.Wrap(err, "unable to get job information")
	}

	jobs := res.GetJobs()
	switch len(jobs) {
	case 0:
		fmt.Println("No jobs to display")
	case 1:
		if err := proto.MarshalText(os.Stdout, jobs[0]); err != nil {
			return errors.Wrap(err, "unable to display job information")
		}
	default:
		table := tablewriter.NewWriter(os.Stdout)
		for _, job := range jobs {
			age := time.Now().Sub(time.Unix(int64(job.GetTimeScheduled()), 0))
			table.Append([]string{
				job.GetId(),
				job.GetStatus().String(),
				fmt.Sprint(age, "ago"),
			})
		}
		table.Render()
	}

	return nil
}

func (c *conn) describeTasks(taskID, jobID string) error {
	req := &pb.DescribeRequest{
		Resource: pb.ResourceType_TASK,
		TaskId:   taskID,
		JobId:    jobID,
	}

	res, err := c.describe(req)
	if err != nil {
		return errors.Wrap(err, "unable to display task information")
	}

	tasks := res.GetTasks()
	switch len(tasks) {
	case 0:
		fmt.Println("No tasks to display")
	case 1:
		if err := proto.MarshalText(os.Stdout, tasks[0]); err != nil {
			return errors.Wrap(err, "unable to display job information")
		}
	default:
		table := tablewriter.NewWriter(os.Stdout)
		for _, task := range tasks {
			age := time.Now().Sub(time.Unix(int64(task.GetTimeCreated()), 0))
			table.Append([]string{
				task.GetId(),
				task.GetStatus().String(),
				fmt.Sprint(age, "ago"),
			})
		}
		table.Render()
	}

	return nil
}

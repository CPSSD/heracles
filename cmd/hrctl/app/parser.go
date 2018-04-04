package app

import (
	"github.com/pkg/errors"
	"github.com/urfave/cli"
)

func parse() *cli.App {
	app := cli.NewApp()
	app.Name = "hrctl"
	app.Authors = []cli.Author{
		{Name: "Heracles Authors", Email: "heracles@cpssd.net"},
	}
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "manager, m",
			Value:  "[::]:8081",
			Usage:  "Address to the manager",
			EnvVar: "HERACLES_MANAGER",
		},
	}
	app.Commands = []cli.Command{
		{
			Name:   "schedule",
			Usage:  "schedule a job from given input file",
			Action: schedule,
			Flags: []cli.Flag{
				cli.StringFlag{
					Name:  "job_file, f",
					Usage: "load the job configuration from `FILE`",
					Value: "",
				},
			},
			ArgsUsage: " ",
		}, {
			Name:      "cancel",
			Usage:     "cancel a job",
			Action:    cancel,
			ArgsUsage: "<job-id>",
		}, {
			Name:  "describe",
			Usage: "describe a resource in heracles",
			Subcommands: []cli.Command{
				{
					Name:      "cluster",
					Usage:     "Information about the heracles cluster",
					ArgsUsage: " ",
					Action:    describeCluster,
				}, {
					Name:      "queue",
					Usage:     "Current jobs in the queue",
					ArgsUsage: " ",
					Action:    describeQueue,
				}, {
					Name:      "job",
					Usage:     "Specific Job information",
					ArgsUsage: "<JOB-ID>",
					Action: func(c *cli.Context) error {
						if c.Args().First() == "" {
							return errors.New("Job ID must be specified")
						}
						return describeJobs(c)
					},
				}, {
					Name:      "jobs",
					Usage:     "List of jobs which are currently in progress",
					ArgsUsage: " ",
					Action:    describeJobs,
				}, {
					Name:      "task",
					Usage:     "Specific job nformation",
					ArgsUsage: "<TASK-ID>",
					Action: func(c *cli.Context) error {
						if c.Args().First() == "" {
							return errors.New("Task ID must be specified")
						}
						return describeTasks(c)
					},
				}, {
					Name:      "tasks",
					Usage:     "Lists of all tasks",
					ArgsUsage: " ",
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "job",
							Usage: "limit tasks to specific job",
							Value: "",
						},
					},
					Action: describeTasks,
				},
			},
		},
	}
	return app
}

func schedule(c *cli.Context) error {
	job, err := loadJob(c.String("job_file"))
	if err != nil {
		return errors.Wrap(err, "unable to get job from file")
	}

	conn, err := connect(c.String("manager"))
	if err != nil {
		return errors.Wrap(err, "unable to connect to manager")
	}

	return conn.schedule(job)
}

func cancel(c *cli.Context) error {
	conn, err := connect(c.String("manager"))
	if err != nil {
		return errors.Wrap(err, "unable to connect to manager")
	}

	jobID := c.Args().First()
	if jobID == "" {
		return errors.New("job ID cannot be empty")
	}

	return conn.cancel(jobID)
}

func describeCluster(c *cli.Context) error {
	conn, err := connect(c.String("manager"))
	if err != nil {
		return errors.Wrap(err, "unable to connect to manager")
	}

	return conn.describeCluster()
}

func describeQueue(c *cli.Context) error {
	conn, err := connect(c.String("manager"))
	if err != nil {
		return errors.Wrap(err, "unable to connect to manager")
	}

	return conn.describeQueue()
}

func describeJobs(c *cli.Context) error {
	conn, err := connect(c.String("manager"))
	if err != nil {
		return errors.Wrap(err, "unable to connect to manager")
	}

	return conn.describeJobs(c.Args().First())
}

func describeTasks(c *cli.Context) error {
	conn, err := connect(c.String("manager"))
	if err != nil {
		return errors.Wrap(err, "unable to connect to manager")
	}

	return conn.describeTasks(c.Args().First(), c.String("job"))
}

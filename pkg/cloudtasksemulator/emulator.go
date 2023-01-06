package cloudtasksemulator

import (
	"context"
	"net"
	"regexp"

	tasks "google.golang.org/genproto/googleapis/cloud/tasks/v2"
	"google.golang.org/grpc"
)

type EmulatorOptions struct {
	Addr                  string
	OpenIDIssuer          string
	HardResetOnPurgeQueue bool
	InitialQueues         []string
}

func StartEmulator(ctx context.Context, opts EmulatorOptions) (string, error) {
	if opts.OpenIDIssuer != "" {
		srv, err := ConfigureOpenIdIssuer(opts.OpenIDIssuer)
		if err != nil {
			return "", err
		}
		defer srv.Shutdown(context.Background())
	}

	if opts.Addr == "" {
		opts.Addr = "localhost:0"
	}

	lis, err := net.Listen("tcp", opts.Addr)
	if err != nil {
		return "", err
	}

	grpcServer := grpc.NewServer()
	emulatorServer := NewServer()
	emulatorServer.Options.HardResetOnPurgeQueue = opts.HardResetOnPurgeQueue
	tasks.RegisterCloudTasksServer(grpcServer, emulatorServer)

	for i := 0; i < len(opts.InitialQueues); i++ {
		if err := createInitialQueue(emulatorServer, opts.InitialQueues[i]); err != nil {
			return "", err
		}
	}

	go func() {
		<-ctx.Done()
		grpcServer.Stop()
	}()

	go func() {
		grpcServer.Serve(lis)
	}()

	return lis.Addr().String(), nil
}

// Creates an initial queue on the emulator
func createInitialQueue(emulatorServer *Server, name string) error {
	r := regexp.MustCompile("/queues/[A-Za-z0-9-]+$")
	parentName := r.ReplaceAllString(name, "")

	queue := &tasks.Queue{Name: name}
	req := &tasks.CreateQueueRequest{
		Parent: parentName,
		Queue:  queue,
	}

	_, err := emulatorServer.CreateQueue(context.TODO(), req)
	if err != nil {
		return err
	}

	return nil
}

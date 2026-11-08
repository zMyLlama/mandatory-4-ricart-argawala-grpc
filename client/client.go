package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	pb "github.com/zMyLlama/mandatory-4-ricart-argawala-grpc/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type server struct {
	pb.UnimplementedAgreementServiceServer
	client *Client
}

type State int

const (
	WANTED State = iota
	HELD
	RELEASED
)

var stateNames = map[State]string{
	WANTED:   "WANTED",
	HELD:     "HELD",
	RELEASED: "RELEASED",
}

type Client struct {
	id       int64
	mu       sync.Mutex
	lamport  int64
	enteredL int64
	state    State
	deferred []chan struct{}
}

func NewClient(id int64) *Client {
	return &Client{
		id:      id,
		lamport: 0,
		state:   RELEASED,
	}
}

func (s *server) Request(ctx context.Context, req *pb.AgreementReq) (*pb.AgreementRes, error) {
	c := s.client

	c.mu.Lock()
	log.Printf("Client %d in state %v received a request from client %d with Lamport timestamp %d", c.id, stateNames[c.state], req.ProcessId, req.Lamport)
	c.lamport = max(c.lamport, req.Lamport) + 1

	// We have priority if our lamport is less than the other lamport, or if they are equal and our id is less than the other id
	hasPriority := c.Compare(req.Lamport, req.ProcessId)
	shouldDefer := c.state == HELD || (c.state == WANTED && hasPriority)

	var deferChan chan struct{}
	if shouldDefer {
		deferChan = make(chan struct{})
		c.deferred = append(c.deferred, deferChan)
		log.Printf("Client %d is deferring the request from client %d", c.id, req.ProcessId)
	} else {
		log.Printf("Client %d is granting the request from client %d", c.id, req.ProcessId)
	}

	// We unblock after we have successfully modified all we need in the client state
	c.mu.Unlock()

	if deferChan != nil {
		select {
		// Since we never put anything into the deferChan, this will block until the channel is closed by the client when it releases the critical section.
		case <-deferChan:
		case <-ctx.Done():
			log.Printf("Client %d's deferred request from client %d was unexpectedly canceled.", c.id, req.ProcessId)
			return nil, ctx.Err()
		}
	}

	return &pb.AgreementRes{Message: "OK"}, nil
}

func (c *Client) Request(ctx context.Context, conn pb.AgreementServiceClient, lamport int64) {
	_, err := conn.Request(ctx, &pb.AgreementReq{
		ProcessId: c.id,
		Lamport:   lamport,
	})
	if err != nil {
		log.Fatalf("Client %d failed to request access: %v", c.id, err)
	}
}

func (c *Client) Enter(conns []pb.AgreementServiceClient) {
	log.Printf("Client %d wants to enter the critical section...", c.id)
	// We lock to ensure all requests are sent with the same lamport timestamp
	c.mu.Lock()

	c.lamport++
	c.enteredL = c.lamport
	c.state = WANTED

	log.Printf("Client %d is requesting access to the critical section with Lamport timestamp %d", c.id, c.lamport)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	for _, conn := range conns {
		wg.Add(1)
		go func(conn pb.AgreementServiceClient, savedLamport int64) {
			defer wg.Done()
			c.Request(ctx, conn, savedLamport)
		}(conn, c.enteredL)
	}

	// Unlock important here before waiting for responses, since they could be deferred and we don't wanna block the responses
	c.mu.Unlock()

	wg.Wait()

	c.mu.Lock()
	c.state = HELD
	log.Printf("Client %d has been granted permission by peers to enter the critical section", c.id)
	c.mu.Unlock()

	// Simulate some work in the critical section
	workTime := time.Duration(rand.Intn(3)+1) * time.Second
	log.Printf("Client %d is doing some important work in the critical section for %v...", c.id, workTime)
	time.Sleep(workTime)
}

// Caller of this method must hold the lock on the client, since it reads the state and lamport timestamp
func (c *Client) Compare(otherLamport int64, otherID int64) bool {
	if c.enteredL < otherLamport {
		return true
	}
	if c.enteredL == otherLamport {
		return c.id < otherID
	}
	return false
}

func (c *Client) Serve() {
	port := 3000 + c.id%1000
	log.Printf("Client %d is attempting to serve on port %d", c.id, port)

	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		log.Fatalf("[CLIENT %d] Failed to listen: %v", c.id, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterAgreementServiceServer(grpcServer, &server{client: c})
	go grpcServer.Serve(lis)

	log.Printf("Client %d is now listening on port %d", c.id, port)
}

func (c *Client) Release() {
	c.mu.Lock()
	defer c.mu.Unlock()

	log.Printf("Client %d is releasing the critical section", c.id)
	for _, ch := range c.deferred {
		close(ch)
	}
	c.state = RELEASED
	c.deferred = make([]chan struct{}, 0)
}

type ConnectResult struct {
	client pb.AgreementServiceClient
	id     int64
}

func (c *Client) Connect(resChan chan ConnectResult, otherID int64) {
	log.Printf("Client %d is attempting to connect to client %d...", c.id, otherID)

	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", 3000+otherID%1000), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Client %d failed to connect to client %d: %v", c.id, otherID, err)
	}

	cl := pb.NewAgreementServiceClient(conn)
	resChan <- ConnectResult{client: cl, id: otherID}
}

func (c *Client) run(otherIDs ...int64) {
	log.Printf("Client %d is starting...", c.id)

	clientConnChannel := make(chan ConnectResult, len(otherIDs))

	go c.Serve()

	// Sleep for a bit to ensure that the clients are listening before attempting to connect to them.
	time.Sleep(3 * time.Second)

	// Now we establish a connection to each of the other "clients"
	for _, id := range otherIDs {
		go c.Connect(clientConnChannel, id)
	}

	// Since we connect concurrently we have them respond back in a channel and we wait for all of them to respond before proceeding.
	conns := make([]pb.AgreementServiceClient, 0, len(otherIDs))
	for range otherIDs {
		conRes := <-clientConnChannel
		conns = append(conns, conRes.client)
		log.Printf("Client %d created a client to %d", c.id, conRes.id)
	}

	// Again we ensure that all the other clients have established connections before proceeding
	time.Sleep(2 * time.Second)

	log.Printf("Client %d is now entering the main loop", c.id)

	// This is our main loop where we will be requesting access to the critical section and then releasing it after a random amount of time.
	// Since our clients and listening are running in other threads we can just loop here and block the thread while waiting for the other clients to respond to our requests.
	for {
		randomSleep := time.Duration(rand.Intn(5)+1) * time.Second
		log.Printf("Client %d is sleeping for %v before requesting access to the critical section", c.id, randomSleep)
		time.Sleep(randomSleep)

		c.Enter(conns)
		c.Release()
	}
}

func main() {
	client_1 := NewClient(1)
	client_2 := NewClient(2)
	client_3 := NewClient(3)

	go client_1.run(client_2.id, client_3.id)
	go client_2.run(client_1.id, client_3.id)
	go client_3.run(client_1.id, client_2.id)

	select {}
}

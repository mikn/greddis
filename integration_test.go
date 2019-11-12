// +build integration

package greddis_test

import (
	"context"
	"log"
	"net"
	"os"
	"testing"

	"github.com/mikn/greddis"
	"github.com/ory/dockertest"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	// pulls an image, creates a container based on it and runs it
	resource, err := pool.Run("redis", "5", []string{})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}
	if err := pool.Retry(func() error {
		conn, err := net.Dial("tcp", "localhost:6379")
		if err != nil {
			return err
		}
		conn.Close()
		return nil
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	code := m.Run()
	if err := pool.Purge(resource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}
	os.Exit(code)
}

func TestGet(t *testing.T) {
	ctx := context.Background()
	client, err := greddis.NewClient(ctx, &greddis.PoolOptions{URL: "tcp://localhost:6379"})
	if err != nil {
		log.Fatalf("Connection error: %s", err)
	}
	key := "testkey"
	val := "testvalue"
	err = client.Set(ctx, key, val, 0)
	if err != nil {
		log.Fatalf("Could not set key! Error: %s", err)
	}
	res, err := client.Get(ctx, key)
	if err != nil {
		log.Fatalf("Could not get key! Error: %s", err)
	}
	var resp string
	err = res.Scan(&resp)
	if err != nil {
		log.Fatalf("Error whilst reading result! Error: %s", err)
	}
	require.Equal(t, val, resp)
}

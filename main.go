package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"cloud.google.com/go/alloydbconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func connectPgx(
	ctx context.Context,
	instURI, user, dbname string,
	opts ...alloydbconn.Option,
) (*pgxpool.Pool, func() error, error) {
	d, err := alloydbconn.NewDialer(ctx, opts...)
	if err != nil {
		noop := func() error { return nil }
		return nil, noop, fmt.Errorf("failed to init Dialer: %v", err)
	}

	cleanup := func() error { return d.Close() }

	dsn := fmt.Sprintf(
		"user=%s dbname=%s sslmode=disable",
		user, dbname,
	)

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, cleanup, fmt.Errorf("failed to parse pgx config: %v", err)
	}

	config.ConnConfig.DialFunc = func(ctx context.Context, _ string, _ string) (net.Conn, error) {
		return d.Dial(ctx, instURI)
	}

	pool, connErr := pgxpool.NewWithConfig(ctx, config)
	if connErr != nil {
		return nil, cleanup, fmt.Errorf("failed to connect: %s", connErr)
	}

	return pool, cleanup, nil
}

func main() {
	// Get configuration from environment variables
	projectID := os.Getenv("PROJECT_ID")
	region := os.Getenv("DB_REGION")
	clusterName := os.Getenv("DB_CLUSTER_NAME")
	instanceName := os.Getenv("DB_INSTANCE_NAME")
	dbUser := os.Getenv("DB_USER")
	dbName := os.Getenv("DB_NAME")

	// Validate required environment variables
	requiredEnvVars := map[string]string{
		"PROJECT_ID":       projectID,
		"DB_REGION":        region,
		"DB_CLUSTER_NAME":  clusterName,
		"DB_INSTANCE_NAME": instanceName,
		"DB_USER":          dbUser,
		"DB_NAME":          dbName,
	}

	for envVar, value := range requiredEnvVars {
		if value == "" {
			log.Fatalf("Required environment variable %s is not set", envVar)
		}
	}

	// Construct the instance URI
	instanceURI := fmt.Sprintf("projects/%s/locations/%s/clusters/%s/instances/%s",
		projectID, region, clusterName, instanceName)

	ctx := context.Background()

	// Connect to the database
	pool, cleanup, err := connectPgx(ctx, instanceURI, dbUser, dbName, alloydbconn.WithIAMAuthN())
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer cleanup()
	defer pool.Close()

	// Test the connection
	var now time.Time
	err = pool.QueryRow(ctx, "SELECT NOW()").Scan(&now)
	if err != nil {
		log.Fatalf("Failed to execute query: %v", err)
	}

	fmt.Printf("Current timestamp from database: %s\n", now)
}

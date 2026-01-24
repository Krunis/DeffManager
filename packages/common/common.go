package common

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

func GetDBConnectionString() string {
	var missingEnvVars []string

	checkEnvVar := func(envVar, envVarName string) {
		if envVar == "" {
			missingEnvVars = append(missingEnvVars, envVarName)
		}
	}

	dbName := os.Getenv("POSTGRES_DB")
	checkEnvVar(dbName, "POSTGRES_DB")

	dbUser := os.Getenv("POSTGRES_USER")
	checkEnvVar(dbUser, "POSTGRES_USER")

	dbHost := os.Getenv("POSTGRES_HOST")
	if dbHost == "" {
		dbHost = "localhost"
	}

	dbPort := os.Getenv("POSTGRES_PORT")
	checkEnvVar(dbPort, "POSTGRES_PORT")

	dbPassword := os.Getenv("POSTGRES_PASSWORD")
	checkEnvVar(dbPassword, "POSTGRES_PASSWORD")

	if len(missingEnvVars) > 0 {
		log.Fatalf("Required environment variables are not set: %s",
			strings.Join(missingEnvVars, ","))
	}

	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", dbUser, dbPassword, dbHost, dbPort, dbName)
}

func ConnectToDB(ctx context.Context, dbConnectionString string) (*pgxpool.Pool, error) {
	var pool *pgxpool.Pool
	var err error = fmt.Errorf("timeout")

	timer := time.NewTimer(time.Second * 25)
	defer timer.Stop()

	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	for{
		select{
		case <-ticker.C:
			pool, err = pgxpool.Connect(ctx, dbConnectionString)
			if err == nil{
				log.Println("Connected to DB")
				return pool, nil
			}

			log.Println("Failed to connect DB. Retry in 5 seconds...")

		case <-timer.C:
			return nil, fmt.Errorf("db connection timeout (25s): %w", err)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

}

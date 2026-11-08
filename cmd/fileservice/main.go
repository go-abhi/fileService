// Command fileservice runs the file service (REST + gRPC).
//
// Configuration comes from FILESERVICE_* environment variables; see the
// README. For a zero-setup local instance with an embedded in-memory S3:
//
//	fileservice -dev
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-abhi/fileService/internal/app"
)

var version = "dev"

func main() {
	dev := flag.Bool("dev", false, "run with an embedded in-memory S3 and no authentication (local development)")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(version)
		return
	}
	if *dev {
		os.Setenv("FILESERVICE_DEV", "true")
	}

	cfg, err := app.ConfigFromEnv(os.Getenv)
	if err != nil {
		fmt.Fprintln(os.Stderr, "configuration error:\n"+err.Error())
		os.Exit(2)
	}
	log := app.NewLogger(cfg)
	log.Info("starting fileservice", "version", version)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	a, err := app.New(ctx, cfg, log)
	if err != nil {
		log.Error("startup failed", "error", err)
		os.Exit(1)
	}
	if err := a.Run(ctx); err != nil {
		os.Exit(1)
	}
}

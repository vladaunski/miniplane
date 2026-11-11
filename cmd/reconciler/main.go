// Package main provides the background worker that processes operations.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vladaunski/miniplane/internal/nodeclient"
	"github.com/vladaunski/miniplane/internal/store"
)

type worker struct {
	store *store.Store
	node  *nodeclient.Client
}

// run polls forever: drain all pending operations, then sleep a second, repeat.
func (w *worker) run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		for w.processOne(ctx) {
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// processOne handles one operation and reports whether there might be more.
func (w *worker) processOne(ctx context.Context) bool {
	work, err := w.store.ClaimNext(ctx)
	if errors.Is(err, store.ErrNoWork) {
		return false
	}
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("claim: %v", err)
		}
		return false
	}

	var opErr error
	switch work.Kind {
	case "create":
		opErr = w.node.Create(ctx, work.VMID, work.TenantID)
	case "delete":
		opErr = w.node.Delete(ctx, work.VMID, work.TenantID)
	default:
		opErr = fmt.Errorf("unknown operation kind %q", work.Kind)
	}

	if opErr != nil {
		log.Printf("operation %s (%s) failed: %v", work.OpID, work.Kind, opErr)
		if err := w.store.Fail(ctx, work.OpID, 1); err != nil {
			log.Printf("mark failed: %v", err)
		}
		return true
	}
	if err := w.store.Complete(ctx, work); err != nil {
		log.Printf("complete %s: %v", work.OpID, err)
		return false
	}
	log.Printf("operation %s (%s) done", work.OpID, work.Kind)
	return true
}

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL is not set")
	}
	nodeURL := os.Getenv("NODE_URL")
	if nodeURL == "" {
		nodeURL = "http://127.0.0.1:9001"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	w := &worker{store: store.New(pool), node: nodeclient.New(nodeURL)}
	log.Println("reconciler started")
	w.run(ctx)
	log.Println("reconciler stopped")
}

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

	"github.com/vladaunski/miniplane/internal/domain"
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

// heartbeat extends the lease while the job runs. If the lease is lost, it cancels the job.
func (w *worker) heartbeat(ctx context.Context, cancelJob context.CancelFunc, work store.Work) {
	t := time.NewTicker(w.store.Lease / 3)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			err := w.store.Heartbeat(ctx, work)
			if errors.Is(err, store.ErrLeaseLost) {
				log.Printf("operation %s: lease lost, cancelling", work.OpID)
				cancelJob()
				return
			}
			if err != nil && ctx.Err() == nil {
				log.Printf("heartbeat %s: %v", work.OpID, err)
			}
		}
	}
}

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

	// jobCtx covers the node call. The heartbeat cancels it if the lease is lost.
	jobCtx, cancelJob := context.WithCancel(ctx)
	defer cancelJob()
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		w.heartbeat(jobCtx, cancelJob, work)
	}()

	var opErr error
	switch work.Kind {
	case domain.OpCreate:
		opErr = w.node.Create(jobCtx, work.VMID, work.TenantID, work.Generation)
	case domain.OpDelete:
		opErr = w.node.Delete(jobCtx, work.VMID, work.TenantID, work.Generation)
	default:
		opErr = fmt.Errorf("unknown operation kind %q: %w", work.Kind, nodeclient.ErrPermanent)
	}
	cancelJob()
	<-hbDone

	// Record the outcome even while shutting down: ctx is cancelled on SIGTERM,
	// and a cancelled context would make these writes fail.
	finishCtx, cancelFinish := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancelFinish()

	if opErr != nil {
		log.Printf("operation %s (%s) failed: %v", work.OpID, work.Kind, opErr)
		limit := w.store.MaxAttempts
		if errors.Is(opErr, nodeclient.ErrPermanent) {
			limit = work.Attempt // retrying cannot help: Fail gives up immediately
		}
		if err := w.store.Fail(finishCtx, work, limit); err != nil && !errors.Is(err, store.ErrLeaseLost) {
			log.Printf("mark failed: %v", err)
		}
		return true
	}
	if err := w.store.Complete(finishCtx, work); err != nil {
		if errors.Is(err, store.ErrLeaseLost) {
			log.Printf("operation %s: lease lost, result discarded", work.OpID)
			return true
		}
		log.Printf("complete %s: %v", work.OpID, err)
		return false
	}
	log.Printf("operation %s (%s) done", work.OpID, work.Kind)
	return true
}

// reconcileLoop periodically compares the database with the node and repairs drift.
func (w *worker) reconcileLoop(ctx context.Context) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		w.reconcileOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (w *worker) reconcileOnce(ctx context.Context) {
	// Ask the node first. If it is unreachable we do nothing: otherwise every VM
	// would look "missing" and we would enqueue a create for all of them.
	onNode, err := w.node.List(ctx)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("reconcile: node unreachable, skipping: %v", err)
		}
		return
	}
	vms, err := w.store.ListVMs(ctx)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("reconcile: list vms: %v", err)
		}
		return
	}

	known := make(map[string]bool, len(vms))
	for _, vm := range vms {
		id := string(vm.ID)
		known[id] = true
		var kind domain.OpKind
		switch domain.Drift(vm, onNode[id]) {
		case domain.ActionCreate:
			kind = domain.OpCreate
		case domain.ActionDelete:
			kind = domain.OpDelete
		default:
			continue
		}
		queued, err := w.store.Repair(ctx, id, kind)
		if err != nil {
			log.Printf("reconcile: repair %s (%s): %v", id, kind, err)
			continue
		}
		if queued {
			log.Printf("reconcile: drift on vm %s, enqueued %s", id, kind)
		}
	}

	// Orphans: on the node but unknown to the database. Report only, because
	// deleting on the strength of a wrong or restored database would be destructive.
	for id := range onNode {
		if !known[id] {
			log.Printf("reconcile: orphan vm %s on node (not in database)", id)
		}
	}
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
	go w.reconcileLoop(ctx)
	log.Println("reconciler started")
	w.run(ctx)
	log.Println("reconciler stopped")
}

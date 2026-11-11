// Package main provides the HTTP API for the control plane.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/vladaunski/miniplane/internal/domain"
	"github.com/vladaunski/miniplane/internal/store"
)

var nameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)

type api struct {
	store *store.Store
}

type operationResponse struct {
	ID         string `json:"id"`
	VMID       string `json:"vm_id"`
	Kind       string `json:"kind"`
	Status     string `json:"status"`
	Generation int64  `json:"vm_generation"`
}

func toResponse(op store.Operation) operationResponse {
	return operationResponse{ID: op.ID, VMID: op.VMID, Kind: op.Kind, Status: op.Status, Generation: op.VMGeneration}
}

// identity reads the tenant and idempotency key from headers.
// X-Tenant-ID is a stand-in: in a real system the tenant comes from an authenticated
// token issued by IAM, never from a header the caller can set to anything.
func identity(w http.ResponseWriter, r *http.Request, needKey bool) (domain.TenantID, string, bool) {
	tenant := r.Header.Get("X-Tenant-ID")
	if tenant == "" {
		http.Error(w, "missing X-Tenant-ID", http.StatusUnauthorized)
		return "", "", false
	}
	key := r.Header.Get("Idempotency-Key")
	if needKey && (key == "" || len(key) > 128) {
		http.Error(w, "Idempotency-Key header required (max 128 chars)", http.StatusBadRequest)
		return "", "", false
	}
	return domain.TenantID(tenant), key, true
}

func (a *api) createVM(w http.ResponseWriter, r *http.Request) {
	tenant, key, ok := identity(w, r, true)
	if !ok {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<10)
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || !nameRe.MatchString(req.Name) {
		http.Error(w, "body must be {\"name\": \"<lowercase letters, digits, dashes>\"}", http.StatusBadRequest)
		return
	}
	op, err := a.store.CreateVM(r.Context(), tenant, req.Name, key)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, toResponse(op))
}

func (a *api) deleteVM(w http.ResponseWriter, r *http.Request) {
	tenant, key, ok := identity(w, r, true)
	if !ok {
		return
	}
	op, err := a.store.DeleteVM(r.Context(), tenant, r.PathValue("id"), key)
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, toResponse(op))
}

func (a *api) getOperation(w http.ResponseWriter, r *http.Request) {
	tenant, _, ok := identity(w, r, false)
	if !ok {
		return
	}
	op, err := a.store.GetOperation(r.Context(), tenant, r.PathValue("id"))
	if err != nil {
		writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toResponse(op))
}

func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNameTaken), errors.Is(err, store.ErrOperationInFlight):
		http.Error(w, err.Error(), http.StatusConflict)
	case errors.Is(err, store.ErrKeyReuse):
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
	case errors.Is(err, store.ErrNotFound):
		http.Error(w, "not found", http.StatusNotFound)
	default:
		log.Printf("internal error: %v", err) // details stay in the log, not in the response
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("encode response: %v", err)
	}
}

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL is not set")
	}
	addr := os.Getenv("API_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8080"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer pool.Close()
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		log.Fatalf("ping: %v", err)
	}

	a := &api{store: store.New(pool)}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /vms", a.createVM)
	mux.HandleFunc("DELETE /vms/{id}", a.deleteVM)
	mux.HandleFunc("GET /operations/{id}", a.getOperation)

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	shutdownDone := make(chan struct{})
	go func() {
		<-ctx.Done() // Ctrl-C or SIGTERM
		shutCtx, cancelShut := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancelShut()
		if err := srv.Shutdown(shutCtx); err != nil {
			log.Printf("shutdown: %v", err)
		}
		close(shutdownDone)
	}()

	safeAddr := strings.ReplaceAll(strings.ReplaceAll(addr, "\n", ""), "\r", "")
	log.Printf("api listening on %q\n", safeAddr)

	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("serve: %v", err)
	}
	<-shutdownDone
}

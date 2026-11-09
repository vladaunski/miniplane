// Command node is the fake data plane: it keeps VMs in memory.
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"
)

type vm struct {
	ID       string `json:"id"`
	TenantID string `json:"tenant_id"`
}

type server struct {
	mu  sync.Mutex // guards vms; handlers run concurrently
	vms map[string]vm
}

// put handles PUT /vms/{id}. Idempotent: creating an existing VM of the same tenant succeeds again.
func (s *server) put(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	r.Body = http.MaxBytesReader(w, r.Body, 1<<10)
	var req struct {
		TenantID string `json:"tenant_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.TenantID == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.vms[id]; ok {
		if existing.TenantID != req.TenantID {
			http.Error(w, "conflict", http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	s.vms[id] = vm{ID: id, TenantID: req.TenantID}
	w.WriteHeader(http.StatusCreated)
}

// delete handles DELETE /vms/{id}?tenant_id=... Idempotent: a missing VM still succeeds.
func (s *server) delete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	tenant := r.URL.Query().Get("tenant_id")
	if tenant == "" {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.vms[id]; ok {
		if existing.TenantID != tenant {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		delete(s.vms, id)
	}
	w.WriteHeader(http.StatusNoContent)
}

// list handles GET /vms: lets callers observe what actually exists.
func (s *server) list(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	out := make([]vm, 0, len(s.vms))
	for _, v := range s.vms {
		out = append(out, v)
	}
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(out); err != nil {
		log.Printf("encode: %v", err)
	}
}

func main() {
	s := &server{vms: map[string]vm{}}
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /vms/{id}", s.put)
	mux.HandleFunc("DELETE /vms/{id}", s.delete)
	mux.HandleFunc("GET /vms", s.list)

	srv := &http.Server{
		Addr:              "127.0.0.1:9001", // localhost only until the node authenticates callers
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Println("node listening on", srv.Addr)
	log.Fatal(srv.ListenAndServe())
}
